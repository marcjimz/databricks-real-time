"""HL7 medallion as a serverless Lakeflow Declarative Pipeline (DLT).

Replaces the classic-compute `bronze_to_silver` + `silver_to_gold` jobs. The gold
layer is intentionally dropped — nothing consumed it (the dashboard reads only the
Lakebase `rt_*` serving tables). Scope here:

  bronze_hl7_raw (Zerobus's Delta target, read as a stream)
      → silver_hl7_parsed        (@dlt.table, HL7 parse + valid rows)
      → silver_hl7_quarantine    (@dlt.table, malformed rows)
      → Lakebase serving sink     (foreach_batch_sink → rt_latest_transactions + rt_stage_metrics)

Why DLT serverless: classic cluster autoscale does not react to Structured
Streaming backlog (the medallion sat stuck at 2 workers). DLT enhanced
autoscaling scales on streaming task-queue/slot signals and is always-on under
serverless — so throughput tracks the ingest rate and the backlog clears.

Config comes from the pipeline `configuration` map (serverless has no
spark_env_vars): hl7.catalog / hl7.schema / hl7.lakebase_* / hl7.endpoint_name.
"""

from __future__ import annotations

import os
import sys

# Make the bundle files root (the dir CONTAINING `pipelines/`) importable so
# `pipelines.lib.*` / `pipelines.dlt.*` resolve. DLT runs this source as a
# notebook cell, so __file__ is NOT defined — instead walk up from the current
# working directory looking for the `pipelines` package, and fall back to
# scanning sys.path entries. Robust to however DLT sets the working dir.
def _find_bundle_root():
    seen = []
    start = os.getcwd()
    d = start
    for _ in range(8):
        if os.path.isdir(os.path.join(d, "pipelines", "lib")):
            return d
        seen.append(d)
        parent = os.path.dirname(d)
        if parent == d:
            break
        d = parent
    # fall back: any sys.path entry that contains pipelines/lib
    for p in list(sys.path):
        if p and os.path.isdir(os.path.join(p, "pipelines", "lib")):
            return p
    return start

_ROOT = _find_bundle_root()
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, StructField, StructType

import pandas as pd

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

CATALOG = spark.conf.get("hl7.catalog", "real_time_mode_demo_catalog")
SCHEMA = spark.conf.get("hl7.schema", "rti_demo")
BRONZE = f"{CATALOG}.{SCHEMA}.bronze_hl7_raw"

_PARSED_SCHEMA = StructType([
    StructField("ok", StringType()),
    StructField("error_code", StringType()),
    StructField("error_detail", StringType()),
    StructField("facility_id", StringType()),
    StructField("message_type", StringType()),
    StructField("patient_mrn", StringType()),
    StructField("unit", StringType()),
    StructField("summary", StringType()),
])


# Read the pure-Python HL7 parser source ON THE DRIVER and ship it inside the
# UDF closure. Executors never ran the driver sys.path shim and the pipeline
# package isn't importable there (ModuleNotFoundError: 'pipelines'), so instead
# of importing on the executor we exec the source into a namespace inside the
# UDF. The parser is stdlib-only (dataclasses), so this is fully self-contained.
def _read_parser_source() -> str:
    import os
    for p in [_ROOT] + list(sys.path):
        cand = os.path.join(p, "pipelines", "lib", "hl7_parser.py")
        if p and os.path.isfile(cand):
            with open(cand) as fh:
                return fh.read()
    raise FileNotFoundError("hl7_parser.py not found on driver")


_PARSER_SRC = _read_parser_source()


@pandas_udf(_PARSED_SCHEMA)
def _parse_udf(hl7_raw: pd.Series) -> pd.DataFrame:
    """Vectorised HL7 parse; one struct row per message. The parser is exec'd
    from source captured on the driver (_PARSER_SRC) so it runs on any executor
    with no filesystem/package dependency."""
    _ns: dict = {}
    exec(_PARSER_SRC, _ns)
    parse_hl7 = _ns["parse_hl7"]

    rows = []
    for raw in hl7_raw:
        out = parse_hl7(raw or "")
        f = out.fields
        rows.append({
            "ok": "1" if out.ok else "0",
            "error_code": out.error_code,
            "error_detail": out.error_detail,
            "facility_id": f.get("facility_id", ""),
            "message_type": f.get("message_type", ""),
            "patient_mrn": f.get("patient_mrn", ""),
            "unit": f.get("unit", ""),
            "summary": f.get("summary", ""),
        })
    return pd.DataFrame(rows)


def _parsed_stream():
    """Bronze stream → parsed struct + real ts_bronze (Delta file commit time).

    ts_bronze is the true Zerobus→bronze land time from _metadata (resolvable
    only on the streaming source, captured here), NOT the app's pre-POST stamp.
    """
    return (
        spark.readStream.table(BRONZE)
        .withColumn("ts_bronze", F.col("_metadata.file_modification_time"))
        .withColumn("p", _parse_udf(F.col("hl7_raw")))
        .withColumn("ts_silver", F.current_timestamp())
    )


@dlt.table(
    name="silver_hl7_parsed",
    comment="Parsed + validated HL7 records (valid only). DLT-managed streaming table.",
    table_properties={"delta.enableDeletionVectors": "true"},
)
def silver_hl7_parsed():
    return (
        _parsed_stream()
        .filter(F.col("p.ok") == "1")
        .select(
            "event_id", "source_path",
            F.col("p.facility_id").alias("facility_id"),
            F.col("p.message_type").alias("message_type"),
            F.col("p.patient_mrn").alias("patient_mrn"),
            F.col("p.unit").alias("unit"),
            F.col("p.summary").alias("summary"),
            "ts_generated", "ts_transport", "ts_bronze", "ts_silver",
        )
    )


@dlt.table(
    name="silver_hl7_quarantine",
    comment="Malformed HL7 records with classified error_code. DLT-managed.",
)
def silver_hl7_quarantine():
    return (
        _parsed_stream()
        .filter(F.col("p.ok") == "0")
        .select(
            "event_id", "source_path",
            F.col("p.error_code").alias("error_code"),
            F.col("p.error_detail").alias("error_detail"),
            "hl7_raw", "ts_generated", "ts_transport", "ts_bronze", "ts_silver",
        )
    )


# --- Lakebase serving sink (Option A: sub-second serving via foreach_batch) ---
# A foreach_batch_sink decorates a (df, batch_id) function that upserts the live
# tail + writes stage metrics into Lakebase (the tables the dashboard reads).
# An append_flow feeds the valid silver stream into it. NOTE: the sink is the
# `@dlt.foreach_batch_sink` decorator — NOT create_sink(..., "foreach_batch").
from pipelines.dlt.lakebase_sink import write_serving  # noqa: E402


@dlt.foreach_batch_sink(name="lakebase_serving")
def _lakebase_serving(df, batch_id):
    # Derive spark from the batch DF's own session — do NOT close over the
    # module-level `spark`/globals (that makes the sink UDF non-serializable and
    # the flow fails + won't restart, freezing the serving writes).
    write_serving(df.sparkSession, df, batch_id)


@dlt.append_flow(target="lakebase_serving")
def to_lakebase():
    # Stream the ALREADY-MATERIALIZED silver table into the sink (not a second
    # bronze re-parse) — halves the parse load and reuses the exact rows written
    # to silver, which already carry all ts_* hops.
    return dlt.read_stream("silver_hl7_parsed")
