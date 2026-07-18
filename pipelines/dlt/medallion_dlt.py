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

# Make the bundle files root importable so `pipelines.lib.*` / `pipelines.dlt.*`
# resolve. This file lives at <root>/pipelines/dlt/medallion_dlt.py, so the root
# is two directories up. DLT provides __file__ for pipeline source modules.
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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


@pandas_udf(_PARSED_SCHEMA)
def _parse_udf(hl7_raw: pd.Series) -> pd.DataFrame:
    """Vectorised HL7 parse; one struct row per message. Imports the parser
    lazily so the closure is self-contained on serverless executors (the parser
    module is shipped with the pipeline; no internet needed — parsing is pure)."""
    from pipelines.lib.hl7_parser import parse_hl7

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
# The valid silver stream feeds a foreach_batch_sink that upserts the live tail
# and writes stage metrics into Lakebase — the tables the dashboard reads.
from pipelines.dlt.lakebase_sink import write_serving  # noqa: E402


dlt.create_sink(
    name="lakebase_serving",
    func=lambda df, batch_id: write_serving(spark, df, batch_id),
)


@dlt.append_flow(target="lakebase_serving")
def to_lakebase():
    # Re-read the valid silver stream (same transform) to feed the sink. Using
    # the source stream (not dlt.read_stream of the managed table) keeps the
    # full row incl. ts_* hops available to the sink.
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
