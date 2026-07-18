"""Shared bronze → silver + quarantine streaming pipeline (spec section 5.2).

Path-invariant: reads ``bronze_hl7_raw`` (written by either front door) and
parses every message with the pure-Python HL7 parser in a pandas UDF. Valid
messages land in ``silver_hl7_parsed``; malformed ones land in
``silver_hl7_quarantine`` with their classified ``error_code``.

foreachBatch does the serving-layer work the dashboard depends on:
  1. idempotent Delta appends (``txnAppId`` / ``txnVersion``) so a retried
     micro-batch never double-writes silver;
  2. a psycopg ``execute_values`` upsert of the newest ≤500 parsed rows into
     Lakebase ``rt_latest_transactions`` (``ON CONFLICT DO NOTHING``) — the
     live tail;
  3. one ``rt_stage_metrics`` row per batch: ``batch_ms``, ``lag_s`` (the v4
     backlog signal = max bronze ts visible − max bronze ts processed) and E2E
     latency percentiles;
  4. periodic retention: trims silver tail >10 min and metrics >60 min every
     100 batches.

Runs as a classic Structured Streaming job (no trigger = back-to-back
micro-batches), checkpoint in the UC Volume. ``TRIGGER_SECONDS`` is exposed for
tuning but unset by default.
"""

from __future__ import annotations

import os
import sys
import time

# Ensure the bundle root (parent of pipelines/) is importable on the DRIVER.
# Under spark_python_task the wrapper exec()s this file with no __file__, and we
# deliberately do NOT set PYTHONPATH via spark_env_vars (it would break executor
# Python workers). So recover the root from BUNDLE_ROOT (set in jobs.yml) and,
# as a fallback, from the running frame's co_filename (two levels up).
_here = sys._getframe().f_code.co_filename
for _cand in (
    os.environ.get("BUNDLE_ROOT", ""),
    os.path.dirname(os.path.dirname(_here)),
):
    if _cand and _cand not in sys.path:
        sys.path.insert(0, _cand)

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType, TimestampType,
)

from pipelines.lib.hl7_parser import parse_hl7

CATALOG = os.environ.get("HL7_CATALOG", "real_time_mode_demo_catalog")
SCHEMA = os.environ.get("HL7_SCHEMA", "rti_demo")
CHECKPOINT_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/bronze_to_silver"
TRIGGER_SECONDS = os.environ.get("TRIGGER_SECONDS")  # unset = back-to-back
LATEST_UPSERT_LIMIT = 500
RETENTION_EVERY = 100
# Files per micro-batch. Each batch has large fixed overhead (pandas-UDF init +
# toPandas collect + Delta commit ~6 s regardless of size), so SMALL batches
# starve throughput: 500 files ≈ 700 rows / 6 s ≈ only ~110 rows/s, barely
# keeping pace and falling behind on any hiccup (lag → tens of seconds). Bigger
# batches amortise that fixed cost — 4000 files/batch pushes effective
# throughput well past the ingest rate so the stream stays caught up. Still
# bounded so the first batch can't try to parse the whole table at once.
MAX_FILES_PER_TRIGGER = int(os.environ.get("MAX_FILES_PER_TRIGGER", "4000"))

# Driver-side caches so we don't open a Lakebase connection / mint an OAuth
# token every micro-batch (which trips Lakebase's connection-rate limit).
_LB_CONN: dict = {"conn": None}
_LB_TOKEN: dict = {"tok": "", "at": 0.0}
_LB_TOKEN_TTL_S = 45 * 60
SILVER_RETAIN_MIN = 10
METRICS_RETAIN_MIN = 60

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
    """Vectorised HL7 parse; one struct row per input message.

    Runs on EXECUTORS, which never execute the driver bootstrap above — so put
    the bundle root on sys.path here too (from BUNDLE_ROOT) and import the
    parser lazily. This keeps the UDF self-contained regardless of how Spark
    ships the closure.
    """
    _root = os.environ.get("BUNDLE_ROOT", "")
    if _root and _root not in sys.path:
        sys.path.insert(0, _root)
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


def _lakebase_password() -> str:
    """The Lakebase password: an OAuth token minted for ENDPOINT_NAME.

    Autoscaling Lakebase has no static secret — the job mints a short-lived
    (1-hr) token per connection as its run-as identity (the project owner). Falls
    back to a static LAKEBASE_PASSWORD only when no endpoint is configured.
    """
    endpoint = os.environ.get("ENDPOINT_NAME")
    if not endpoint:
        return os.environ.get("LAKEBASE_PASSWORD", "")
    now = time.monotonic()
    if _LB_TOKEN["tok"] and (now - _LB_TOKEN["at"]) < _LB_TOKEN_TTL_S:
        return _LB_TOKEN["tok"]
    from databricks.sdk import WorkspaceClient

    _LB_TOKEN["tok"] = WorkspaceClient().postgres.generate_database_credential(
        endpoint=endpoint).token
    _LB_TOKEN["at"] = now
    return _LB_TOKEN["tok"]


def _lakebase_conn():
    """A REUSED psycopg connection to Lakebase (None when unconfigured).

    Opened once and cached on the driver — NOT per micro-batch. Back-to-back
    micro-batches opening a fresh connection (and minting a new OAuth token)
    each time trips Lakebase's "connection attempt rate limit". We keep one
    long-lived connection, reopen it only if it has been closed/broken, and mint
    the OAuth token at most once per ~45 min.
    """
    host = os.environ.get("LAKEBASE_HOST")
    if not host:
        return None
    import psycopg

    conn = _LB_CONN.get("conn")
    if conn is not None and not conn.closed:
        return conn
    _LB_CONN["conn"] = psycopg.connect(
        host=host,
        port=int(os.environ.get("LAKEBASE_PORT", "5432")),
        dbname=os.environ.get("LAKEBASE_DATABASE", "rti_demo"),
        user=os.environ.get("LAKEBASE_USER", ""),
        password=_lakebase_password(),
        sslmode="require",
        connect_timeout=10,
    )
    return _LB_CONN["conn"]


def _upsert_latest(conn, rows: list[tuple]) -> int:
    """Upsert newest parsed rows into rt_latest_transactions (idempotent).

    Returns the wall-clock duration of the upsert in milliseconds — the
    Lakebase-write portion of ``batch_ms`` the stage rail surfaces (spec 7.2).
    """
    if not conn or not rows:
        return 0
    lb_start = time.perf_counter()
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO rt_latest_transactions
              (event_id, source_path, facility_id, message_type, patient_mrn,
               unit, summary, ts_generated, ts_bronze, ts_silver)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            rows,
        )
    conn.commit()
    return int((time.perf_counter() - lb_start) * 1000)


def _write_stage_metric(conn, source_path, batch_id, rows_written, batch_ms,
                        lag_s, p50, p95, p99, bronze_ms=None, silver_ms=None,
                        lakebase_ms=None, quarantined=0, annotation=None) -> None:
    if not conn:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO rt_stage_metrics
              (source_path, pipeline, batch_id, rows_written, batch_ms, lag_s,
               bronze_ms, silver_ms, lakebase_ms, quarantined,
               e2e_p50_ms, e2e_p95_ms, e2e_p99_ms, annotation)
            VALUES (%s,'bronze_to_silver',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (source_path, batch_id, rows_written, batch_ms, lag_s,
             bronze_ms, silver_ms, lakebase_ms, quarantined, p50, p95, p99, annotation),
        )
    conn.commit()


def _hop_medians_ms(pdf: pd.DataFrame) -> tuple[int, int]:
    """Median bronze hop (ts_bronze−ts_generated) and silver hop (ts_silver−ts_bronze).

    Pure helper over the collected batch frame so the per-hop math is unit
    testable without Spark. Returns ``(bronze_ms, silver_ms)``, clamped ≥0.
    """
    if pdf.empty:
        return 0, 0
    gen = pd.to_datetime(pdf["ts_generated"])
    bronze = pd.to_datetime(pdf["ts_bronze"])
    silver = pd.to_datetime(pdf["ts_silver"])
    bronze_ms = ((bronze - gen).dt.total_seconds() * 1000).clip(lower=0).median()
    silver_ms = ((silver - bronze).dt.total_seconds() * 1000).clip(lower=0).median()
    return int(bronze_ms or 0), int(silver_ms or 0)


def _retention(conn) -> None:
    if not conn:
        return
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM rt_stage_metrics WHERE batch_ts < now() - interval '%s minutes'"
            % METRICS_RETAIN_MIN
        )
        cur.execute(
            "DELETE FROM rt_gen_metrics WHERE ts < now() - interval '%s minutes'"
            % METRICS_RETAIN_MIN
        )
    conn.commit()


def _percentile(series: pd.Series, q: float) -> int:
    if series.empty:
        return 0
    return int(series.quantile(q))


def _make_foreach_batch(spark: SparkSession):
    """Build the foreachBatch handler closing over per-stream retention state."""
    state = {"batches": 0}

    def _handle(batch_df: DataFrame, batch_id: int) -> None:
        start = time.perf_counter()
        conn = _lakebase_conn()
        ok = False
        try:
            parsed = (
                batch_df
                # ts_bronze is the REAL Zerobus→bronze land time (Delta data-file
                # commit ts), captured as _file_land at the streaming read below —
                # NOT the app's datetime.now() before the POST (which made the
                # bronze hop ~0). File-level granularity: rows in one Zerobus
                # insert share a land time, which is accurate (they land together).
                .withColumn("ts_bronze", F.col("_file_land"))
                .withColumn("p", _parse_udf(F.col("hl7_raw")))
                .withColumn("ts_silver", F.current_timestamp())
            )
            parsed.cache()

            valid = parsed.filter(F.col("p.ok") == "1").select(
                "event_id", "source_path",
                F.col("p.facility_id").alias("facility_id"),
                F.col("p.message_type").alias("message_type"),
                F.col("p.patient_mrn").alias("patient_mrn"),
                F.col("p.unit").alias("unit"),
                F.col("p.summary").alias("summary"),
                "ts_generated", "ts_transport", "ts_bronze", "ts_silver",
            )
            bad = parsed.filter(F.col("p.ok") == "0").select(
                "event_id", "source_path",
                F.col("p.error_code").alias("error_code"),
                F.col("p.error_detail").alias("error_detail"),
                "hl7_raw", "ts_generated", "ts_transport", "ts_bronze", "ts_silver",
            )

            app_id = f"bronze_to_silver_{SCHEMA}"
            (valid.write.format("delta")
                .option("txnAppId", app_id).option("txnVersion", batch_id)
                .mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_hl7_parsed"))
            (bad.write.format("delta")
                .option("txnAppId", f"{app_id}_q").option("txnVersion", batch_id)
                .mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.silver_hl7_quarantine"))

            # newest ≤500 valid rows → Lakebase live tail; keep the write duration
            # (the Lakebase-write portion of batch_ms surfaced on the stage rail)
            latest = (valid.orderBy(F.col("ts_silver").desc())
                      .limit(LATEST_UPSERT_LIMIT).toPandas())
            rows = [
                (r.event_id, r.source_path, r.facility_id, r.message_type,
                 r.patient_mrn, r.unit, r.summary,
                 r.ts_generated, r.ts_bronze, r.ts_silver)
                for r in latest.itertuples(index=False)
            ]
            lakebase_ms = _upsert_latest(conn, rows)

            # metrics: batch_ms, lag_s (backlog), per-hop medians, E2E percentiles
            pdf = parsed.select(
                "source_path", "ts_generated", "ts_bronze", "ts_silver"
            ).toPandas()
            quarantined = int(bad.count())
            batch_ms = int((time.perf_counter() - start) * 1000)
            source_path = pdf["source_path"].iloc[0] if not pdf.empty else "zerobus"
            bronze_ms, silver_ms = _hop_medians_ms(pdf)
            e2e = pd.Series(dtype="float64")
            lag_s = 0.0
            if not pdf.empty:
                e2e = (pd.to_datetime(pdf["ts_silver"]) - pd.to_datetime(pdf["ts_generated"])) \
                    .dt.total_seconds() * 1000
                frontier = spark.sql(
                    f"SELECT max(ts_bronze) m FROM {CATALOG}.{SCHEMA}.bronze_hl7_raw"
                ).collect()[0]["m"]
                processed = pd.to_datetime(pdf["ts_bronze"]).max()
                if frontier is not None and pd.notnull(processed):
                    lag_s = max(0.0, (pd.Timestamp(frontier) - processed).total_seconds())
            # rows_written = ALL valid rows landed in silver this batch, not the
            # ≤500 Lakebase-tail cap (len(rows)) — else 'landed/s' under-reports.
            silver_written = len(pdf) - quarantined
            _write_stage_metric(
                conn, source_path, batch_id, silver_written, batch_ms, round(lag_s, 2),
                _percentile(e2e, 0.50), _percentile(e2e, 0.95), _percentile(e2e, 0.99),
                bronze_ms=bronze_ms, silver_ms=silver_ms, lakebase_ms=lakebase_ms,
                quarantined=quarantined,
            )

            state["batches"] += 1
            if state["batches"] % RETENTION_EVERY == 0:
                spark.sql(
                    f"DELETE FROM {CATALOG}.{SCHEMA}.silver_hl7_parsed "
                    f"WHERE ts_silver < current_timestamp() - INTERVAL {SILVER_RETAIN_MIN} MINUTES"
                )
                _retention(conn)

            parsed.unpersist()
            ok = True
        finally:
            # Keep the cached connection OPEN across batches (see _lakebase_conn).
            # Only drop it on failure so the next batch reconnects cleanly —
            # closing every batch is what caused the connection-rate-limit storm.
            if conn is not None and not ok:
                try:
                    conn.close()
                except Exception:
                    pass
                _LB_CONN["conn"] = None

    return _handle


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    # Elastic parallelism: let Adaptive Query Execution size shuffle partitions
    # to the actual per-batch data instead of a fixed low count (the old
    # hardcoded shuffle.partitions=8 throttled parallelism and, combined with a
    # maxFilesPerTrigger cap, kept batches so small the cluster never needed to
    # autoscale — so it never did). AQE + no ingestion cap lets each batch pull
    # all available data, exposing true load so the 2–8 worker autoscaler
    # actually scales up under pressure and back down when idle.
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    # Read from the start of the (wipe-emptied) bronze table. NOT
    # startingVersion=latest — after a TRUNCATE it resolves to the empty
    # post-truncate version and the stream never advances (silver stuck at 0).
    # maxFilesPerTrigger BOUNDS each micro-batch: without it, the first batch
    # tries to parse the entire accumulated bronze table (tens of thousands of
    # rows) in one collect/pandas-UDF pass and never completes — silver stays 0.
    # A bounded batch commits fast and back-to-back batches then keep pace.
    stream = (
        spark.readStream
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .table(f"{CATALOG}.{SCHEMA}.bronze_hl7_raw")
        # Capture the Delta data-file commit time here at the read — _metadata is
        # a file-source pseudo-column only resolvable on the streaming source,
        # not on the foreachBatch DataFrame. Downstream we use it as ts_bronze
        # (the true Zerobus land time).
        .withColumn("_file_land", F.col("_metadata.file_modification_time"))
    )
    writer = (
        stream.writeStream
        .foreachBatch(_make_foreach_batch(spark))
        .option("checkpointLocation", CHECKPOINT_ROOT)
        .outputMode("append")
    )
    if TRIGGER_SECONDS:
        writer = writer.trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
    writer.start().awaitTermination()


if __name__ == "__main__":
    main()
