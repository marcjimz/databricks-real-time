"""Shared silver → gold windowed-aggregate pipeline (spec section 5.3).

Reads ``silver_hl7_parsed`` and rolls it up into 10 s tumbling windows keyed by
``source_path`` (30 s watermark). Three gold tables land per window:
  - ``gold_throughput_10s``   — msg_count by (window, source_path, facility, type)
  - ``gold_latency_10s``      — E2E p50/p95/p99 by (window, source_path)
  - ``gold_census_delta_10s`` — admits/discharges/net by (window, source_path, facility)

foreachBatch assigns each silver row to its 10 s window, aggregates within the
batch, and MERGEs into the gold Delta tables. Windows span micro-batches, so an
upsert keyed by the window tuple is required — a plain append would duplicate a
window every time a later batch adds rows to it. Throughput/census counts
accumulate on match; latency percentiles are last-writer-wins (windows finalize
inside the watermark, so the final batch touching a window has the whole picture).

The newest throughput windows are also upserted into Lakebase
``rt_gold_snapshots`` for the trend charts, and one ``rt_stage_metrics`` row
(``pipeline='silver_to_gold'``) records ``batch_ms`` per batch.

Gold trails live by ~30-40 s (watermark close): the dashboard uses gold for the
census/throughput trend view and Lakebase for everything live. Runs as a classic
Structured Streaming job; checkpoint in the UC Volume.
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Iterable
from typing import TYPE_CHECKING

# Ensure the bundle root (parent of pipelines/) is importable on the DRIVER —
# see the matching note in bronze_to_silver.py (no __file__, no PYTHONPATH env;
# use BUNDLE_ROOT with a co_filename fallback).
_here = sys._getframe().f_code.co_filename
for _cand in (
    os.environ.get("BUNDLE_ROOT", ""),
    os.path.dirname(os.path.dirname(_here)),
):
    if _cand and _cand not in sys.path:
        sys.path.insert(0, _cand)

if TYPE_CHECKING:  # heavy deps live on the cluster, not the local/app env
    from pyspark.sql import DataFrame, SparkSession

CATALOG = os.environ.get("HL7_CATALOG", "real_time_mode_demo_catalog")
SCHEMA = os.environ.get("HL7_SCHEMA", "rti_demo")
CHECKPOINT_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/silver_to_gold"
TRIGGER_SECONDS = os.environ.get("TRIGGER_SECONDS")  # unset = back-to-back
WINDOW_SECONDS = 10
WATERMARK = "30 seconds"
SNAPSHOT_UPSERT_LIMIT = 200
ADMIT_TYPE = "ADT^A01"
DISCHARGE_TYPE = "ADT^A03"


def _window_start_epoch(ts_epoch_s: float, window_s: int = WINDOW_SECONDS) -> int:
    """Floor an epoch-seconds timestamp to its tumbling-window start (pure).

    Mirrors Spark's ``window(ts, '10 seconds').start`` so the per-window keying
    is unit testable without Spark. Returns the window-start epoch in seconds.
    """
    return int(ts_epoch_s // window_s) * window_s


def _census_counts(message_types: Iterable[str]) -> tuple[int, int, int]:
    """Admits (ADT^A01), discharges (ADT^A03), net census over a message stream.

    Pure helper so the census-delta math is testable without Spark. Net census
    is admits − discharges (may be negative when discharges outrun admits).
    """
    admits = sum(1 for m in message_types if m == ADMIT_TYPE)
    discharges = sum(1 for m in message_types if m == DISCHARGE_TYPE)
    return admits, discharges, admits - discharges


def _upsert_snapshots(conn, rows: list[tuple]) -> None:
    """Upsert the newest throughput windows into Lakebase rt_gold_snapshots."""
    if not conn or not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO rt_gold_snapshots
              (window_start, source_path, facility_id, message_type, msg_count)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (window_start, source_path, facility_id, message_type)
            DO UPDATE SET msg_count = EXCLUDED.msg_count
            """,
            rows,
        )
    conn.commit()


def _write_stage_metric(conn, source_path, batch_id, rows_written, batch_ms) -> None:
    if not conn:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO rt_stage_metrics
              (source_path, pipeline, batch_id, rows_written, batch_ms)
            VALUES (%s,'silver_to_gold',%s,%s,%s)
            """,
            (source_path, batch_id, rows_written, batch_ms),
        )
    conn.commit()


def _merge_gold(spark: SparkSession, agg: DataFrame, table: str,
                keys: list[str], additive: list[str], overwrite: list[str]) -> None:
    """MERGE a per-batch aggregate into a gold Delta table.

    ``additive`` columns accumulate (target + source) so counts stay correct as
    later micro-batches add rows to an open window; ``overwrite`` columns are
    last-writer-wins (used for percentiles, which can't be summed).
    """
    if agg.rdd.isEmpty():
        return
    # Register the temp view and run the MERGE on the SAME session. Inside
    # foreachBatch the batch DataFrame belongs to a cloned SparkSession, not the
    # closure-captured `spark`; a view created on one session is invisible to
    # spark.sql() on the other (TABLE_OR_VIEW_NOT_FOUND). Use agg.sparkSession
    # for both so they always match.
    session = agg.sparkSession
    view = f"_gold_src_{table.rsplit('.', 1)[-1]}"
    agg.createOrReplaceTempView(view)
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    set_add = [f"t.{c} = t.{c} + s.{c}" for c in additive]
    set_over = [f"t.{c} = s.{c}" for c in overwrite]
    set_clause = ", ".join(set_add + set_over)
    cols = keys + additive + overwrite
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join(f"s.{c}" for c in cols)
    session.sql(
        f"""
        MERGE INTO {table} t
        USING {view} s
        ON {on}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )


def _make_foreach_batch(spark: SparkSession):
    """Build the foreachBatch handler that rolls silver rows into gold windows."""

    def _handle(batch_df: DataFrame, batch_id: int) -> None:
        from pyspark.sql import functions as F

        from pipelines.bronze_to_silver import _lakebase_conn

        from pipelines.bronze_to_silver import _LB_CONN

        start = time.perf_counter()
        conn = _lakebase_conn()
        ok = False
        try:
            windowed = batch_df.withColumn(
                "window_start", F.window("ts_generated", f"{WINDOW_SECONDS} seconds").start
            )

            throughput = windowed.groupBy(
                "window_start", "source_path", "facility_id", "message_type"
            ).agg(F.count("*").alias("msg_count"))

            latency = (
                windowed
                .withColumn(
                    "e2e_ms",
                    (F.col("ts_silver").cast("double") - F.col("ts_generated").cast("double")) * 1000,
                )
                .groupBy("window_start", "source_path")
                .agg(
                    F.expr("percentile_approx(e2e_ms, 0.50)").cast("int").alias("e2e_p50_ms"),
                    F.expr("percentile_approx(e2e_ms, 0.95)").cast("int").alias("e2e_p95_ms"),
                    F.expr("percentile_approx(e2e_ms, 0.99)").cast("int").alias("e2e_p99_ms"),
                    F.count("*").alias("msg_count"),
                )
            )

            census = (
                windowed.groupBy("window_start", "source_path", "facility_id")
                .agg(
                    F.sum(F.when(F.col("message_type") == ADMIT_TYPE, 1).otherwise(0)).alias("admits"),
                    F.sum(F.when(F.col("message_type") == DISCHARGE_TYPE, 1).otherwise(0)).alias("discharges"),
                )
                .withColumn("net_census", F.col("admits") - F.col("discharges"))
            )

            _merge_gold(
                spark, throughput, f"{CATALOG}.{SCHEMA}.gold_throughput_10s",
                keys=["window_start", "source_path", "facility_id", "message_type"],
                additive=["msg_count"], overwrite=[],
            )
            _merge_gold(
                spark, latency, f"{CATALOG}.{SCHEMA}.gold_latency_10s",
                keys=["window_start", "source_path"],
                additive=["msg_count"],
                overwrite=["e2e_p50_ms", "e2e_p95_ms", "e2e_p99_ms"],
            )
            _merge_gold(
                spark, census, f"{CATALOG}.{SCHEMA}.gold_census_delta_10s",
                keys=["window_start", "source_path", "facility_id"],
                additive=["admits", "discharges", "net_census"], overwrite=[],
            )

            snap = (
                throughput.orderBy(F.col("window_start").desc())
                .limit(SNAPSHOT_UPSERT_LIMIT).toPandas()
            )
            rows = [
                (r.window_start, r.source_path, r.facility_id, r.message_type, int(r.msg_count))
                for r in snap.itertuples(index=False)
            ]
            _upsert_snapshots(conn, rows)

            source_path = rows[0][1] if rows else "zerobus"
            batch_ms = int((time.perf_counter() - start) * 1000)
            _write_stage_metric(conn, source_path, batch_id, len(rows), batch_ms)
            ok = True
        finally:
            # Keep the cached connection open across batches; drop only on error
            # (see the matching note in bronze_to_silver._lakebase_conn).
            if conn is not None and not ok:
                try:
                    conn.close()
                except Exception:
                    pass
                _LB_CONN["conn"] = None

    return _handle


def main() -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    # Elastic parallelism via AQE (matches bronze_to_silver) instead of a fixed
    # low shuffle-partition count, so windowed aggregations scale with load.
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    # Read from the start of the (wipe-emptied) silver table — NOT
    # startingVersion=latest (it fails to advance after a TRUNCATE; see
    # bronze_to_silver). skipChangeCommits stays: bronze_to_silver periodically
    # DELETEs old silver rows for retention, which a streaming source would
    # otherwise reject (DELTA_SOURCE_TABLE_IGNORE_CHANGES).
    stream = (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table(f"{CATALOG}.{SCHEMA}.silver_hl7_parsed")
        .withWatermark("ts_generated", WATERMARK)
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
