"""Lakebase serving-layer sink for the DLT medallion pipeline.

DLT owns the managed Delta tables (bronze/silver); this module owns the
side-effect writes into the external Lakebase (Postgres) serving tables the live
dashboard reads — ``rt_latest_transactions`` (live tail) and ``rt_stage_metrics``
(stage rail + charts). It is wired into the pipeline as a
``foreach_batch_sink`` (see medallion_dlt.py), which is DLT's supported escape
hatch for reverse-ETL into targets without a native streaming writer.

Design (carried over from the proven classic pipeline):
  * ONE reused psycopg connection + a cached OAuth token (refreshed <1 h), so
    back-to-back micro-batches never trip Lakebase's connection-rate limit.
  * Config comes from Spark conf (``spark.conf.get``) — serverless pipelines do
    not support spark_env_vars, so the pipeline ``configuration`` map supplies
    HL7_CATALOG/SCHEMA/LAKEBASE_*/ENDPOINT_NAME.
  * OAuth token minted via the pinned databricks-sdk ``.postgres`` API (the sdk
    version is pinned in the pipeline Environment, so ``.postgres`` is present).
"""

from __future__ import annotations

import time

# Driver-side caches (one per pipeline driver process) — NOT per micro-batch.
_LB_CONN: dict = {"conn": None}
_LB_TOKEN: dict = {"tok": "", "at": 0.0}
_LB_TOKEN_TTL_S = 45 * 60
LATEST_UPSERT_LIMIT = 500


def _cfg(spark, key: str, default: str = "") -> str:
    """Read a pipeline configuration value (set in resources/dlt_pipeline.yml)."""
    try:
        return spark.conf.get(f"hl7.{key}", default)
    except Exception:
        return default


def _lakebase_token(spark) -> str:
    endpoint = _cfg(spark, "endpoint_name")
    if not endpoint:
        return _cfg(spark, "lakebase_password")
    now = time.monotonic()
    if _LB_TOKEN["tok"] and (now - _LB_TOKEN["at"]) < _LB_TOKEN_TTL_S:
        return _LB_TOKEN["tok"]
    from databricks.sdk import WorkspaceClient

    _LB_TOKEN["tok"] = WorkspaceClient().postgres.generate_database_credential(
        endpoint=endpoint).token
    _LB_TOKEN["at"] = now
    return _LB_TOKEN["tok"]


def _lakebase_conn(spark):
    """A reused psycopg connection to Lakebase (None when unconfigured).

    Reused across micro-batches (opened once). Retries with backoff on Lakebase's
    "connection attempt rate limit" — the sink opening/reopening connections in a
    burst (e.g. after a restart, or the token refresh) can trip it; a short
    backoff lets the limiter recover instead of failing the whole batch.
    """
    host = _cfg(spark, "lakebase_host")
    if not host:
        return None
    import psycopg

    conn = _LB_CONN.get("conn")
    if conn is not None and not conn.closed:
        return conn
    last_exc = None
    for attempt in range(5):
        try:
            _LB_CONN["conn"] = psycopg.connect(
                host=host,
                port=int(_cfg(spark, "lakebase_port", "5432")),
                dbname=_cfg(spark, "lakebase_database", "rti_demo"),
                user=_cfg(spark, "lakebase_user"),
                password=_lakebase_token(spark),
                sslmode="require",
                connect_timeout=10,
                autocommit=True,
            )
            return _LB_CONN["conn"]
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if "rate limit" in str(exc).lower():
                time.sleep(2.0 * (attempt + 1))  # 2,4,6,8,10s backoff
                continue
            raise
    raise last_exc


def _drop_conn() -> None:
    conn = _LB_CONN.get("conn")
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
        _LB_CONN["conn"] = None


def write_serving(spark, batch_df, batch_id: int) -> None:
    """foreach_batch handler: upsert the live tail + write one stage-metric row.

    Receives each silver micro-batch. Collects only the newest <=500 rows
    (bounded, small) for the live tail, and computes per-hop medians + E2E
    percentiles for the stage rail — mirroring the classic pipeline's serving
    writes so the dashboard is unchanged. Idempotent (ON CONFLICT DO NOTHING).
    Reconnects once on a dropped connection.
    """
    import datetime as _dt

    import pandas as pd
    from pyspark.sql import functions as F

    for attempt in (1, 2):
        conn = _lakebase_conn(spark)
        if conn is None:
            return
        try:
            start = time.perf_counter()
            # newest <=500 valid rows → live tail (bounded collect, by design)
            latest = (batch_df.orderBy(F.col("ts_silver").desc())
                      .limit(LATEST_UPSERT_LIMIT).toPandas())
            rows = [
                (r.event_id, r.source_path, r.facility_id, r.message_type,
                 r.patient_mrn, r.unit, r.summary,
                 r.ts_generated, r.ts_bronze, r.ts_silver)
                for r in latest.itertuples(index=False)
            ]
            if rows:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO rt_latest_transactions
                          (event_id, source_path, facility_id, message_type,
                           patient_mrn, unit, summary, ts_generated, ts_bronze, ts_silver)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (event_id) DO NOTHING
                        """,
                        rows,
                    )
            # Serving landing time = when these rows become queryable in Lakebase.
            # This is the instant we finished the upsert (ts_lakebase DEFAULT now()
            # is stamped here too), captured on the driver in UTC so the serving
            # hop is measured against the same wall clock as ts_silver.
            landed = _dt.datetime.now(_dt.timezone.utc)

            # Per-hop medians + true E2E percentiles over the batch (small frame).
            # NOTE: the serving hop is silver → LANDED IN LAKEBASE (ts_lakebase −
            # ts_silver), NOT the psycopg write wall-time — the write is trivial
            # (~40 ms); what matters for a serving SLA is how long a parsed record
            # takes to become queryable, which is dominated by the flow trigger
            # cadence. E2E is generate → landed (the full trip), not gen → silver.
            pdf = batch_df.select("source_path", "ts_generated", "ts_bronze",
                                  "ts_silver").toPandas()
            src = pdf["source_path"].iloc[0] if not pdf.empty else "zerobus"
            bronze_ms = silver_ms = lakebase_ms = 0
            p50 = p95 = p99 = 0
            rows_written = len(pdf)
            if not pdf.empty:
                landed_ts = pd.Timestamp(landed)
                gen = pd.to_datetime(pdf["ts_generated"], utc=True)
                brz = pd.to_datetime(pdf["ts_bronze"], utc=True)
                slv = pd.to_datetime(pdf["ts_silver"], utc=True)
                bronze_ms = int(((brz - gen).dt.total_seconds() * 1000).clip(lower=0).median() or 0)
                silver_ms = int(((slv - brz).dt.total_seconds() * 1000).clip(lower=0).median() or 0)
                lakebase_ms = int(((landed_ts - slv).dt.total_seconds() * 1000).clip(lower=0).median() or 0)
                e2e = ((landed_ts - gen).dt.total_seconds() * 1000).clip(lower=0)
                p50, p95, p99 = (int(e2e.quantile(0.50)), int(e2e.quantile(0.95)),
                                 int(e2e.quantile(0.99)))
            batch_ms = int((time.perf_counter() - start) * 1000)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO rt_stage_metrics
                      (source_path, pipeline, batch_id, rows_written, batch_ms, lag_s,
                       bronze_ms, silver_ms, lakebase_ms, quarantined,
                       e2e_p50_ms, e2e_p95_ms, e2e_p99_ms, annotation)
                    VALUES (%s,'bronze_to_silver',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (src, batch_id, rows_written, batch_ms, 0.0,
                     bronze_ms, silver_ms, lakebase_ms, 0, p50, p95, p99, None),
                )
            return
        except Exception:
            _drop_conn()          # reconnect once on a broken connection
            if attempt == 2:
                return
