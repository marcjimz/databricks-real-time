"""Lakebase (Postgres) access for the dashboard and generator telemetry.

The app reads the live serving tables at 1 Hz (live tail, stage/gen metrics)
and the supervisor writes ``rt_gen_metrics`` rollups through here. Everything
degrades gracefully: when ``LAKEBASE_HOST`` is unset (local dev before the
instance is provisioned) the client reports ``configured == False`` and every
read returns empty, so the app still boots and renders its empty states.

Connection + OAuth token are CACHED and reused across calls. Opening a fresh
psycopg connection (and minting a new OAuth token) costs ~3-5 s each; doing that
per query made the 1 Hz refresh (4 reads/tick) take ~14 s, so the dashboard
never caught up and sat permanently on "Loading...". We now keep one long-lived
connection guarded by a lock, refresh the token before its ~1 h expiry, and
transparently reconnect on any connection error.
"""

from __future__ import annotations

import threading
import time

from config import Config

_TOKEN_TTL_S = 45 * 60   # refresh the 1 h OAuth token comfortably before expiry


class LakebaseClient:
    """Thin psycopg wrapper over the Lakebase serving tables (pooled + cached)."""

    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._w = None            # WorkspaceClient, created lazily on first token mint
        self._token = ""          # cached OAuth token
        self._token_at = 0.0      # monotonic time the token was minted
        self._token_lock = threading.Lock()
        # Two cached connections on separate locks: reads come from the Dash
        # request threads, writes from the supervisor thread. Keeping them apart
        # means telemetry writes never block the 1 Hz dashboard reads.
        self._conns: dict[str, object] = {"read": None, "write": None}
        self._locks = {"read": threading.Lock(), "write": threading.Lock()}

    @property
    def configured(self) -> bool:
        return bool(self._cfg.lakebase_host)

    def _password(self) -> str:
        """The Lakebase password, cached until near its ~1 h expiry.

        For an autoscaling project the password is a short-lived OAuth token
        minted from ``ENDPOINT_NAME``; there is no static secret. When no
        endpoint is configured (local Postgres) fall back to ``LAKEBASE_PASSWORD``.
        """
        import os

        endpoint = self._cfg.endpoint_name
        if not endpoint:
            return os.environ.get("LAKEBASE_PASSWORD", "")
        with self._token_lock:
            if self._token and (time.monotonic() - self._token_at) < _TOKEN_TTL_S:
                return self._token
            if self._w is None:
                from databricks.sdk import WorkspaceClient

                self._w = WorkspaceClient()
            self._token = self._w.postgres.generate_database_credential(endpoint=endpoint).token
            self._token_at = time.monotonic()
            return self._token

    def _new_connection(self):
        import psycopg

        return psycopg.connect(
            host=self._cfg.lakebase_host,
            port=self._cfg.lakebase_port,
            dbname=self._cfg.lakebase_database,
            user=self._cfg.lakebase_user,
            password=self._password(),
            sslmode="require",
            connect_timeout=5,
            autocommit=True,        # reads only; avoids idle-in-transaction holds
        )

    def _connection(self, role: str):
        """Return the cached connection for a role, (re)opening if missing/dead."""
        conn = self._conns[role]
        if conn is not None and conn.closed:
            conn = None
        if conn is None:
            conn = self._new_connection()
            self._conns[role] = conn
        return conn

    def _drop(self, role: str) -> None:
        conn = self._conns[role]
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._conns[role] = None

    def _execute(self, role: str, sql: str, params, fetch: bool):
        """Run a statement on the role's cached connection, reconnecting once."""
        from psycopg.rows import dict_row

        with self._locks[role]:
            for attempt in (1, 2):
                try:
                    conn = self._connection(role)
                    factory = dict_row if fetch else None
                    with conn.cursor(row_factory=factory) as cur:
                        cur.execute(sql, params)
                        return list(cur.fetchall()) if fetch else None
                except Exception:
                    self._drop(role)      # transparent reconnect on a dead conn
                    if attempt == 2:
                        return [] if fetch else None
        return [] if fetch else None

    # -- reads (dashboard) ---------------------------------------------------
    def _query(self, sql: str, params: tuple = ()) -> list[dict]:
        if not self.configured:
            return []
        return self._execute("read", sql, params, fetch=True) or []

    def latest_transactions(self, limit: int = 25,
                            source_path: str | None = None) -> list[dict]:
        """Newest transactions for the live tail (slot G).

        Path-scoped when ``source_path`` is given (the norm — the tail should show
        only the ACTIVE path's records, matching the rest of the dashboard). Left
        unfiltered only when a caller explicitly wants a cross-path view. Every
        record carries ``source_path`` ('zerobus' | 'eventhub'), stamped at
        generation and preserved through bronze → silver → serving, so the filter
        is exact.
        """
        where = "WHERE source_path = %s" if source_path else ""
        params = (source_path, limit) if source_path else (limit,)
        return self._query(
            f"""
            SELECT event_id, source_path, facility_id, message_type,
                   patient_mrn, unit, summary,
                   ts_generated, ts_bronze, ts_silver, ts_lakebase,
                   -- per-hop deltas (ms) for the E2E trip, each clamped ≥0
                   GREATEST(EXTRACT(EPOCH FROM (ts_bronze   - ts_generated)), 0) * 1000 AS bronze_ms,
                   GREATEST(EXTRACT(EPOCH FROM (ts_silver   - ts_bronze))   , 0) * 1000 AS silver_ms,
                   GREATEST(EXTRACT(EPOCH FROM (ts_lakebase - ts_silver))   , 0) * 1000 AS lakebase_ms,
                   -- full trip: generation → serving-layer landing
                   GREATEST(EXTRACT(EPOCH FROM (ts_lakebase - ts_generated)), 0) * 1000 AS e2e_ms
            FROM rt_latest_transactions
            {where}
            ORDER BY ts_lakebase DESC
            LIMIT %s
            """,
            params,
        )

    def serving_e2e_p95_ms(self, source_path: str | None = None) -> float | None:
        """Full-trip (ingest → Lakebase) p95 latency over the freshest rows.

        Computed from rt_latest_transactions (ts_lakebase − ts_generated) so the
        hero number matches the tail's e2e column. The stage-metric e2e stops at
        silver, so it must NOT be used for the serving-layer headline. Path-scoped
        when source_path is given so the hero reflects only the ACTIVE path (else
        a stale other-path burst could skew the headline once both paths run).
        """
        where = "AND source_path = %s" if source_path else ""
        params = (source_path,) if source_path else ()
        rows = self._query(
            f"""
            SELECT percentile_cont(0.95) WITHIN GROUP (
                     ORDER BY EXTRACT(EPOCH FROM (ts_lakebase - ts_generated)) * 1000
                   ) AS p95
            FROM rt_latest_transactions
            WHERE ts_lakebase > now() - interval '60 seconds' {where}
            """,
            params,
        )
        p95 = rows[0]["p95"] if rows else None
        return float(p95) if p95 is not None else None

    def recent_stage_metrics(self, source_path: str, seconds: int = 600) -> list[dict]:
        """Stage-metric rows (incl. annotations) for the charts, newest first."""
        return self._query(
            """
            SELECT batch_ts, source_path, pipeline, batch_id, rows_written, batch_ms,
                   lag_s, bronze_ms, silver_ms, lakebase_ms, quarantined,
                   e2e_p50_ms, e2e_p95_ms, e2e_p99_ms, annotation
            FROM rt_stage_metrics
            WHERE source_path = %s AND batch_ts > now() - make_interval(secs => %s)
            ORDER BY batch_ts DESC
            """,
            (source_path, seconds),
        )

    def recent_gen_metrics(self, source_path: str, seconds: int = 600) -> list[dict]:
        """Per-worker generator rollups for the throughput 'sent' series (slot E)."""
        return self._query(
            """
            SELECT ts, source_path, worker_id, sent,
                   ack_p50_ms, ack_p95_ms, ack_p99_ms, throttled
            FROM rt_gen_metrics
            WHERE source_path = %s AND ts > now() - make_interval(secs => %s)
            ORDER BY ts DESC
            """,
            (source_path, seconds),
        )

    def stage_snapshot(self, source_path: str) -> dict | None:
        """AVERAGED bronze_to_silver hop metrics over the last 60 s — the stage
        rail's per-hop breakdown (slot D). Averaged (not the single latest batch)
        so the bronze/silver/lakebase badges read as a stable typical breakdown
        of the E2E trip rather than jumping with each batch. lag/batch use the
        latest values (they're point-in-time backlog signals, not per-row hops).
        """
        rows = self._query(
            """
            SELECT avg(broker_ms)   AS broker_ms,
                   avg(bronze_ms)   AS bronze_ms,
                   avg(silver_ms)   AS silver_ms,
                   avg(lakebase_ms) AS lakebase_ms,
                   avg(e2e_p50_ms)  AS e2e_p50_ms,
                   avg(e2e_p95_ms)  AS e2e_p95_ms,
                   avg(e2e_p99_ms)  AS e2e_p99_ms,
                   avg(batch_ms)    AS batch_ms,
                   max(lag_s)       AS lag_s,
                   sum(quarantined) AS quarantined
            FROM rt_stage_metrics
            WHERE source_path = %s AND pipeline = 'bronze_to_silver'
              AND batch_ts > now() - interval '60 seconds'
            """,
            (source_path,),
        )
        return rows[0] if rows and rows[0].get("bronze_ms") is not None else None

    def freshness_seconds(self, source_path: str) -> float | None:
        """Age (s) of the newest landed row for a path — the freshness probe (slot D)."""
        rows = self._query(
            """
            SELECT EXTRACT(EPOCH FROM (now() - max(ts_lakebase))) AS age_s
            FROM rt_latest_transactions
            WHERE source_path = %s
            """,
            (source_path,),
        )
        age = rows[0]["age_s"] if rows else None
        return float(age) if age is not None else None

    def path_summary(self, source_path: str) -> dict | None:
        rows = self._query(
            "SELECT * FROM rt_path_summary WHERE source_path = %s", (source_path,)
        )
        return rows[0] if rows else None

    def write_path_summary(self, source_path: str) -> None:
        """Freeze a path's last-run stats into rt_path_summary (the 'Previous run'
        card). Called when LEAVING a path (on switch/stop) so the other card shows
        the run you just finished. E2E percentiles come from the freshest serving
        rows; peak_rate is the max per-second total sent (summed across workers)
        from rt_gen_metrics. ON CONFLICT updates the single per-path row."""
        if not self.configured:
            return
        self._execute(
            "write",
            """
            INSERT INTO rt_path_summary
              (source_path, last_run_ended, peak_rate, e2e_p50_ms, e2e_p95_ms, quarantined)
            SELECT %s, now(),
                   COALESCE((
                     SELECT max(per_sec) FROM (
                       SELECT sum(sent) AS per_sec FROM rt_gen_metrics
                       WHERE source_path = %s AND ts > now() - interval '120 seconds'
                       GROUP BY date_trunc('second', ts)
                     ) g), 0)::int,
                   COALESCE(percentile_cont(0.50) WITHIN GROUP (
                     ORDER BY EXTRACT(EPOCH FROM (ts_lakebase - ts_generated))*1000), 0)::int,
                   COALESCE(percentile_cont(0.95) WITHIN GROUP (
                     ORDER BY EXTRACT(EPOCH FROM (ts_lakebase - ts_generated))*1000), 0)::int,
                   0
            FROM rt_latest_transactions
            WHERE source_path = %s AND ts_lakebase > now() - interval '120 seconds'
            ON CONFLICT (source_path) DO UPDATE SET
              last_run_ended = EXCLUDED.last_run_ended,
              peak_rate      = GREATEST(rt_path_summary.peak_rate, EXCLUDED.peak_rate),
              e2e_p50_ms     = EXCLUDED.e2e_p50_ms,
              e2e_p95_ms     = EXCLUDED.e2e_p95_ms,
              quarantined    = EXCLUDED.quarantined
            """,
            (source_path, source_path, source_path),
            fetch=False,
        )

    # -- reset (demo housekeeping) -------------------------------------------
    def reset_serving(self) -> None:
        """Truncate the Lakebase serving tables for a clean demo start.

        Clears the live tail, per-batch stage metrics, and generator rollups so
        the dashboard shows an empty slate; the medallion UC tables and stream
        checkpoints are reset separately (see scripts/reset_demo.py).
        """
        if not self.configured:
            return
        for tbl in ("rt_latest_transactions", "rt_stage_metrics",
                    "rt_gen_metrics"):
            self._execute("write", f"TRUNCATE TABLE {tbl}", (), fetch=False)

    # -- writes (generator telemetry) ---------------------------------------
    def write_annotation(self, source_path: str, label: str) -> None:
        """Drop a chart marker (spec 6.3 E/F) — every control change annotates.

        A minimal ``rt_stage_metrics`` row (``pipeline='annotation'``) whose
        non-null ``annotation`` the charts render as a vertical marker line.
        """
        if not self.configured or not label:
            return
        self._execute(
            "write",
            """
            INSERT INTO rt_stage_metrics (source_path, pipeline, annotation)
            VALUES (%s, 'annotation', %s)
            """,
            (source_path, label),
            fetch=False,
        )

    def write_gen_metrics(self, rows: list[dict]) -> None:
        """Persist supervisor rollups to rt_gen_metrics (best-effort)."""
        if not self.configured or not rows:
            return
        with self._locks["write"]:
            for attempt in (1, 2):
                try:
                    conn = self._connection("write")
                    with conn.cursor() as cur:
                        cur.executemany(
                            """
                            INSERT INTO rt_gen_metrics
                              (source_path, worker_id, sent,
                               ack_p50_ms, ack_p95_ms, ack_p99_ms, throttled)
                            VALUES (%(source_path)s, %(worker_id)s, %(sent)s,
                                    %(ack_p50_ms)s, %(ack_p95_ms)s, %(ack_p99_ms)s,
                                    %(throttled)s)
                            """,
                            rows,
                        )
                    return
                except Exception:
                    self._drop("write")
                    if attempt == 2:
                        return
