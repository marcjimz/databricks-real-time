"""Phase 0 smoke test: prove the Zerobus REST front door end-to-end.

Pushes a small burst of synthetic HL7 bronze records straight to
``bronze_hl7_raw`` via the Zerobus Direct Write REST API, then queries the
table back to confirm the rows landed and the ``ts_bronze`` column DEFAULT
fired. Also reports single-worker batched throughput so we can set an honest
generator slider cap for later phases.

Usage:
  python scripts/phase0_zerobus_smoke.py [--count 50] [--batch 10]
      [--profile fe-vm-real-time-mode-demo] [--warehouse <id>]
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from datetime import datetime
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))
# app/ modules import each other flat (from config / from generator...), matching
# the flattened Databricks App runtime — make that importable here too.
sys.path.insert(0, str(_REPO / "app"))

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.service.sql import StatementState  # noqa: E402

from app.config import CONFIG  # noqa: E402
from app.generator.hl7_factory import HL7Factory  # noqa: E402
from app.generator.profiles import profile_for  # noqa: E402
from app.generator.zerobus_client import ZerobusRestClient  # noqa: E402

# Only the real bronze columns go over the wire; the generator-only underscore
# hints (_summary/_expected_error) are dropped. Path A stamps ts_bronze itself,
# right before the durable POST (Zerobus ingestion forbids column DEFAULTs).
# Zerobus encodes TIMESTAMP columns as epoch MICROSECONDS (verified: millis/sec
# land in 1970).
_BRONZE_COLS = (
    "event_id", "source_path", "facility_id", "message_type",
    "hl7_raw", "gen_worker_id",
)


def _iso_to_micros(iso: str) -> int:
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1_000_000)


def _to_bronze(rec: dict, path: str) -> dict:
    out = {k: rec.get(k) for k in _BRONZE_COLS}
    out["source_path"] = path
    out["ts_generated"] = _iso_to_micros(rec["ts_generated"])
    out["ts_bronze"] = int(datetime.now().timestamp() * 1_000_000)
    return out


async def _push(count: int, batch: int) -> tuple[int, float]:
    path = CONFIG.ingest_path  # "zerobus"
    factory = HL7Factory(profile_for(path), worker_id="phase0-smoke", malformed_pct=0.0)
    client = ZerobusRestClient(CONFIG, table=CONFIG.table("bronze_hl7_raw"))

    sent = 0
    start = time.perf_counter()
    try:
        pending: list[dict] = []
        for _ in range(count):
            pending.append(_to_bronze(factory.make(), path))
            if len(pending) >= batch:
                res = await client.insert(pending)
                if not res.ok:
                    raise RuntimeError(f"insert failed [{res.status_code}]: {res.error}")
                print(f"  batch ok: {res.count:>3} rows  {res.latency_ms:6.1f} ms")
                sent += res.count
                pending = []
        if pending:
            res = await client.insert(pending)
            if not res.ok:
                raise RuntimeError(f"insert failed [{res.status_code}]: {res.error}")
            print(f"  batch ok: {res.count:>3} rows  {res.latency_ms:6.1f} ms")
            sent += res.count
    finally:
        await client.aclose()
    return sent, time.perf_counter() - start


def _query_one(ws: WorkspaceClient, warehouse: str, sql: str) -> list:
    resp = ws.statement_execution.execute_statement(
        warehouse_id=warehouse, statement=sql, wait_timeout="30s"
    )
    if resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        resp = ws.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error.message if resp.status and resp.status.error else "unknown"
        raise RuntimeError(f"query failed: {err}")
    return resp.result.data_array[0]


def _verify(ws: WorkspaceClient, warehouse: str, expected: int) -> None:
    # Zerobus commits are durable at HTTP 200 but become query-visible after a
    # short micro-batch lag; poll until the rows appear (or time out).
    table = CONFIG.table("bronze_hl7_raw")
    q = (
        f"SELECT count(*) AS n, count(ts_bronze) AS n_bronze, "
        f"       min(from_unixtime(unix_micros(ts_bronze)/1000000)) AS first_bronze, "
        f"       max(from_unixtime(unix_micros(ts_bronze)/1000000)) AS last_bronze "
        f"FROM {table} WHERE source_path = 'zerobus'"
    )
    deadline = time.time() + 30
    n = n_bronze = 0
    first_bronze = last_bronze = None
    while time.time() < deadline:
        n, n_bronze, first_bronze, last_bronze = _query_one(ws, warehouse, q)
        if int(n) >= expected:
            break
        time.sleep(3)

    print(f"\nbronze_hl7_raw (source_path='zerobus'):")
    print(f"  rows                : {n} (expected >= {expected})")
    print(f"  ts_bronze populated : {n_bronze}/{n}")
    print(f"  ts_bronze range     : {first_bronze}  ->  {last_bronze}")
    if int(n) < expected:
        raise RuntimeError(f"only {n} of {expected} rows visible within timeout")
    if int(n_bronze) != int(n):
        raise RuntimeError("ts_bronze not populated on every row")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=50)
    ap.add_argument("--batch", type=int, default=10)
    ap.add_argument("--profile", default="fe-vm-real-time-mode-demo")
    ap.add_argument("--warehouse", default="1916c91c970b63b5")
    args = ap.parse_args()

    print(f"Zerobus endpoint : {CONFIG.zerobus_endpoint}")
    print(f"Target table     : {CONFIG.table('bronze_hl7_raw')}")
    print(f"Pushing {args.count} record(s) in batches of {args.batch}\n")

    ws = WorkspaceClient(profile=args.profile)
    # Start from a clean slate so the row count is an exact assertion.
    _query_one(ws, args.warehouse,
               f"DELETE FROM {CONFIG.table('bronze_hl7_raw')} WHERE source_path = 'zerobus'")

    sent, elapsed = asyncio.run(_push(args.count, args.batch))
    rate = sent / elapsed if elapsed else 0.0
    print(f"\nPushed {sent} record(s) in {elapsed:.2f}s  ->  {rate:.1f} rec/s (single worker)")

    _verify(ws, args.warehouse, expected=sent)
    print("\nPhase 0 smoke test PASSED.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
