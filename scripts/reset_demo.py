"""Reset the HL7 RTI demo to a clean slate.

Truncates the medallion UC tables (bronze/silver/gold), truncates the Lakebase
serving tables, and clears the Structured Streaming checkpoints so the next
pipeline run starts from an empty state (and, with ``startingVersion=latest`` on
the reads, only processes events generated after the restart).

Run this BEFORE a demo, then unpause the medallion job. Usage:

    python scripts/reset_demo.py --profile fe-vm-real-time-mode-demo

Requires: a SQL warehouse (UC truncates + volume clear via `REMOVE`), Lakebase
OAuth (serving-table truncates). The app's in-memory counters are cleared
separately by the dashboard Reset button.
"""

from __future__ import annotations

import argparse

from databricks.sdk import WorkspaceClient

CATALOG = "real_time_mode_demo_catalog"
SCHEMA = "rti_demo"
LAKEBASE_HOST = "ep-steep-mountain-d2c3ahvt.database.us-east-1.cloud.databricks.com"
LAKEBASE_DB = "rti_demo"
LAKEBASE_ENDPOINT = "projects/rti-demo/branches/production/endpoints/primary"

UC_TABLES = ("bronze_hl7_raw", "silver_hl7_parsed", "silver_hl7_quarantine",
             "gold_throughput_10s", "gold_latency_10s", "gold_census_delta_10s")
LAKEBASE_TABLES = ("rt_latest_transactions", "rt_stage_metrics",
                   "rt_gen_metrics", "rt_gold_snapshots")
CHECKPOINTS = (f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/bronze_to_silver",
               f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/silver_to_gold")


def _rmrf(w: WorkspaceClient, path: str) -> None:
    """Recursively delete a UC Volume directory (delete_directory is non-recursive)."""
    try:
        entries = list(w.files.list_directory_contents(path))
    except Exception as e:  # missing checkpoint is fine
        print(f"  skip {path}: {str(e)[:50]}")
        return
    for e in entries:
        if e.is_directory:
            _rmrf(w, e.path)
        else:
            try:
                w.files.delete(e.path)
            except Exception:
                pass
    try:
        w.files.delete_directory(path)
        print(f"  removed {path}")
    except Exception as ex:
        print(f"  dir del fail {path}: {str(ex)[:50]}")


def _run_sql(w: WorkspaceClient, warehouse_id: str, sql: str) -> None:
    r = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql,
        catalog=CATALOG, schema=SCHEMA, wait_timeout="30s",
    )
    print(f"  {r.status.state}: {sql[:70]}")


def reset(profile: str, lakebase_user: str) -> None:
    w = WorkspaceClient(profile=profile)
    warehouse_id = next(iter(w.warehouses.list())).id

    print("Truncating UC medallion tables…")
    for t in UC_TABLES:
        _run_sql(w, warehouse_id, f"TRUNCATE TABLE {CATALOG}.{SCHEMA}.{t}")

    print("Clearing stream checkpoints…")
    for path in CHECKPOINTS:
        _rmrf(w, path)  # delete_directory is non-recursive; walk + delete

    print("Truncating Lakebase serving tables…")
    import psycopg

    token = w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT).token
    conn = psycopg.connect(host=LAKEBASE_HOST, port=5432, dbname=LAKEBASE_DB,
                           user=lakebase_user, password=token, sslmode="require",
                           connect_timeout=10, autocommit=True)
    with conn.cursor() as cur:
        for t in LAKEBASE_TABLES:
            try:
                cur.execute(f"TRUNCATE TABLE {t}")
                print(f"  truncated {t}")
            except Exception as e:
                print(f"  skip {t}: {str(e)[:60]}")
    conn.close()
    print("Reset complete. Unpause the medallion job to start clean.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="fe-vm-real-time-mode-demo")
    ap.add_argument("--lakebase-user", default="marcin.jimenez@databricks.com",
                    help="Postgres role = the identity the OAuth token is minted for")
    args = ap.parse_args()
    reset(args.profile, args.lakebase_user)
