"""Reset the HL7 RTI demo to a clean slate.

Truncates the shared bronze landing table, truncates the Lakebase serving tables,
and (optionally) full-refreshes the serverless DLT medallion so silver is rebuilt
from an empty bronze.

DLT owns silver_hl7_parsed / silver_hl7_quarantine and their checkpoints — you
do NOT truncate them or hand-delete their checkpoints (that corrupts the pipeline
state). Instead, after clearing bronze, trigger a **full-refresh** DLT update
(``--full-refresh``), which resets the pipeline's own streaming state and
reprocesses from the (now empty) bronze table. Bronze is the only Structured
Streaming source we still checkpoint outside DLT (Zerobus writes it directly).

Run this BEFORE a demo. Usage:

    python scripts/reset_demo.py --profile fe-vm-real-time-mode-demo
    python scripts/reset_demo.py --profile fe-vm-real-time-mode-demo --full-refresh

Requires: a SQL warehouse (UC truncates), Lakebase OAuth (serving-table
truncates). The app's in-memory counters are cleared separately by the dashboard
Reset button.
"""

from __future__ import annotations

import argparse

from databricks.sdk import WorkspaceClient

CATALOG = "real_time_mode_demo_catalog"
SCHEMA = "rti_demo"
LAKEBASE_HOST = "ep-steep-mountain-d2c3ahvt.database.us-east-1.cloud.databricks.com"
LAKEBASE_DB = "rti_demo"
LAKEBASE_ENDPOINT = "projects/rti-demo/branches/production/endpoints/primary"
DLT_PIPELINE_NAME = "hl7-rti-medallion-dlt"

# Only the shared bronze landing table is truncated here — silver is DLT-managed
# and reset via a full-refresh update, not a TRUNCATE.
UC_TABLES = ("bronze_hl7_raw",)
LAKEBASE_TABLES = ("rt_latest_transactions", "rt_stage_metrics", "rt_gen_metrics")


def _run_sql(w: WorkspaceClient, warehouse_id: str, sql: str) -> None:
    r = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql,
        catalog=CATALOG, schema=SCHEMA, wait_timeout="30s",
    )
    print(f"  {r.status.state}: {sql[:70]}")


def _full_refresh_dlt(w: WorkspaceClient) -> None:
    """Trigger a full-refresh update on the serverless DLT medallion.

    Full-refresh resets DLT's own streaming state and rebuilds silver from the
    (now truncated) bronze — the correct way to reset a DLT-managed table (never
    TRUNCATE it or hand-delete its checkpoint)."""
    pipe = next((p for p in w.pipelines.list_pipelines()
                 if p.name == DLT_PIPELINE_NAME), None)
    if pipe is None:
        print(f"  DLT pipeline '{DLT_PIPELINE_NAME}' not found — skipping refresh")
        return
    w.pipelines.start_update(pipeline_id=pipe.pipeline_id, full_refresh=True)
    print(f"  full-refresh started on {DLT_PIPELINE_NAME} ({pipe.pipeline_id})")


def reset(profile: str, lakebase_user: str, full_refresh: bool) -> None:
    w = WorkspaceClient(profile=profile)
    warehouse_id = next(iter(w.warehouses.list())).id

    print("Truncating bronze landing table…")
    for t in UC_TABLES:
        _run_sql(w, warehouse_id, f"TRUNCATE TABLE {CATALOG}.{SCHEMA}.{t}")

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

    if full_refresh:
        print("Full-refreshing the serverless DLT medallion…")
        _full_refresh_dlt(w)
        print("Reset complete. DLT is rebuilding silver from empty bronze.")
    else:
        print("Reset complete. Run with --full-refresh to also reset DLT state, "
              "or the DLT medallion will keep streaming from the truncated bronze.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="fe-vm-real-time-mode-demo")
    ap.add_argument("--full-refresh", action="store_true",
                    help="also trigger a full-refresh update on the DLT medallion")
    ap.add_argument("--lakebase-user", default="marcin.jimenez@databricks.com",
                    help="Postgres role = the identity the OAuth token is minted for")
    args = ap.parse_args()
    reset(args.profile, args.lakebase_user, args.full_refresh)
