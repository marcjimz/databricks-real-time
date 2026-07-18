"""Render and execute a .sql file against a Databricks SQL warehouse.

Substitutes {catalog} / {schema} from config, splits on semicolons, and runs
each statement via the Statement Execution API. Used for UC DDL setup.

Usage:
  python scripts/run_sql.py sql/01_uc_tables.sql \
      [--warehouse <id>] [--profile <name>] [--catalog <c>] [--schema <s>]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.service.sql import StatementState  # noqa: E402

from app.config import CONFIG  # noqa: E402


def split_statements(sql: str) -> list[str]:
    out, buf = [], []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or not stripped:
            continue
        buf.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(buf).strip().rstrip(";").strip()
            if stmt:
                out.append(stmt)
            buf = []
    tail = "\n".join(buf).strip().rstrip(";").strip()
    if tail:
        out.append(tail)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("sql_file")
    ap.add_argument("--warehouse", default="1916c91c970b63b5")
    ap.add_argument("--profile", default="fe-vm-real-time-mode-demo")
    ap.add_argument("--catalog", default=CONFIG.catalog)
    ap.add_argument("--schema", default=CONFIG.schema)
    ap.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                    help="extra template substitutions, e.g. --set principal=<app-id>")
    args = ap.parse_args()

    subs = {"catalog": args.catalog, "schema": args.schema}
    for pair in args.set:
        key, _, value = pair.partition("=")
        subs[key.strip()] = value.strip()

    raw = Path(args.sql_file).read_text()
    rendered = raw.format(**subs)
    statements = split_statements(rendered)

    ws = WorkspaceClient(profile=args.profile)
    print(f"Executing {len(statements)} statement(s) on warehouse {args.warehouse} "
          f"[{args.catalog}.{args.schema}]\n")

    for i, stmt in enumerate(statements, 1):
        label = " ".join(stmt.split())[:80]
        resp = ws.statement_execution.execute_statement(
            warehouse_id=args.warehouse, statement=stmt, wait_timeout="30s"
        )
        state = resp.status.state
        if state == StatementState.PENDING or state == StatementState.RUNNING:
            resp = ws.statement_execution.get_statement(resp.statement_id)
            state = resp.status.state
        if state != StatementState.SUCCEEDED:
            err = resp.status.error.message if resp.status and resp.status.error else "unknown"
            print(f"  [{i}/{len(statements)}] FAILED: {label}\n      -> {err}")
            return 1
        print(f"  [{i}/{len(statements)}] ok: {label}")

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
