# DLT / Lakeflow Serverless Migration Plan

**Branch:** `feature/dlt-serverless-migration`
**Goal:** Move the medallion streaming pipeline from classic-compute Structured Streaming
jobs to **serverless Lakeflow Declarative Pipelines (DLT)** with **enhanced autoscaling**,
to fix the streaming-backlog-doesn't-autoscale problem — while preserving **sub-second
Lakebase serving** (Option A: `foreach_batch_sink` + psycopg, chosen for speed).

**Egress prerequisite: CONFIRMED.** Serverless → Lakebase psycopg egress verified working
(TCP+TLS reached the Postgres server; only auth rejected). No NCC/allowlist blocker.

---

## Why (the problem this solves)

- Classic cluster autoscale does **not** react to Structured Streaming backlog → the
  medallion cluster sat stuck at 2 workers, ~110 rows/s, lag building to tens of seconds.
- DLT **enhanced autoscaling** scales on task-queue/slot-utilization (streaming-aware),
  is always-on under serverless, and adds vertical scaling. Cold start improves via warm
  pools (use **Performance-Optimized** mode for latency).
- Serverless is GA for Lakeflow pipelines; no DBR knob (channel `CURRENT`/`PREVIEW`),
  deps via the Environment panel (`%pip`), Unity Catalog mandatory (already using it).

## What maps cleanly (declarative Delta)

| Current | DLT target |
|---|---|
| `readStream.table(bronze_hl7_raw)` | streaming source flow |
| HL7 parse pandas UDF (`pipelines/lib/hl7_parser.py`) | runs inside a `@dlt.table` transform (≤1 GB UDF mem; **no internet from UDF**) |
| valid → `silver_hl7_parsed` (append, txnAppId idempotency) | `@dlt.table` streaming table (DLT owns once-only) |
| bad → `silver_hl7_quarantine` | **DLT expectations** (`@dlt.expect_or_drop` / quarantine) — native upgrade, DQ metrics free |
| `silver_to_gold` windowed aggs + gold (3 tables) | **DROPPED** — nothing reads gold (see below) |

## What does NOT fit (needs `foreach_batch_sink` — Option A)

These are side-effect writes to **external Lakebase**, not managed Delta:
- `_upsert_latest` → `rt_latest_transactions` (live tail) — sub-second serving.
- `_upsert_snapshots` → `rt_gold_snapshots`.
- `_write_stage_metric` → `rt_stage_metrics` (both pipelines).
- gen-metrics rollup → `rt_gen_metrics` (this stays in the app supervisor, not the pipeline).

**Pattern:** `dlt.create_sink(..., "foreach_batch_sink")` + `@dlt.append_flow` feeding it,
with the psycopg connection-reuse + OAuth-token-cache logic lifted verbatim from the
current `_lakebase_conn`. We own idempotency (ON CONFLICT DO NOTHING/UPDATE) — same as today.
Constraints: sinks are Python-only, streaming-only, no expectations, no full-refresh cleanup
of external rows.

## Design decisions (former "risks" — all mitigated, not accepted)

1. **OAuth token / SDK `.postgres` — MITIGATED by pinning deps.** Do NOT rely on the
   serverless-bundled SDK. Pin `databricks-sdk` (a version with `.postgres`, ≥0.120) in the
   DLT **Environment `dependencies`** alongside `psycopg[binary]` — deterministic, same
   mechanism we already use. Also use the **latest serverless environment version** (client
   `4`/current) so the base image is modern. No REST fallback needed. (The earlier REST 400s
   were just a missing `request_id`/instance-name — irrelevant once we mint via the pinned SDK.)
2. **`.toPandas()` — NOT an antipattern here; kept bounded by design.** The only collect is the
   live-tail's newest **≤500 rows** (`.orderBy(ts_silver desc).limit(500).toPandas()`) — a
   small, bounded, correct driver collect for the serving tail. We NEVER collect a full batch.
   Metric writes are single-row. So there is no scale antipattern to mitigate — the pattern is
   intentionally bounded.
3. **`spark_env_vars` unsupported on serverless → use pipeline `configuration`.** Move
   `HL7_CATALOG/SCHEMA/ENDPOINT_NAME/LAKEBASE_*` into the pipeline **configuration** map, read
   via `spark.conf.get(...)`. The BUNDLE_ROOT/co_filename import shim is dropped (DLT loads the
   module files directly). This is a clean config change, not a risk.
4. **pandas UDF has no internet** — psycopg writes live in the `foreach_batch_sink` (driver
   batch), never in the parse UDF. Already how the code is structured today.
5. **Performance-Optimized mode = STANDARD CONFIG (not optional).** Enabled on the pipeline for
   this latency demo. `serverless: true` + Performance-Optimized in `resources/dlt_pipeline.yml`.

## DAB / resource changes

- New `resources/dlt_pipeline.yml`: `pipelines:` resource, `serverless: true`,
  `photon: true`, `channel: CURRENT`, `development: true`, `continuous: true`,
  libraries → the DLT notebook/py files, `configuration:` map for catalog/schema/lakebase.
- Retire `resources/jobs.yml` medallion job (keep as fallback until DLT proven).
- App unchanged (still reads Lakebase). `scripts/reset_demo.py` updated for DLT
  checkpoint/state reset semantics (DLT manages its own storage — likely just table truncates).

## Execution phases (concrete, reviewable)

**Phase 0 — Scaffold (no behavior change)**
- Create `pipelines/dlt/` with `medallion_dlt.py` (the DLT notebook/module).
- Add `resources/dlt_pipeline.yml`. Keep classic jobs.yml intact.

**Phase 1 — Declarative bronze→silver (Delta only)**
- Bronze stays the Zerobus target table; DLT `readStream`s it (decision 1).
- `@dlt.table silver_hl7_parsed` = parse UDF + `@dlt.expect_or_drop`/quarantine.
- **No gold** (dropped — decision 2). No silver_to_gold.
- Deploy, validate silver populates + enhanced autoscaling engages under load.

**Phase 2 — Lakebase serving sinks (Option A)**
- Lift `_lakebase_conn` (pooled conn + token cache) into a sink helper module.
- `create_sink` + `append_flow` for the tables the dashboard reads:
  **rt_latest_transactions** (live tail) and **rt_stage_metrics** (rail/charts).
  (rt_gold_snapshots dropped with gold; rt_gen_metrics stays written by the app supervisor.)
- Verify sub-second serving latency to the dashboard.

**Phase 3 — Cutover + cleanup**
- Point app at DLT-fed Lakebase (no app change — table names identical).
- **Retire the classic medallion job**: remove it from `resources/jobs.yml` (per decision 3),
  delete `pipelines/silver_to_gold.py`, and drop the gold DDL from `sql/01_uc_tables.sql`
  (+ rt_gold_snapshots from `sql/02_lakebase.sql`).
  Update `scripts/reset_demo.py` (drop gold/snapshot truncates + DLT reset semantics). Update tests.
- Full end-to-end validation on the live dashboard (moving axis, per-hop, latency under load),
  confirming enhanced autoscaling engages and latency stays flat as rate increases.

## Parallelization (only if isolated)

Concrete, isolated sub-tasks that COULD run in parallel once Phase 0 scaffold exists:
- **A:** author `medallion_dlt.py` bronze-read + silver `@dlt.table` (parse UDF + quarantine
  expectations) — self-contained, no gold.
- **B:** author the Lakebase sink helper module (pooled conn + token cache, reused from
  `_lakebase_conn`) — self-contained, testable standalone.
- **C:** author `resources/dlt_pipeline.yml` (serverless, Performance-Optimized, pinned deps,
  configuration map) — self-contained.
Sequential/serial (NOT parallel): deploy, validate, cutover — these depend on A+B+C landing
and touch live state, so single-threaded. Given the reduced scope (no gold), this may not
even need sub-agents — it's small enough to execute directly.

## Decisions — RESOLVED (2026-07-17)

1. **Bronze stays as Zerobus's direct target; DLT `readStream`s it.** ✅ (least disruption; the
   Zerobus REST writer in the app is unchanged.)
2. **DROP the gold layer entirely** (CORRECTED after code review). ✅
   Validation found NOTHING reads the `gold_*` tables — the live dashboard reads only the
   `rt_*` Lakebase serving tables (rt_latest_transactions, rt_stage_metrics, rt_gen_metrics).
   The gold tables + `rt_gold_snapshots` are written but never consumed. So the entire
   `silver_to_gold` pipeline is dropped from the DLT migration. New scope:
   **bronze → silver + Lakebase serving sinks. No gold, no silver_to_gold, no _merge_gold,
   no auto_cdc, no windowed aggregation.** Biggest simplification of the whole migration.
   (auto_cdc/windowed-agg would be the right tools IF we later add a consumed gold tier.)
3. **Retire the classic `jobs.yml` medallion job** — it's committed in `main` history, and we're
   on a feature branch, so we cut over cleanly rather than carrying a fallback. Delete
   `resources/jobs.yml`'s medallion job in Phase 3; the git history on `main` is the fallback. ✅
