-- Lakebase (Postgres) serving layer for the HL7 Real-Time Intelligence demo.
--
-- Applied to the Lakebase instance's application database (not Unity Catalog).
-- These tables back the live dashboard: the medallion pipeline upserts into
-- them from foreachBatch, and the Dash app reads them at 1 Hz. Everything the
-- UI shows comes from here — the serverless DLT medallion writes them via its
-- Lakebase serving sink (there is no gold tier).
--
-- Path-keyed by source_path ('zerobus' | 'eventhub') so a single schema serves
-- both ingestion front doors and the two paths can be compared side by side.

-- Newest transaction per event, the live tail + active-path freshness probe.
CREATE TABLE IF NOT EXISTS rt_latest_transactions (
  event_id      text PRIMARY KEY,
  source_path   text,
  facility_id   text,
  message_type  text,
  patient_mrn   text,
  unit          text,
  summary       text,
  ts_generated  timestamptz,          -- app clock at HL7 generation
  ts_bronze     timestamptz,          -- Zerobus durable-write time
  ts_silver     timestamptz,          -- parse/validate commit time
  ts_lakebase   timestamptz DEFAULT now()   -- serving-layer upsert time
);
-- Existing deployments: add ts_bronze if the table predates the 4-hop trip.
ALTER TABLE rt_latest_transactions ADD COLUMN IF NOT EXISTS ts_bronze timestamptz;
CREATE INDEX IF NOT EXISTS rt_latest_transactions_ts_lakebase_idx
  ON rt_latest_transactions (ts_lakebase DESC);

-- One row per pipeline micro-batch: throughput, backlog (lag_s), per-hop
-- latency and E2E latency percentiles. Drives the stage rail and both
-- time-series charts. The per-hop columns (bronze_ms/silver_ms/lakebase_ms)
-- are measured in bronze_to_silver's foreachBatch so the stage rail (slot D)
-- shows real hop latency rather than a fabricated split; quarantined is the
-- per-batch count of records routed to the quarantine table.
CREATE TABLE IF NOT EXISTS rt_stage_metrics (
  batch_ts     timestamptz DEFAULT now(),
  source_path  text,
  pipeline     text,                 -- eh_ingest | bronze_to_silver (DLT medallion)
  batch_id     bigint,
  rows_written int,
  batch_ms     int,
  lag_s        numeric,              -- v4: bronze frontier − silver frontier (backlog)
  broker_ms    int,                  -- Path B: median ts_transport − ts_generated (EH accept); NULL on Path A
  bronze_ms    int,                  -- median ts_bronze − ts_generated (transport + land)
  silver_ms    int,                  -- median ts_silver − ts_bronze (parse + commit)
  lakebase_ms  int,                  -- wall time of the rt_latest_transactions upsert
  quarantined  int,                  -- records sent to quarantine this batch
  e2e_p50_ms   int,
  e2e_p95_ms   int,
  e2e_p99_ms   int,
  annotation   text                  -- non-null = chart marker (e.g. path switch, burst)
);
CREATE INDEX IF NOT EXISTS rt_stage_metrics_batch_ts_idx
  ON rt_stage_metrics (batch_ts DESC);
-- Idempotent columns for instances created before the per-hop columns landed.
ALTER TABLE rt_stage_metrics ADD COLUMN IF NOT EXISTS bronze_ms   int;
ALTER TABLE rt_stage_metrics ADD COLUMN IF NOT EXISTS silver_ms   int;
ALTER TABLE rt_stage_metrics ADD COLUMN IF NOT EXISTS lakebase_ms int;
ALTER TABLE rt_stage_metrics ADD COLUMN IF NOT EXISTS quarantined int;
-- Path B broker hop: median ts_transport − ts_generated (Event Hubs accept
-- time). NULL/0 on Path A (Zerobus has no broker). Drives the rail's broker badge.
ALTER TABLE rt_stage_metrics ADD COLUMN IF NOT EXISTS broker_ms   int;

-- (rt_gold_snapshots removed: the gold tier was dropped in the DLT serverless
-- migration — nothing consumed it. The dashboard reads rt_latest_transactions +
-- rt_stage_metrics below, both fed by the DLT Lakebase serving sink.)

-- Per-worker generator telemetry rolled up at 1 s: send rate, ack latency,
-- throttle state. Drives the active-path panel and throughput "sent" line.
CREATE TABLE IF NOT EXISTS rt_gen_metrics (
  ts          timestamptz DEFAULT now(),
  source_path text,
  worker_id   text,
  sent        int,
  ack_p50_ms  int,
  ack_p95_ms  int,
  ack_p99_ms  int,
  throttled   int
);
CREATE INDEX IF NOT EXISTS rt_gen_metrics_ts_idx
  ON rt_gen_metrics (ts DESC);

-- Frozen summary of the last completed run per path, for the side-by-side
-- "previous run" comparison panel. Upserted on path switch / stop.
CREATE TABLE IF NOT EXISTS rt_path_summary (
  source_path    text PRIMARY KEY,
  last_run_ended timestamptz,
  peak_rate      int,
  e2e_p50_ms     int,
  e2e_p95_ms     int,
  quarantined    int
);
