-- Unity Catalog objects for the HL7 Real-Time Intelligence demo.
--
-- Rendered and executed by scripts/run_sql.py, which substitutes {catalog} and
-- {schema} from config. The catalog is a pre-existing prerequisite; this script
-- only creates the schema, a checkpoint volume, and the medallion tables.
--
-- Shared landing table `bronze_hl7_raw` is written by BOTH ingestion paths, and
-- each path stamps ts_bronze explicitly at write time (Zerobus ingestion does
-- not support column DEFAULT values):
--   Path A (Zerobus REST): app stamps ts_bronze just before the durable POST.
--   Path B (Event Hubs):   eventhub_to_bronze job stamps ts_transport + ts_bronze.

CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
  COMMENT 'HL7 v2 real-time intelligence demo (synthetic data only)';

CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints
  COMMENT 'Structured Streaming checkpoints for the medallion pipelines';

-- ---------------------------------------------------------------------------
-- Bronze: raw HL7 envelope, one row per generated message, shared by both paths
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bronze_hl7_raw (
  event_id      STRING    NOT NULL COMMENT 'MSH-10 message control id (uuid)',
  source_path   STRING    NOT NULL COMMENT 'zerobus | eventhub — stamped at generation',
  facility_id   STRING             COMMENT 'MSH-4 sending facility',
  message_type  STRING             COMMENT 'MSH-9 e.g. ADT^A01, ORU^R01',
  hl7_raw       STRING             COMMENT 'pipe-delimited HL7 v2.5 payload',
  gen_worker_id STRING             COMMENT 'generator worker that emitted the record',
  ts_generated  TIMESTAMP          COMMENT 'app clock at generation',
  ts_transport  TIMESTAMP          COMMENT 'Path B Kafka record ts; NULL on Path A',
  ts_bronze     TIMESTAMP          COMMENT 'ingest time, stamped by the writer of each path'
)
USING DELTA
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true'
);

-- ---------------------------------------------------------------------------
-- Silver + Gold: intentionally NOT declared here.
--
-- The medallion is a serverless DLT (Lakeflow) pipeline (resources/dlt_pipeline.yml,
-- pipelines/dlt/medallion_dlt.py). DLT OWNS and materializes its output tables —
-- `silver_hl7_parsed` and `silver_hl7_quarantine` are DLT-managed streaming
-- tables. Declaring them here as plain MANAGED Delta tables would collide with
-- DLT on deploy ("Could not materialize … a MANAGED table already exists"), so
-- their DDL is removed. This file now provisions only the shared bronze landing
-- table (above, written by Zerobus) + the checkpoints volume.
--
-- The old gold_* aggregates are dropped entirely: nothing consumed them (the
-- dashboard reads the Lakebase rt_* serving tables), so the DLT migration is
-- bronze → silver → Lakebase serving only — no gold tier.
