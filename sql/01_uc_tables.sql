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
-- Silver: parsed + validated clean records
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_hl7_parsed (
  event_id      STRING NOT NULL,
  source_path   STRING NOT NULL,
  facility_id   STRING,
  message_type  STRING,
  patient_mrn   STRING,
  unit          STRING,
  summary       STRING    COMMENT 'human-readable one-line clinical summary',
  ts_generated  TIMESTAMP,
  ts_transport  TIMESTAMP,
  ts_bronze     TIMESTAMP,
  ts_silver     TIMESTAMP COMMENT 'parse/validate commit time'
)
USING DELTA;

-- ---------------------------------------------------------------------------
-- Quarantine: records that failed parse/validate (queryable, not dropped)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_hl7_quarantine (
  event_id      STRING,
  source_path   STRING,
  error_code    STRING COMMENT 'TRUNCATED_SEGMENT | BAD_SEPARATOR | BAD_TIMESTAMP | PARSE_ERROR',
  error_detail  STRING,
  hl7_raw       STRING,
  ts_generated  TIMESTAMP,
  ts_transport  TIMESTAMP,          -- Path B Kafka record ts; NULL on Path A (matches silver_hl7_parsed)
  ts_bronze     TIMESTAMP,
  ts_silver     TIMESTAMP
)
USING DELTA;

-- ---------------------------------------------------------------------------
-- Gold: 10s tumbling-window aggregates, keyed by source_path (30s watermark)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.gold_throughput_10s (
  window_start TIMESTAMP NOT NULL,
  source_path  STRING    NOT NULL,
  facility_id  STRING,
  message_type STRING,
  msg_count    BIGINT
)
USING DELTA;

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.gold_latency_10s (
  window_start TIMESTAMP NOT NULL,
  source_path  STRING    NOT NULL,
  e2e_p50_ms   INT,
  e2e_p95_ms   INT,
  e2e_p99_ms   INT,
  msg_count    BIGINT
)
USING DELTA;

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.gold_census_delta_10s (
  window_start TIMESTAMP NOT NULL,
  source_path  STRING    NOT NULL,
  facility_id  STRING,
  admits       BIGINT,
  discharges   BIGINT,
  net_census   BIGINT
)
USING DELTA;
