# Databricks notebook source
# MAGIC %md
# MAGIC # HL7 Real-Time Intelligence — Query the unified data across every layer
# MAGIC
# MAGIC The demo streams synthetic HL7 v2 events **Zerobus → bronze → silver → Lakebase serving**
# MAGIC via a **serverless Lakeflow Declarative Pipeline (DLT)** with enhanced autoscaling.
# MAGIC
# MAGIC The same live records are queryable **at every layer**:
# MAGIC
# MAGIC | Layer | Object | Type |
# MAGIC |---|---|---|
# MAGIC | Bronze | `bronze_hl7_raw` | MANAGED Delta landing table (written by Zerobus) |
# MAGIC | Silver | `silver_hl7_parsed`, `silver_hl7_quarantine` | **DLT-managed STREAMING TABLES** |
# MAGIC | Serving | `rt_latest_transactions`, `rt_stage_metrics` | Lakebase (Postgres) — read by the dashboard |
# MAGIC
# MAGIC This notebook covers the UC layers (bronze + silver). The Lakebase serving queries
# MAGIC (Postgres) are in the repo `README.md` (query a Postgres client with an OAuth token).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — catalog / schema widgets
# MAGIC Defaults are the demo's catalog + schema; override the widgets at the top of the
# MAGIC notebook to point at any other deployment.

# COMMAND ----------

dbutils.widgets.text("catalog", "real_time_mode_demo_catalog", "Catalog")
dbutils.widgets.text("schema", "rti_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
print(f"Querying {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze — latest raw messages as they land (Delta)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_id, source_path, message_type, ts_generated, ts_bronze
# MAGIC FROM ${catalog}.${schema}.bronze_hl7_raw
# MAGIC ORDER BY ts_bronze DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver — latest parsed records (DLT streaming table)
# MAGIC `silver_hl7_parsed` is a **STREAMING_TABLE** — DLT owns and continuously
# MAGIC materializes it. Query it exactly like any table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_id, facility_id, message_type, unit, patient_mrn, ts_bronze, ts_silver
# MAGIC FROM ${catalog}.${schema}.silver_hl7_parsed
# MAGIC ORDER BY ts_silver DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC Malformed messages are routed (not dropped) to a parallel streaming table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_id, error_code, error_detail, ts_silver
# MAGIC FROM ${catalog}.${schema}.silver_hl7_quarantine
# MAGIC ORDER BY ts_silver DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unification — trace one event bronze → silver with per-hop latency
# MAGIC Join the raw landing table to the streaming table on `event_id` to reconstruct
# MAGIC each record's journey and measure every hop in milliseconds.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT b.event_id,
# MAGIC        b.source_path,
# MAGIC        b.message_type,
# MAGIC        (unix_millis(s.ts_bronze) - unix_millis(b.ts_generated)) AS bronze_ms,  -- generate → land
# MAGIC        (unix_millis(s.ts_silver) - unix_millis(s.ts_bronze))    AS silver_ms,  -- land → parsed
# MAGIC        (unix_millis(s.ts_silver) - unix_millis(b.ts_generated)) AS e2e_ms      -- generate → silver
# MAGIC FROM ${catalog}.${schema}.bronze_hl7_raw   b
# MAGIC JOIN ${catalog}.${schema}.silver_hl7_parsed s USING (event_id)
# MAGIC ORDER BY s.ts_silver DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Reconcile counts across layers
# MAGIC Bronze and silver stay within a few hundred rows of each other — that gap *is* the
# MAGIC in-flight backlog. Quarantine holds whatever failed parse/validate.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze_hl7_raw'        AS layer, count(*) AS rows FROM ${catalog}.${schema}.bronze_hl7_raw
# MAGIC UNION ALL SELECT 'silver_hl7_parsed',     count(*)        FROM ${catalog}.${schema}.silver_hl7_parsed
# MAGIC UNION ALL SELECT 'silver_hl7_quarantine', count(*)        FROM ${catalog}.${schema}.silver_hl7_quarantine
# MAGIC ORDER BY rows DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Live clinical view off the streaming table
# MAGIC The streaming table is queryable for real analytics, not just plumbing — e.g. a
# MAGIC live per-facility census (admits vs. discharges) over the last 5 minutes.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT facility_id,
# MAGIC        count(*)                                          AS msgs,
# MAGIC        sum(CASE WHEN message_type = 'ADT^A01' THEN 1 END) AS admits,
# MAGIC        sum(CASE WHEN message_type = 'ADT^A03' THEN 1 END) AS discharges
# MAGIC FROM ${catalog}.${schema}.silver_hl7_parsed
# MAGIC WHERE ts_silver > current_timestamp() - INTERVAL 5 MINUTES
# MAGIC GROUP BY facility_id
# MAGIC ORDER BY facility_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakebase serving layer (Postgres)
# MAGIC The serving tables the dashboard reads live in Lakebase (Postgres), not Unity
# MAGIC Catalog — query them from any Postgres client with a short-lived OAuth token:
# MAGIC
# MAGIC ```bash
# MAGIC export PGPASSWORD="$(databricks postgres generate-database-credential \
# MAGIC   projects/rti-demo/branches/production/endpoints/primary -o json | jq -r .token)"
# MAGIC psql "host=ep-steep-mountain-d2c3ahvt.database.us-east-1.cloud.databricks.com \
# MAGIC       port=5432 dbname=rti_demo user=<your-workspace-email> sslmode=require"
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC -- latest served rows, end-to-end latency, and freshness
# MAGIC SELECT event_id, source_path, message_type,
# MAGIC        round(EXTRACT(EPOCH FROM (ts_silver - ts_generated)) * 1000) AS e2e_ms,
# MAGIC        round(EXTRACT(EPOCH FROM (now()     - ts_lakebase)))         AS age_s
# MAGIC FROM rt_latest_transactions ORDER BY ts_lakebase DESC LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC See the repo `README.md` for the full set of Lakebase serving queries.
