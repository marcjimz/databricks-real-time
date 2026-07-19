"""Path B front door — Event Hubs (Kafka) → bronze_hl7_raw (spec section 5.1).

Deliberately THIN: it deserialises the JSON envelope the app produces to the
Event Hubs Kafka endpoint, stamps the two Path-B-only timestamps, and appends to
the SAME ``bronze_hl7_raw`` table Zerobus writes — so everything downstream (the
serverless DLT medallion → silver → Lakebase) is byte-for-byte identical between
the two paths. No parsing or validation here; that stays in the shared medallion,
which keeps the paths directly comparable ("swap the front door, keep the house").

Two Path-B timestamps, both stamped HERE (Zerobus stamps its own on the producer
side; Event Hubs records are stamped on the consumer/ingest side):
  * ``ts_transport`` — the Kafka record timestamp (when Event Hubs accepted it):
    this is the broker hop the stage rail shows only on Path B.
  * ``ts_bronze``    — ``current_timestamp()`` at the append: the land time.

The broker hop (``broker_ms`` = ts_transport − ts_generated) is NOT computed
here: ``ts_transport`` rides through to silver, and the medallion's Lakebase sink
already writes ``rt_stage_metrics`` — so the broker breakdown is derived there
(one serving writer, not two). This job stays a pure Kafka → bronze landing step.

Runs as a classic Structured Streaming job (Kafka source is not supported on
serverless; and RTM cannot write Delta — see the RTM express-lane memo). No
trigger = back-to-back micro-batches; checkpoint in the UC Volume.
"""

from __future__ import annotations

import os
import sys

# Bundle root importable on the DRIVER (same shim as bronze_to_silver: the
# spark_python_task wrapper exec()s this with no __file__, and we don't set
# PYTHONPATH via spark_env_vars). Recover from BUNDLE_ROOT, else the frame path.
_here = sys._getframe().f_code.co_filename
for _cand in (
    os.environ.get("BUNDLE_ROOT", ""),
    os.path.dirname(os.path.dirname(_here)),
):
    if _cand and _cand not in sys.path:
        sys.path.insert(0, _cand)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

CATALOG = os.environ.get("HL7_CATALOG", "real_time_mode_demo_catalog")
SCHEMA = os.environ.get("HL7_SCHEMA", "rti_demo")
BRONZE = f"{CATALOG}.{SCHEMA}.bronze_hl7_raw"
CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/eventhub_to_bronze"

# Event Hubs Kafka endpoint (host:9093) + the SASL password (the EH connection
# string). Supplied as job env from a Databricks secret scope — NEVER inline.
EH_BOOTSTRAP = os.environ.get("EVENTHUB_BOOTSTRAP", "")
EH_TOPIC = os.environ.get("EVENTHUB_TOPIC", "hl7-events")
EH_CONNECTION_STRING = os.environ.get("EVENTHUB_CONNECTION_STRING", "")

# The JSON envelope the app's KafkaSink produces (see app/generator/sinks.py
# _ENVELOPE_COLS). ts_generated arrives as an ISO string.
_ENVELOPE_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("source_path", StringType()),
    StructField("facility_id", StringType()),
    StructField("message_type", StringType()),
    StructField("hl7_raw", StringType()),
    StructField("gen_worker_id", StringType()),
    StructField("ts_generated", StringType()),
])

def _eh_kafka_options() -> dict:
    """SASL_SSL/PLAIN options for the Event Hubs Kafka endpoint.

    Username is the literal ``$ConnectionString``; password is the EH connection
    string (the same auth the confluent-kafka producer uses on the app side)."""
    jaas = (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        'required username="$ConnectionString" '
        f'password="{EH_CONNECTION_STRING}";'
    )
    return {
        "kafka.bootstrap.servers": EH_BOOTSTRAP,
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": jaas,
        "subscribe": EH_TOPIC,
        # Only messages produced after the stream starts — the demo is live, and
        # this keeps Path B comparable to Zerobus's startingVersion=latest.
        "startingOffsets": "latest",
        "failOnDataLoss": "false",
    }


def _foreach_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Append the micro-batch to the shared bronze table. Thin by design —
    parse/validate/serve all happen in the downstream medallion."""
    (batch_df
     .select("event_id", "source_path", "facility_id", "message_type",
             "hl7_raw", "gen_worker_id",
             "ts_generated", "ts_transport", "ts_bronze")
     .write.format("delta").mode("append").saveAsTable(BRONZE))


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    if not EH_BOOTSTRAP or not EH_CONNECTION_STRING:
        raise SystemExit(
            "EVENTHUB_BOOTSTRAP / EVENTHUB_CONNECTION_STRING not set — cannot start "
            "eventhub_to_bronze (see resources/eventhub_job.yml secrets wiring)."
        )

    raw = (spark.readStream.format("kafka")
           .options(**_eh_kafka_options())
           .load())

    # value = the JSON envelope; kafka 'timestamp' = the broker accept time.
    parsed = (raw
              .select(F.col("value").cast("string").alias("json"),
                      F.col("timestamp").alias("kafka_ts"))
              .select(F.from_json("json", _ENVELOPE_SCHEMA).alias("e"), "kafka_ts")
              .select(
                  F.col("e.event_id").alias("event_id"),
                  # source_path is always 'eventhub' on this path (trust the ingest
                  # side, not the producer field, so the tag can't be spoofed).
                  F.lit("eventhub").alias("source_path"),
                  F.col("e.facility_id").alias("facility_id"),
                  F.col("e.message_type").alias("message_type"),
                  F.col("e.hl7_raw").alias("hl7_raw"),
                  F.col("e.gen_worker_id").alias("gen_worker_id"),
                  F.to_timestamp("e.ts_generated").alias("ts_generated"),
                  # ts_transport = Kafka record ts (the broker hop, Path B only)
                  F.col("kafka_ts").alias("ts_transport"),
                  # ts_bronze = land time, stamped here at ingest
                  F.current_timestamp().alias("ts_bronze"),
              ))

    (parsed.writeStream
     .foreachBatch(_foreach_batch)
     .option("checkpointLocation", CHECKPOINT)
     .outputMode("append")
     .start()
     .awaitTermination())


if __name__ == "__main__":
    main()
