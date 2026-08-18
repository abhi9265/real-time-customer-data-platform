# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka → Bronze Streaming
# MAGIC
# MAGIC Production-shaped ingestion with event-time watermarks, duplicate suppression,
# MAGIC malformed-event quarantine, and durable checkpoints.

# COMMAND ----------

from src.streaming.bronze_stream import read_kafka_stream, parse_and_classify_events, start_bronze_queries

BOOTSTRAP_SERVERS = "${KAFKA_BOOTSTRAP_SERVERS}"
TOPIC = "customer-events"
BRONZE_PATH = "${BRONZE_PATH}/customer_events"
QUARANTINE_PATH = "${BRONZE_PATH}/customer_events_quarantine"
CHECKPOINT_ROOT = "${CHECKPOINT_ROOT}/customer_events"

# COMMAND ----------

kafka_df = read_kafka_stream(spark, BOOTSTRAP_SERVERS, TOPIC)
valid_df, quarantine_df = parse_and_classify_events(kafka_df, allowed_lateness="15 minutes")

# COMMAND ----------

valid_query, quarantine_query = start_bronze_queries(
    valid_df, quarantine_df, BRONZE_PATH, QUARANTINE_PATH, CHECKPOINT_ROOT
)

print(f"Bronze query: {valid_query.id}")
print(f"Quarantine query: {quarantine_query.id}")
