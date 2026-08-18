"""Kafka -> Bronze Delta Structured Streaming pipeline.

The pipeline keeps raw Kafka payloads for replay while exposing a validated
Bronze event projection for downstream processing.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.streaming.event_schema import EVENT_SCHEMA


def read_kafka_stream(spark: SparkSession, bootstrap_servers: str, topic: str) -> DataFrame:
    """Create the Kafka streaming source; offsets are persisted by the query checkpoint."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_classify_events(kafka_df: DataFrame, allowed_lateness: str = "15 minutes") -> tuple[DataFrame, DataFrame]:
    """Parse JSON events and split valid events from malformed/quarantined records."""
    parsed = (
        kafka_df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_payload"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("event", F.from_json("raw_payload", EVENT_SCHEMA))
        .withColumn("event_id", F.col("event.event_id"))
        .withColumn("event_type", F.col("event.event_type"))
        .withColumn("event_version", F.col("event.event_version"))
        .withColumn("event_timestamp", F.to_timestamp("event.event_timestamp"))
        .withColumn("user_id", F.col("event.user_id"))
        .withColumn("product_id", F.col("event.product_id"))
        .withColumn("session_id", F.col("event.session_id"))
        .withColumn("properties", F.col("event.properties"))
        .withColumn("ingested_at", F.current_timestamp())
    )

    classified = parsed.withColumn(
        "quarantine_reason",
        F.when(F.col("event").isNull(), F.lit("INVALID_JSON_OR_SCHEMA"))
        .when(F.col("event_id").isNull(), F.lit("MISSING_EVENT_ID"))
        .when(F.col("event_timestamp").isNull(), F.lit("INVALID_EVENT_TIMESTAMP"))
        .when(F.col("user_id").isNull(), F.lit("MISSING_USER_ID"))
        .otherwise(F.lit(None).cast("string")),
    )

    valid = (
        classified.filter(F.col("quarantine_reason").isNull())
        .withWatermark("event_timestamp", allowed_lateness)
        .dropDuplicates(["event_id"])
        .select(
            "event_id", "event_type", "event_version", "event_timestamp",
            "user_id", "product_id", "session_id", "properties",
            "topic", "partition", "offset", "kafka_timestamp", "ingested_at",
        )
    )

    quarantine = classified.filter(F.col("quarantine_reason").isNotNull()).select(
        "raw_payload", "quarantine_reason", "topic", "partition", "offset",
        "kafka_timestamp", "ingested_at",
    )
    return valid, quarantine


def start_bronze_queries(
    valid_df: DataFrame,
    quarantine_df: DataFrame,
    bronze_path: str,
    quarantine_path: str,
    checkpoint_root: str,
):
    """Start independently checkpointed Bronze and quarantine sinks."""
    valid_query = (
        valid_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_root}/bronze")
        .trigger(processingTime="30 seconds")
        .start(bronze_path)
    )
    quarantine_query = (
        quarantine_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_root}/quarantine")
        .trigger(processingTime="30 seconds")
        .start(quarantine_path)
    )
    return valid_query, quarantine_query
