"""Kafka -> Bronze Structured Streaming entrypoint.

The job is intentionally small and composable: connection/configuration is
kept at the edge while parsing, quality and persistence remain testable.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_version", StringType(), False),
    StructField("event_timestamp", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("trace_id", StringType(), True),
    StructField("properties", StringType(), True),
])


def parse_kafka_events(kafka_df: DataFrame) -> DataFrame:
    """Parse contract-shaped Kafka values and retain Kafka metadata."""
    parsed = (
        kafka_df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("topic"), F.col("partition"), F.col("offset"), F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
        )
        .select("kafka_key", "topic", "partition", "offset", "kafka_timestamp", "event.*")
    )
    return parsed.withColumn("ingested_at", F.current_timestamp())


def prepare_bronze(kafka_df: DataFrame) -> DataFrame:
    """Apply event-time parsing and a replay-safe ingestion timestamp."""
    return parse_kafka_events(kafka_df).withColumn(
        "event_timestamp", F.to_timestamp("event_timestamp")
    )


def build_bronze_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "latest",
) -> DataFrame:
    """Create the streaming DataFrame; no side effects occur until writeStream."""
    return spark.readStream.format("kafka").option(
        "kafka.bootstrap.servers", bootstrap_servers
    ).option("subscribe", topic).option("startingOffsets", starting_offsets).option(
        "failOnDataLoss", "false"
    ).load().transform(prepare_bronze)


def write_bronze(
    events: DataFrame,
    output_path: str,
    checkpoint_path: str,
    trigger_once: bool = False,
):
    """Persist an append-only Bronze Delta stream with checkpointing."""
    writer = (
        events.withWatermark("event_timestamp", "15 minutes")
        .dropDuplicates(["event_id"])
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
    )
    return (writer.trigger(availableNow=True) if trigger_once else writer.trigger(processingTime="30 seconds")).start()
