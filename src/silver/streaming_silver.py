"""Incremental Silver transformations for customer events.

Silver is responsible for conformance, deterministic deduplication,
quality classification, and audit metadata. The functions are expressed as
DataFrame transformations so they can be reused by streaming and backfill
jobs.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_EVENT_TYPES = {
    "user_registered",
    "user_login",
    "product_viewed",
    "cart_added",
    "checkout_started",
    "order_created",
    "payment_completed",
}


def normalize_events(bronze_df: DataFrame) -> DataFrame:
    """Normalize event fields and derive stable analytics attributes."""
    return (
        bronze_df
        .withColumn("event_type", F.lower(F.trim("event_type")))
        .withColumn("user_id", F.trim("user_id"))
        .withColumn("session_id", F.trim("session_id"))
        .withColumn("product_id", F.trim("product_id"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_hour", F.hour("event_timestamp"))
        .withColumn("processed_at", F.current_timestamp())
    )


def classify_quality(events_df: DataFrame) -> DataFrame:
    """Assign deterministic quality status and a single actionable reason."""
    return events_df.withColumn(
        "quality_status",
        F.when(F.col("event_id").isNull(), "REJECTED")
        .when(F.col("user_id").isNull(), "REJECTED")
        .when(F.col("event_timestamp").isNull(), "REJECTED")
        .when(~F.col("event_type").isin(*sorted(REQUIRED_EVENT_TYPES)), "REJECTED")
        .otherwise("VALID"),
    ).withColumn(
        "quality_reason",
        F.when(F.col("event_id").isNull(), "MISSING_EVENT_ID")
        .when(F.col("user_id").isNull(), "MISSING_USER_ID")
        .when(F.col("event_timestamp").isNull(), "INVALID_EVENT_TIMESTAMP")
        .when(~F.col("event_type").isin(*sorted(REQUIRED_EVENT_TYPES)), "UNKNOWN_EVENT_TYPE")
        .otherwise(F.lit(None).cast("string")),
    )


def deduplicate_batch(events_df: DataFrame) -> DataFrame:
    """Keep the latest observation for each event identity in a batch."""
    return (
        events_df
        .filter(F.col("quality_status") == "VALID")
        .dropDuplicates(["event_id"])
    )


def split_silver_and_quarantine(events_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separate trusted Silver events from records requiring remediation."""
    classified = classify_quality(normalize_events(events_df))
    silver = classified.filter("quality_status = 'VALID'")
    quarantine = classified.filter("quality_status = 'REJECTED'")
    return silver, quarantine


def affected_customer_keys(events_df: DataFrame) -> DataFrame:
    """Return the customer keys touched by an incremental batch."""
    return events_df.select("user_id").where(F.col("user_id").isNotNull()).dropDuplicates()


def silver_merge_sql(catalog: str, schema: str) -> str:
    """Return an idempotent Delta MERGE for the Silver event table."""
    return f"""
MERGE INTO {catalog}.{schema}.customer_events AS target
USING silver_events_updates AS source
ON target.event_id = source.event_id
WHEN MATCHED AND source.processed_at > target.processed_at THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
"""
