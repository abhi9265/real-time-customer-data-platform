"""Incremental Silver transformations for customer events."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_EVENT_TYPES = {
    "user_registered", "user_login", "product_viewed", "cart_added",
    "checkout_started", "order_created", "payment_completed",
}


def _trim_if_present(df: DataFrame, column: str) -> DataFrame:
    """Normalize an optional string column without assuming it exists."""
    if column not in df.columns:
        return df
    return df.withColumn(column, F.trim(F.col(column)))


def normalize_events(bronze_df: DataFrame) -> DataFrame:
    """Normalize event fields and derive stable analytics attributes."""
    df = bronze_df.withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
    for column in ("user_id", "session_id", "product_id"):
        df = _trim_if_present(df, column)
    if "event_timestamp" in df.columns:
        df = df.withColumn("event_date", F.to_date("event_timestamp")) \
               .withColumn("event_hour", F.hour("event_timestamp"))
    return df.withColumn("processed_at", F.current_timestamp())


def classify_quality(events_df: DataFrame) -> DataFrame:
    """Assign deterministic quality status and one actionable reason."""
    timestamp_invalid = (
        F.col("event_timestamp").isNull()
        if "event_timestamp" in events_df.columns else F.lit(False)
    )
    return events_df.withColumn(
        "quality_status",
        F.when(F.col("event_id").isNull(), "REJECTED")
        .when(F.col("user_id").isNull(), "REJECTED")
        .when(timestamp_invalid, "REJECTED")
        .when(~F.col("event_type").isin(*sorted(REQUIRED_EVENT_TYPES)), "REJECTED")
        .otherwise("VALID"),
    ).withColumn(
        "quality_reason",
        F.when(F.col("event_id").isNull(), "MISSING_EVENT_ID")
        .when(F.col("user_id").isNull(), "MISSING_USER_ID")
        .when(timestamp_invalid, "INVALID_EVENT_TIMESTAMP")
        .when(~F.col("event_type").isin(*sorted(REQUIRED_EVENT_TYPES)), "UNKNOWN_EVENT_TYPE")
        .otherwise(F.lit(None).cast("string")),
    )


def deduplicate_batch(events_df: DataFrame) -> DataFrame:
    """Keep one trusted observation for each event identity."""
    return events_df.filter(F.col("quality_status") == "VALID").dropDuplicates(["event_id"])


def split_silver_and_quarantine(events_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separate trusted Silver events from records requiring remediation."""
    classified = classify_quality(normalize_events(events_df))
    return classified.filter("quality_status = 'VALID'"), classified.filter("quality_status = 'REJECTED'")


def affected_customer_keys(events_df: DataFrame) -> DataFrame:
    """Return distinct customer keys touched by an incremental batch."""
    return events_df.select("user_id").where(F.col("user_id").isNotNull()).dropDuplicates()


def silver_merge_sql(catalog: str, schema: str) -> str:
    """Return an idempotent Delta MERGE contract for Silver events."""
    return f"""
MERGE INTO {catalog}.{schema}.customer_events AS target
USING silver_events_updates AS source
ON target.event_id = source.event_id
WHEN MATCHED AND source.processed_at > target.processed_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""
