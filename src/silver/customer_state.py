"""Incremental customer state aggregation contracts."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_customer_state(events_df: DataFrame) -> DataFrame:
    """Aggregate event facts into one incremental state row per customer."""
    return events_df.groupBy("user_id").agg(
        F.max("event_timestamp").alias("last_event_at"),
        F.count("event_id").alias("event_count"),
        F.countDistinct("session_id").alias("session_count"),
        F.sum(F.when(F.col("event_type") == "order_created", 1).otherwise(0)).alias("order_count"),
        F.sum(F.when(F.col("event_type") == "payment_completed", 1).otherwise(0)).alias("payment_count"),
    )


def customer_state_merge_sql(catalog: str, schema: str) -> str:
    """Return the Delta MERGE contract for incremental customer state."""
    return f"""
MERGE INTO {catalog}.{schema}.customer_state AS target
USING customer_state_updates AS source
ON target.user_id = source.user_id
WHEN MATCHED AND source.last_event_at > target.last_event_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""
