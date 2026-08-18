"""Incremental customer-state derivation from Silver events."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_customer_state(events_df: DataFrame) -> DataFrame:
    """Build the latest event and activity counters per affected customer."""
    return events_df.groupBy("user_id").agg(
        F.max("event_timestamp").alias("last_event_timestamp"),
        F.count("*").alias("event_count"),
        F.countDistinct("session_id").alias("session_count"),
        F.countDistinct("product_id").alias("products_touched"),
        F.sum(F.when(F.col("event_type") == "order_created", 1).otherwise(0)).alias("order_count"),
        F.sum(F.when(F.col("event_type") == "payment_completed", 1).otherwise(0)).alias("payment_count"),
    ).withColumn("updated_at", F.current_timestamp())


def customer_state_merge_sql(catalog: str, schema: str) -> str:
    """Return an idempotent upsert contract for customer state."""
    return f"""
MERGE INTO {catalog}.{schema}.customer_state AS target
USING customer_state_updates AS source
ON target.user_id = source.user_id
WHEN MATCHED AND source.updated_at >= target.updated_at THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
"""
