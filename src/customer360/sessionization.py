"""Sessionization utilities for product-event streams."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def assign_sessions(events_df: DataFrame, inactivity_minutes: int = 30) -> DataFrame:
    """Assign a deterministic session number per customer using inactivity gaps."""
    window = Window.partitionBy("user_id").orderBy("event_timestamp", "event_id")
    with_previous = events_df.withColumn("previous_event_at", F.lag("event_timestamp").over(window))
    with_gap = with_previous.withColumn(
        "new_session",
        F.when(F.col("previous_event_at").isNull(), 1)
        .when(
            (F.col("event_timestamp").cast("long") - F.col("previous_event_at").cast("long"))
            > inactivity_minutes * 60,
            1,
        )
        .otherwise(0),
    )
    session_window = window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return with_gap.withColumn(
        "session_number", F.sum("new_session").over(session_window)
    ).drop("new_session")


def build_session_facts(events_df: DataFrame) -> DataFrame:
    """Produce one row per customer session."""
    return events_df.groupBy("user_id", "session_number").agg(
        F.min("event_timestamp").alias("session_start_at"),
        F.max("event_timestamp").alias("session_end_at"),
        F.count("event_id").alias("event_count"),
        F.countDistinct("product_id").alias("products_viewed"),
        F.sum(F.when(F.col("event_type") == "cart_added", 1).otherwise(0)).alias("cart_adds"),
        F.sum(F.when(F.col("event_type") == "order_created", 1).otherwise(0)).alias("orders"),
    ).withColumn(
        "session_duration_seconds",
        F.col("session_end_at").cast("long") - F.col("session_start_at").cast("long"),
    )
