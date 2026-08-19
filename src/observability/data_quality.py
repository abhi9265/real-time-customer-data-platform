"""Reusable deterministic data-quality checks for streaming batches."""
from __future__ import annotations
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def evaluate_events(df: DataFrame) -> DataFrame:
    """Return row-level DQ status and a stable failure reason."""
    return df.withColumn(
        "dq_status",
        F.when(F.col("event_id").isNull(), "FAIL")
         .when(F.col("user_id").isNull(), "FAIL")
         .when(F.col("event_timestamp").isNull(), "FAIL")
         .when(F.col("event_type").isNull(), "FAIL")
         .otherwise("PASS"),
    ).withColumn(
        "dq_reason",
        F.when(F.col("event_id").isNull(), "MISSING_EVENT_ID")
         .when(F.col("user_id").isNull(), "MISSING_USER_ID")
         .when(F.col("event_timestamp").isNull(), "MISSING_EVENT_TIMESTAMP")
         .when(F.col("event_type").isNull(), "MISSING_EVENT_TYPE")
         .otherwise(F.lit(None).cast("string")),
    )


def quality_metrics(df: DataFrame) -> DataFrame:
    """Produce aggregate DQ metrics suitable for an audit table."""
    return df.agg(
        F.count("*").alias("records_checked"),
        F.sum(F.when(F.col("dq_status") == "FAIL", 1).otherwise(0)).alias("records_failed"),
    ).withColumn("failure_rate", F.col("records_failed") / F.col("records_checked"))
