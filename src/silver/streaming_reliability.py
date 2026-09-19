"""Reliability helpers for deterministic streaming behavior."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def deduplicate_batch_deterministic(events_df: DataFrame) -> DataFrame:
    """Keep the newest observation for each event_id deterministically."""
    # Timestamp ties are possible when events are retried or arrive with the
    # same source timestamps. Add a stable row fingerprint so row_number does
    # not depend on Spark execution order for those ties.
    tie_breaker = F.xxhash64(*[F.col(column) for column in sorted(events_df.columns)])
    window = Window.partitionBy("event_id").orderBy(
        F.col("event_timestamp").desc_nulls_last(),
        F.col("processed_at").desc_nulls_last(),
        tie_breaker.desc(),
    )
    return (
        events_df.filter(F.col("quality_status") == "VALID")
        .withColumn("_event_rank", F.row_number().over(window))
        .filter(F.col("_event_rank") == 1)
        .drop("_event_rank")
    )


def apply_event_time_watermark(
    events_df: DataFrame,
    delay: str = "10 minutes",
) -> DataFrame:
    """Attach the event-time watermark contract used by streaming queries."""
    if "event_timestamp" not in events_df.columns:
        raise ValueError("event_timestamp is required for watermarking")
    return events_df.withWatermark("event_timestamp", delay)


def streaming_batch_metrics(
    events_df: DataFrame,
    batch_id: int,
) -> dict[str, int]:
    """Return stable per-batch counters for operational observability."""
    total = events_df.count()
    rejected = events_df.filter(F.col("quality_status") == "REJECTED").count()
    valid = events_df.filter(F.col("quality_status") == "VALID").count()
    duplicate_rows = (\n        events_df.groupBy("event_id")\n        .count()\n        .filter(F.col("count") > 1)\n        .select(F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("duplicates"))\n        .collect()[0]["duplicates"]\n    )\n    return {\n        "batch_id": batch_id,\n        "events_received": total,\n        "events_valid": valid,\n        "events_rejected": rejected,\n        "events_duplicate": int(duplicate_rows or 0),\n    }
