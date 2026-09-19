from datetime import datetime

import pytest
from pyspark.sql import functions as F

from src.silver.streaming_reliability import (
    apply_event_time_watermark,
    deduplicate_batch_deterministic,
)


def test_deduplicate_batch_keeps_latest_event_observation(spark):
    rows = [
        ("e1", "u1", datetime(2026, 1, 1, 10, 0), datetime(2026, 1, 1, 10, 1), "VALID"),
        ("e1", "u1", datetime(2026, 1, 1, 10, 0), datetime(2026, 1, 1, 10, 2), "VALID"),
        ("e2", "u2", datetime(2026, 1, 1, 10, 5), datetime(2026, 1, 1, 10, 6), "REJECTED"),
    ]
    df = spark.createDataFrame(
        rows,
        "event_id string, user_id string, event_timestamp timestamp, "
        "processed_at timestamp, quality_status string",
    )

    result = deduplicate_batch_deterministic(df)
    values = result.select("event_id", "processed_at").orderBy("event_id").collect()

    assert [(r.event_id, r.processed_at) for r in values] == [
        ("e1", datetime(2026, 1, 1, 10, 2))
    ]


def test_watermark_requires_event_timestamp(spark):
    df = spark.createDataFrame([(1,)], ["event_id"])
    with pytest.raises(ValueError, match="event_timestamp"):
        apply_event_time_watermark(df)


def test_watermark_is_attached_to_event_time(spark):
    df = spark.createDataFrame(
        [(1, datetime(2026, 1, 1, 10, 0))],
        "event_id int, event_timestamp timestamp",
    )
    watermarked = apply_event_time_watermark(df, "15 minutes")
    assert "event_timestamp" in watermarked.columns
