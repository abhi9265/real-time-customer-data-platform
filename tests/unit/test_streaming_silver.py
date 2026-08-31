from datetime import UTC, datetime

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from src.silver.streaming_silver import (
    affected_customer_keys,
    deduplicate_batch,
    split_silver_and_quarantine,
)


FULL_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
])


def test_valid_events_are_normalized_and_rejected_events_quarantined(spark):
    df = spark.createDataFrame([
        ("evt-1", " USER_LOGIN ", " usr-1 ", " ses-1 ", None, datetime(2026, 8, 18, 10, 0, tzinfo=UTC)),
        (None, "user_login", "usr-2", "ses-2", None, datetime(2026, 8, 18, 10, 1, tzinfo=UTC)),
        ("evt-3", "unknown_event", "usr-3", "ses-3", None, datetime(2026, 8, 18, 10, 2, tzinfo=UTC)),
    ], FULL_SCHEMA)

    silver, quarantine = split_silver_and_quarantine(df)
    assert silver.count() == 1
    assert silver.first()["event_type"] == "user_login"
    assert silver.first()["user_id"] == "usr-1"
    assert quarantine.count() == 2


def test_duplicate_event_identity_is_removed(spark):
    df = spark.createDataFrame([
        ("evt-1", "user_login", "usr-1", "ses-1", None, datetime(2026, 8, 18, 10, 0, tzinfo=UTC)),
        ("evt-1", "user_login", "usr-1", "ses-1", None, datetime(2026, 8, 18, 10, 1, tzinfo=UTC)),
    ], FULL_SCHEMA)
    silver, _ = split_silver_and_quarantine(df)
    assert deduplicate_batch(silver).count() == 1


def test_incremental_customer_keys_are_distinct(spark):
    df = spark.createDataFrame([("usr-1",), ("usr-1",), ("usr-2",)], ["user_id"])
    assert {row.user_id for row in affected_customer_keys(df).collect()} == {"usr-1", "usr-2"}
