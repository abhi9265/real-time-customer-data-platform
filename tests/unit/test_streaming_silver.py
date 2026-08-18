from src.silver.streaming_silver import (
    affected_customer_keys,
    deduplicate_batch,
    split_silver_and_quarantine,
)


def test_valid_events_are_normalized_and_rejected_events_quarantined(spark):
    df = spark.createDataFrame(
        [
            ("evt-1", " USER_LOGIN ", " usr-1 ", " ses-1 ", None, "2026-08-18 10:00:00"),
            (None, "user_login", "usr-2", "ses-2", None, "2026-08-18 10:01:00"),
            ("evt-3", "unknown_event", "usr-3", "ses-3", None, "2026-08-18 10:02:00"),
        ],
        ["event_id", "event_type", "user_id", "session_id", "product_id", "event_timestamp"],
    ).withColumn("event_timestamp", __import__("pyspark").sql.functions.to_timestamp("event_timestamp"))

    silver, quarantine = split_silver_and_quarantine(df)

    assert silver.count() == 1
    assert silver.first()["event_type"] == "user_login"
    assert silver.first()["user_id"] == "usr-1"
    assert quarantine.count() == 2


def test_duplicate_event_identity_is_removed(spark):
    df = spark.createDataFrame(
        [("evt-1", "user_login", "usr-1"), ("evt-1", "user_login", "usr-1")],
        ["event_id", "event_type", "user_id"],
    )

    silver, _ = split_silver_and_quarantine(df)
    result = deduplicate_batch(silver)

    assert result.count() == 1


def test_incremental_customer_keys_are_distinct(spark):
    df = spark.createDataFrame(
        [("usr-1",), ("usr-1",), ("usr-2",)], ["user_id"]
    )

    result = affected_customer_keys(df)

    assert {row.user_id for row in result.collect()} == {"usr-1", "usr-2"}
