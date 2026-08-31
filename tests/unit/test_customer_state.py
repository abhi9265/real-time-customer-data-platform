from datetime import UTC, datetime

from src.silver.customer_state import build_customer_state


def test_customer_state_aggregates_incremental_events(spark):
    df = spark.createDataFrame([
        ("usr-1", "evt-1", "ses-1", "user_login", datetime(2026, 8, 18, 10, 0, tzinfo=UTC)),
        ("usr-1", "evt-2", "ses-2", "order_created", datetime(2026, 8, 18, 10, 5, tzinfo=UTC)),
        ("usr-1", "evt-3", "ses-2", "payment_completed", datetime(2026, 8, 18, 10, 6, tzinfo=UTC)),
        ("usr-2", "evt-4", "ses-3", "user_login", datetime(2026, 8, 18, 11, 0, tzinfo=UTC)),
    ], ["user_id", "event_id", "session_id", "event_type", "event_timestamp"])

    result = {row.user_id: row for row in build_customer_state(df).collect()}

    assert result["usr-1"].event_count == 3
    assert result["usr-1"].session_count == 2
    assert result["usr-1"].order_count == 1
    assert result["usr-1"].payment_count == 1
