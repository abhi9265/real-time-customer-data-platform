from datetime import UTC, datetime

from src.customer360.scd2_customer import detect_customer_changes
from src.customer360.sessionization import assign_sessions, build_session_facts


def test_sessionization_splits_after_inactivity_window(spark):
    df = spark.createDataFrame([
        ("usr-1", "evt-1", datetime(2026, 8, 18, 10, 0, tzinfo=UTC), "product_viewed", "prd-1"),
        ("usr-1", "evt-2", datetime(2026, 8, 18, 10, 10, tzinfo=UTC), "cart_added", "prd-1"),
        ("usr-1", "evt-3", datetime(2026, 8, 18, 11, 0, tzinfo=UTC), "product_viewed", "prd-2"),
    ], ["user_id", "event_id", "event_timestamp", "event_type", "product_id"])

    result = assign_sessions(df, inactivity_minutes=30)
    rows = result.orderBy("event_timestamp").select("session_number").collect()

    assert [row.session_number for row in rows] == [1, 1, 2]


def test_session_facts_capture_business_metrics(spark):
    df = spark.createDataFrame([
        ("usr-1", 1, "evt-1", datetime(2026, 8, 18, 10, 0, tzinfo=UTC), "product_viewed", "prd-1"),
        ("usr-1", 1, "evt-2", datetime(2026, 8, 18, 10, 5, tzinfo=UTC), "order_created", "prd-1"),
    ], ["user_id", "session_number", "event_id", "event_timestamp", "event_type", "product_id"])

    row = build_session_facts(df).first()
    assert row.event_count == 2
    assert row.products_viewed == 1
    assert row.orders == 1
    assert row.session_duration_seconds == 300


def test_scd2_change_detection_returns_new_and_changed_customers(spark):
    current = spark.createDataFrame([
        ("cust-1", "old@example.com", "IN", True),
        ("cust-2", "same@example.com", "IN", True),
    ], ["customer_id", "email", "country", "is_current"])
    incoming = spark.createDataFrame([
        ("cust-1", "new@example.com", "IN"),
        ("cust-2", "same@example.com", "IN"),
        ("cust-3", "new@example.com", "US"),
    ], ["customer_id", "email", "country"])

    result = {row.customer_id for row in detect_customer_changes(current, incoming).collect()}
    assert result == {"cust-1", "cust-3"}
