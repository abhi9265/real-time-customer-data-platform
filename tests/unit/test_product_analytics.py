from datetime import date

from src.gold.product_analytics import (
    conversion_funnel,
    daily_customer_kpis,
    product_performance,
)


def test_daily_customer_kpis_preserve_customer_day_grain(spark):
    df = spark.createDataFrame([
        (date(2026, 8, 18), "u1", "e1", "s1", "product_viewed", "p1"),
        (date(2026, 8, 18), "u1", "e2", "s1", "cart_added", "p1"),
        (date(2026, 8, 18), "u1", "e3", "s1", "order_created", "p1"),
    ], ["event_date", "user_id", "event_id", "session_id", "event_type", "product_id"])

    row = daily_customer_kpis(df).first()
    assert row.events == 3
    assert row.sessions == 1
    assert row.products_viewed == 1
    assert row.cart_adds == 1
    assert row.orders == 1


def test_funnel_counts_users_once_per_stage(spark):
    df = spark.createDataFrame([
        (date(2026, 8, 18), "u1", "e1", "product_viewed"),
        (date(2026, 8, 18), "u1", "e2", "product_viewed"),
        (date(2026, 8, 18), "u1", "e3", "cart_added"),
        (date(2026, 8, 18), "u2", "e4", "product_viewed"),
    ], ["event_date", "user_id", "event_id", "event_type"])

    row = conversion_funnel(df).first()
    assert row.viewers == 2
    assert row.cart_users == 1
    assert row.view_to_cart_rate == 0.5


def test_product_performance_uses_event_grain(spark):
    df = spark.createDataFrame([
        (date(2026, 8, 18), "p1", "e1", "u1", "product_viewed"),
        (date(2026, 8, 18), "p1", "e2", "u1", "product_viewed"),
        (date(2026, 8, 18), "p1", "e3", "u1", "order_created"),
    ], ["event_date", "product_id", "event_id", "user_id", "event_type"])

    row = product_performance(df).first()
    assert row.views == 2
    assert row.orders == 1
