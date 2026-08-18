"""Gold-layer product analytics with explicit metric grain.

The module keeps metric definitions reusable and makes the grain of every
aggregate explicit so downstream consumers do not accidentally double-count.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def daily_customer_kpis(events_df: DataFrame) -> DataFrame:
    """Build one row per customer/day."""
    return events_df.groupBy("event_date", "user_id").agg(
        F.countDistinct("event_id").alias("events"),
        F.countDistinct("session_id").alias("sessions"),
        F.countDistinct(F.when(F.col("event_type") == "product_viewed", F.col("product_id"))).alias("products_viewed"),
        F.sum(F.when(F.col("event_type") == "cart_added", 1).otherwise(0)).alias("cart_adds"),
        F.sum(F.when(F.col("event_type") == "checkout_started", 1).otherwise(0)).alias("checkouts"),
        F.sum(F.when(F.col("event_type") == "order_created", 1).otherwise(0)).alias("orders"),
        F.sum(F.when(F.col("event_type") == "payment_completed", 1).otherwise(0)).alias("payments"),
    )


def daily_revenue(events_df: DataFrame) -> DataFrame:
    """Build one row per event date from payment events.

    Revenue is sourced from the payment event property rather than inferred
    from event counts. Missing/invalid amounts are excluded from revenue.
    """
    return (
        events_df.filter(F.col("event_type") == "payment_completed")
        .withColumn("amount", F.col("properties.amount").cast("decimal(18,2)"))
        .filter(F.col("amount").isNotNull() & (F.col("amount") >= 0))
        .groupBy("event_date")
        .agg(
            F.sum("amount").alias("revenue"),
            F.countDistinct("event_id").alias("successful_payments"),
            F.countDistinct("user_id").alias("paying_customers"),
        )
    )


def conversion_funnel(events_df: DataFrame) -> DataFrame:
    """Build one row per event date for the canonical product funnel."""
    return events_df.groupBy("event_date").agg(
        F.countDistinct(F.when(F.col("event_type") == "product_viewed", F.col("user_id"))).alias("viewers"),
        F.countDistinct(F.when(F.col("event_type") == "cart_added", F.col("user_id"))).alias("cart_users"),
        F.countDistinct(F.when(F.col("event_type") == "checkout_started", F.col("user_id"))).alias("checkout_users"),
        F.countDistinct(F.when(F.col("event_type") == "order_created", F.col("user_id"))).alias("buyers"),
    ).withColumn(
        "view_to_cart_rate",
        F.when(F.col("viewers") > 0, F.col("cart_users") / F.col("viewers")),
    ).withColumn(
        "cart_to_checkout_rate",
        F.when(F.col("cart_users") > 0, F.col("checkout_users") / F.col("cart_users")),
    ).withColumn(
        "checkout_to_order_rate",
        F.when(F.col("checkout_users") > 0, F.col("buyers") / F.col("checkout_users")),
    )


def product_performance(events_df: DataFrame) -> DataFrame:
    """Build one row per event date/product for product analytics."""
    return events_df.filter(F.col("product_id").isNotNull()).groupBy(
        "event_date", "product_id"
    ).agg(
        F.countDistinct(F.when(F.col("event_type") == "product_viewed", F.col("event_id"))).alias("views"),
        F.countDistinct(F.when(F.col("event_type") == "cart_added", F.col("event_id"))).alias("cart_adds"),
        F.countDistinct(F.when(F.col("event_type") == "order_created", F.col("event_id"))).alias("orders"),
        F.countDistinct(F.when(F.col("event_type") == "user_login", F.col("user_id"))).alias("active_users"),
    )
