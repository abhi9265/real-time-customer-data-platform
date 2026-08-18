"""SCD Type 2 customer dimension helpers for CDC-driven updates."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def detect_customer_changes(current_df: DataFrame, incoming_df: DataFrame) -> DataFrame:
    """Return new or materially changed customer records."""
    current = current_df.filter(F.col("is_current") == True).select(
        "customer_id", F.col("email").alias("current_email"), F.col("country").alias("current_country")
    )
    return (
        incoming_df.alias("incoming")
        .join(current.alias("current"), "customer_id", "left")
        .where(
            F.col("current.customer_id").isNull()
            | ~F.col("incoming.email").eqNullSafe(F.col("current_email"))
            | ~F.col("incoming.country").eqNullSafe(F.col("current_country"))
        )
        .select("incoming.*")
    )


def scd2_merge_sql(catalog: str, schema: str) -> str:
    """Return the expire-and-insert contract for the customer dimension."""
    table = f"{catalog}.{schema}.dim_customer"
    return f"""
-- Expire the current version when tracked customer attributes change.
MERGE INTO {table} AS target
USING customer_changes AS source
ON target.customer_id = source.customer_id
AND target.is_current = true
WHEN MATCHED AND (
    NOT target.email <=> source.email
    OR NOT target.country <=> source.country
) THEN UPDATE SET
    target.effective_to = current_timestamp(),
    target.is_current = false;

-- Insert the new current version. Run in the same orchestration transaction.
INSERT INTO {table}
(customer_sk, customer_id, email, country, effective_from, effective_to, is_current)
SELECT
    xxhash64(customer_id, current_timestamp()),
    customer_id,
    email,
    country,
    current_timestamp(),
    TIMESTAMP '9999-12-31 23:59:59',
    true
FROM customer_changes;
"""
