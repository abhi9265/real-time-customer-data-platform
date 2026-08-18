"""Canonical Spark schema for customer product events."""

from pyspark.sql.types import (
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType,
)


EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_version", StringType(), False),
        StructField("event_timestamp", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("product_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("properties", MapType(StringType(), StringType()), True),
    ]
)
