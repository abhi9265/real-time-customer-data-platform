"""Canonical Spark schema for customer product events."""

from pyspark.sql.types import MapType, StringType, StructField, StructType

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_version", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("properties", MapType(StringType(), StringType()), True),
])
