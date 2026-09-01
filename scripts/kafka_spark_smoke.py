"""Verify Kafka -> Spark Structured Streaming end-to-end."""

import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "customer-events")
PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"


def main() -> None:
    spark = (
        SparkSession.builder.appName("rtdp-kafka-smoke")
        .master("local[2]")
        .config("spark.jars.packages", PACKAGE)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
        .select(F.col("value").cast("string").alias("payload"))
    )

    query = (
        events.writeStream.format("memory")
        .queryName("kafka_smoke_events")
        .outputMode("append")
        .start()
    )

    deadline = time.time() + 60
    observed = 0
    while time.time() < deadline:
        query.processAllAvailable()
        observed = spark.sql("SELECT COUNT(*) AS n FROM kafka_smoke_events").first()["n"]
        if observed >= 5:
            break
        time.sleep(1)

    query.stop()
    spark.stop()

    if observed < 5:
        raise AssertionError(f"Kafka -> Spark smoke test observed {observed}/5 events")

    print(f"Kafka -> Spark Structured Streaming verified: {observed} events observed")


if __name__ == "__main__":
    main()
