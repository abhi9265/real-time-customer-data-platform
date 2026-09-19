"""Verify Spark Kafka checkpoint/restart does not replay committed offsets.

This is a local integration test for the operational behavior that matters when
a streaming job is restarted: records already committed through the checkpoint
must not be written a second time.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "customer-events")
PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"


def _start_query(spark: SparkSession, checkpoint: str, output: str):
    events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
        .select(
            F.col("offset"),
            F.col("partition"),
            F.col("value").cast("string").alias("payload"),
        )
    )
    return (
        events.writeStream.format("parquet")
        .option("path", output)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .start()
    )


def _wait_for_rows(spark: SparkSession, output: str, expected: int) -> int:
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            observed = spark.read.parquet(output).count()
        except Exception:
            observed = 0
        if observed >= expected:
            return observed
        time.sleep(1)
    return observed


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="kafka-checkpoint-smoke-"))
    checkpoint = str(root / "checkpoint")
    output = str(root / "output")

    spark = (
        SparkSession.builder.appName("rtdp-kafka-checkpoint-restart")
        .master("local[2]")
        .config("spark.jars.packages", PACKAGE)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        first = _start_query(spark, checkpoint, output)
        first.processAllAvailable()
        observed_first = _wait_for_rows(spark, output, expected=5)
        first.stop()

        if observed_first != 5:
            raise AssertionError(
                f"Initial Kafka -> Spark run observed {observed_first}/5 events"
            )

        # Restart with the same checkpoint and no new Kafka records. Spark must
        # resume from the committed offsets rather than append duplicates.
        second = _start_query(spark, checkpoint, output)
        second.processAllAvailable()
        time.sleep(2)
        observed_after_restart = spark.read.parquet(output).count()
        second.stop()

        if observed_after_restart != 5:
            raise AssertionError(
                "Checkpoint restart changed the persisted event count: "
                f"expected 5, got {observed_after_restart}"
            )

        print(
            "Kafka -> Spark checkpoint/restart verified: "
            f"{observed_after_restart} committed events remained exactly-once at the sink"
        )
    finally:
        spark.stop()
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
