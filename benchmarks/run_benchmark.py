"""Run a reproducible local Spark benchmark for customer-event transformations."""
from __future__ import annotations

import csv
import json
import os
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from src.silver.streaming_silver import affected_customer_keys, deduplicate_batch, split_silver_and_quarantine


EVENTS = int(os.getenv("BENCHMARK_EVENTS", "100000"))
OUT = os.getenv("BENCHMARK_OUT", "benchmark-results")


def main() -> None:
    spark = SparkSession.builder.master("local[2]").appName("customer-streaming-benchmark").config("spark.ui.enabled", "false").getOrCreate()
    try:
        start = time.perf_counter()
        now = datetime.now(timezone.utc)
        rows = []
        for i in range(EVENTS):
            event_id = f"evt-{i // 2:08d}" if i % 20 == 0 else f"evt-{i:08d}"
            event_type = "unknown_event" if i % 50 == 0 else "product_viewed"
            user_id = None if i % 100 == 0 else f"user-{i % 5000:05d}"
            rows.append((event_id, event_type, user_id, f"session-{i % 10000:05d}", f"product-{i % 1000:04d}", now))
        df = spark.createDataFrame(rows, "event_id string, event_type string, user_id string, session_id string, product_id string, event_timestamp timestamp")
        silver, quarantine = split_silver_and_quarantine(df)
        trusted = deduplicate_batch(silver)
        customers = affected_customer_keys(trusted)
        counts = {"input_events": df.count(), "valid_events": silver.count(), "quarantined_events": quarantine.count(), "deduplicated_events": trusted.count(), "affected_customers": customers.count()}
        elapsed = time.perf_counter() - start
        result = {"workload_events": EVENTS, "runtime_seconds": round(elapsed, 3), "events_per_second": round(EVENTS / elapsed, 2), **counts}
        os.makedirs(OUT, exist_ok=True)
        with open(os.path.join(OUT, "results.json"), "w", encoding="utf-8") as handle:
            json.dump(result, handle, indent=2)
        with open(os.path.join(OUT, "results.csv"), "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=result.keys()); writer.writeheader(); writer.writerow(result)
        print(json.dumps(result, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
