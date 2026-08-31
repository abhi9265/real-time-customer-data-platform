# Benchmark Methodology

## Purpose

Measure deterministic streaming transformation behavior locally before introducing a real Kafka deployment.

## Protocol

1. Fix Python, PySpark, repository revision, and event schema version.
2. Generate a controlled event workload with a recorded event count.
3. Measure processing time and derived throughput for the local Spark path.
4. Record valid, quarantined, duplicate, and late-event counts where the test exposes them.
5. Repeat each workload at least three times and report the median.
6. Store results with the commit SHA and execution environment.

## Reliability scenarios

Benchmark scenarios should include normal traffic, duplicate events, late events, out-of-order events, and invalid events.

## Reporting rule

Do not commit estimated or invented Kafka throughput or latency. Real Kafka measurements belong to a separately labeled integration benchmark once a reproducible Kafka environment exists.