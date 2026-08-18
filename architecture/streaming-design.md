# Streaming Design

## Event Lifecycle

```text
Producer -> Kafka -> Streaming Read -> Contract Validation -> Deduplication -> Bronze -> Silver -> Gold
```

## Event-Time Processing

`event_timestamp` is the business event time. Processing time is retained separately for operational monitoring.

Watermarks will be applied to bounded stateful operations so the platform can tolerate late events without keeping unbounded state.

## Deduplication

Primary key: `event_id`.

Secondary protection: `(event_id, event_version)` for version-aware replay. A duplicate event must not produce a second business effect downstream.

## Checkpointing

Each streaming query uses a durable environment-specific checkpoint path. Checkpoints are treated as operational state and are never shared between independent queries.

## Backpressure and Throughput

The first implementation will expose configurable ingestion limits so throughput can be tuned without changing transformation code. Processing latency and input rate will be recorded for benchmarking.

## Late Data

Late events remain valid when they fall inside the configured watermark horizon. Events arriving beyond the horizon are routed to a late-data quarantine path for reconciliation rather than silently discarded.

## Schema Evolution

Every event carries `event_type` and `event_version`. Validation uses versioned contracts. Additive compatible fields can evolve within a major event version; breaking changes require a new version.
