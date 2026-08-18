# Kafka → Spark Structured Streaming → Bronze

## Processing contract

```text
Kafka
  ↓ offsets
Structured Streaming
  ├── parse + schema validation
  ├── malformed-event quarantine
  ├── 15-minute event-time watermark
  ├── event_id deduplication
  ↓
Bronze Delta
```

## Reliability semantics

Kafka remains an at-least-once source. The platform uses stable event identity, checkpointed offsets/state, watermark-bounded deduplication, and Delta transactional sinks to make downstream processing duplicate-safe.

## Operational trade-offs

- `failOnDataLoss=false` prevents a query from stopping when Kafka retention removes an offset; production deployments must pair this with source-lag and offset-gap monitoring.
- Checkpoint paths must remain stable across compatible deployments.
- A checkpoint must not be reused after an incompatible query-definition change.
- Quarantined payloads are retained for remediation and controlled replay.

## Production monitoring

At minimum monitor input rows/sec, processing latency, source lag, state-store size, watermark delay, quarantine rate, and late-event rate.

## Replay

Bronze preserves enough source lineage to support downstream replay. A production backfill should use a separate checkpoint/query identity and a bounded event-time range rather than mutating the live streaming checkpoint.
