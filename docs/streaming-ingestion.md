# Kafka → Spark Structured Streaming → Bronze

## Processing contract

```text
Kafka
  |
  | offsets
  v
Structured Streaming
  |
  +-- parse + schema validation
  |        |
  |        +--> quarantine
  |
  +-- event-time watermark (15m)
  |
  +-- dropDuplicates(event_id)
  |
  v
Bronze Delta
```

## Reliability semantics

### Checkpointing

Each sink has a durable checkpoint location. Kafka offsets and streaming state are recovered from the checkpoint after driver failure.

### Event time

Business logic uses `event_timestamp`, not Kafka ingestion time. A 15-minute watermark bounds state while allowing reasonably late events.

### Deduplication

`event_id` is the logical event identity. `dropDuplicates(event_id)` prevents repeated delivery from creating duplicate Bronze records while the watermark limits retained state.

### Data loss

`failOnDataLoss=false` prevents a streaming query from stopping when Kafka retention has removed an offset. This is an operational trade-off: production deployments should pair it with source-lag monitoring and an alert when offset gaps occur.

### Quarantine

Malformed JSON, missing event IDs, invalid timestamps, and missing user IDs are written to a separate Delta stream with the raw payload and reason code for remediation/replay.

## Exactly-once discussion

Spark Structured Streaming provides strong processing guarantees when checkpoints and transactional sinks are used correctly. The platform therefore treats **idempotent event identity + durable checkpoint + Delta transactional writes** as the practical end-to-end correctness boundary. Kafka delivery itself remains at-least-once, so consumers must remain duplicate-safe.

## Operational requirements

- Checkpoint paths must be stable across deployments.
- Do not reuse a checkpoint between incompatible query definitions.
- Monitor input rows/sec, processing latency, state-store size, and source lag.
- Alert on quarantine rate and late-event rate.
- Retain raw quarantined payloads long enough to support replay.
