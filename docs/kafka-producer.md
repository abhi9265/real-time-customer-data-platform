# Kafka Event Producer

## Purpose

The producer generates realistic customer-product events for local development, integration testing, and controlled replay.

## Reliability design

- `event_id` is the stable event identity.
- The Kafka message key is `event_id`, preserving deterministic partitioning for retries.
- Producer idempotence is enabled.
- `acks=all` is required for durable broker acknowledgement.
- Retries and delivery timeout are explicitly configured.
- The CLI generates a bounded number of events so test runs are deterministic.

## Event flow

```text
Event generator
      |
      v
Contract-shaped JSON
      |
      v
Kafka Producer
      |
      +--> key = event_id
      |
      v
customer-events topic
```

## Partitioning decision

The first implementation uses `event_id` as the Kafka key to make event identity stable. For production customer-level ordering requirements, the partition key should be changed to `user_id` (or an explicit aggregate key) so all events for one customer remain ordered within a partition. This trade-off will be validated in the streaming ingestion phase.

## Next

The next phase introduces Spark Structured Streaming ingestion with Kafka offsets, checkpointing, event-time watermarks, duplicate suppression, malformed-event quarantine, and Bronze Delta persistence.
