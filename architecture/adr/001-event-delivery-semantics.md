# ADR 001 — Event Delivery Semantics

## Decision

Use **at-least-once delivery** between Kafka and Spark Structured Streaming, with **idempotent downstream processing**.

## Why

Exactly-once end-to-end semantics across independently managed producers, brokers, and sinks are operationally expensive to guarantee. The platform instead makes duplicates harmless through stable event identifiers and transactional Delta writes.

## Consequences

Positive:
- Safe retries
- Reprocessing and replay are supported
- Operational failures do not require source-side coordination
- The sink remains the business-effect boundary

Trade-off:
- Every state-changing sink operation must enforce idempotency
- Event IDs become part of the data contract
