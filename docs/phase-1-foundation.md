# Phase 1 — Platform Foundation

## Why this phase matters

The platform starts with contracts and architecture rather than notebooks. This keeps streaming behavior, event semantics, and operational guarantees explicit before implementation begins.

## Initial Guarantees

- Every event has a stable `event_id`.
- Every event declares its `event_type` and `event_version`.
- Business time is represented by `event_timestamp`.
- Processing time is tracked separately by the pipeline.
- At-least-once delivery is expected from the transport layer.
- Downstream writes must be idempotent.
- Invalid events are observable and quarantinable.
- Breaking schema changes require a new event version.

## Phase 2 Entry Criteria

Before implementing the Kafka producer, these controls should remain true:

1. Event schemas validate through automated tests.
2. ADRs describe delivery and versioning semantics.
3. The repository has a reproducible Python test environment.
4. The streaming design defines watermark and checkpoint behavior.
