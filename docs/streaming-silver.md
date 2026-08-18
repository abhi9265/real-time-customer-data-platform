# Streaming Silver Design

## Objective

Transform Bronze customer events into a trusted, replayable Silver layer while keeping downstream customer state incremental.

## Flow

Bronze Delta → normalize → quality gate → deduplicate → Silver `customer_events` → affected customer keys → incremental `customer_state`

## Quality contract

Rejected records are retained separately from trusted Silver records with an actionable `quality_reason`:

- `MISSING_EVENT_ID`
- `MISSING_USER_ID`
- `INVALID_EVENT_TIMESTAMP`
- `UNKNOWN_EVENT_TYPE`

## Idempotency

`event_id` is the event identity used by the Silver Delta MERGE. Reprocessing the same Bronze data therefore updates an existing event rather than creating a second event row.

## Replay and late events

Bronze remains the replay source. Event timestamps are preserved as business event time, while `processed_at` records ingestion processing time. A production implementation should use a bounded event-time watermark for streaming state and a separate replay/backfill path for events arriving beyond the operational watermark.

## Incremental customer state

Only customers touched by the current batch need downstream recomputation. This prevents every micro-batch from scanning the complete customer population.

## Production extensions

- domain-specific idempotency keys for producer retries
- schema enforcement and evolution policy
- Delta expectations / observability metrics
- dead-letter remediation workflow
- checkpoint isolation per deployment environment
- state reconciliation and periodic full backfill
