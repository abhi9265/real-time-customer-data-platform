# Streaming Silver Design

## Responsibility

Silver is the trusted, conformed event layer. It is deliberately separated from Bronze so downstream consumers do not need to understand malformed payloads, source formatting, or raw ingestion details.

## Processing model

```text
Bronze Delta
    |
    v
Normalize
    |
    v
Quality classification
    |-------------------|
    v                   v
VALID               REJECTED
    |                   |
    v                   v
Deduplicate         Quarantine
    |
    v
Silver customer_events
    |
    +--> affected customer keys
    |
    +--> customer_state
```

## Incremental semantics

The event identity is `event_id`. A batch can be replayed safely because the Silver table uses an idempotent Delta `MERGE` on that identity. Downstream customer-state updates are limited to customer keys touched by the current batch.

## Quality rules

Rejected events include:

- Missing event ID
- Missing user ID
- Invalid event timestamp
- Unknown event type

The quarantine record retains the quality reason and raw lineage so remediation and replay are possible.

## Late events

The Bronze stream uses a 15-minute event-time watermark. Silver preserves the event timestamp and can be replayed from Bronze for a larger historical correction window. Production orchestration should distinguish normal streaming lateness from explicit backfill/reprocessing jobs.

## Important design trade-off

`dropDuplicates(event_id)` provides deterministic event identity, but it does not solve business-level duplicate events when upstream systems emit different IDs for the same action. A future domain-specific idempotency key can combine fields such as `user_id + event_type + event_timestamp + source_event_id` where the source guarantees those semantics.
