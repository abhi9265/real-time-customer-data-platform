# Customer 360, Sessionization and SCD2

## Sessionization

Events are ordered by `user_id`, `event_timestamp`, and `event_id`. A new session begins after a configurable inactivity threshold (30 minutes by default). The implementation is deterministic for replayed input.

## Customer state

Session facts provide an analytical boundary for user behavior: duration, event count, products touched, cart activity, and orders. This becomes a reusable input for Gold analytics and ML feature generation.

## CDC + SCD Type 2

Customer changes are compared against the current dimension version. When tracked attributes change:

```text
Current version
    ↓
expire effective_to + is_current=false
    ↓
insert new version
    ↓
new row becomes is_current=true
```

Historical records remain queryable with effective timestamps.

## Idempotency considerations

CDC processing must use a stable source change identifier or source commit/LSN where available. The SCD2 MERGE contract should be executed with a deterministic change batch so retries do not create multiple current versions.

## Late-arriving CDC

A CDC record with an older source sequence must not blindly overwrite a newer customer version. Production ingestion should persist source sequence metadata and reject or reconcile out-of-order changes according to the source's ordering guarantees.

## AI readiness

Session and customer-state tables provide clean foundations for features such as purchase propensity, churn signals, next-best-action, and customer embeddings without exposing raw event noise directly to model pipelines.
