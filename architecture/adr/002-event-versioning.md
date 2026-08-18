# ADR 002 — Event Versioning

## Decision

Use explicit `event_type` plus integer `event_version` fields and store schemas under version-controlled paths.

Breaking event-contract changes require a new event version. Compatible additive changes may be introduced only when consumers remain backward compatible.

## Consequences

- Consumers can validate contracts deterministically.
- Historical events remain interpretable.
- Producers and consumers can migrate independently.
- Schema compatibility must be tested in CI.
