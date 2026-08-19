# Real-Time Customer Data Platform

[![CI](https://github.com/abhi9265/real-time-customer-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/abhi9265/real-time-customer-data-platform/actions/workflows/ci.yml)

A production-oriented **real-time customer data platform** designed around a realistic product-event workload: Kafka ingestion, Spark Structured Streaming, Delta Lake, contract enforcement, quarantine, incremental Customer 360, CDC/SCD2 and analytics serving.

> **Engineering goal:** make the data path observable, replayable, testable and safe enough to support downstream BI/ML/AI workloads.

## Architecture

```text
Product Applications
        │
        ▼
 Contract-shaped Event Producer
        │
        ▼
      Kafka
        │
        ▼
Spark Structured Streaming
        │
        ├── schema parsing
        ├── event-time watermark
        ├── event-id deduplication
        └── checkpointing
        │
   ┌────┴────┐
   ▼         ▼
Bronze    Quarantine
 Delta       Delta
   │
   ▼
 Silver Events
   │
   ├── deterministic DQ
   ├── customer state
   └── sessionization
          │
          ▼
     Customer 360
          │
        CDC/SCD2
          │
          ▼
        Gold
   ┌──────┼────────┐
   ▼      ▼        ▼
  KPIs  Revenue  Funnels
          │
          ▼
       BI / ML / AI

Cross-cutting: CI/CD · audit · quality · replay · observability
```

## Engineering Capabilities

| Area | Demonstrated capability |
|---|---|
| Streaming | Kafka, Structured Streaming, event-time processing, watermarks |
| Reliability | Idempotent producer, stable event identity, deduplication, checkpoints |
| Replay | `startingOffsets` control and `availableNow` batch mode |
| Lakehouse | Bronze/Silver/Gold Delta architecture |
| Contracts | Versioned JSON event envelope and domain event schemas |
| Data quality | Deterministic validation, rejection reasons, quarantine and metrics |
| Customer data | Sessionization, customer state, CDC/SCD2 |
| Analytics | Customer KPIs, revenue, funnel and product metrics |
| Observability | Pipeline audit records, record counts, quarantine counts and watermark tracking |
| Testing | PySpark/local unit tests plus event-contract tests |
| CI/CD | GitHub Actions on pull requests and `main` |
| AI readiness | Governed Customer 360 and analytics context for downstream feature/RAG workloads |

## Repository Structure

```text
.github/workflows/       CI automation
architecture/            system design and ADRs
docs/                    engineering and operational design
schemas/events/          versioned event contracts
src/producer/             deterministic Kafka event producer
src/streaming/            Kafka → Bronze Structured Streaming
src/silver/               Silver normalization and quality
src/customer360/          sessionization, state and SCD2
src/gold/                 analytics serving layer
src/observability/        DQ and pipeline audit helpers
tests/                    unit and data-contract tests
```

## Operational Design

The platform treats failures as data, not silent exceptions:

- malformed or incomplete events are classified with deterministic reasons;
- trusted and rejected records can be separated before downstream serving;
- checkpoints provide restart/recovery state;
- event IDs provide an idempotency key for duplicate delivery;
- event-time watermarks bound late-data state;
- audit records capture input/output/quarantine counts and pipeline status;
- `availableNow` supports bounded replay/backfill runs from a configured Kafka offset boundary.

## Development Workflow

```text
Design / Issue
     ↓
Feature branch
     ↓
Implementation + tests
     ↓
Pull request
     ↓
GitHub Actions
     ↓
Review + validation
     ↓
main
```

## Current Status

**Implemented:** Kafka producer foundation, contract-shaped events, Kafka → Bronze Structured Streaming, Silver quality/deduplication, Customer 360/SCD2, Gold product analytics, DQ metrics and pipeline-audit foundations.

**Next:** production deployment contracts, SLA monitoring, performance benchmarks, stronger integration tests and a governed AI/ML consumer layer.

## Interview Topics

- Kafka partitioning and delivery semantics
- Idempotency and duplicate delivery
- Event-time versus processing-time
- Watermarks and late-arriving events
- Checkpoint recovery and replay
- Spark state/shuffle trade-offs
- Incremental versus full recomputation
- CDC ordering and SCD Type 2
- Data-quality quarantine design
- Auditability and pipeline SLAs
- Lakehouse modeling and serving-layer grain
- Designing data foundations for AI/ML workloads
