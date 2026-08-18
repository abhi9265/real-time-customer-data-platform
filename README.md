# Real-Time Customer Data Platform

[![CI](https://github.com/abhi9265/real-time-customer-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/abhi9265/real-time-customer-data-platform/actions/workflows/ci.yml)

A production-oriented **real-time data platform** that demonstrates how a product engineering team can ingest customer events, enforce data contracts, process streams with Spark, maintain historical customer state, and serve analytics-ready datasets.

> **Portfolio focus:** distributed data processing, streaming reliability, incremental computation, CDC/SCD2, data quality, testing, CI/CD, and AI-ready data foundations.

## Architecture

```text
Product Applications
        │
        ▼
   Event Producer
        │
        ▼
      Kafka
        │
        ▼
Spark Structured Streaming
        │
   ┌────┴────┐
   ▼         ▼
Bronze    Quarantine
 Delta       Delta
   │
   ▼
 Silver Events
   │
   ├── Quality + Deduplication
   ├── Customer State
   └── Sessionization
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
   │      │        │
   └──────┼────────┘
          ▼
   BI / ML / AI workloads
```

## Engineering Capabilities

| Area | Demonstrated capability |
|---|---|
| Streaming | Kafka, Structured Streaming, checkpoints, watermarks |
| Reliability | Event identity, deduplication, replay-oriented design |
| Lakehouse | Bronze/Silver/Gold Delta architecture |
| Data quality | Contract validation, rejection reasons, quarantine |
| Customer data | Sessionization, customer state, CDC/SCD2 |
| Analytics | Customer KPIs, revenue, funnel and product metrics |
| Testing | PySpark unit tests with local Spark fixture |
| CI/CD | GitHub Actions on pull requests and `main` |
| AI readiness | Customer-state and analytics foundations for feature/embedding pipelines |

## Repository Structure

```text
.github/workflows/     CI automation
architecture/          system design and ADRs
docs/                  engineering and operational design
schemas/               event contracts and versions
src/                   ingestion, streaming, Silver, Customer 360 and Gold
tests/                 unit and data-contract tests
pyproject.toml         Python project and test dependencies
```

## Development Workflow

Changes are developed through feature branches and pull requests. CI validates tests before changes are merged to `main`.

```text
Issue / design
    ↓
Feature branch
    ↓
Implementation + tests
    ↓
Pull request
    ↓
GitHub Actions
    ↓
Review / validation
    ↓
main
```

## Current Status

**Core streaming, Silver, Customer 360/SCD2, and Gold analytics foundations are implemented.**

The next engineering milestones are production hardening: observability, data-quality SLAs, pipeline audit, performance benchmarks, deployment/environment contracts, and AI/ML feature pipelines.

## Interview Topics

The project is intentionally designed to support system-design discussions around:

- Kafka partitioning and delivery semantics
- Watermarks and late-arriving events
- Idempotency and replay
- Spark state and shuffle behavior
- Incremental versus full recomputation
- CDC ordering and SCD Type 2
- Data-quality failure handling
- Lakehouse modeling and serving-layer grain
- CI/CD and production reliability
- Designing data foundations for AI/ML workloads
