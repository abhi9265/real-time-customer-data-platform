# Real-Time Customer Data Platform

[![CI](https://github.com/abhi9265/real-time-customer-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/abhi9265/real-time-customer-data-platform/actions/workflows/ci.yml)

A production-oriented **real-time customer data platform prototype** demonstrating how product events can be validated, processed with Spark, historized and transformed into analytics-ready customer datasets.

> **Portfolio focus:** distributed data processing, streaming reliability, incremental computation, CDC/SCD2, data quality, testing, CI/CD, and AI-ready data foundations.

## Business Problem

Product applications generate a continuous stream of customer activity. A useful data platform must handle invalid events, duplicates, late arrivals and changing customer attributes while still producing trustworthy customer and product analytics.

This repository demonstrates the core engineering patterns for that problem without requiring access to a production Kafka cluster or customer data.

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
| Streaming | Kafka and Structured Streaming design, checkpoints and watermarks |
| Reliability | Event identity, deduplication and replay-oriented design |
| Lakehouse | Bronze/Silver/Gold Delta architecture |
| Data quality | Contract validation, rejection reasons and quarantine |
| Customer data | Sessionization, customer state and CDC/SCD2 patterns |
| Analytics | Customer KPIs, revenue, funnel and product metrics |
| Testing | PySpark unit tests and data-contract tests |
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

## Local Development

The repository is designed to be testable locally without production infrastructure:

```bash
python -m pip install -e .[test]
pytest
```

The test suite uses a local Spark environment for transformation-level verification. Kafka and a production streaming deployment are integration concerns and are not represented as a production service in this repository.

## Event-to-Analytics Flow

A typical event moves through the platform as follows:

```text
Product event
    ↓
Schema / contract validation
    ├── invalid → quarantine
    └── valid
          ↓
       Bronze
          ↓
   dedup + quality rules
          ↓
       Silver
          ↓
 customer state + sessions
          ↓
     Customer 360
          ↓
      CDC / SCD2
          ↓
        Gold
          ↓
   KPI / revenue / funnel
```

## Development Workflow

Changes are developed through feature branches and pull requests. CI validates tests and linting before changes are merged to `main`.

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

### Implemented foundations

- Event contract/schema validation
- Streaming transformation patterns
- Bronze/Silver/Gold data-layer design
- Data-quality and quarantine patterns
- Event deduplication/replay-oriented logic
- Customer state and sessionization foundations
- Customer 360 and CDC/SCD2 patterns
- Analytics-ready KPI, revenue and funnel foundations
- Local automated tests and GitHub Actions CI

### Production hardening / integration work

- Kafka environment and topic/deployment contracts
- End-to-end streaming observability and operational SLAs
- Production deployment configuration
- Performance and load benchmarks
- Full integration testing against managed Kafka/Spark infrastructure
- AI/ML feature pipelines and model-serving integration

This distinction is intentional: the repository documents the engineering foundations that are implemented separately from infrastructure and production hardening that require a real deployment environment.

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
