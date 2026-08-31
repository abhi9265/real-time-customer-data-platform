# Real-Time Customer Data Platform

[![CI](https://github.com/abhi9265/real-time-customer-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/abhi9265/real-time-customer-data-platform/actions/workflows/ci.yml)

> **Real-time data engineering portfolio project:** process customer events with Spark Structured Streaming, Kafka-oriented ingestion, data quality, deduplication, Customer 360 and CDC/SCD2 patterns.
>
> **Topics:** `PySpark` · `Spark Structured Streaming` · `Kafka` · `Delta Lake` · `Customer 360` · `CDC` · `SCD2` · `Data Quality` · `Data Engineering`

## At a glance

A production-oriented **real-time customer data platform prototype** showing how product events can be validated, processed with Spark, historized and transformed into analytics-ready customer datasets.

> **Evidence boundary:** the repository verifies its streaming-oriented transformation path locally. Kafka and managed streaming infrastructure remain explicit production integration boundaries; no production Kafka deployment or load-test result is claimed without the corresponding environment and evidence.

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

## Kafka Streaming Flow

The intended production event path is **producer → Kafka → Spark Structured Streaming → Bronze → Silver → Customer 360 → Gold**. The producer/Kafka boundary handles continuous event delivery, while Spark owns validation, stateful transformation, deduplication and downstream lakehouse processing.

```text
┌──────────────┐     ┌──────────┐     ┌──────────────────────┐
│ Product App  │ ──► │  Kafka   │ ──► │ Spark Structured     │
│ / Producer   │     │  Topic   │     │ Streaming            │
└──────────────┘     └──────────┘     └──────────┬───────────┘
                                                  │
                         ┌────────────────────────┴──────────────┐
                         ▼                                       ▼
                  ┌─────────────┐                         ┌─────────────┐
                  │   Bronze    │                         │ Quarantine  │
                  │ raw events  │                         │ bad events  │
                  └──────┬──────┘                         └─────────────┘
                         ▼
                  ┌─────────────┐
                  │   Silver    │
                  │ quality +   │
                  │ dedup/state │
                  └──────┬──────┘
                         ▼
                  ┌─────────────┐
                  │ Customer 360│
                  │ + CDC/SCD2  │
                  └──────┬──────┘
                         ▼
                  ┌─────────────┐
                  │    Gold     │
                  │ KPI/revenue │
                  │ /funnel     │
                  └─────────────┘
```

**Current evidence:** local Spark tests verify the streaming-oriented transformation path. Kafka remains a production integration boundary; the README does not claim a live Kafka cluster or end-to-end Kafka throughput test. fileciteturn1066file0

## Architecture → Code Map

| Architecture component | Repository implementation | Purpose |
|---|---|---|
| Event contracts | `schemas/` | Versioned event definitions and validation |
| Ingestion / streaming | `src/` | Event ingestion and Structured Streaming transformations |
| Bronze | streaming/data-layer modules | Raw event persistence and replay-oriented processing |
| Silver | streaming/Silver modules | Quality rules, deduplication and curated events |
| Customer 360 | Customer-state modules | Customer state and sessionization foundations |
| CDC / SCD2 | CDC/SCD2 modules | Historization and change-processing patterns |
| Gold | `src/gold/product_analytics.py` | Customer KPIs, revenue, funnel and product metrics |
| Tests | `tests/` | Unit and data-contract verification |
| Architecture | `architecture/` | System design and ADRs |

## Gold Layer → BI Report Preview

The Gold layer is designed as the **analytics serving contract** for BI consumers. The repository's actual Gold code defines daily customer KPIs, daily revenue, conversion funnel and product-performance datasets. fileciteturn1069file0

### Executive Customer & Product Analytics

```text
┌──────────────────────────────────────────────────────────────────┐
│ REAL-TIME CUSTOMER 360 — GOLD LAYER / BI REPORT PREVIEW         │
├──────────────┬──────────────┬──────────────┬────────────────────┤
│  EVENTS      │  SESSIONS    │  ORDERS      │  REVENUE           │
│  Daily       │  Daily       │  Daily       │  Daily             │
│  Customer KPIs│ Customer KPI │ Customer KPI │ Payment events     │
├──────────────┴──────────────┴──────────────┴────────────────────┤
│                                                                  │
│  CONVERSION FUNNEL                                                │
│  Product Views  ──►  Cart Adds  ──►  Checkout  ──►  Orders      │
│       │                 │                │              │          │
│     viewers         cart_users     checkout_users    buyers       │
│                                                                  │
├──────────────────────────────────────────────────────────────────┤
│  PRODUCT PERFORMANCE                                              │
│  Views  |  Cart Adds  |  Orders  |  Active Users  |  Date/Product│
├──────────────────────────────────────────────────────────────────┤
│  Serving consumers: Power BI / BI / ML / AI                       │
└──────────────────────────────────────────────────────────────────┘
```

### Gold metric contract

| Gold dataset | Grain | Example measures |
|---|---|---|
| Daily customer KPIs | Customer × Day | Events, sessions, products viewed, cart adds, checkouts, orders, payments |
| Daily revenue | Day | Revenue, successful payments, paying customers |
| Conversion funnel | Day | Viewers, cart users, checkout users, buyers, conversion rates |
| Product performance | Product × Day | Views, cart adds, orders, active users |

> **Presentation note:** this is a **README BI/report-style preview**, not a claim that a Power BI report has been deployed. It shows recruiters how the Gold layer can be consumed by a dashboard and makes the serving-layer grain explicit.

## Engineering Capabilities

| Area | Demonstrated capability |
|---|---|
| Streaming | Spark Structured Streaming and Kafka-oriented design |
| Reliability | Event identity, deduplication, replay-oriented processing |
| State | Customer state and sessionization foundations |
| Data quality | Contract validation, rejection reasons and quarantine |
| Lakehouse | Bronze/Silver/Gold Delta architecture |
| Customer data | Customer 360 and CDC/SCD2 patterns |
| Analytics | Customer KPIs, revenue, funnel and product metrics |
| Testing | Local Spark tests and data-contract tests |
| CI/CD | GitHub Actions on pull requests and `main` |
| AI readiness | Customer-state and analytics foundations for feature/embedding pipelines |

## Execution Evidence

### Verified local path

The repository provides a reproducible local verification path without requiring production Kafka infrastructure:

```bash
python -m pip install -e .[test]
pytest
```

The documented demo uses local Spark to verify streaming-oriented transformations, event contracts, quality handling and customer-state logic. fileciteturn1066file0

### What the demo proves

```text
product event
    ↓
contract validation
    ├── invalid → quarantine
    └── valid → Bronze
                    ↓
             dedup + quality
                    ↓
                  Silver
                    ↓
          customer state/session
                    ↓
               Customer 360
                    ↓
                 Gold
```

This is **local transformation evidence**. Kafka remains a production integration boundary, and the repository explicitly avoids fabricating managed Kafka/Spark deployment or end-to-end load-test evidence. fileciteturn1066file0

## Benchmark / Verification Reference

The repository includes an executable benchmark harness for measuring the streaming verification suite. It runs a targeted PySpark test suite, records runtime and pass/fail status, and writes JSON/CSV result files under `benchmark-results/`. fileciteturn1064file0

Run it with:

```bash
python benchmarks/run_benchmark.py
```

> **Important:** benchmark numbers should be reported only from generated result artifacts from an actual run. This README intentionally does not invent a runtime or throughput figure.

## Run the Demo

```bash
git clone https://github.com/abhi9265/real-time-customer-data-platform.git
cd real-time-customer-data-platform
python -m pip install -e .[test]
pytest
```

For the short reproducible path, see [`docs/DEMO.md`](docs/DEMO.md).

## Event-to-Analytics Flow

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

## Repository Structure

```text
.github/workflows/     CI automation
architecture/          system design and ADRs
docs/                  engineering and operational design
schemas/               event contracts and versions
src/                   ingestion, streaming, Silver, Customer 360 and Gold
tests/                 unit and data-contract tests
benchmarks/            executable verification benchmark
pyproject.toml         Python project and test dependencies
```

## Development Workflow

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
- Executable benchmark harness for the local verification suite

### Production hardening / integration work

- Kafka environment and topic/deployment contracts
- End-to-end streaming observability and operational SLAs
- Production deployment configuration
- Managed Kafka/Spark integration testing
- Production-scale performance/load benchmarks
- AI/ML feature pipelines and model-serving integration

This distinction is intentional: the repository documents the engineering foundations that are implemented separately from infrastructure and production hardening that require a real deployment environment.

## Interview Topics

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
