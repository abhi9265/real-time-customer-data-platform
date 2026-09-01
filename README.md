# Real-Time Customer Data Platform

[![CI](https://github.com/abhi9265/real-time-customer-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/abhi9265/real-time-customer-data-platform/actions/workflows/ci.yml)

> **Real-time data engineering portfolio project:** process customer events with Spark Structured Streaming, Kafka ingestion, data quality, deduplication, Customer 360 and CDC/SCD2 patterns.
>
> **Topics:** `PySpark` · `Spark Structured Streaming` · `Kafka` · `Delta Lake` · `Customer 360` · `CDC` · `SCD2` · `Data Quality` · `Data Engineering`

## At a glance

A production-oriented **real-time customer data platform prototype** showing how product events can be validated, processed with Spark, historized and transformed into analytics-ready customer datasets.

> **Evidence boundary:** the repository now includes a reproducible Docker-based Kafka environment and a GitHub Actions integration job that publishes deterministic events to Kafka and verifies that Spark Structured Streaming consumes them. Managed Kafka/Spark infrastructure and production-scale load-test results are not claimed.

## Business Problem

Product applications generate a continuous stream of customer activity. A useful data platform must handle invalid events, duplicates, late arrivals and changing customer attributes while still producing trustworthy customer and product analytics.

This repository demonstrates the core engineering patterns for that problem without requiring access to production customer data.

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

## Verified Kafka Streaming Flow

The repository now verifies the core event boundary end-to-end in GitHub Actions:

```text
Deterministic Producer
        │
        ▼
Docker Kafka topic: customer-events
        │
        ▼
Spark Structured Streaming
        │
        ▼
5 JSON events observed
```

The smoke test uses `docker-compose.kafka.yml`, publishes five deterministic customer events, starts a local Spark Structured Streaming query using the Spark Kafka connector, and fails unless all five events are observed. This proves an actual Kafka → Spark execution path rather than a README-only architecture claim.

The verification environment is intentionally local/reproducible. It does **not** claim managed Kafka deployment, production throughput, or production SLA evidence.

## Kafka Streaming Flow

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

## Architecture → Code Map

| Architecture component | Repository implementation | Purpose |
|---|---|---|
| Event contracts | `schemas/` | Versioned event definitions and validation |
| Kafka demo environment | `docker-compose.kafka.yml` | Reproducible local Kafka broker |
| Event producer | `scripts/kafka_producer.py` | Deterministic customer-event publisher |
| Kafka → Spark verification | `scripts/kafka_spark_smoke.py` | End-to-end streaming smoke test |
| Ingestion / streaming | `src/` | Event ingestion and Structured Streaming transformations |
| Bronze | streaming/data-layer modules | Raw event persistence and replay-oriented processing |
| Silver | streaming/Silver modules | Quality rules, deduplication and curated events |
| Customer 360 | Customer-state modules | Customer state and sessionization foundations |
| CDC / SCD2 | CDC/SCD2 modules | Historization and change-processing patterns |
| Gold | `src/gold/product_analytics.py` | Customer KPIs, revenue, funnel and product metrics |
| Tests | `tests/` | Unit and data-contract verification |
| CI/CD | `.github/workflows/ci.yml` | Tests plus Kafka → Spark integration verification |
| Architecture | `architecture/` | System design and ADRs |

## Gold Layer → BI Report Preview

The Gold layer is designed as the **analytics serving contract** for BI consumers. The repository's actual Gold code defines daily customer KPIs, daily revenue, conversion funnel and product-performance datasets.

### Executive Customer & Product Analytics

```text
┌──────────────────────────────────────────────────────────────────┐
│ REAL-TIME CUSTOMER 360 — GOLD LAYER / BI REPORT PREVIEW         │
├──────────────┬──────────────┬──────────────┬────────────────────┤
│  EVENTS      │  SESSIONS    │  ORDERS      │  REVENUE           │
│  Daily       │  Daily       │  Daily       │  Daily             │
│  Customer KPIs│ Customer KPI │ Customer KPI │ Payment events     │
├──────────────┴──────────────┴──────────────┴────────────────────┤
│  CONVERSION FUNNEL                                               │
│  Product Views  ──►  Cart Adds  ──►  Checkout  ──►  Orders     │
│                                                                  │
├──────────────────────────────────────────────────────────────────┤
│  PRODUCT PERFORMANCE                                              │
│  Views | Cart Adds | Orders | Active Users | Date/Product       │
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

> **Presentation note:** this is a **README BI/report-style preview**, not a claim that a Power BI report has been deployed.

## Execution Evidence

### Local Kafka demo

Prerequisites: Docker and Python 3.11+.

```bash
docker compose -f docker-compose.kafka.yml up -d
python -m pip install -e .[test,integration]
python scripts/kafka_producer.py
python scripts/kafka_spark_smoke.py
docker compose -f docker-compose.kafka.yml down -v
```

The smoke test publishes five deterministic events and verifies that Spark Structured Streaming observes all five through the Kafka connector.

### GitHub Actions verification

Every push/PR runs:

1. Python tests and Ruff checks.
2. A clean Kafka container.
3. The deterministic event producer.
4. Spark Structured Streaming with the Kafka connector.
5. An assertion that all five published events were consumed.
6. Kafka diagnostics on failure.

This gives the repository an automated **Kafka → Spark execution gate** in addition to its unit/data-contract tests.

## Benchmark / Verification Reference

The repository includes an executable benchmark harness for the streaming verification suite. It measures the local transformation test path; it should not be interpreted as Kafka throughput unless the Kafka integration workflow is explicitly being measured.

Run:

```bash
python benchmarks/run_benchmark.py
```

> Benchmark numbers should be reported only from generated result artifacts from an actual run. No runtime or throughput figure is invented in this README.

## Run the Tests

```bash
python -m pip install -e .[test]
pytest
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
scripts/               Kafka producer and Spark integration smoke test
docker-compose.kafka.yml local Kafka/KRaft environment
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
    ├── unit/data-contract tests
    └── Kafka → Spark integration smoke test
    ↓
Review / validation
    ↓
main
```

## Current Status

### Implemented and verified

- Event contract/schema validation
- Reproducible Kafka/KRaft local environment
- Deterministic Kafka event producer
- Kafka → Spark Structured Streaming integration smoke test
- Bronze/Silver/Gold data-layer design
- Data-quality and quarantine patterns
- Event deduplication/replay-oriented logic
- Customer state and sessionization foundations
- Customer 360 and CDC/SCD2 patterns
- Analytics-ready KPI, revenue and funnel foundations
- Local automated tests and GitHub Actions CI
- Executable benchmark harness for the local verification suite

### Production hardening / integration work

- Managed Kafka/Spark production deployment
- End-to-end streaming observability and operational SLAs
- Production-scale performance/load benchmarks
- Multi-broker/high-availability Kafka validation
- AI/ML feature pipelines and model-serving integration

This distinction is intentional: the repository documents what is actually implemented and verified separately from infrastructure and production hardening that require a real deployment environment.

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
