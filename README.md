# Real-Time Customer Data Platform

[![CI](https://github.com/abhi9265/real-time-customer-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/abhi9265/real-time-customer-data-platform/actions/workflows/ci.yml)

A production-oriented, event-driven **Data Engineering platform** built to demonstrate how a modern product company can ingest, govern, process, model, observe, and serve real-time customer data — with an architecture designed to support downstream AI/ML workloads.

> **Portfolio goal:** demonstrate strong fundamentals in distributed systems, streaming, Spark, Delta Lake, CDC/SCD2, data quality, testing, CI/CD, analytics modeling, and AI-ready data architecture.

## What this project demonstrates

- **Streaming:** Kafka + Spark Structured Streaming
- **Lakehouse:** Bronze / Silver / Gold on Delta Lake
- **Reliability:** checkpointing, watermarking, replay, idempotency, deduplication
- **Data modeling:** Customer 360, sessionization, SCD Type 2, analytical grains
- **Quality:** contract validation, quarantine paths, deterministic quality rules
- **Analytics:** customer KPIs, revenue, conversion funnel, product performance
- **Engineering:** PySpark unit tests, GitHub Actions CI, feature branches and PR-based delivery
- **AI readiness:** customer-state and behavioral datasets designed for feature engineering, embeddings, RAG/AI analytics, and ML pipelines

## Architecture

```text
                 PRODUCT APPLICATIONS / SERVICES
                              |
                 +------------+-------------+
                 |                          |
            Product Events             CDC / APIs
                 |                          |
                 +------------+-------------+
                              v
                         Apache Kafka
                              |
                              v
                 Spark Structured Streaming
                              |
                    +---------+---------+
                    |                   |
                    v                   v
                 BRONZE            QUARANTINE
                 Delta                Delta
                    |
                    v
                 SILVER
          +---------+----------+
          |         |          |
          v         v          v
       Events    Sessions   Customer State
          |         |          |
          +---------+----------+
                    |
          +---------+----------+
          |                    |
          v                    v
      Customer 360           SCD2
          |                    |
          +---------+----------+
                    v
                   GOLD
          +---------+----------+
          |         |          |
          v         v          v
       KPIs      Funnel     Revenue/Product
          |         |          |
          +---------+----------+
                    |
             +------+------+
             |             |
             v             v
          BI / SQL      AI / ML
```

## Technology stack

| Area | Technology |
|---|---|
| Language | Python, SQL |
| Streaming | Apache Kafka, Spark Structured Streaming |
| Processing | PySpark |
| Storage | Delta Lake / Databricks-oriented lakehouse design |
| Modeling | Medallion architecture, Customer 360, SCD Type 2 |
| Quality | JSON Schema, quarantine, unit tests |
| CI/CD | GitHub Actions |
| Analytics | Gold analytical models / semantic-ready metrics |
| AI/ML | AI-ready feature and behavioral data architecture |

## Implemented today

### 1. Event contracts
Versioned customer/product event schemas establish a producer-consumer contract before streaming code consumes data.

### 2. Kafka producer
The project includes deterministic event generation and an idempotent Kafka producer with durable acknowledgements, retries, delivery timeout, and stable event identity.

### 3. Streaming Bronze
The ingestion design covers event-time processing, watermarking, checkpointing, duplicate suppression, malformed-event quarantine, and replay from Bronze.

### 4. Streaming Silver
Silver normalizes and quality-classifies events, separates trusted data from quarantine, deduplicates by event identity, and exposes incremental customer-state updates.

### 5. Customer 360
Sessionization uses inactivity-based boundaries. CDC/SCD Type 2 design preserves historical customer versions with effective dating and current-record tracking.

### 6. Gold analytics
Gold models have explicit grain and business semantics for:

- Customer/day KPIs
- Daily revenue
- Conversion funnel
- Product performance

Funnel stages use distinct users to prevent event-level double counting.

## Reliability model

The platform is designed around **at-least-once event delivery plus idempotent downstream processing** rather than assuming that a single component magically provides end-to-end exactly-once semantics.

Key mechanisms:

- Stable `event_id`
- Kafka producer idempotence
- Checkpointed streaming offsets
- Event-time watermarks
- Deduplication
- Delta transactional writes / MERGE contracts
- Quarantine and replay paths
- Explicit handling of late-arriving data

## Testing and CI

Every major layer has unit tests. GitHub Actions installs the project test dependencies and runs the complete pytest suite on pull requests and pushes to `main`.

```text
Feature branch
     |
     v
Pull Request
     |
     v
GitHub Actions
     |
     +--> tests
     |
     +--> Spark unit tests
     |
     +--> data-contract tests
     |
     v
Merge to main
```

## Repository structure

```text
real-time-customer-data-platform/
├── .github/workflows/        # CI/CD
├── architecture/             # system design + ADRs
├── schemas/                   # versioned event contracts
├── src/
│   ├── producer/              # Kafka event generation
│   ├── silver/                # conformance + incremental state
│   ├── customer360/           # sessions + SCD2
│   └── gold/                  # product analytics
├── tests/
│   ├── unit/
│   └── data_contract/
├── docs/                      # engineering design notes
├── benchmarks/                # performance experiments
├── pyproject.toml
└── README.md
```

## Engineering roadmap

The core batch/streaming foundation is implemented. The next production-hardening work is intentionally separated from the core pipeline:

- [ ] Data-quality SLA framework and scorecards
- [ ] Pipeline audit/run metadata framework
- [ ] Streaming observability and freshness monitoring
- [ ] Spark performance benchmark suite
- [ ] Dev/test/prod Databricks deployment configuration
- [ ] Unity Catalog naming and governance conventions
- [ ] Power BI semantic model / serving documentation
- [ ] Feature engineering and feature-store contract
- [ ] Embedding pipeline + retrieval dataset
- [ ] AI/RAG analytics layer
- [ ] Failure injection and recovery tests

The roadmap is deliberately explicit: **planned capabilities are not represented as implemented features.**

## Interview starting points

If you are reviewing this repository, the most useful design discussions are:

1. Why Kafka + Structured Streaming instead of batch ingestion?
2. How do checkpointing, watermarks, and deduplication interact?
3. What does at-least-once mean end to end?
4. How would you handle late or out-of-order events?
5. Why is event identity different from business idempotency?
6. How would you implement SCD Type 2 safely under retries?
7. How do you prevent funnel and revenue double counting?
8. How would you scale Spark when a customer or product becomes a hot key?
9. How would you replay a corrupted downstream partition?
10. How would you turn Customer 360 into ML features or an AI retrieval layer?

## Design documentation

- [System design](architecture/system-design.md)
- [Streaming design](architecture/streaming-design.md)
- [Event delivery ADR](architecture/adr/001-event-delivery-semantics.md)
- [Event versioning ADR](architecture/adr/002-event-versioning.md)
- [Kafka producer design](docs/kafka-producer.md)
- [Streaming Silver design](docs/streaming-silver.md)
- [Customer 360 / SCD2](docs/customer360.md)
- [Gold product analytics](docs/gold-product-analytics.md)
- [Showcase and interview guide](docs/showcase.md)

## Local development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[test]'
pytest
```

The Kafka producer can generate bounded local traffic when a Kafka broker is available:

```bash
python -m src.producer.main --bootstrap-servers localhost:9092 --topic customer-events --count 100
```

## Status

**Core streaming platform: implemented.**  
**Production hardening + AI layer: next phase.**
