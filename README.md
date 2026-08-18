# Real-Time Customer Data Platform

A production-oriented, event-driven data platform designed to demonstrate real-time Data Engineering patterns used in modern product companies.

## Objective

Build a reliable customer-event platform capable of ingesting high-volume product events, validating and evolving event contracts, processing streams with Spark Structured Streaming, maintaining customer state, and publishing low-latency analytics datasets.

## Core Architecture

```text
Product Applications / Services
            |
            v
      Event Producer
            |
            v
        Apache Kafka
            |
            v
  Spark Structured Streaming
            |
      +-----+------+
      |            |
      v            v
   Bronze       Quarantine
   Delta           Delta
      |
      v
   Silver
   Delta
      |
  +---+----------------+
  |                    |
  v                    v
Customer 360       Real-Time KPIs
  |                    |
  +---------+----------+
            |
            v
       Gold / Serving
        |          |
        v          v
      BI / SQL   ML Features
```

## Technology Stack

- Apache Kafka
- Azure Databricks
- Spark Structured Streaming
- PySpark
- Delta Lake
- Python
- SQL
- GitHub Actions
- Docker (local development)
- Power BI / SQL serving (planned)

## Engineering Topics

- Event-driven architecture
- Streaming ingestion and checkpointing
- Watermarks and late-arriving events
- Idempotent processing
- Event deduplication
- Schema contracts and schema evolution
- CDC and state management
- SCD Type 2
- Data-quality quarantine
- Observability and pipeline SLAs
- Performance and shuffle optimization
- CI/CD and environment promotion
- Failure recovery and replay

## Repository Structure

```text
real-time-customer-data-platform/
├── .github/workflows/
├── architecture/
│   ├── system-design.md
│   ├── streaming-design.md
│   └── adr/
├── schemas/
│   ├── events/
│   └── versions/
├── src/
│   ├── ingestion/
│   ├── streaming/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── cdc/
│   ├── quality/
│   └── common/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── data_contract/
├── configs/
│   ├── dev/
│   ├── test/
│   └── prod/
├── benchmarks/
├── notebooks/
├── infra/
├── docs/
├── pyproject.toml
└── README.md
```

## Delivery Roadmap

### Phase 1 — Platform foundation
- System design and architecture decisions
- Event contract and versioning strategy
- Local development contract
- Repository conventions

### Phase 2 — Event ingestion
- Kafka producer
- Event generation
- Partitioning and message keys

### Phase 3 — Streaming processing
- Structured Streaming ingestion
- Checkpointing
- Watermarks
- Deduplication
- Bronze/Silver processing

### Phase 4 — Customer state and CDC
- Change-data capture simulation
- Current-state reconstruction
- SCD Type 2 history
- Customer 360

### Phase 5 — Real-time analytics
- Streaming Gold tables
- Sliding/tumbling window KPIs
- Conversion and engagement metrics

### Phase 6 — Production hardening
- Data-quality gates
- Observability
- Performance benchmarks
- CI/CD
- Failure and recovery tests

## Status

**Phase 1 — Platform foundation**
