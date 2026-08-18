# System Design

## Problem

The platform ingests product interaction events from web, mobile, and backend services and makes them available for low-latency analytics while preserving replayability and data quality.

## Design Goals

1. Low-latency event processing
2. Durable replayable event history
3. At-least-once ingestion with idempotent downstream semantics
4. Explicit event contracts and controlled schema evolution
5. Separation of raw, validated, and serving data
6. Observable processing with freshness and latency SLAs
7. Horizontal scalability

## Core Components

- **Event producers:** emit immutable domain events with stable identifiers.
- **Kafka:** durable event transport and partitioned log.
- **Spark Structured Streaming:** distributed stream processing.
- **Delta Bronze:** source-aligned append-only event history.
- **Delta Silver:** validated, deduplicated, conformed events.
- **Gold serving layer:** customer state and analytical aggregates.
- **Observability layer:** records processing metrics, failures, and watermarks.

## Partitioning Strategy

Kafka partitioning will use a stable business key such as `user_id` when per-user ordering matters. The platform will avoid assuming global ordering across partitions.

## Delivery Semantics

The design assumes at-least-once event delivery and makes processing idempotent using `event_id` plus target-table merge semantics. Checkpoint state protects streaming progress while Delta provides transactional writes.

## Failure Model

A failed micro-batch is retried from the last successful checkpoint. Bronze remains replayable, allowing Silver or Gold logic to be reprocessed without re-ingesting the source producer.

## Scalability

Compute is horizontally scalable through Spark partitions. The design avoids single-node state, keeps transformations partition-friendly, and isolates expensive aggregations to the Gold layer.
