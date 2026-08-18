# Project Showcase Guide

## 60-second explanation

> I built an event-driven customer data platform using Kafka and Spark Structured Streaming. Events land in a Bronze Delta layer, pass through Silver quality gates and deduplication, and are transformed into Customer 360, SCD Type 2 history, and Gold product analytics. The design is explicitly replayable and incremental, and the repository uses contract tests, PySpark tests, and GitHub Actions CI. The next layer extends the platform with observability, data-quality SLAs, performance benchmarking, and AI/ML feature and retrieval pipelines.

## What makes it engineering-heavy

### Distributed systems

- Kafka partitions and message keys
- At-least-once delivery
- Idempotent producer behavior
- Consumer checkpointing
- Event-time semantics
- Watermarks and late data
- Replay and recovery

### Lakehouse engineering

- Bronze/Silver/Gold separation
- Delta transactional write patterns
- Incremental processing
- Explicit analytical grain
- CDC/SCD Type 2 history

### Reliability

- Schema contracts
- Quarantine path
- Stable event identity
- Deduplication
- Retry-safe processing
- Test automation

### Analytics

- Customer/day KPIs
- Conversion funnel
- Revenue
- Product performance
- Customer session facts

### AI readiness

The platform deliberately creates clean behavioral and historical data before adding AI. The intended AI path is:

```text
Customer 360
   ↓
Feature engineering
   ↓
Training / serving datasets
   ↓
Embeddings + retrieval index
   ↓
RAG / AI analytics
   ↓
Evaluation + observability
```

## Interview questions to prepare

### Streaming

**Q: Why use event time instead of processing time?**  
A: Business behavior happened at event time. Processing time can distort windows when events arrive late.

**Q: What does the watermark do?**  
A: It defines how long the streaming engine waits for late event-time data and bounds state retention.

**Q: Does Kafka + Spark automatically give exactly-once end to end?**  
A: No. The platform should reason about delivery semantics at every boundary and make downstream writes idempotent.

### Data modeling

**Q: Why SCD Type 2?**  
A: It preserves historical customer attributes so analysis can reproduce the state that existed at a point in time.

**Q: How do you prevent funnel double counting?**  
A: Define the metric grain explicitly and count distinct users at each funnel stage instead of raw events.

### Performance

**Q: What happens when a customer becomes a hot key?**  
A: Investigate skew, partition strategy, state size, and whether the aggregation can be split into pre-aggregation and a final merge.

**Q: How do you avoid recomputing the entire Gold layer?**  
A: Track affected dates/keys from each incremental batch and recompute only the impacted partitions, while retaining a controlled backfill path.

### Recovery

**Q: How would you recover from bad data?**  
A: Preserve raw Bronze data, quarantine invalid records, fix the transformation or contract, then replay the affected range with a controlled backfill.

### AI

**Q: Why is this a good AI data platform?**  
A: AI quality depends on trustworthy source data. Customer 360, historical state, event lineage, freshness, and feature consistency provide a reliable substrate for models and retrieval systems.

## What not to claim

Do not claim that the repository currently has a production Kafka cluster, live Databricks workspace, Power BI dashboard, feature store, or RAG service. Those are the next engineering phases unless the repository contains the corresponding implementation.
