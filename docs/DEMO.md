# Local Streaming Demo

This repository separates transformation verification from production Kafka infrastructure.

## Run the verified local path

```bash
python -m pip install -e .[test]
pytest
```

The tests use local Spark to verify streaming-oriented transformations, event contracts, quality handling and customer-state logic.

## Event lifecycle

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

Kafka remains an explicit production integration boundary. A managed Kafka/Spark deployment and end-to-end load test should be added only when an actual infrastructure environment is available; this repository does not fabricate that evidence.
