"""Publish deterministic customer events to the local Kafka demo topic."""

import json
import os
import time

from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "customer-events")

EVENTS = [
    {"event_id": "demo-001", "customer_id": "C001", "event_type": "product_view", "product_id": "P100", "event_ts": "2026-09-01T07:00:00Z"},
    {"event_id": "demo-002", "customer_id": "C001", "event_type": "cart_add", "product_id": "P100", "event_ts": "2026-09-01T07:00:03Z"},
    {"event_id": "demo-003", "customer_id": "C002", "event_type": "product_view", "product_id": "P200", "event_ts": "2026-09-01T07:00:05Z"},
    {"event_id": "demo-004", "customer_id": "C002", "event_type": "checkout", "product_id": "P200", "event_ts": "2026-09-01T07:00:08Z"},
    {"event_id": "demo-005", "customer_id": "C002", "event_type": "order", "product_id": "P200", "event_ts": "2026-09-01T07:00:12Z"},
]


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    for event in EVENTS:
        producer.send(TOPIC, value=event).get(timeout=10)
    producer.flush()
    producer.close()
    print(f"Published {len(EVENTS)} events to {TOPIC}")


if __name__ == "__main__":
    for attempt in range(10):
        try:
            main()
            break
        except Exception:
            if attempt == 9:
                raise
            time.sleep(2)
