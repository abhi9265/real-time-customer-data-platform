"""Kafka producer wrapper with delivery callbacks and retry-safe keys."""

from __future__ import annotations

from typing import Callable

from confluent_kafka import Producer

from src.producer.event_generator import serialize_event


class EventProducer:
    """Small production-shaped Kafka producer abstraction.

    The event_id is used as the Kafka message key so retries preserve the same
    partitioning identity and downstream consumers can deduplicate safely.
    """

    def __init__(self, bootstrap_servers: str, client_id: str = "customer-platform-producer"):
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "acks": "all",
                "enable.idempotence": True,
                "retries": 10,
                "delivery.timeout.ms": 120000,
            }
        )

    def publish(self, topic: str, event: dict, on_delivery: Callable | None = None) -> None:
        """Publish one event using event_id as the stable Kafka key."""
        self._producer.produce(
            topic=topic,
            key=event["event_id"].encode("utf-8"),
            value=serialize_event(event),
            on_delivery=on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        """Wait for outstanding messages and return the number still queued."""
        return self._producer.flush(timeout)
