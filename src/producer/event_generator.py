"""Deterministic product-event generator for local Kafka development.

The generator produces contract-shaped events without requiring a live Kafka
cluster, making it useful for unit tests, local development, and replay tests.
"""

from __future__ import annotations

import json
import random
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime

EVENT_TYPES = (
    "user_registered",
    "user_login",
    "product_viewed",
    "cart_added",
    "checkout_started",
    "order_created",
    "payment_completed",
)


def generate_event(event_type: str, user_id: str, seed: int | None = None) -> dict:
    """Create one deterministic, contract-compatible event."""
    rng = random.Random(seed)
    event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{user_id}:{event_type}:{seed}"))
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_version": 1,
        "event_timestamp": datetime.now(UTC).isoformat(),
        "user_id": user_id,
        "product_id": f"prd_{rng.randint(1000, 9999)}" if event_type in {"product_viewed", "cart_added"} else None,
        "session_id": f"ses_{rng.randint(100000, 999999)}",
        "properties": {"source": "web", "generator_version": "1.0"},
    }


def generate_events(count: int, seed: int = 42) -> Iterator[dict]:
    """Yield repeatable test traffic for a bounded number of events."""
    rng = random.Random(seed)
    for index in range(count):
        event_type = EVENT_TYPES[rng.randrange(len(EVENT_TYPES))]
        user_id = f"usr_{rng.randint(1000, 9999)}"
        yield generate_event(event_type, user_id, seed + index)


def serialize_event(event: dict) -> bytes:
    """Serialize an event as compact UTF-8 JSON for a Kafka value."""
    return json.dumps(event, separators=(",", ":"), sort_keys=True).encode("utf-8")
