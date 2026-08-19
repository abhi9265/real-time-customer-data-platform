"""Deterministic, contract-shaped customer event generator."""
from __future__ import annotations
import json
import random
import uuid
from datetime import datetime, timezone
from typing import Iterator

EVENT_TYPES = ("user_registered", "user_login", "product_viewed", "cart_added", "checkout_started", "order_created", "payment_completed")


def _business_properties(event_type: str, rng: random.Random) -> dict:
    if event_type in {"product_viewed", "cart_added"}:
        return {"source": "web", "generator_version": "2.0", "product_id": f"prd_{rng.randint(1000, 9999)}"}
    if event_type in {"order_created", "payment_completed"}:
        return {"source": "web", "generator_version": "2.0", "order_id": f"ord_{rng.randint(100000, 999999)}", "amount": round(rng.uniform(299, 9999), 2), "currency": "INR", "payment_method": rng.choice(["card", "upi", "wallet"])}
    return {"source": "web", "generator_version": "2.0"}


def generate_event(event_type: str, user_id: str, seed: int | None = None) -> dict:
    """Create one deterministic event with realistic commerce attributes."""
    if event_type not in EVENT_TYPES:
        raise ValueError(f"Unsupported event_type: {event_type}")
    rng = random.Random(seed)
    identity = f"{user_id}:{event_type}:{seed}"
    event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, identity))
    return {
        "event_id": event_id, "event_type": event_type, "event_version": 1,
        "event_timestamp": datetime.now(timezone.utc).isoformat(), "user_id": user_id,
        "product_id": f"prd_{rng.randint(1000, 9999)}" if event_type in {"product_viewed", "cart_added"} else None,
        "session_id": f"ses_{rng.randint(100000, 999999)}", "source": "web",
        "trace_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"trace:{identity}")),
        "properties": _business_properties(event_type, rng),
    }


def generate_events(count: int, seed: int = 42) -> Iterator[dict]:
    """Yield repeatable traffic for local development and replay tests."""
    rng = random.Random(seed)
    for index in range(count):
        yield generate_event(EVENT_TYPES[rng.randrange(len(EVENT_TYPES))], f"usr_{rng.randint(1000, 9999)}", seed + index)


def serialize_event(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":"), sort_keys=True).encode("utf-8")
