import json
from pathlib import Path

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[2]


def load_schema(filename: str) -> dict:
    return json.loads((ROOT / "schemas" / "events" / filename).read_text())


def test_product_event_contract_accepts_valid_event():
    schema = load_schema("user_interaction.v1.json")
    event = {
        "event_id": "evt-001",
        "event_type": "product_viewed",
        "event_version": 1,
        "event_timestamp": "2026-08-18T10:00:00Z",
        "user_id": "usr-001",
        "session_id": "ses-001",
        "product_id": "prd-001",
        "properties": {"source": "web"},
    }
    Draft202012Validator(schema).validate(event)


def test_order_contract_rejects_negative_amount():
    schema = load_schema("order_created.v1.json")
    event = {
        "event_id": "evt-002",
        "event_type": "order_created",
        "event_version": 1,
        "event_timestamp": "2026-08-18T10:00:00Z",
        "user_id": "usr-001",
        "order_id": "ord-001",
        "currency": "USD",
        "total_amount": -10,
    }
    errors = list(Draft202012Validator(schema).iter_errors(event))
    assert errors
