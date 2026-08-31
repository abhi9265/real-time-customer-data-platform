from src.producer.event_generator import (
    generate_event,
    generate_events,
    serialize_event,
)


def test_event_has_contract_identity_fields():
    event = generate_event("product_viewed", "usr_1001", seed=7)

    assert event["event_type"] == "product_viewed"
    assert event["event_version"] == 1
    assert event["user_id"] == "usr_1001"
    assert event["event_id"]


def test_generator_is_repeatable_for_replay_tests():
    first = list(generate_events(10, seed=123))
    second = list(generate_events(10, seed=123))

    assert [(e["event_id"], e["event_type"]) for e in first] == [
        (e["event_id"], e["event_type"]) for e in second
    ]


def test_event_serializes_to_utf8_json():
    event = generate_event("user_login", "usr_1001", seed=1)
    payload = serialize_event(event)

    assert isinstance(payload, bytes)
    assert b'"event_type":"user_login"' in payload
