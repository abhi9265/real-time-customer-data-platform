from src.producer.event_generator import generate_event
from src.observability.pipeline_audit import build_audit_record


def test_generated_payment_event_contains_business_payload():
    event = generate_event("payment_completed", "usr_1001", seed=7)
    assert event["source"] == "web"
    assert event["trace_id"]
    assert event["properties"]["amount"] > 0
    assert event["properties"]["currency"] == "INR"
    assert event["properties"]["order_id"].startswith("ord_")


def test_audit_record_tracks_pipeline_outcome():
    record = build_audit_record("run-1", "bronze_ingestion", "kafka", 100, 97, 3, "SUCCESS", "2026-08-19T10:00:00Z")
    assert record["records_in"] == 100
    assert record["records_quarantined"] == 3
    assert record["status"] == "SUCCESS"
