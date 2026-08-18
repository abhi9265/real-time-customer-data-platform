from pyspark.sql.functions import to_timestamp

from src.streaming.bronze_stream import parse_and_classify_events


def test_valid_event_is_projected(spark):
    kafka = spark.createDataFrame(
        [
            (
                b"evt-1",
                b'{"event_id":"evt-1","event_type":"product_viewed","event_version":1,"event_timestamp":"2026-08-18T10:00:00Z","user_id":"usr-1","product_id":"prd-1","session_id":"ses-1","properties":{"source":"web"}}',
                "customer-events",
                0,
                10,
                "2026-08-18 10:00:01",
            )
        ],
        "key binary, value binary, topic string, partition int, offset long, timestamp string",
    ).withColumn("timestamp", to_timestamp("timestamp"))

    valid, quarantine = parse_and_classify_events(kafka)

    assert valid.count() == 1
    assert quarantine.count() == 0
    assert valid.first()["event_id"] == "evt-1"


def test_malformed_event_is_quarantined(spark):
    kafka = spark.createDataFrame(
        [(b"evt-bad", b"{not-json", "customer-events", 0, 11, "2026-08-18 10:00:01")],
        "key binary, value binary, topic string, partition int, offset long, timestamp string",
    ).withColumn("timestamp", to_timestamp("timestamp"))

    valid, quarantine = parse_and_classify_events(kafka)

    assert valid.count() == 0
    assert quarantine.count() == 1
    assert quarantine.first()["quarantine_reason"] == "INVALID_JSON_OR_SCHEMA"
