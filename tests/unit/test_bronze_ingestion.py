from pyspark.sql import Row

from src.streaming.bronze_ingestion import prepare_bronze


def test_prepare_bronze_parses_contract_event(spark):
    source = spark.createDataFrame([
        Row(
            key="evt-1", topic="customer.events", partition=0, offset=10,
            timestamp=None,
            value=b'{"event_id":"evt-1","event_type":"payment_completed","event_version":1,'
                  b'"event_timestamp":"2026-08-19T10:00:00Z","user_id":"usr-1",'
                  b'"session_id":"ses-1","product_id":null,"source":"web",'
                  b'"trace_id":"trace-1","properties":"{\\"amount\\":100}"}'
        )
    ])
    result = prepare_bronze(source).collect()[0]
    assert result.event_id == "evt-1"
    assert result.event_type == "payment_completed"
    assert result.user_id == "usr-1"
    assert result.trace_id == "trace-1"
