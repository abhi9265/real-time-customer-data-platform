"""Pipeline audit record helpers.

The audit model is intentionally storage-neutral so it can be persisted to
Delta, a warehouse, or a test DataFrame without changing pipeline logic.
"""
from __future__ import annotations
from datetime import datetime, timezone


def build_audit_record(
    run_id: str,
    pipeline_name: str,
    source: str,
    records_in: int,
    records_out: int,
    records_quarantined: int,
    status: str,
    watermark: str | None = None,
    error_message: str | None = None,
) -> dict:
    return {
        "run_id": run_id, "pipeline_name": pipeline_name, "source": source,
        "records_in": records_in, "records_out": records_out,
        "records_quarantined": records_quarantined, "status": status,
        "watermark": watermark, "error_message": error_message,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
