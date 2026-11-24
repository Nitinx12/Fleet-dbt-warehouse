"""Unit tests for observational stream reconciliation logic."""

from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any

import pytest
from fleet_stream.monitor import reconcile as reconcile_module
from fleet_stream.settings import from_env

pytestmark = pytest.mark.unit


def test_date_key_sql_reexpresses_gold_key() -> None:
    """Reconciliation uses the same integer date key as the gold layer."""
    assert reconcile_module.date_key_sql("scheduled_datetime") == (
        "COALESCE(TO_CHAR(CAST(scheduled_datetime AS DATE), 'YYYYMMDD')::INTEGER, -1)"
    )


def test_date_key_sql_refuses_unsafe_columns() -> None:
    """Only plain column identifiers may be interpolated into SQL."""
    with pytest.raises(ValueError, match="invalid date column"):
        reconcile_module.date_key_sql(
            "facility_id; DROP TABLE gold.fct_delivery_events"
        )


def test_classify_uses_valid_audit_statuses_and_drift_messages() -> None:
    """Tolerance breaches stay within the audit helper's accepted statuses."""
    match = reconcile_module.classify("raw", 10, 10, 0)
    within = reconcile_module.classify("window", 105, 100, 5)
    mismatch = reconcile_module.classify("window", 110, 100, 5)

    assert match["status"] == "MATCH"
    assert within["status"] == "WITHIN_TOLERANCE"
    assert within["message"].startswith("DRIFT")
    assert mismatch["status"] == "MISMATCH"
    assert mismatch["message"].startswith("DRIFT")


def test_build_comparisons_compares_raw_and_window_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both stream views are compared per facility and business date."""
    key = ("FAC-1", 20260101)
    monkeypatch.setattr(
        reconcile_module,
        "raw_counts",
        lambda contract, hours: {key: {"events": 10, "on_time": 8, "detention": 3}},
    )
    monkeypatch.setattr(
        reconcile_module,
        "window_counts",
        lambda hours: {key: {"events": 9, "on_time": 7, "detention": 3}},
    )
    monkeypatch.setattr(
        reconcile_module,
        "gold_counts",
        lambda hours: {key: {"events": 10, "on_time": 8, "detention": 3}},
    )

    rows = reconcile_module.build_comparisons(
        reconcile_module.load_contract(), from_env()
    )

    assert len(rows) == 2
    assert rows[0]["status"] == "MATCH"
    assert rows[0]["target_layer"] == "gold"
    assert rows[1]["status"] == "MISMATCH"
    assert rows[1]["source_object"] == "realtime.facility_service_window"


def test_reconcile_skips_with_audit_note_when_no_stream_is_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A disabled speed layer is reported without failing the daily batch."""
    calls: list[dict[str, Any]] = []
    fake_audit = SimpleNamespace(
        log_error=lambda **kwargs: calls.append(kwargs),
        log_reconciliation=lambda batch_id, comparisons: len(comparisons),
    )
    monkeypatch.setitem(sys.modules, "utils.audit", fake_audit)
    monkeypatch.setattr(reconcile_module, "fetchone", lambda sql, params: None)

    assert (
        reconcile_module.reconcile(
            42,
            from_env(),
            airflow_dag_id="custom_dag",
            airflow_run_id="run-42",
        )
        == 0
    )
    assert calls[0]["error_code"] == "STREAM_SKIPPED"
    assert calls[0]["batch_id"] == 42
    assert calls[0]["airflow_dag_id"] == "custom_dag"
    assert "run-42" in calls[0]["error_message"]


def test_reconcile_links_comparisons_to_airflow_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every comparison carries the daily DAG run that produced it."""
    captured: list[dict[str, Any]] = []

    def capture(batch_id: int, comparisons: list[dict[str, Any]]) -> int:
        captured.extend(comparisons)
        return len(comparisons)

    fake_audit = SimpleNamespace(
        log_error=lambda **kwargs: None, log_reconciliation=capture
    )
    monkeypatch.setitem(sys.modules, "utils.audit", fake_audit)
    monkeypatch.setattr(
        reconcile_module,
        "fetchone",
        lambda sql, params: ("ENABLED", "dev.fleet.delivery_events.v1"),
    )
    monkeypatch.setattr(
        reconcile_module,
        "build_comparisons",
        lambda contract, settings: [
            {"message": "raw: counts match", "status": "MATCH"}
        ],
    )

    assert reconcile_module.reconcile(42, from_env(), airflow_run_id="manual__42") == 1
    assert captured[0]["message"].endswith("airflow_run_id=manual__42")
