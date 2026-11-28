"""Test idempotent control-batch finalisation."""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pytest

AIRFLOW_ROOT = Path(__file__).resolve().parents[1] / "airflow"
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from helpers.scripts import finalize_audit


@pytest.mark.unit
def test_finalizer_only_calls_control_function(monkeypatch: pytest.MonkeyPatch) -> None:
    """Leave append-only audit tables untouched."""
    statements: list[str] = []

    class _Connection:
        def execute(self, statement: object, params: object) -> None:
            statements.append(str(statement))

    class _Engine:
        def begin(self) -> _Context:
            return _Context()

    class _Context:
        def __enter__(self) -> _Connection:
            return _Connection()

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr(finalize_audit, "get_postgres_engine", _Engine)

    assert finalize_audit.finalize_batch(7, "SUCCEEDED") is True
    assert "control.finish_batch" in statements[0]
    assert "audit." not in statements[0].lower()
    assert "update" not in statements[0].lower()
    assert "delete" not in statements[0].lower()


@pytest.mark.unit
def test_start_batch_accepts_airflow_rerun_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cleared task can reuse the same DAG run identity."""
    calls: list[dict[str, object]] = []

    class _Result:
        def scalar_one(self) -> int:
            return 12

    class _Connection:
        def execute(self, statement: object, params: dict[str, object]) -> _Result:
            calls.append(params)
            return _Result()

    class _Engine:
        def begin(self) -> _Context:
            return _Context()

    class _Context:
        def __enter__(self) -> _Connection:
            return _Connection()

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr(finalize_audit, "get_postgres_engine", _Engine)

    assert finalize_audit.start_batch("fleet_daily_load", "run-1", None) == 12
    assert calls[0]["run_id"] == "run-1"


@pytest.mark.unit
def test_stale_cleanup_flag_accepts_only_explicit_truthy_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep the maintenance switch off unless explicitly enabled."""
    monkeypatch.delenv("FLEET_STALE_CLEANUP_ENABLED", raising=False)
    assert finalize_audit.stale_cleanup_enabled() is False

    for value in ("1", "true", "TRUE", "yes", " YES "):
        monkeypatch.setenv("FLEET_STALE_CLEANUP_ENABLED", value)
        assert finalize_audit.stale_cleanup_enabled() is True

    for value in ("0", "false", "no", "", "on"):
        monkeypatch.setenv("FLEET_STALE_CLEANUP_ENABLED", value)
        assert finalize_audit.stale_cleanup_enabled() is False


@pytest.mark.unit
def test_stale_batch_max_age_reads_configured_hours(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use six hours unless the operator supplies a valid override."""
    monkeypatch.delenv("FLEET_STALE_BATCH_MAX_AGE_HOURS", raising=False)
    assert finalize_audit.stale_batch_max_age() == timedelta(hours=6)

    monkeypatch.setenv("FLEET_STALE_BATCH_MAX_AGE_HOURS", "2.5")
    assert finalize_audit.stale_batch_max_age() == timedelta(hours=2.5)

    monkeypatch.setenv("FLEET_STALE_BATCH_MAX_AGE_HOURS", "bad")
    with pytest.raises(ValueError, match="numeric"):
        finalize_audit.stale_batch_max_age()


@pytest.mark.unit
def test_fail_stale_batches_only_calls_bound_control_function(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Never touch audit tables or interpolate the interval into SQL."""
    statements: list[str] = []
    params: list[dict[str, object]] = []

    class _Result:
        def scalar_one(self) -> int:
            return 4

    class _Connection:
        def execute(self, statement: object, parameters: dict[str, object]) -> _Result:
            statements.append(str(statement))
            params.append(parameters)
            return _Result()

    class _Engine:
        def begin(self) -> _Context:
            return _Context()

    class _Context:
        def __enter__(self) -> _Connection:
            return _Connection()

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr(finalize_audit, "get_postgres_engine", _Engine)
    monkeypatch.delenv("FLEET_STALE_BATCH_MAX_AGE_HOURS", raising=False)

    assert finalize_audit.fail_stale_batches() == 4
    assert statements == ["SELECT control.fail_stale_batches(:max_age)"]
    assert params[0]["max_age"] == timedelta(hours=6)
    assert "audit." not in statements[0].lower()

    monkeypatch.setenv("FLEET_STALE_BATCH_MAX_AGE_HOURS", "bad")
    assert finalize_audit.fail_stale_batches(timedelta(hours=2)) == 4
    assert params[-1]["max_age"] == timedelta(hours=2)
