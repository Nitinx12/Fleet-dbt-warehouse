"""Unit tests for the strict stream contract boundary."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml
from fleet_stream.contract import (
    CONTRACT_PATH,
    ContractError,
    alignment_issues,
    load_contract,
    validate_event,
)

pytestmark = pytest.mark.unit


def _event() -> dict[str, object]:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return {
        "event_id": "evt-1",
        "load_id": "load-1",
        "trip_id": "trip-1",
        "stop_type": "PICKUP",
        "facility_id": "FAC-1",
        "scheduled_datetime": now.isoformat(),
        "actual_datetime": (now + timedelta(minutes=5)).isoformat(),
        "detention_minutes": 0,
        "on_time_flag": False,
        "location_city": "Pune",
        "location_state": "MH",
        "loaded_at": now.isoformat(),
        "event_type": "ARRIVED",
        "event_time": (now + timedelta(minutes=5)).isoformat(),
    }


def test_validate_event_accepts_a_contract_event() -> None:
    """A complete typed event must pass with no reasons."""
    result = validate_event(_event())

    assert result.ok
    assert result.reasons == ()


def test_validate_event_rejects_extra_weak_and_out_of_range_fields() -> None:
    """Strict validation must reject every schema deviation."""
    event = _event()
    event["event_type"] = "CREATED"
    event["detention_minutes"] = "7"
    event["license_number"] = "MH-01-12345678901"

    result = validate_event(event)

    assert not result.ok
    assert "unknown field: license_number" in result.reasons
    assert "wrong type: detention_minutes expected int" in result.reasons
    assert "value outside accepted set: event_type" in result.reasons


def test_load_contract_refuses_pii_columns(tmp_path: Path) -> None:
    """A forbidden column can never enter the stream contract."""
    data = yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))
    data["columns"].append(
        {"name": "license_number", "raw_type": "string", "staged_type": "string"}
    )
    path = tmp_path / "pii.yml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")

    with pytest.raises(ContractError, match="forbidden column"):
        load_contract(path)


def test_alignment_issues_detects_realtime_in_dbt_sources(tmp_path: Path) -> None:
    """The batch project must not declare the disposable stream schema."""
    source = tmp_path / "sources.yml"
    source.write_text("version: 2\nsources:\n  - name: realtime\n", encoding="utf-8")

    issues = alignment_issues(dbt_sources_path=source)

    assert issues == ["dbt bronze sources must not declare realtime"]
