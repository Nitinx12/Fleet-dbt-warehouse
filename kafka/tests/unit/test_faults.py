"""Unit tests for deterministic replay fault injection."""

from __future__ import annotations

from typing import Any

import pytest
from fleet_stream.contract import load_contract, validate_event
from fleet_stream.events import expand_source_row
from fleet_stream.producer.faults import FaultInjector
from fleet_stream.settings import SettingsError, from_env

pytestmark = pytest.mark.unit

ROW = {
    "event_id": "evt-1",
    "load_id": "load-1",
    "trip_id": "trip-1",
    "event_type": "Pickup",
    "facility_id": "FAC-1",
    "scheduled_datetime": "2026-01-01 08:00:00.000000",
    "actual_datetime": "2026-01-01 08:05:00.000000",
    "detention_minutes": "0",
    "on_time_flag": "False",
    "location_city": "Pune",
    "location_state": "MH",
    "loaded_at": "2026-01-01T08:30:00Z",
}


def _settings(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> Any:
    defaults = {"ENV": "dev", "KAFKA_TOPIC_PREFIX": "dev.", "STREAM_SOURCE": "fixture"}
    for key, value in (defaults | overrides).items():
        monkeypatch.setenv(key, value)
    return from_env()


def test_fault_injector_creates_valid_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A duplicate fraction sends the same key and payload twice."""
    contract = load_contract()
    settings = _settings(monkeypatch, STREAM_DUPLICATE_FRACTION="1")
    events = expand_source_row(ROW)

    planned = FaultInjector(settings, contract.allowed_lateness_minutes).plan(events)

    assert len(planned) == 4
    assert all(item.duplicate for item in planned)
    assert all(validate_event(item.event, contract).ok for item in planned)


def test_fault_injector_makes_late_events_after_watermark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Late faults move later events behind the observed watermark."""
    contract = load_contract()
    settings = _settings(monkeypatch, STREAM_LATE_FRACTION="1")
    first = expand_source_row(ROW)
    second = {
        **first[0],
        "event_id": "evt-2",
        "event_time": "2026-01-01T09:05:00+00:00",
    }
    events = first + [second]

    planned = FaultInjector(settings, contract.allowed_lateness_minutes).plan(events)

    assert all(item.late for item in planned)
    assert planned[-1].event["event_time"] < planned[0].event["event_time"]
    assert all(validate_event(item.event, contract).ok for item in planned)


def test_fault_injector_refuses_production(monkeypatch: pytest.MonkeyPatch) -> None:
    """No fault fraction is accepted when ENV=prod."""
    with pytest.raises(SettingsError, match="fault injection is refused"):
        _settings(monkeypatch, ENV="prod", STREAM_LATE_FRACTION="0.5")
