"""Unit tests for delivery lifecycle event expansion."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from fleet_stream.contract import load_contract, validate_event
from fleet_stream.events import (
    EventExpansionError,
    dedupe_key,
    event_time,
    expand_source_row,
    parse_flag,
    parse_source_timestamp,
    stop_type,
)

pytestmark = pytest.mark.unit


def _source_row(detention: int = 0) -> dict[str, Any]:
    return {
        "event_id": "evt-1",
        "load_id": "load-1",
        "trip_id": "trip-1",
        "event_type": "Pickup",
        "facility_id": "FAC-1",
        "scheduled_datetime": "2026-01-01 08:00:00.000000",
        "actual_datetime": "2026-01-01 08:05:00.000000",
        "detention_minutes": str(detention),
        "on_time_flag": "False",
        "location_city": "Pune",
        "location_state": "MH",
        "loaded_at": "2026-01-01T08:30:00Z",
    }


def test_expand_source_row_creates_valid_ordered_lifecycle() -> None:
    """ARRIVED uses actual time and DEPARTED adds detention minutes."""
    contract = load_contract()
    events = expand_source_row(_source_row(62))

    assert [event["event_type"] for event in events] == ["ARRIVED", "DEPARTED"]
    assert events[0]["event_time"] == "2026-01-01T08:05:00+00:00"
    assert events[1]["event_time"] == "2026-01-01T09:07:00+00:00"
    assert events[1]["stop_type"] == "PICKUP"
    assert dedupe_key(events[0]) == ("evt-1", "ARRIVED")
    assert dedupe_key(events[1]) == ("evt-1", "DEPARTED")
    assert all(validate_event(event, contract).ok for event in events)


def test_event_time_is_always_utc_aware() -> None:
    """A naive source timestamp is interpreted as UTC."""
    expanded = expand_source_row(_source_row())
    value = event_time(expanded[0])

    assert value.tzinfo == UTC
    assert value == datetime(2026, 1, 1, 8, 5, tzinfo=UTC)


def test_helpers_reject_unrecognised_values() -> None:
    """Unknown flags, stop types and negative detention fail fast."""
    with pytest.raises(EventExpansionError):
        parse_flag("maybe")
    with pytest.raises(EventExpansionError):
        stop_type("DELIVERY NOTE")
    with pytest.raises(EventExpansionError):
        expand_source_row(_source_row(-1))


def test_parse_source_timestamp_normalises_zulu() -> None:
    """Zulu timestamps parse as UTC."""
    parsed = parse_source_timestamp("2026-01-01T08:05:00Z")

    assert parsed == datetime(2026, 1, 1, 8, 5, tzinfo=UTC)
    assert parsed.tzinfo == UTC
