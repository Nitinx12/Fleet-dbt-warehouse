"""Expand one source delivery row into ordered lifecycle events."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any


class EventExpansionError(ValueError):
    """Raised when a source row cannot produce contract-valid events."""


def parse_source_timestamp(value: Any) -> datetime:
    """Parse a source timestamp and return a UTC-aware datetime."""
    if not isinstance(value, str) or not value.strip():
        raise EventExpansionError("timestamp must be a non-empty string")
    raw = value.strip()
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_flag(value: Any) -> bool:
    """Normalise the source on-time flag without guessing."""
    if isinstance(value, bool):
        return value
    normalised = str(value).strip().lower()
    if normalised in {"true", "1", "yes", "y"}:
        return True
    if normalised in {"false", "0", "no", "n"}:
        return False
    raise EventExpansionError("on_time_flag is not a recognised boolean")


def parse_detention(value: Any) -> int:
    """Parse detention minutes as a non-negative integer."""
    try:
        minutes = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise EventExpansionError("detention_minutes is not an integer") from exc
    if minutes < 0:
        raise EventExpansionError("detention_minutes must be non-negative")
    return minutes


def _text(row: Mapping[str, Any], name: str) -> str:
    value = row.get(name)
    if not isinstance(value, str) or not value.strip():
        raise EventExpansionError(f"{name} must be a non-empty string")
    return value.strip()


def stop_type(value: Any) -> str:
    """Return the batch stop type in its canonical uppercase form."""
    normalised = str(value).strip().upper().replace(" ", "_")
    if normalised not in {"PICKUP", "DELIVERY"}:
        raise EventExpansionError("event_type is not a supported stop type")
    return normalised


def dedupe_key(event: Mapping[str, Any]) -> tuple[str, str]:
    """Return the idempotency key for one lifecycle event."""
    return str(event["event_id"]), str(event["event_type"])


def event_time(event: Mapping[str, Any]) -> datetime:
    """Return the contract-declared event time."""
    return parse_source_timestamp(event["event_time"])


def expand_source_row(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return ARRIVED and DEPARTED events for one source row."""
    event_id = _text(row, "event_id")
    scheduled = parse_source_timestamp(row.get("scheduled_datetime"))
    arrived = parse_source_timestamp(row.get("actual_datetime"))
    detention = parse_detention(row.get("detention_minutes"))
    departed = arrived + timedelta(minutes=detention)
    shared = {
        "event_id": event_id,
        "load_id": _text(row, "load_id"),
        "trip_id": _text(row, "trip_id"),
        "stop_type": stop_type(row.get("event_type")),
        "facility_id": _text(row, "facility_id"),
        "scheduled_datetime": scheduled.isoformat(),
        "actual_datetime": arrived.isoformat(),
        "detention_minutes": detention,
        "on_time_flag": parse_flag(row.get("on_time_flag")),
        "location_city": _text(row, "location_city"),
        "location_state": _text(row, "location_state"),
        "loaded_at": parse_source_timestamp(row.get("loaded_at")).isoformat(),
    }
    return [
        {**shared, "event_type": "ARRIVED", "event_time": arrived.isoformat()},
        {**shared, "event_type": "DEPARTED", "event_time": departed.isoformat()},
    ]
