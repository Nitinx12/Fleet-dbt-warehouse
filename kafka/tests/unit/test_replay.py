"""Unit tests for the replay driver."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest
from fleet_stream.contract import load_contract
from fleet_stream.producer.replay import replay
from fleet_stream.settings import from_env

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


class FakePublisher:
    """Collect published events in memory."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.invalid: list[tuple[str, dict[str, Any]]] = []
        self.flushed = False

    def publish(self, event: dict[str, Any]) -> None:
        self.events.append(event)

    def publish_invalid(self, reason: str, payload: Mapping[str, Any]) -> None:
        self.invalid.append((reason, dict(payload)))

    def flush(self) -> None:
        self.flushed = True


def _settings(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> Any:
    defaults = {
        "ENV": "dev",
        "KAFKA_TOPIC_PREFIX": "dev.",
        "STREAM_SOURCE": "fixture",
        "STREAM_DUPLICATE_FRACTION": "0",
        "STREAM_LATE_FRACTION": "0",
    }
    for key, value in (defaults | overrides).items():
        monkeypatch.setenv(key, value)
    return from_env()


def test_replay_expands_and_publishes_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One source row produces ARRIVED and DEPARTED events."""
    publisher = FakePublisher()
    stats = replay([ROW], publisher, _settings(monkeypatch), load_contract())

    assert publisher.flushed
    assert stats.source_rows == 1
    assert stats.events == 2
    assert stats.published == 2
    assert [event["event_type"] for event in publisher.events] == [
        "ARRIVED",
        "DEPARTED",
    ]


def test_replay_counts_seeded_duplicates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Duplicate faults are visible in the replay summary."""
    publisher = FakePublisher()
    settings = _settings(monkeypatch, STREAM_DUPLICATE_FRACTION="1")
    stats = replay([ROW], publisher, settings, load_contract())

    assert stats.published == 4
    assert stats.duplicates == 4
    assert (
        len({(event["event_id"], event["event_type"]) for event in publisher.events})
        == 2
    )


def test_replay_honours_pacing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A replay-speed multiplier pauses once per outgoing event."""
    sleeps: list[float] = []
    monkeypatch.setattr("fleet_stream.producer.replay.time.sleep", sleeps.append)
    settings = _settings(monkeypatch, STREAM_REPLAY_SPEED="50")

    replay([ROW], FakePublisher(), settings, load_contract())

    assert sleeps == [0.5, 0.5]


def test_replay_rejects_invalid_source_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid source values are routed to the DLQ, not the event topic."""
    publisher = FakePublisher()
    bad_row = {**ROW, "detention_minutes": "-1"}
    stats = replay([bad_row], publisher, _settings(monkeypatch), load_contract())

    assert stats.rejected == 1
    assert stats.published == 0
    assert publisher.events == []
    assert publisher.invalid[0][0] == "source_row"


def test_replay_honours_time_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    """The from/to window filters events before fault injection."""
    publisher = FakePublisher()
    later = datetime(2027, 1, 1, tzinfo=UTC)
    stats = replay(
        [ROW],
        publisher,
        _settings(monkeypatch),
        load_contract(),
        from_time=later,
    )

    assert stats.events == 0
    assert stats.published == 0
