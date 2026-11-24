"""Unit tests for watchdog lag and freshness probes."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from fleet_stream.contract import load_contract
from fleet_stream.monitor import lag
from fleet_stream.settings import from_env

pytestmark = pytest.mark.unit


def test_lag_from_offsets_uses_largest_partition_backlog() -> None:
    """Lag is the largest high-minus-low offset across partitions."""
    earliest = {"p0": SimpleNamespace(offset=10), "p1": SimpleNamespace(offset=4)}
    latest = {"p0": SimpleNamespace(offset=15), "p1": SimpleNamespace(offset=9)}

    assert lag.lag_from_offsets(earliest, latest) == 5


def test_lag_from_offsets_handles_empty_topic() -> None:
    """A topic with no partitions reports zero without failing."""
    assert lag.lag_from_offsets({}, {}) == 0


def test_probe_skips_streams_that_are_not_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No health row is written until the registry enables the stream."""
    monkeypatch.setattr(lag, "fetchone", lambda sql, params: None)

    def fail_write(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("health must not be written")

    monkeypatch.setattr(lag, "write_health", fail_write)

    assert lag.probe(from_env(), load_contract()) is None


def test_probe_writes_breach_health(monkeypatch: pytest.MonkeyPatch) -> None:
    """Breached lag or freshness is recorded in the append-only health table."""
    monkeypatch.setenv("KAFKA_LAG_THRESHOLD", "10")
    monkeypatch.setenv("STREAM_FRESHNESS_THRESHOLD_MINUTES", "15")
    captured: list[tuple[str, str, str, Any]] = []

    def fake_fetchone(sql: str, params: Any = ()) -> Any:
        if "control.stream" in sql:
            return ("ENABLED", "dev.fleet.delivery_events.v1")
        if "audit.stream_health" in sql:
            return (2, 3, 4, 5, 6)
        raise AssertionError("unexpected query")

    monkeypatch.setattr(lag, "fetchone", fake_fetchone)
    monkeypatch.setattr(lag, "topic_backlog", lambda settings, topic: 11)
    monkeypatch.setattr(lag, "newest_event_age_seconds", lambda: 1_000.0)
    monkeypatch.setattr(
        lag,
        "write_health",
        lambda stream, topic, source, metrics: captured.append(
            (stream, topic, source, metrics)
        ),
    )

    metrics = lag.probe(from_env(), load_contract())

    assert metrics is not None
    assert metrics.partition_lag == 11
    assert metrics.rejected == 5
    assert (metrics.message or "").startswith("BREACH")
    assert captured[0][2] == "WATCHDOG"
