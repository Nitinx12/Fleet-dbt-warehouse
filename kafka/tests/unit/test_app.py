"""Unit tests for the Spark application's checkpoint and registration helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from fleet_stream.contract import load_contract
from fleet_stream.job import app
from fleet_stream.settings import from_env

pytestmark = pytest.mark.unit


def test_watermark_round_trip(tmp_path: Path) -> None:
    """Watermarks survive process restarts through UTC JSON files."""
    value = datetime(2026, 1, 1, 12, 30, tzinfo=UTC)

    app.write_watermark(tmp_path, value)

    assert app.read_watermark(tmp_path) == value


def test_watermark_is_none_before_first_commit(tmp_path: Path) -> None:
    """A fresh checkpoint volume has no event-time state."""
    assert app.read_watermark(tmp_path) is None


def test_as_utc_treats_naive_spark_timestamps_as_utc() -> None:
    """Spark collect() returns naive datetimes; compare them as UTC."""
    naive = datetime(2026, 1, 1, 8, 0)

    assert app.as_utc(naive) == datetime(2026, 1, 1, 8, 0, tzinfo=UTC)
    assert app.as_utc(datetime(2026, 1, 1, 8, 0, tzinfo=UTC)) == datetime(
        2026, 1, 1, 8, 0, tzinfo=UTC
    )


def test_watermark_round_trip_accepts_naive_values(tmp_path: Path) -> None:
    """A naive batch max can be persisted against an aware checkpoint value."""
    app.write_watermark(tmp_path, datetime(2026, 1, 1, 8, 0))
    previous = app.read_watermark(tmp_path)
    batch_max = app.as_utc(datetime(2026, 1, 1, 9, 0))

    assert previous is not None
    assert max(previous, batch_max) == batch_max


def test_kafka_source_options_use_prefixed_topic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The source subscribes only to the environment-prefixed contract topic."""
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("KAFKA_TOPIC_PREFIX", "dev.")
    settings = from_env()

    options = app.kafka_source_options(
        settings, settings.topic("fleet.delivery_events.v1")
    )

    assert options["subscribe"] == "dev.fleet.delivery_events.v1"
    assert options["startingOffsets"] == "earliest"
    assert options["failOnDataLoss"] == "false"


def test_assert_registered_refuses_missing_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The job must not start before the registry row is enabled."""
    monkeypatch.setattr(app, "fetchone", lambda sql, params: None)
    contract = load_contract()

    with pytest.raises(app.StreamJobError, match="not ENABLED"):
        app.assert_registered(from_env(), contract)


def test_assert_registered_accepts_enabled_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ENABLED registry row allows the supervised job to start."""
    monkeypatch.setattr(app, "fetchone", lambda sql, params: ("ENABLED",))
    contract = load_contract()

    app.assert_registered(from_env(), contract)
