"""Unit tests for stream settings validation."""

from __future__ import annotations

import pytest
from fleet_stream.settings import SettingsError, from_env

pytestmark = pytest.mark.unit

KEYS = (
    "ENV",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC_PREFIX",
    "KAFKA_LAG_THRESHOLD",
    "STREAM_SOURCE",
    "STREAM_REPLAY_SPEED",
    "STREAM_LATE_FRACTION",
    "STREAM_DUPLICATE_FRACTION",
    "STREAM_SEED",
    "STREAM_WINDOW_MINUTES",
    "STREAM_WATERMARK_MINUTES",
    "STREAM_TRIGGER_SECONDS",
    "STREAM_DETENTION_THRESHOLD_MINUTES",
    "STREAM_CHECKPOINT_DIR",
    "STREAM_FRESHNESS_THRESHOLD_MINUTES",
    "STREAM_MIN_WINDOW_AGE_HOURS",
    "STREAM_RAW_TOLERANCE_PCT",
    "STREAM_WINDOW_TOLERANCE_PCT",
)


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove stream variables before each test."""
    for key in KEYS:
        monkeypatch.delenv(key, raising=False)


def test_from_env_applies_topic_prefix_and_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use safe non-production defaults and a consistent topic prefix."""
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("KAFKA_TOPIC_PREFIX", "dev.")
    settings = from_env()

    assert settings.env == "dev"
    assert settings.bootstrap_servers == "localhost:9094"
    assert settings.topic("fleet.delivery_events.v1") == "dev.fleet.delivery_events.v1"
    assert settings.window_minutes == 15
    assert settings.watermark_minutes == 10
    assert settings.detention_threshold_minutes == 1
    assert settings.raw_tolerance_pct == 0
    assert settings.window_tolerance_pct == 5


def test_from_env_refuses_fault_injection_in_prod(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production must never publish seeded late or duplicate events."""
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("STREAM_LATE_FRACTION", "0.2")

    with pytest.raises(SettingsError, match="fault injection is refused"):
        from_env()


def test_from_env_rejects_out_of_range_fraction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject invalid probabilities before any producer starts."""
    monkeypatch.setenv("STREAM_DUPLICATE_FRACTION", "1.5")

    with pytest.raises(SettingsError, match="between 0 and 1"):
        from_env()
