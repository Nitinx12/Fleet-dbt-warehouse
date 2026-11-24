"""Parse and validate Kafka and stream environment settings."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


class SettingsError(ValueError):
    """Raised when a streaming environment variable is invalid."""


def _text(name: str, default: str) -> str:
    value = os.getenv(name, default)
    return value.strip()


def _integer(name: str, default: int, *, minimum: int = 1) -> int:
    raw = _text(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(f"{name} must be an integer") from exc
    if value < minimum:
        raise SettingsError(f"{name} must be >= {minimum}")
    return value


def _fraction(name: str, default: float = 0.0) -> float:
    raw = _text(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise SettingsError(f"{name} must be a number") from exc
    if not 0 <= value <= 1:
        raise SettingsError(f"{name} must be between 0 and 1")
    return value


def _percentage(name: str, default: float) -> float:
    raw = _text(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise SettingsError(f"{name} must be a number") from exc
    if value < 0:
        raise SettingsError(f"{name} must be >= 0")
    return value


@dataclass(frozen=True)
class StreamSettings:
    """Validated runtime configuration shared by stream components."""

    env: str
    bootstrap_servers: str
    topic_prefix: str
    source: str
    replay_speed: float
    late_fraction: float
    duplicate_fraction: float
    seed: int | None
    window_minutes: int
    watermark_minutes: int
    trigger_seconds: int
    detention_threshold_minutes: int
    checkpoint_dir: Path
    lag_threshold: int
    freshness_threshold_minutes: int
    raw_tolerance_pct: float
    window_tolerance_pct: float
    min_window_age_hours: int

    def topic(self, base_name: str) -> str:
        """Apply the environment prefix to a topic name."""
        return f"{self.topic_prefix}{base_name}" if self.topic_prefix else base_name

    def require_faults_allowed(self) -> None:
        """Refuse seeded faults in production."""
        if self.env == "prod" and (
            self.late_fraction > 0 or self.duplicate_fraction > 0
        ):
            raise SettingsError("fault injection is refused when ENV=prod")


def from_env() -> StreamSettings:
    """Build validated settings from environment variables."""
    source = _text("STREAM_SOURCE", "live")
    if source not in {"live", "fixture"}:
        raise SettingsError("STREAM_SOURCE must be live or fixture")

    env = _text("ENV", "dev")
    if env not in {"dev", "ci", "prod"}:
        raise SettingsError("ENV must be dev, ci or prod")

    seed_raw = _text("STREAM_SEED", "")
    seed = int(seed_raw) if seed_raw else None
    settings = StreamSettings(
        env=env,
        bootstrap_servers=_text("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic_prefix=_text("KAFKA_TOPIC_PREFIX", "dev."),
        source=source,
        replay_speed=_percentage("STREAM_REPLAY_SPEED", 0.0),
        late_fraction=_fraction("STREAM_LATE_FRACTION"),
        duplicate_fraction=_fraction("STREAM_DUPLICATE_FRACTION"),
        seed=seed,
        window_minutes=_integer("STREAM_WINDOW_MINUTES", 15),
        watermark_minutes=_integer("STREAM_WATERMARK_MINUTES", 10),
        trigger_seconds=_integer("STREAM_TRIGGER_SECONDS", 30),
        detention_threshold_minutes=_integer("STREAM_DETENTION_THRESHOLD_MINUTES", 1),
        checkpoint_dir=Path(
            _text("STREAM_CHECKPOINT_DIR", "/var/lib/fleet-stream/checkpoints")
        ),
        lag_threshold=_integer("KAFKA_LAG_THRESHOLD", 1000, minimum=0),
        freshness_threshold_minutes=_integer("STREAM_FRESHNESS_THRESHOLD_MINUTES", 15),
        raw_tolerance_pct=_percentage("STREAM_RAW_TOLERANCE_PCT", 0),
        window_tolerance_pct=_percentage("STREAM_WINDOW_TOLERANCE_PCT", 5),
        min_window_age_hours=_integer("STREAM_MIN_WINDOW_AGE_HOURS", 24),
    )
    settings.require_faults_allowed()
    return settings
