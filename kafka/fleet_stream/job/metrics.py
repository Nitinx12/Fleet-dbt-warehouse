"""Append batch and watchdog health evidence to audit.stream_health."""

from __future__ import annotations

from dataclasses import dataclass

from fleet_stream.job.db import execute

INSERT_HEALTH = """
INSERT INTO audit.stream_health (
    stream_name, topic, source, partition_lag, newest_event_age_seconds,
    rows_raw, rows_window, rows_alert, rejected, late_dropped, message
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


@dataclass(frozen=True)
class HealthMetrics:
    """Counts and probe values for one health record."""

    rows_raw: int = 0
    rows_window: int = 0
    rows_alert: int = 0
    rejected: int = 0
    late_dropped: int = 0
    partition_lag: int | None = None
    newest_event_age_seconds: float | None = None
    message: str | None = None


def write_health(
    stream_name: str, topic: str, source: str, metrics: HealthMetrics
) -> None:
    """Write one append-only health row using an accepted source label."""
    if source not in {"JOB", "WATCHDOG"}:
        raise ValueError("source must be JOB or WATCHDOG")
    execute(
        INSERT_HEALTH,
        (
            stream_name,
            topic,
            source,
            metrics.partition_lag,
            metrics.newest_event_age_seconds,
            metrics.rows_raw,
            metrics.rows_window,
            metrics.rows_alert,
            metrics.rejected,
            metrics.late_dropped,
            metrics.message,
        ),
    )
