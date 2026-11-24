"""Read the latest watchdog health row for stream alerting."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text

from utils.connection import get_postgres_engine

LATEST_HEALTH = """
SELECT message, partition_lag, newest_event_age_seconds
  FROM audit.stream_health
 WHERE stream_name = :stream_name AND source = 'WATCHDOG'
 ORDER BY observed_at DESC
 LIMIT 1
"""


@dataclass(frozen=True)
class StreamHealth:
    """The latest watchdog conclusion for one stream."""

    message: str | None
    partition_lag: int | None
    newest_event_age_seconds: float | None


def latest_watchdog_health(stream_name: str = "delivery_events") -> StreamHealth | None:
    """Return the newest watchdog row, or None before the first probe."""
    with get_postgres_engine().connect() as connection:
        row = (
            connection.execute(text(LATEST_HEALTH), {"stream_name": stream_name})
            .mappings()
            .first()
        )
    if row is None:
        return None
    return StreamHealth(
        message=row["message"],
        partition_lag=row["partition_lag"],
        newest_event_age_seconds=row["newest_event_age_seconds"],
    )
