"""Kafka offset probes used by the watchdog health record."""

from __future__ import annotations

from typing import Any

from fleet_stream.contract import Contract
from fleet_stream.job.db import fetchone
from fleet_stream.job.metrics import HealthMetrics, write_health
from fleet_stream.settings import StreamSettings


class StreamMonitorError(RuntimeError):
    """Raised when a stream health probe cannot complete."""


def lag_from_offsets(earliest: dict[Any, Any], latest: dict[Any, Any]) -> int:
    """Turn earliest/latest offset futures into the largest partition backlog."""
    lags = []
    for partition, info in latest.items():
        beginning = earliest[partition].offset
        lags.append(max(int(info.offset) - int(beginning), 0))
    return max(lags, default=0)


def topic_backlog(settings: StreamSettings, topic: str) -> int:
    """Return the largest topic high-minus-low backlog through the admin API."""
    try:
        from confluent_kafka import KafkaException, TopicPartition
        from confluent_kafka.admin import AdminClient, OffsetSpec
    except ImportError as exc:
        raise StreamMonitorError("confluent-kafka is required for lag probes") from exc

    try:
        admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
        metadata = admin.list_topics(topic=topic, timeout=10)
        partitions = metadata.topics[topic].partitions
        keys = [TopicPartition(topic, partition) for partition in partitions]
        earliest = {
            key: future.result(timeout=10)
            for key, future in admin.list_offsets(
                {key: OffsetSpec.earliest() for key in keys}
            ).items()
        }
        latest = {
            key: future.result(timeout=10)
            for key, future in admin.list_offsets(
                {key: OffsetSpec.latest() for key in keys}
            ).items()
        }
    except KafkaException as exc:
        raise StreamMonitorError(f"lag probe failed for {topic}: {exc}") from exc
    return lag_from_offsets(earliest, latest)


REGISTRATION = """
SELECT status, topic
  FROM control.stream
 WHERE stream_name = %s
"""

LATEST_JOB = """
SELECT rows_raw, rows_window, rows_alert, rejected, late_dropped
  FROM audit.stream_health
 WHERE stream_name = %s AND source = 'JOB'
 ORDER BY observed_at DESC
 LIMIT 1
"""

NEWEST_AGE = """
SELECT EXTRACT(EPOCH FROM (now() - max(loaded_at)))
  FROM realtime.delivery_events_raw
"""


def enabled_topic(stream_name: str) -> str | None:
    """Return the registered topic only when the stream is explicitly enabled."""
    row = fetchone(REGISTRATION, (stream_name,))
    if row is None or row[0] != "ENABLED":
        return None
    return str(row[1])


def newest_event_age_seconds() -> float | None:
    """Return the age of the newest landed event using source arrival time."""
    row = fetchone(NEWEST_AGE)
    if row is None or row[0] is None:
        return None
    return max(float(row[0]), 0.0)


def probe(settings: StreamSettings, contract: Contract) -> HealthMetrics | None:
    """Append one watchdog health row and return it; skip unregistered streams."""
    topic = enabled_topic(contract.stream)
    if topic is None:
        return None

    lag = topic_backlog(settings, topic)
    age = newest_event_age_seconds()
    job = fetchone(LATEST_JOB, (contract.stream,))
    breaches = []
    if lag > settings.lag_threshold:
        breaches.append(f"lag={lag} threshold={settings.lag_threshold}")
    if age is not None and age > settings.freshness_threshold_minutes * 60:
        breaches.append(
            f"freshness={int(age)}s threshold={settings.freshness_threshold_minutes * 60}s"
        )
    metrics = HealthMetrics(
        rows_raw=int(job[0]) if job else 0,
        rows_window=int(job[1]) if job else 0,
        rows_alert=int(job[2]) if job else 0,
        rejected=int(job[3]) if job else 0,
        late_dropped=int(job[4]) if job else 0,
        partition_lag=lag,
        newest_event_age_seconds=age,
        message=("BREACH " + " ".join(breaches)) if breaches else "OK topic-high-low",
    )
    write_health(contract.stream, topic, "WATCHDOG", metrics)
    return metrics
