"""Create and verify Kafka topics from the stream topic contract."""

from __future__ import annotations

import logging
from typing import Any

from fleet_stream.contract import load_topics
from fleet_stream.settings import StreamSettings

log = logging.getLogger(__name__)


class TopicError(RuntimeError):
    """Raised when Kafka cannot create or list the required topics."""


def topic_specs(settings: StreamSettings) -> list[dict[str, Any]]:
    """Return contract topics with the environment prefix applied."""
    specs = []
    for topic in load_topics():
        spec = dict(topic)
        spec["name"] = settings.topic(str(topic["name"]))
        specs.append(spec)
    return specs


def create_topics(settings: StreamSettings) -> list[str]:
    """Create missing topics idempotently; never delete or shrink them."""
    try:
        from confluent_kafka import KafkaException
        from confluent_kafka.admin import AdminClient, NewTopic
    except ImportError as exc:
        raise TopicError("confluent-kafka is required to create topics") from exc

    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics)
    new_topics = []
    created: list[str] = []
    for spec in topic_specs(settings):
        name = str(spec["name"])
        created.append(name)
        if name in existing:
            continue
        new_topics.append(
            NewTopic(
                name,
                num_partitions=int(spec["partitions"]),
                replication_factor=int(spec["replication_factor"]),
                config={
                    "retention.ms": str(spec["retention_ms"]),
                    "cleanup.policy": str(spec["cleanup_policy"]),
                },
            )
        )
    if new_topics:
        futures = admin.create_topics(new_topics, validate_only=False)
        for name, future in futures.items():
            try:
                future.result(timeout=30)
            except KafkaException as exc:
                raise TopicError(f"failed to create topic {name}: {exc}") from exc
    log.info("topics ready: %s", ", ".join(created))
    return created
