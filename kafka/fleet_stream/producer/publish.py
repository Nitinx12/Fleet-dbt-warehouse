"""Publish contract-valid delivery events to Kafka idempotently."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

from fleet_stream.events import dedupe_key

log = logging.getLogger(__name__)


class PublishError(RuntimeError):
    """Raised when Kafka cannot accept or deliver a batch."""


def _create_producer(bootstrap_servers: str) -> Any:
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise PublishError("confluent-kafka is required to publish events") from exc
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 20,
            "delivery.timeout.ms": 30_000,
        }
    )


class KafkaPublisher:
    """Small Kafka adapter with no transformation logic."""

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str,
        producer: Any | None = None,
        *,
        dlq_topic: str | None = None,
    ) -> None:
        self.topic = topic
        self.dlq_topic = dlq_topic
        self._producer = producer or _create_producer(bootstrap_servers)
        self._published = 0
        self._errors = 0

    @property
    def published(self) -> int:
        """Return the number of records handed to the producer."""
        return self._published

    def _on_delivery(self, error: Any, _message: Any) -> None:
        if error is None:
            return
        self._errors += 1
        log.error("kafka delivery failed: %s", error)

    def _enqueue(self, topic: str, value: bytes, key: bytes) -> None:
        while True:
            try:
                self._producer.produce(
                    topic, value=value, key=key, callback=self._on_delivery
                )
                break
            except BufferError:
                self._producer.poll(0.1)
        self._producer.poll(0)

    def publish(self, event: Mapping[str, Any]) -> None:
        """Serialise one event as strict JSON and enqueue it by idempotency key."""
        key = "|".join(dedupe_key(event)).encode("utf-8")
        value = json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8")
        self._enqueue(self.topic, value, key)
        self._published += 1

    def publish_invalid(self, reason: str, payload: Mapping[str, Any]) -> None:
        """Send a rejected payload to the DLQ with its reason but no offsets."""
        if self.dlq_topic is None:
            raise PublishError("DLQ topic is not configured")
        value = json.dumps(
            {
                "reason": reason,
                "original": dict(payload),
                "topic": "",
                "partition": None,
                "offset": None,
            },
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        self._enqueue(self.dlq_topic, value, reason.encode("utf-8"))

    def flush(self) -> None:
        """Wait for delivery and fail closed on undelivered records."""
        remaining = int(self._producer.flush(30_000))
        if remaining or self._errors:
            raise PublishError(
                f"kafka delivery incomplete: remaining={remaining}, errors={self._errors}"
            )
