"""Unit tests for the Kafka publisher adapter."""

from __future__ import annotations

import json
from typing import Any

import pytest
from fleet_stream.producer.publish import KafkaPublisher, PublishError

pytestmark = pytest.mark.unit


class FakeProducer:
    """Capture produce calls without contacting a broker."""

    def __init__(self, remaining: int = 0, fail_until_poll: int = 0) -> None:
        self.calls: list[dict[str, Any]] = []
        self.remaining = remaining
        self.flushed = False
        self.polls = 0
        self.fail_until_poll = fail_until_poll

    def poll(self, timeout: float) -> int:
        self.polls += 1
        return 0

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        callback: Any,
    ) -> None:
        if self.polls < self.fail_until_poll:
            raise BufferError("Local: Queue full")
        self.calls.append(
            {"topic": topic, "value": value, "key": key, "callback": callback}
        )

    def flush(self, timeout: int) -> int:
        self.flushed = True
        assert timeout == 30_000
        return self.remaining


def test_publisher_serialises_strict_json_with_dedupe_key() -> None:
    """Events are keyed by event id and lifecycle type for idempotent sinks."""
    producer = FakeProducer()
    publisher = KafkaPublisher("ci.fleet.delivery_events.v1", "broker:9092", producer)
    event = {
        "event_id": "evt-1",
        "event_type": "ARRIVED",
        "facility_id": "FAC-1",
    }

    publisher.publish(event)
    publisher.flush()

    assert producer.flushed
    assert publisher.published == 1
    assert producer.calls[0]["topic"] == "ci.fleet.delivery_events.v1"
    assert producer.calls[0]["key"] == b"evt-1|ARRIVED"
    assert json.loads(producer.calls[0]["value"]) == event


def test_publisher_sends_rejects_to_the_contract_dlq() -> None:
    """Rejected payloads keep their reason and original value without offsets."""
    producer = FakeProducer()
    publisher = KafkaPublisher(
        "ci.fleet.delivery_events.v1",
        "broker:9092",
        producer,
        dlq_topic="ci.fleet.delivery_events.v1.dlq",
    )

    publisher.publish_invalid("contract", {"event_id": "evt-bad"})
    publisher.flush()

    assert producer.calls[0]["topic"] == "ci.fleet.delivery_events.v1.dlq"
    body = json.loads(producer.calls[0]["value"])
    assert body["reason"] == "contract"
    assert body["original"] == {"event_id": "evt-bad"}
    assert body["offset"] is None


def test_publisher_fails_on_undelivered_records() -> None:
    """A partial delivery is an error, never a silent success."""
    publisher = KafkaPublisher("topic", "broker:9092", FakeProducer(remaining=1))
    publisher.publish({"event_id": "evt-1", "event_type": "ARRIVED"})

    with pytest.raises(PublishError, match="delivery incomplete"):
        publisher.flush()


def test_publisher_polls_when_the_local_queue_is_full() -> None:
    """A full local queue is drained by poll and retried, not raised."""
    producer = FakeProducer(fail_until_poll=1)
    publisher = KafkaPublisher("topic", "broker:9092", producer)

    publisher.publish({"event_id": "evt-1", "event_type": "ARRIVED"})

    assert producer.polls >= 2
    assert publisher.published == 1
    assert len(producer.calls) == 1
