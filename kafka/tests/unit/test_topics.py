"""Unit tests for prefixed Kafka topic specifications."""

from __future__ import annotations

import pytest
from fleet_stream.admin.topics import topic_specs
from fleet_stream.settings import from_env

pytestmark = pytest.mark.unit


def test_topic_specs_apply_prefix_without_mutating_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Admin topics match the contract while respecting the environment prefix."""
    monkeypatch.setenv("ENV", "ci")
    monkeypatch.setenv("KAFKA_TOPIC_PREFIX", "ci.")
    settings = from_env()

    specs = topic_specs(settings)

    assert {spec["name"] for spec in specs} == {
        "ci.fleet.delivery_events.v1",
        "ci.fleet.delivery_events.v1.dlq",
    }
    assert all(spec["partitions"] >= 1 for spec in specs)
    assert all(spec["replication_factor"] == 1 for spec in specs)
    assert all(spec["cleanup_policy"] == "delete" for spec in specs)
