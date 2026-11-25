"""Unit tests for stream target safety guards."""

from __future__ import annotations

import pytest
from fleet_stream.admin.guard import StreamGuardError, assert_safe_target
from fleet_stream.settings import from_env

pytestmark = pytest.mark.unit


def _settings(monkeypatch: pytest.MonkeyPatch, env: str, prefix: str) -> object:
    monkeypatch.setenv("ENV", env)
    monkeypatch.setenv("KAFKA_TOPIC_PREFIX", prefix)
    return from_env()


def test_guard_allows_dev_target(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dev may write only prefixed dev databases and topics."""
    monkeypatch.setenv("PG_DBNAME", "fleet_dev")
    settings = _settings(monkeypatch, "dev", "dev.")

    assert_safe_target(settings)


def test_guard_allows_local_warehouse_in_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dev may write the local fleet_warehouse database."""
    monkeypatch.setenv("PG_DBNAME", "fleet_warehouse")
    settings = _settings(monkeypatch, "dev", "dev.")

    assert_safe_target(settings)


def test_guard_refuses_unknown_database_in_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dev must still reject databases outside the approved list."""
    monkeypatch.setenv("PG_DBNAME", "fleet_prod")
    settings = _settings(monkeypatch, "dev", "dev.")

    with pytest.raises(StreamGuardError, match="fleet_warehouse"):
        assert_safe_target(settings)


def test_guard_refuses_warehouse_in_ci(monkeypatch: pytest.MonkeyPatch) -> None:
    """CI may only write its own prefixed databases."""
    monkeypatch.setenv("PG_DBNAME", "fleet_warehouse")
    settings = _settings(monkeypatch, "ci", "ci.")

    with pytest.raises(StreamGuardError, match="fleet_dev, fleet_it"):
        assert_safe_target(settings)


def test_guard_refuses_unconfirmed_prod(monkeypatch: pytest.MonkeyPatch) -> None:
    """Production streaming is opt-in and explicitly confirmed."""
    settings = _settings(monkeypatch, "prod", "prod.")

    with pytest.raises(StreamGuardError, match="STREAM_ALLOW_PROD"):
        assert_safe_target(settings)
