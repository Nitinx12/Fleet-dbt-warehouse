"""Unit tests for disposable realtime reset behavior."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
from fleet_stream.admin import reset
from fleet_stream.settings import from_env

pytestmark = pytest.mark.unit


def test_reset_truncates_only_realtime_and_removes_checkpoint(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Reset clears stream state and never names a batch layer."""
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("PG_DBNAME", "fleet_dev")
    monkeypatch.setenv("KAFKA_TOPIC_PREFIX", "dev.")
    monkeypatch.setenv("CONFIRM", "yes")
    checkpoint = tmp_path / "checkpoints" / "delivery_events_v1"
    checkpoint.mkdir(parents=True)
    (checkpoint / "watermark.json").write_text("{}", encoding="utf-8")
    calls: list[str] = []
    monkeypatch.setattr(reset, "execute", lambda sql: calls.append(sql))
    settings = replace(from_env(), checkpoint_dir=checkpoint)

    reset.reset_stream(settings)

    assert len(calls) == 1
    assert "TRUNCATE TABLE" in calls[0]
    assert "realtime.delivery_events_raw" in calls[0]
    assert "bronze" not in calls[0]
    assert "gold" not in calls[0]
    assert not checkpoint.exists()


def test_reset_refuses_missing_confirmation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An accidental reset is blocked without explicit confirmation."""
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("PG_DBNAME", "fleet_dev")
    monkeypatch.setenv("KAFKA_TOPIC_PREFIX", "dev.")
    monkeypatch.delenv("CONFIRM", raising=False)
    monkeypatch.setattr(reset, "execute", lambda sql: None)
    settings = replace(from_env(), checkpoint_dir=tmp_path / "checkpoints")

    with pytest.raises(ValueError, match="CONFIRM=yes"):
        reset.reset_stream(settings)
