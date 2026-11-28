"""Stream CLI wrappers must dispatch to the guarded fleet_stream operations."""

from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from scripts import (
    stream_admin,
    stream_job,
    stream_monitor,
    stream_producer,
    stream_reconcile,
)

_CONTRACT = SimpleNamespace(
    topic="delivery_events", dlq_topic="delivery_dlq", stream="delivery_events"
)
_SETTINGS = SimpleNamespace(
    source="fixture",
    bootstrap_servers="broker:9092",
    topic=lambda name: f"test_{name}",
)


def _argv(monkeypatch: pytest.MonkeyPatch, *args: str) -> None:
    """Replace process arguments for one CLI invocation."""
    monkeypatch.setattr(sys, "argv", ["cli.py", *args])


@pytest.mark.parametrize("command", ["topics", "register", "status", "reset"])
def test_stream_admin_dispatches_guarded_commands(
    monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    """Every administrative command runs after the target safety check."""
    calls: list[str] = []
    monkeypatch.setattr(stream_admin, "from_env", lambda: _SETTINGS)
    monkeypatch.setattr(stream_admin, "load_contract", lambda: _CONTRACT)
    monkeypatch.setattr(
        stream_admin,
        "assert_safe_target",
        lambda settings: calls.append("guard"),
    )
    monkeypatch.setattr(
        stream_admin, "create_topics", lambda settings: calls.append("topics")
    )
    monkeypatch.setattr(
        stream_admin,
        "register_stream",
        lambda settings, contract: calls.append("register"),
    )
    monkeypatch.setattr(
        stream_admin, "stream_status", lambda stream: calls.append("status")
    )
    monkeypatch.setattr(
        stream_admin, "reset_stream", lambda settings: calls.append("reset")
    )
    _argv(monkeypatch, command)

    stream_admin.main()

    assert calls == ["guard", command]


def test_stream_job_delegates_to_the_supervised_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The job wrapper only starts the Spark application."""
    calls: list[str] = []
    monkeypatch.setattr(stream_job, "run", lambda: calls.append("run"))

    stream_job.main()

    assert calls == ["run"]


def test_stream_monitor_skips_when_stream_is_disabled(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A missing health result logs a skip instead of failing the watchdog."""
    monkeypatch.setattr(stream_monitor, "from_env", lambda: _SETTINGS)
    monkeypatch.setattr(stream_monitor, "load_contract", lambda: _CONTRACT)
    monkeypatch.setattr(stream_monitor, "probe", lambda *_args: None)
    caplog.set_level(logging.INFO)

    stream_monitor.main()

    assert "stream health skipped" in caplog.text


def test_stream_monitor_logs_lag_and_freshness(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Probe metrics reach the watchdog log."""
    metrics = SimpleNamespace(
        partition_lag=4, newest_event_age_seconds=9, rejected=1, late_dropped=2
    )
    monkeypatch.setattr(stream_monitor, "from_env", lambda: _SETTINGS)
    monkeypatch.setattr(stream_monitor, "load_contract", lambda: _CONTRACT)
    monkeypatch.setattr(stream_monitor, "probe", lambda *_args: metrics)
    caplog.set_level(logging.INFO)

    stream_monitor.main()

    assert "lag=4 age=9 rejected=1 late=2" in caplog.text


def test_stream_producer_timestamp_defaults_to_utc() -> None:
    """Naive CLI bounds are interpreted as UTC."""
    assert stream_producer._timestamp("2026-01-02T03:04:05") == datetime(
        2026, 1, 2, 3, 4, 5, tzinfo=UTC
    )
    aware = stream_producer._timestamp("2026-01-02T03:04:05+05:30")
    assert aware.utcoffset() is not None and aware.utcoffset().total_seconds() == 19800


def test_stream_producer_reads_the_requested_source(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Fixture replays use the fixture path; live replays use bounded source rows."""
    seen: dict[str, Any] = {}
    calls: list[str] = []

    class Publisher:
        def __init__(self, topic: str, servers: str, dlq_topic: str) -> None:
            seen["publisher"] = (topic, servers, dlq_topic)

    monkeypatch.setattr(stream_producer, "from_env", lambda: _SETTINGS)
    monkeypatch.setattr(stream_producer, "load_contract", lambda: _CONTRACT)
    monkeypatch.setattr(
        stream_producer,
        "read_fixture_rows",
        lambda path=None: calls.append("fixture") or [{"id": 1}],
    )
    monkeypatch.setattr(
        stream_producer,
        "read_live_rows",
        lambda limit: calls.append(f"live:{limit}") or iter([{"id": 2}]),
    )
    monkeypatch.setattr(stream_producer, "KafkaPublisher", Publisher)
    monkeypatch.setattr(
        stream_producer,
        "replay",
        lambda *_args, **_kwargs: SimpleNamespace(published=7),
    )
    caplog.set_level(logging.INFO)

    _argv(monkeypatch, "--fixture", str(tmp_path / "rows.json"))
    stream_producer.main()
    assert calls == ["fixture"]
    assert seen["publisher"] == (
        "test_delivery_events",
        "broker:9092",
        "test_delivery_dlq",
    )
    assert "records=7" in caplog.text

    _argv(monkeypatch, "--source", "live", "--limit", "3")
    stream_producer.main()
    assert calls[-1] == "live:3"


def test_stream_reconcile_passes_airflow_context(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The reconciliation wrapper forwards the batch context it was given."""
    seen: dict[str, Any] = {}
    monkeypatch.setattr(stream_reconcile, "from_env", lambda: _SETTINGS)

    def reconcile(
        batch_id: int, settings: Any, *, airflow_dag_id: str, airflow_run_id: str
    ) -> int:
        seen.update(
            batch_id=batch_id,
            settings=settings,
            airflow_dag_id=airflow_dag_id,
            airflow_run_id=airflow_run_id,
        )
        return 2

    monkeypatch.setattr(stream_reconcile, "reconcile", reconcile)
    caplog.set_level(logging.INFO)
    _argv(
        monkeypatch,
        "--batch-id",
        "12",
        "--airflow-dag-id",
        "dag",
        "--airflow-run-id",
        "run",
    )

    stream_reconcile.main()

    assert seen == {
        "batch_id": 12,
        "settings": _SETTINGS,
        "airflow_dag_id": "dag",
        "airflow_run_id": "run",
    }
    assert "comparisons=2" in caplog.text
