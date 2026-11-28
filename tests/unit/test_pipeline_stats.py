"""pipeline_stats reports are read-only and JSON-safe."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterator
from contextlib import nullcontext
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Self

import pytest

from scripts import pipeline_stats as stats


class FakeResult:
    """Minimal SQLAlchemy result with mappings()."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def mappings(self) -> FakeResult:
        return self

    def all(self) -> list[dict[str, Any]]:
        return self.rows

    def one(self) -> dict[str, Any]:
        return self.rows[0]


class FakeConnection:
    """Capture statements while returning scripted result rows."""

    def __init__(self, results: list[list[dict[str, Any]]]) -> None:
        self._results: Iterator[list[dict[str, Any]]] = iter(results)
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    def begin(self) -> Any:
        return nullcontext()

    def execute(
        self, statement: Any, params: dict[str, Any] | None = None
    ) -> FakeResult:
        sql = str(statement)
        self.statements.append(sql)
        self.params.append(dict(params or {}))
        if sql == "SET TRANSACTION READ ONLY":
            return FakeResult([])
        return FakeResult(next(self._results))


class FakeEngine:
    """Return one fake connection."""

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def connect(self) -> FakeConnection:
        return self.connection


def _connection() -> FakeConnection:
    """Return a connection with one row for each report query."""
    return FakeConnection(
        [
            [
                {
                    "successful_runs": 2,
                    "median_seconds": Decimal("12.345"),
                    "p95_seconds": None,
                }
            ],
            [
                {
                    "task_id": "ingest_loads",
                    "rows_processed": 900,
                    "duration_seconds": Decimal("3.456"),
                    "ended_at": datetime(2026, 1, 1, tzinfo=UTC),
                }
            ],
            [
                {
                    "schema_name": "public",
                    "table_name": "big_table",
                    "table_size": "8 MB",
                    "indexes_size": "1 MB",
                    "total_size_bytes": "9 MB",
                }
            ],
        ]
    )


@pytest.mark.unit
def test_production_guard_requires_explicit_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production reports fail without ENV=prod CONFIRM=yes."""
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.delenv("CONFIRM", raising=False)
    with pytest.raises(RuntimeError, match="ENV=prod CONFIRM=yes"):
        stats._require_production_confirmation()

    monkeypatch.setenv("CONFIRM", "yes")
    assert stats._require_production_confirmation() is None

    monkeypatch.setenv("ENV", "dev")
    monkeypatch.delenv("CONFIRM", raising=False)
    assert stats._require_production_confirmation() is None


@pytest.mark.unit
def test_positive_int_rejects_bad_cli_values() -> None:
    """CLI numbers must be positive integers."""
    assert stats._positive_int("7") == 7
    with pytest.raises(argparse.ArgumentTypeError):
        stats._positive_int("zero")
    with pytest.raises(argparse.ArgumentTypeError):
        stats._positive_int("0")


@pytest.mark.unit
def test_build_report_uses_read_only_bound_queries() -> None:
    """All three queries run in one read-only transaction with bound values."""
    connection = _connection()

    report = stats.build_report(
        pipeline="fleet_daily", days=7, limit=3, engine=FakeEngine(connection)
    )

    assert connection.statements[0] == "SET TRANSACTION READ ONLY"
    assert "percentile_cont" in connection.statements[1]
    assert ":task_pattern" in connection.statements[2]
    assert "pg_statio_user_tables" in connection.statements[3]
    assert connection.params[1] == {"pipeline": "fleet_daily", "days": 7}
    assert connection.params[2]["task_pattern"] == "ingest\\_%"
    assert connection.params[2]["limit"] == 3

    assert report["pipeline"] == "fleet_daily"
    assert report["days"] == 7
    assert report["durations"] == {
        "successful_runs": 2,
        "median_seconds": 12.35,
        "p95_seconds": None,
    }
    assert report["largest_ingest_tasks"] == [
        {
            "task_id": "ingest_loads",
            "rows_processed": 900,
            "duration_seconds": 3.46,
            "ended_at": "2026-01-01T00:00:00+00:00",
        }
    ]
    assert report["largest_relations"][0]["table_name"] == "big_table"


@pytest.mark.unit
def test_build_report_limits_relations() -> None:
    """The committed metadata query is limited after execution."""
    connection = _connection()
    connection._results = iter(
        [
            [{"successful_runs": 0, "median_seconds": None, "p95_seconds": None}],
            [],
            [
                {"schema_name": "public", "table_name": "a", "table_size": "2 MB"},
                {"schema_name": "public", "table_name": "b", "table_size": "1 MB"},
            ],
        ]
    )

    report = stats.build_report(limit=1, engine=FakeEngine(connection))

    assert [row["table_name"] for row in report["largest_relations"]] == ["a"]


@pytest.mark.unit
def test_render_formats(capsys: pytest.CaptureFixture[str]) -> None:
    """Text and JSON renderings expose the same numbers."""
    report = {
        "pipeline": None,
        "days": 14,
        "durations": {
            "successful_runs": 1,
            "median_seconds": 12.5,
            "p95_seconds": 12.5,
        },
        "largest_ingest_tasks": [
            {
                "task_id": "ingest_loads",
                "rows_processed": 5,
                "duration_seconds": 1.0,
                "ended_at": None,
            }
        ],
        "largest_relations": [
            {
                "schema_name": "public",
                "table_name": "big",
                "table_size": "8 MB",
                "total_size_bytes": "9 MB",
            }
        ],
    }

    text = stats.render_text(report)
    assert "Successful runs: 1" in text
    assert "- ingest_loads: 5 rows" in text
    assert "- public.big: 8 MB (9 MB total)" in text

    assert json.loads(stats.render_json(report)) == report
    assert capsys.readouterr().out == ""


@pytest.mark.unit
def test_main_writes_json_without_print(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The CLI writes to stdout through sys.stdout.write."""
    report = {
        "pipeline": "p",
        "days": 3,
        "durations": {},
        "largest_ingest_tasks": [],
        "largest_relations": [],
    }
    seen: dict[str, Any] = {}

    def fake_build_report(**kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return report

    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.setattr(stats, "build_report", fake_build_report)

    assert (
        stats.main(
            ["--pipeline", "p", "--days", "3", "--limit", "2", "--format", "json"]
        )
        == 0
    )
    assert seen == {"pipeline": "p", "days": 3, "limit": 2}
    assert json.loads(capsys.readouterr().out) == report
