"""Report pipeline timings, ingest volumes and relation sizes read-only."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.types import Integer, String

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.connection import get_postgres_engine

ROOT = Path(__file__).resolve().parents[1]
SIZE_SQL_PATH = ROOT / "sql/metadata/09_list_table_sizes.sql"
DEFAULT_DAYS = 14
DEFAULT_LIMIT = 10

_DURATIONS_SQL = """
with runs as (
    select
        batch_id,
        min(event_at) filter (where event_type = 'STARTED') as started_at,
        min(event_at) filter (where event_type = 'SUCCEEDED') as succeeded_at
    from audit.pipeline_run_log
    where event_type in ('STARTED', 'SUCCEEDED')
      and (
          cast(:pipeline as text) is null
          or pipeline_name = cast(:pipeline as text)
      )
      and event_at >= now() - cast(:days as integer) * interval '1 day'
    group by batch_id
)
select
    count(*) filter (
        where started_at is not null and succeeded_at is not null
    ) as successful_runs,
    round((
        percentile_cont(0.5) within group (
            order by extract(epoch from (succeeded_at - started_at))
        )
        filter (where started_at is not null and succeeded_at is not null)
    )::numeric, 2) as median_seconds,
    round((
        percentile_cont(0.95) within group (
            order by extract(epoch from (succeeded_at - started_at))
        )
        filter (where started_at is not null and succeeded_at is not null)
    )::numeric, 2) as p95_seconds
from runs
"""

_TASKS_SQL = """
select
    t.task_id,
    t.rows_processed,
    round(t.duration_seconds::numeric, 2) as duration_seconds,
    t.ended_at
from audit.task_run_log as t
where t.status = 'SUCCEEDED'
  and t.task_id like :task_pattern escape '\\'
  and t.started_at >= now() - cast(:days as integer) * interval '1 day'
  and t.rows_processed is not null
  and (
      cast(:pipeline as text) is null
      or exists (
          select 1
          from audit.pipeline_run_log as p
          where p.batch_id = t.batch_id
            and p.pipeline_name = cast(:pipeline as text)
      )
  )
order by t.rows_processed desc, t.started_at desc
limit cast(:limit as integer)
"""


def _positive_int(value: str) -> int:
    """Parse a positive CLI integer."""
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected an integer, got {value!r}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def _require_production_confirmation() -> None:
    """Fail closed for production reports without explicit confirmation."""
    if os.getenv("ENV", "").lower() == "prod" and os.getenv("CONFIRM") != "yes":
        raise RuntimeError("production reports require ENV=prod CONFIRM=yes")


def _as_float(value: Any) -> float | None:
    """Return a JSON-safe rounded number or None."""
    if value is None:
        return None
    return round(float(value), 2)


def _mappings(result: Any) -> list[dict[str, Any]]:
    """Return result rows as plain dictionaries."""
    return [dict(row) for row in result.mappings().all()]


def _fetch_durations(
    connection: Connection, pipeline: str | None, days: int
) -> dict[str, Any]:
    """Return successful-run count and duration percentiles in seconds."""
    statement = text(_DURATIONS_SQL).bindparams(
        bindparam("pipeline", type_=String),
        bindparam("days", type_=Integer),
    )
    row = (
        connection.execute(statement, {"pipeline": pipeline, "days": days})
        .mappings()
        .one()
    )
    return {
        "successful_runs": int(row["successful_runs"]),
        "median_seconds": _as_float(row["median_seconds"]),
        "p95_seconds": _as_float(row["p95_seconds"]),
    }


def _fetch_tasks(
    connection: Connection, pipeline: str | None, days: int, limit: int
) -> list[dict[str, Any]]:
    """Return the largest successful ingest tasks."""
    statement = text(_TASKS_SQL).bindparams(
        bindparam("pipeline", type_=String),
        bindparam("days", type_=Integer),
        bindparam("limit", type_=Integer),
        bindparam("task_pattern", type_=String),
    )
    rows = _mappings(
        connection.execute(
            statement,
            {
                "pipeline": pipeline,
                "days": days,
                "limit": limit,
                "task_pattern": "ingest\\_%",
            },
        )
    )
    return [
        {
            "task_id": row["task_id"],
            "rows_processed": int(row["rows_processed"]),
            "duration_seconds": _as_float(row["duration_seconds"]),
            "ended_at": (
                row["ended_at"].isoformat() if row["ended_at"] is not None else None
            ),
        }
        for row in rows
    ]


def _fetch_relations(connection: Connection, limit: int) -> list[dict[str, Any]]:
    """Return the largest relations from the committed metadata query."""
    sql = SIZE_SQL_PATH.read_text(encoding="utf-8").strip().rstrip(";")
    rows = _mappings(connection.execute(text(sql)))
    return [dict(row) for row in rows[:limit]]


def build_report(
    pipeline: str | None = None,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    engine: Engine | None = None,
) -> dict[str, Any]:
    """Collect all statistics in one read-only Postgres transaction."""
    pg = engine or get_postgres_engine()
    with pg.connect() as connection, connection.begin():
        connection.execute(text("SET TRANSACTION READ ONLY"))
        durations = _fetch_durations(connection, pipeline, days)
        tasks = _fetch_tasks(connection, pipeline, days, limit)
        relations = _fetch_relations(connection, limit)
    return {
        "pipeline": pipeline,
        "days": days,
        "durations": durations,
        "largest_ingest_tasks": tasks,
        "largest_relations": relations,
    }


def render_text(report: dict[str, Any]) -> str:
    """Render the report as a short operator-facing summary."""
    durations = report["durations"]
    lines = [
        f"Pipeline: {report['pipeline'] or 'all'}",
        f"Window: last {report['days']} days",
        f"Successful runs: {durations['successful_runs']}",
        f"Median duration: {durations['median_seconds']} s",
        f"p95 duration: {durations['p95_seconds']} s",
        "",
        "Largest successful ingest tasks:",
    ]
    for task in report["largest_ingest_tasks"]:
        lines.append(f"- {task['task_id']}: {task['rows_processed']} rows")
    lines.extend(["", "Largest relations:"])
    for relation in report["largest_relations"]:
        lines.append(
            f"- {relation['schema_name']}.{relation['table_name']}: "
            f"{relation['table_size']} ({relation['total_size_bytes']} total)"
        )
    return "\n".join(lines) + "\n"


def render_json(report: dict[str, Any]) -> str:
    """Render the report as stable JSON."""
    return json.dumps(report, indent=2, sort_keys=True, default=str) + "\n"


def main(argv: list[str] | None = None) -> int:
    """Run the read-only report command."""
    parser = argparse.ArgumentParser(description="Report fleet pipeline statistics")
    parser.add_argument("--pipeline", help="pipeline name to filter")
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args(argv)

    _require_production_confirmation()
    report = build_report(
        pipeline=args.pipeline,
        days=args.days,
        limit=args.limit,
    )
    output = render_json(report) if args.format == "json" else render_text(report)
    sys.stdout.write(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
