"""Publisher tests cover safe publication, rollback and batch/audit boundaries."""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Self

import pytest
from sqlalchemy.exc import SQLAlchemyError

from scripts import publish_marts as publish
from tests.fakes import (
    FakeDBConnection,
    FakeDBCursor,
    FakeEngine,
    FakeResult,
)

COLUMNS = [
    publish.Column("mart_key", "character varying", "STRING"),
    publish.Column("amount", "numeric", "DECIMAL(18,2)"),
    publish.Column("mart_loaded_at", "timestamp with time zone", "TIMESTAMP"),
]


def _pg(handler: Any = None) -> FakeEngine:
    """Return a fake Postgres engine with an empty default handler."""
    return FakeEngine(handler or (lambda _sql, _params: FakeResult()))


def _cursor(**responses: list[tuple[Any, ...]]) -> FakeDBCursor:
    """Return a scripted Databricks cursor."""
    return FakeDBCursor(responses)


def _args(batch_id: int | None, dag_id: str = "publish_local") -> SimpleNamespace:
    """Return parsed-argument shape used by main()."""
    return SimpleNamespace(
        batch_id=batch_id, airflow_dag_id=dag_id, airflow_run_id="run-1"
    )


@pytest.fixture
def audit_calls(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[Any]]:
    """Replace audit functions with recorders that need no database."""
    recorded: dict[str, list[Any]] = {"errors": [], "recon": []}

    class Task:
        rows_processed: int | None = None

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

    def log_error(**kwargs: Any) -> None:
        recorded["errors"].append(kwargs)

    def log_reconciliation(batch: int, rows: list[Any], **kwargs: Any) -> None:
        recorded["recon"].append((batch, rows, kwargs))

    monkeypatch.setattr(
        publish,
        "audit",
        SimpleNamespace(
            task_run=lambda *_a, **_kw: Task(),
            error_summary=lambda error, op: f"{op} ({type(error).__name__})",
            log_error=log_error,
            log_reconciliation=log_reconciliation,
        ),
    )
    return recorded


def test_arguments_accept_airflow_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The publisher accepts an externally owned batch id."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "publish.py",
            "--batch-id",
            "22",
            "--airflow-dag-id",
            "dag",
            "--airflow-run-id",
            "run",
        ],
    )
    args = publish._arguments()
    assert (args.batch_id, args.airflow_dag_id, args.airflow_run_id) == (
        22,
        "dag",
        "run",
    )


def test_databricks_identifiers_are_checked_and_quoted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catalog, schema and table names are validated before interpolation."""
    monkeypatch.setattr(publish.config, "DATABRICKS_CATALOG", "fleet_cat")
    monkeypatch.setattr(publish.config, "DATABRICKS_SCHEMA", "marts")
    assert publish._schema_name() == "`fleet_cat`.`marts`"
    assert publish._table_name("mart_sales_daily") == (
        "`fleet_cat`.`marts`.`mart_sales_daily`"
    )
    assert publish._pg_identifier('we"ird') == '"we""ird"'

    monkeypatch.setattr(publish.config, "DATABRICKS_SCHEMA", "")
    with pytest.raises(publish.PublishError, match="DATABRICKS_SCHEMA is required"):
        publish._schema_name()

    monkeypatch.setattr(publish.config, "DATABRICKS_SCHEMA", "Bad-Schema")
    with pytest.raises(publish.PublishError, match="snake_case"):
        publish._schema_name()


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"data_type": "character", "column_name": "c"}, "STRING"),
        ({"data_type": "uuid", "column_name": "c"}, "STRING"),
        ({"data_type": "text", "column_name": "c"}, "STRING"),
        ({"data_type": "boolean", "column_name": "c"}, "BOOLEAN"),
        ({"data_type": "smallint", "column_name": "c"}, "SMALLINT"),
        ({"data_type": "integer", "column_name": "c"}, "INT"),
        ({"data_type": "bigint", "column_name": "c"}, "BIGINT"),
        ({"data_type": "real", "column_name": "c"}, "FLOAT"),
        ({"data_type": "double precision", "column_name": "c"}, "DOUBLE"),
        ({"data_type": "date", "column_name": "c"}, "DATE"),
        ({"data_type": "timestamp without time zone", "column_name": "c"}, "TIMESTAMP"),
    ],
)
def test_supported_postgres_types_map_to_databricks(
    row: dict[str, Any], expected: str
) -> None:
    """Every approved mart column type has an exact Databricks counterpart."""
    assert publish._databricks_type(row) == expected


@pytest.mark.parametrize(
    "row",
    [
        {
            "column_name": "amount",
            "data_type": "numeric",
            "numeric_precision": 2,
            "numeric_scale": 4,
        },
        {
            "column_name": "amount",
            "data_type": "numeric",
            "numeric_precision": 39,
            "numeric_scale": 2,
        },
        {"column_name": "bad", "data_type": "jsonb"},
    ],
)
def test_unsupported_types_fail_closed(row: dict[str, Any]) -> None:
    """Numeric typmods and unknown types cannot create a drifting target."""
    with pytest.raises(publish.PublishError):
        publish._databricks_type(row)


def test_source_columns_require_key_and_timestamp_watermark() -> None:
    """The source contract is derived from Postgres for every publication."""
    rows = [
        {
            "column_name": "mart_key",
            "data_type": "character varying",
            "numeric_precision": None,
            "numeric_scale": None,
        },
        {
            "column_name": "mart_loaded_at",
            "data_type": "timestamp with time zone",
            "numeric_precision": None,
            "numeric_scale": None,
        },
    ]
    pg = _pg(lambda _sql, _params: FakeResult(rows=rows))
    columns = publish.source_columns(pg, "mart_sales_daily")
    assert [column.name for column in columns] == ["mart_key", "mart_loaded_at"]
    assert publish._watermark_is_aware(columns) is True

    with pytest.raises(publish.PublishError, match="does not exist"):
        publish.source_columns(_pg(), "mart_missing")

    with pytest.raises(publish.PublishError, match="missing"):
        publish.source_columns(
            _pg(lambda _sql, _params: FakeResult(rows=[rows[0]])), "mart_x"
        )

    naive_rows = [
        rows[0],
        {**rows[1], "data_type": "date"},
    ]
    with pytest.raises(publish.PublishError, match="must be a timestamp"):
        publish.source_columns(
            _pg(lambda _sql, _params: FakeResult(rows=naive_rows)), "mart_x"
        )


def test_watermark_normalisation_matches_source_timezone() -> None:
    """Databricks timestamps are UTC instants; naive sources stay naive."""
    aware = datetime(2026, 1, 2, 3, tzinfo=UTC)
    assert publish._normalise_watermark(aware, True) == aware
    assert publish._normalise_watermark(aware, False) == aware.replace(tzinfo=None)
    assert publish._normalise_watermark("2026-01-02", True) == "2026-01-02"
    assert publish._normalise_watermark(None, True) is None

    offset = timezone(timedelta(hours=5, minutes=30))
    local = datetime(2026, 1, 2, 8, 30, tzinfo=offset)
    assert publish._normalise_watermark(local, True) == datetime(
        2026, 1, 2, 3, tzinfo=UTC
    )


def test_snapshot_rejects_null_watermarks() -> None:
    """One MVCC statement supplies all counts and fails on incomplete rows."""
    high = datetime(2026, 1, 3, tzinfo=UTC)
    pg = _pg(lambda _sql, _params: FakeResult(rows=[(0, 12, 3, high)]))
    snap = publish._snapshot(pg, "mart_x", datetime(2026, 1, 1, tzinfo=UTC))
    assert snap == publish.Snapshot(12, 3, 0, high)

    calls = pg.connections[0].calls
    assert "count(*) FILTER (WHERE" in calls[0][0]
    assert "watermark" in calls[0][1]

    first_pg = _pg(lambda _sql, _params: FakeResult(rows=[(0, 5, 5, high)]))
    first = publish._snapshot(first_pg, "mart_x", None)
    assert first.pending_rows == 5
    assert "watermark" not in first_pg.connections[0].calls[0][1]

    with pytest.raises(publish.PublishError, match="NULL mart_loaded_at"):
        publish._snapshot(
            _pg(lambda _sql, _params: FakeResult(rows=[(2, 5, 5, high)])),
            "mart_x",
            None,
        )


def test_source_rows_are_streamed_in_bounded_batches() -> None:
    """Pending rows are read from one server-side cursor with bound values."""
    rows = [(f"k{i}", i) for i in range(2)]
    pg = _pg(lambda _sql, _params: FakeResult(rows=rows))
    batches = list(
        publish._source_rows(
            pg,
            "mart_x",
            COLUMNS,
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 2, tzinfo=UTC),
            ordered=True,
        )
    )
    assert batches == [rows]
    sql, params = pg.connections[0].calls[0]
    assert "ORDER BY" in sql and "mart_key" in sql
    assert set(params) == {"high", "watermark"}

    unordered = list(
        publish._source_rows(
            _pg(lambda _sql, _params: FakeResult(rows=rows)),
            "mart_x",
            COLUMNS,
            None,
            None,
            False,
        )
    )
    assert unordered == [rows]


def test_inspect_target_detects_drift_and_interrupted_first_load() -> None:
    """The Databricks table must match the Postgres contract exactly."""
    table_rows = [
        ("mart_key", "string"),
        ("amount", "decimal(18,2)"),
        ("mart_loaded_at", "timestamp"),
        ("# Partitioning", ""),
        (
            "Table Properties",
            "publish_marts.status=loading,`delta.minReaderVersion`=1",
        ),
    ]
    cursor = _cursor(**{"DESCRIBE TABLE EXTENDED": table_rows})
    assert publish._inspect_target(cursor, "mart_x", COLUMNS) is True

    ready = _cursor(
        **{
            "DESCRIBE TABLE EXTENDED": [
                *table_rows[:4],
                ("Table Properties", "publish_marts.status=ready"),
            ]
        }
    )
    assert publish._inspect_target(ready, "mart_x", COLUMNS) is False

    drifted = _cursor(
        **{"DESCRIBE TABLE EXTENDED": [("mart_key", "string"), ("# x", "")]}
    )
    with pytest.raises(publish.PublishError, match="schema drift"):
        publish._inspect_target(drifted, "mart_x", COLUMNS)


def test_target_helpers_count_version_rollback_and_drop() -> None:
    """Rollback and cleanup never hide the original failure."""
    cursor = _cursor(**{"SELECT count(*)": [(4,)], "DESCRIBE HISTORY": [(3,)]})
    assert publish._count(cursor, "mart_x") == 4
    assert publish._table_version(cursor, "mart_x") == 3

    no_history = _cursor(**{"DESCRIBE HISTORY": []})
    with pytest.raises(publish.PublishError, match="no Delta history"):
        publish._table_version(no_history, "mart_x")

    publish._rollback(cursor, "mart_x", 2)
    assert "RESTORE TABLE" in cursor.calls[-1]

    failing = _cursor()
    failing.execute = lambda _sql: (_ for _ in ()).throw(RuntimeError("down"))
    publish._rollback(failing, "mart_x", 1)
    publish._drop(failing, "mart_x")


def test_prepare_schema_drops_only_legacy_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Existing allowlisted tables are returned; stale helpers are removed."""
    cursor = _cursor(
        **{
            "SHOW TABLES IN": [
                ("mart_sales_daily",),
                ("mart_sales_daily__new",),
                ("mart_truck_cost_monthly__prev",),
                ("something_else",),
            ]
        }
    )
    connection = FakeDBConnection(cursor)
    monkeypatch.setattr(publish, "open_databricks_connection", lambda: connection)

    existing = publish._prepare_schema()

    assert existing == {"mart_sales_daily"}
    assert sum("DROP TABLE" in call for call in cursor.calls) == 2
    assert connection.closed is True


def test_literals_and_chunks_stay_bounded() -> None:
    """Escaped literals and size budgets keep MERGE statements below limits."""
    assert publish._render_str("a\\b'c") == "'a\\\\b''c'"
    assert publish._render_datetime(datetime(2026, 1, 2, 3)) == (
        "'2026-01-02 03:00:00.000000+00:00'"
    )
    assert publish._render_decimal(Decimal("1.20")) == "1.20"
    with pytest.raises(publish.PublishError, match="non-finite"):
        publish._render_decimal(Decimal("NaN"))
    assert publish._render_float(float("nan")) == "CAST('NaN' AS DOUBLE)"
    assert publish._render_float(float("inf")) == "CAST('Infinity' AS DOUBLE)"
    assert publish._render_float(float("-inf")) == "CAST('-Infinity' AS DOUBLE)"
    assert publish._render_float(1.5) == "1.5"
    with pytest.raises(publish.PublishError, match="unsupported value type"):
        publish._sql_literal({1, 2})

    chunks = list(publish._values_chunks(iter([(1,), (2,), (3,)]), 20, max_chars=30))
    assert [count for _values, count in chunks] == [2, 1]


def test_merge_statement_parts_include_cast_and_key() -> None:
    """Inline MERGE casts every column and keys on mart_key."""
    prefix, suffix = publish._merge_statement_parts("mart_x", COLUMNS[:2])
    assert prefix.startswith("MERGE INTO")
    assert "CAST(`mart_key` AS STRING)" in prefix
    assert "ON target.`mart_key` = source.`mart_key`" in suffix
    assert "target.`amount` = source.`amount`" in suffix
    assert "WHEN NOT MATCHED THEN INSERT" in suffix


def test_merge_rows_reports_what_was_sent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The merge count reflects rows actually sent to Databricks."""
    monkeypatch.setattr(
        publish,
        "_source_rows",
        lambda *_a, **_kw: ([(f"k{i}", i, None) for i in range(4)] for _ in range(1)),
    )
    cursor = _cursor()
    sent = publish._merge_rows(
        _pg(), cursor, "mart_x", COLUMNS, None, datetime(2026, 1, 2, tzinfo=UTC)
    )
    assert sent == 4
    assert len(cursor.calls) == 1


def test_load_rows_uses_multiple_writers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Large first loads fan out over independent Databricks connections."""
    cursors: list[FakeDBCursor] = []

    def connect() -> FakeDBConnection:
        cursor = _cursor()
        cursors.append(cursor)
        return FakeDBConnection(cursor)

    monkeypatch.setattr(publish, "open_databricks_connection", connect)
    monkeypatch.setattr(
        publish,
        "_source_rows",
        lambda *_a, **_kw: (
            [(f"k{i}", i, None) for i in range(start, start + 2)] for start in (0, 2)
        ),
    )
    total = publish._load_rows(_pg(), _cursor(), "mart_x", COLUMNS, None, 2)
    assert total == 4
    assert len(cursors) == 2
    assert sum(len(cursor.calls) for cursor in cursors) >= 1


def test_load_rows_reraises_worker_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """A writer error stops the producer and reaches the caller."""

    def connect() -> FakeDBConnection:
        cursor = _cursor()
        cursor.execute = lambda _sql: (_ for _ in ()).throw(
            SQLAlchemyError("write failed")
        )
        return FakeDBConnection(cursor)

    monkeypatch.setattr(publish, "open_databricks_connection", connect)
    monkeypatch.setattr(
        publish,
        "_source_rows",
        lambda *_a, **_kw: ([(f"k{i}", i, None) for i in range(4)] for _ in range(1)),
    )
    with pytest.raises(SQLAlchemyError, match="write failed"):
        publish._load_rows(_pg(), _cursor(), "mart_x", COLUMNS, None, 2)


def test_incremental_merges_changes_and_reconciles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inclusive watermarks make a retried merge idempotent."""
    snap = publish.Snapshot(7, 3, 0, datetime(2026, 1, 3, tzinfo=UTC))
    monkeypatch.setattr(publish, "_snapshot", lambda *_a: snap)
    monkeypatch.setattr(publish, "_table_version", lambda *_a: 4)
    monkeypatch.setattr(publish, "_merge_rows", lambda *_a: 3)
    monkeypatch.setattr(publish, "_count", lambda *_a: 7)
    rolled: list[Any] = []
    monkeypatch.setattr(publish, "_rollback", lambda *_a: rolled.append(_a))
    cursor = _cursor(**{"SELECT max": [(datetime(2026, 1, 2, tzinfo=UTC), 4)]})

    result = publish._incremental(_pg(), cursor, "mart_x", COLUMNS)

    assert result == (snap, 3, 7)
    assert rolled == []

    monkeypatch.setattr(publish, "_merge_rows", lambda *_a: 2)
    with pytest.raises(publish.PublishError, match="merged 2 rows, expected 3"):
        publish._incremental(_pg(), cursor, "mart_x", COLUMNS)
    assert rolled and rolled[-1][-1] == 4


def test_incremental_without_pending_rows_uses_existing_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unchanged mart still reconciles against its current target count."""
    monkeypatch.setattr(
        publish, "_snapshot", lambda *_a: publish.Snapshot(4, 0, 0, None)
    )
    monkeypatch.setattr(
        publish,
        "_table_version",
        lambda *_a: pytest.fail("no merge means no restore point"),
    )
    cursor = _cursor(**{"SELECT max": [(None, 4)]})
    snap, changed, target = publish._incremental(_pg(), cursor, "mart_x", COLUMNS)
    assert (changed, target, snap.source_rows) == (0, 4, 4)


def test_initial_load_marks_the_table_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A first load only becomes readable after counts reconcile."""
    snap = publish.Snapshot(2, 2, 0, datetime(2026, 1, 2, tzinfo=UTC))
    monkeypatch.setattr(publish, "_snapshot", lambda *_a: snap)
    monkeypatch.setattr(publish, "_load_rows", lambda *_a: 2)
    monkeypatch.setattr(publish, "_count", lambda *_a: 2)
    cursor = _cursor()

    assert publish._initial_load(_pg(), cursor, "mart_x", COLUMNS) == (snap, 2, 2)
    assert "CREATE OR REPLACE TABLE" in cursor.calls[0]
    assert "SET TBLPROPERTIES" in cursor.calls[-1] and "ready" in cursor.calls[-1]

    failing = _cursor()
    monkeypatch.setattr(
        publish,
        "_load_rows",
        lambda *_a: (_ for _ in ()).throw(publish.PublishError("bad row")),
    )
    with pytest.raises(publish.PublishError, match="bad row"):
        publish._initial_load(_pg(), failing, "mart_x", COLUMNS)
    assert any("DROP TABLE" in call for call in failing.calls)


def test_publish_one_chooses_first_or_incremental_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Existing tables are inspected; interrupted ones are rebuilt."""
    monkeypatch.setattr(publish, "source_columns", lambda *_a: COLUMNS)
    connection = FakeDBConnection(_cursor())
    monkeypatch.setattr(publish, "open_databricks_connection", lambda: connection)
    snap = publish.Snapshot(5, 2, 0, None)
    seen: list[str] = []
    monkeypatch.setattr(
        publish, "_inspect_target", lambda *_a: seen.append("inspect") or False
    )
    monkeypatch.setattr(
        publish, "_incremental", lambda *_a: seen.append("incremental") or (snap, 2, 5)
    )
    monkeypatch.setattr(
        publish, "_initial_load", lambda *_a: seen.append("initial") or (snap, 5, 5)
    )

    result = publish.publish_one(_pg(), "mart_x", True)
    assert result == publish.PublishResult("mart_x", 5, 2, 5)
    assert seen == ["inspect", "incremental"]

    seen.clear()
    monkeypatch.setattr(
        publish, "_inspect_target", lambda *_a: seen.append("inspect") or True
    )

    publish.publish_one(_pg(), "mart_x", True)
    assert seen == ["inspect", "initial"]

    seen.clear()
    publish.publish_one(_pg(), "mart_x", False)
    assert seen == ["initial"]
    assert connection.closed is True


def test_batch_helpers_handle_retries_and_external_runs() -> None:
    """Only a single local retry batch may be resumed."""
    pg = _pg(
        lambda sql, _params: (
            FakeResult(scalar=44.0)
            if "start_batch" in sql
            else FakeResult(rows=[{"batch_id": 44, "airflow_run_id": "run-1"}])
        )
    )
    assert publish._start_batch(pg, "dag", "run-1") == 44
    assert publish._resume_batch(pg, "dag") == (44, "run-1")

    empty = _pg(lambda _sql, _params: FakeResult(rows=[]))
    assert publish._resume_batch(empty, "dag") is None
    publish._finish_batch(pg, 44, "SUCCEEDED")
    assert any("finish_batch" in sql for sql, _ in pg.connections[-1].calls)


def test_reconcile_logs_comparison_to_the_given_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Published counts reach audit with the Postgres target."""
    seen: list[Any] = []
    pg = _pg()
    monkeypatch.setattr(
        publish.audit,
        "log_reconciliation",
        lambda batch, rows, **kwargs: seen.append((batch, rows, kwargs)),
    )
    publish._reconcile(pg, 9, publish.PublishResult("mart_x", 7, 2, 7))
    batch, rows, kwargs = seen[0]
    assert batch == 9 and kwargs["target"] is pg
    assert rows[0]["status"] == "MATCH"
    assert rows[0]["message"] == "incremental_rows=2"


def test_main_publishes_every_mart(
    monkeypatch: pytest.MonkeyPatch, audit_calls: dict[str, list[Any]]
) -> None:
    """All five allowlisted marts are published and reconciled independently."""
    pg = _pg()
    finished: list[tuple[Any, ...]] = []
    monkeypatch.setattr(publish, "_arguments", lambda: _args(None))
    monkeypatch.setattr(publish, "get_postgres_engine", lambda: pg)
    monkeypatch.setattr(publish, "_resume_batch", lambda *_a: None)
    monkeypatch.setattr(publish, "_start_batch", lambda *_a: 10)
    monkeypatch.setattr(
        publish, "_prepare_schema", lambda: set(publish.PUBLISHABLE_MARTS)
    )
    monkeypatch.setattr(
        publish,
        "publish_one",
        lambda _pg, mart, exists: publish.PublishResult(mart, 4, 1, 4),
    )
    monkeypatch.setattr(publish, "_finish_batch", lambda *a: finished.append(a))

    assert publish.main() == 0

    assert len(audit_calls["recon"]) == 5
    assert finished == [(pg, 10, "SUCCEEDED", None)]
    assert audit_calls["errors"] == []
    assert pg.disposed is True


def test_main_keeps_independent_marts_going_on_failure(
    monkeypatch: pytest.MonkeyPatch, audit_calls: dict[str, list[Any]]
) -> None:
    """One bad mart is audited but does not block the other four."""
    pg = _pg()
    finished: list[tuple[Any, ...]] = []
    monkeypatch.setattr(publish, "_arguments", lambda: _args(15))
    monkeypatch.setattr(publish, "get_postgres_engine", lambda: pg)
    monkeypatch.setattr(
        publish, "_prepare_schema", lambda: set(publish.PUBLISHABLE_MARTS)
    )

    def publish_one(_pg: Any, mart: str, _exists: bool) -> publish.PublishResult:
        if mart == "mart_truck_cost_monthly":
            raise publish.PublishError("schema drift")
        return publish.PublishResult(mart, 1, 1, 1)

    monkeypatch.setattr(publish, "publish_one", publish_one)
    monkeypatch.setattr(publish, "_finish_batch", lambda *a: finished.append(a))

    assert publish.main() == 1

    assert len(audit_calls["recon"]) == 4
    assert len(audit_calls["errors"]) == 2
    assert finished == []


def test_main_closes_a_resumed_batch_even_when_finish_fails(
    monkeypatch: pytest.MonkeyPatch, audit_calls: dict[str, list[Any]]
) -> None:
    """Batch cleanup failures are logged but never mask the publish result."""
    pg = _pg()
    monkeypatch.setattr(publish, "_arguments", lambda: _args(None))
    monkeypatch.setattr(publish, "get_postgres_engine", lambda: pg)
    monkeypatch.setattr(publish, "_resume_batch", lambda *_a: (21, "resumed"))
    monkeypatch.setattr(
        publish,
        "_prepare_schema",
        lambda: (_ for _ in ()).throw(SQLAlchemyError("offline")),
    )
    monkeypatch.setattr(
        publish,
        "_finish_batch",
        lambda *_a: (_ for _ in ()).throw(RuntimeError("finish failed")),
    )

    assert publish.main() == 1

    assert audit_calls["errors"][0]["error_code"] == "MART_PUBLISH_FAILED"
    assert "offline" in audit_calls["errors"][0]["error_message"] or (
        "SQLAlchemyError" in audit_calls["errors"][0]["error_message"]
    )
    assert pg.disposed is True
