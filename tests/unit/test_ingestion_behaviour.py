"""Ingestion extraction, bronze writes and batch flow use contract-bound behaviour."""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Self

import pytest
from pymongo.errors import PyMongoError
from sqlalchemy.exc import SQLAlchemyError

from scripts import ingestion
from tests.fakes import FakeCollection, FakeEngine, FakeMongoClient, FakeResult

CONTRACT_YAML = """\
# header comment
contract_version: 1.0.0
source: mysql.fleet_registry.loads
landing_table: bronze.mysql__loads
owner: team
primary_key:
- load_id
extraction:
  mode: incremental
  cursor: updated_at
  lookback_days: 14
columns:
- name: load_id
  type: string
  nullable: false
- name: updated_at
  type: timestamp
  staged_type: timestamp
"""


def _contract(path: Path, text: str = CONTRACT_YAML) -> Path:
    """Write one small contract file."""
    path.write_text(text, encoding="utf-8")
    return path


def _spec(
    *,
    load_type: str = "INCREMENTAL",
    system: str = "mysql",
    table: str = "loads",
    target: str = "mysql__loads",
    columns: tuple[str, ...] = ("id", "amount", "updated_at"),
    pk: tuple[str, ...] = ("id",),
    cursor: str | None = "updated_at",
) -> ingestion.Source:
    """Build a source spec without touching the registry."""
    return ingestion.Source(
        source_table_id=7,
        system=system,
        schema="fleet_registry" if system == "mysql" else "fleet_operations",
        table=table,
        target=target,
        load_type=load_type,
        cursor=cursor,
        watermark_type="TIMESTAMP",
        pk=pk,
        columns=columns,
    )


def _router(*pairs: tuple[str, FakeResult]) -> Any:
    """Return a handler that answers by SQL fragment, defaulting to an empty result."""

    def handler(sql: str, _params: dict[str, Any]) -> FakeResult:
        for fragment, result in pairs:
            if fragment in sql:
                return result
        return FakeResult()

    return handler


def test_read_contract_parses_the_subset_ingestion_needs(tmp_path: Path) -> None:
    """Comments, nested columns and scalar quoting all parse deterministically."""
    contract = ingestion.read_contract(_contract(tmp_path / "loads.yml"))

    assert contract["source"] == "mysql.fleet_registry.loads"
    assert contract["landing_table"] == "bronze.mysql__loads"
    assert contract["primary_key"] == ["load_id"]
    assert contract["extraction"] == {
        "mode": "incremental",
        "cursor": "updated_at",
        "lookback_days": 14,
    }
    assert contract["columns"][0]["name"] == "load_id"
    assert contract["columns"][0]["type"] == "string"


def test_read_contract_fails_on_missing_required_keys(tmp_path: Path) -> None:
    """An incomplete contract never reaches the registry."""
    path = _contract(tmp_path / "bad.yml", "source: mysql.fleet_registry.loads\n")
    with pytest.raises(ValueError, match="contract is missing"):
        ingestion.read_contract(path)


@pytest.mark.parametrize("name", ["bad-name", "a.b", "1id", "x;drop"])
def test_identifiers_reject_sql_fragments(name: str) -> None:
    """Only plain SQL identifiers are accepted from contracts."""
    with pytest.raises(ValueError, match="unsafe identifier"):
        ingestion._ident(name, "test")


def test_registry_row_rejects_unsafe_or_unknown_contracts() -> None:
    """Landing schema, extraction mode and cursor are validated before use."""
    base: dict[str, Any] = {
        "source": "mysql.fleet_registry.loads",
        "landing_table": "bronze.mysql__loads",
        "extraction": {"mode": "incremental", "cursor": "updated_at"},
        "primary_key": ["load_id"],
        "columns": [{"name": "load_id"}, {"name": "updated_at", "type": "date"}],
    }
    with pytest.raises(ValueError, match="landing schema must be bronze"):
        ingestion._registry_row({**base, "landing_table": "gold.mysql__loads"})
    with pytest.raises(ValueError, match="unknown extraction mode"):
        ingestion._registry_row({**base, "extraction": {"mode": "append"}})
    with pytest.raises(ValueError, match="incremental needs a cursor"):
        ingestion._registry_row({**base, "extraction": {"mode": "incremental"}})
    with pytest.raises(ValueError, match="unsafe identifier"):
        ingestion._registry_row({**base, "primary_key": ["id); drop table x"]})

    row = ingestion._registry_row(base)
    assert row["load_type"] == "INCREMENTAL"
    assert row["watermark_type"] == "DATE"
    assert row["pk"] == ["load_id"]
    full = ingestion._registry_row(
        {**base, "extraction": {"mode": "full_replace"}, "primary_key": "load_id"}
    )
    assert full["load_type"] == "FULL"
    assert full["cursor"] is None and full["watermark_type"] is None


def test_registry_upsert_orders_full_loads_first() -> None:
    """FULL loads get low load orders and incremental loads start at 110."""
    contracts = [
        {
            "source": "mongodb.fleet_operations.events",
            "landing_table": "bronze.mongo__events",
            "extraction": {"mode": "incremental", "cursor": "loaded_at"},
            "primary_key": ["event_id"],
            "columns": [
                {"name": "event_id"},
                {"name": "loaded_at", "type": "timestamp"},
            ],
        },
        {
            "source": "mysql.fleet_registry.trucks",
            "landing_table": "bronze.mysql__trucks",
            "extraction": {"mode": "full_replace"},
            "primary_key": ["truck_id"],
            "columns": [{"name": "truck_id"}],
        },
    ]
    pg = FakeEngine(
        _router(
            ("RETURNING pipeline_id", FakeResult(scalar=3)),
            ("RETURNING source_table_id", FakeResult(scalar=11)),
        )
    )

    sources = ingestion._ensure_registry(pg, contracts)

    assert [source.table for source in sources] == ["trucks", "events"]
    assert [source.load_type for source in sources] == ["FULL", "INCREMENTAL"]
    inserted = [
        call[1]["load_order"]
        for call in pg.connections[0].calls
        if "load_order" in call[1]
    ]
    assert inserted == [10, 110]


def test_control_helpers_use_the_supplied_ids() -> None:
    """Batch and watermark calls bind ids rather than interpolate them."""
    pg = FakeEngine(
        _router(
            ("control.start_batch", FakeResult(scalar=5.0)),
            ("control.finish_batch", FakeResult(rows=[(1,)])),
            ("control.get_watermark", FakeResult(scalar="2026-01-01 00:00:00")),
            ("control.advance_watermark", FakeResult(rows=[(1,)])),
        )
    )
    spec = _spec()

    assert ingestion._start_batch(pg, "run-1", "dag-1") == 5
    assert ingestion._watermark(pg, spec) == "2026-01-01 00:00:00"
    assert ingestion._watermark(pg, _spec(load_type="FULL", cursor=None)) is None
    ingestion._advance_watermark(pg, spec, 5, "2026-01-02 00:00:00")
    ingestion._finish_batch(pg, 5, "FAILED", "failed (ValueError)")

    calls = [call for connection in pg.connections for call in connection.calls]
    assert calls[0][1]["run_id"] == "run-1"
    assert calls[0][1]["dag_id"] == "dag-1"
    assert calls[2][1]["source_table_id"] == 7
    assert calls[2][1]["value"] == "2026-01-02 00:00:00"
    assert calls[3][1]["status"] == "FAILED"


def test_cursor_formatting_and_type_mapping() -> None:
    """Watermarks use ISO text; MySQL types map to native Postgres types."""
    assert ingestion._lower_as("2026-01-02T03:04:05", "TIMESTAMP") == datetime(
        2026, 1, 2, 3, 4, 5
    )
    assert ingestion._lower_as("2026-01-02T03:04:05", "DATE") == date(2026, 1, 2)
    assert ingestion._watermark_text(datetime(2026, 1, 2, 3, 4, 5)) == (
        "2026-01-02 03:04:05"
    )
    assert ingestion._watermark_text(date(2026, 1, 2)) == "2026-01-02"
    assert ingestion._watermark_text("42") == "42"
    assert ingestion._pg_type("varchar") == "text"
    assert ingestion._pg_type("bigint") == "bigint"
    assert ingestion._pg_type("json") == "text"
    assert ingestion._pg_type("mystery") == "text"


def test_column_types_fail_on_contract_drift() -> None:
    """A source missing a contract column is rejected before landing."""
    mysql = FakeEngine(
        lambda _sql, _params: FakeResult(rows=[("id", "int"), ("amount", "decimal")])
    )
    spec = _spec(columns=("id", "amount", "updated_at"))
    with pytest.raises(ValueError, match="missing source columns"):
        ingestion._column_types(mysql, spec)

    spec = _spec(columns=("id", "amount"))
    assert ingestion._column_types(mysql, spec) == {
        "id": "integer",
        "amount": "numeric",
    }


def test_ensure_bronze_adds_missing_columns_and_key() -> None:
    """Landing tables gain new contract columns but never change an existing key."""
    pg = FakeEngine(
        _router(
            ("information_schema.columns", FakeResult(rows=[("id",), ("amount",)])),
            ("pg_index", FakeResult(rows=[("id",)])),
        )
    )
    spec = _spec(columns=("id", "amount"), pk=("id",))
    ingestion._ensure_bronze(pg, spec, {"id": "integer", "amount": "numeric"})
    statements = [sql for sql, _ in pg.connections[0].calls]
    assert any("ADD COLUMN updated_at" in sql for sql in statements) is False
    assert any(
        "CREATE TABLE IF NOT EXISTS bronze.mysql__loads" in sql for sql in statements
    )

    pg2 = FakeEngine(
        _router(
            ("information_schema.columns", FakeResult(rows=[("id",)])),
            ("pg_index", FakeResult(rows=[("id",)])),
        )
    )
    ingestion._ensure_bronze(pg2, spec, {"id": "integer", "amount": "numeric"})
    assert any(
        "ADD COLUMN amount numeric" in sql for sql, _ in pg2.connections[0].calls
    )


def test_ensure_bronze_rejects_a_different_primary_key() -> None:
    """An incompatible existing key must not be silently upserted."""
    pg = FakeEngine(
        _router(
            ("information_schema.columns", FakeResult(rows=[("id",), ("amount",)])),
            ("pg_index", FakeResult(rows=[("amount",)])),
        )
    )
    with pytest.raises(ValueError, match="does not match"):
        ingestion._ensure_bronze(
            pg, _spec(columns=("id", "amount")), {"id": "integer", "amount": "numeric"}
        )


def test_ensure_bronze_adds_a_missing_primary_key() -> None:
    """A table created without a key gets the contract key added."""
    pg = FakeEngine(
        _router(
            ("information_schema.columns", FakeResult(rows=[("id",)])),
            ("pg_index", FakeResult(rows=[])),
        )
    )
    ingestion._ensure_bronze_primary_key(
        pg.connect(), _spec(columns=("id",), pk=("id",))
    )
    assert any("ADD PRIMARY KEY (id)" in sql for sql, _ in pg.connections[0].calls)


def test_extract_mysql_full_and_incremental() -> None:
    """FULL ignores watermarks; INCREMENTAL bounds the cursor on both sides."""
    rows = [{"id": 1, "amount": 2, "updated_at": datetime(2026, 1, 2)}]
    mysql = FakeEngine(
        _router(
            ("MAX(`updated_at`)", FakeResult(scalar=datetime(2026, 1, 3))),
            ("SELECT `id`", FakeResult(rows=rows)),
        )
    )
    spec = _spec()
    upper, extracted = ingestion._extract_mysql(mysql, spec, "2026-01-01 00:00:00")
    assert upper == datetime(2026, 1, 3)
    assert extracted == rows
    bound = mysql.connections[0].calls[-1]
    assert "`updated_at` > :lower" in bound[0]
    assert bound[1]["lower"] == datetime(2026, 1, 1)

    upper, extracted = ingestion._extract_mysql(mysql, spec, None)
    assert upper == datetime(2026, 1, 3)
    assert ":lower" not in mysql.connections[-1].calls[-1][0]

    empty = FakeEngine(_router(("MAX(`updated_at`)", FakeResult(scalar=None))))
    assert ingestion._extract_mysql(empty, spec, None) == (None, [])

    full = FakeEngine(_router(("SELECT `id`", FakeResult(rows=rows))))
    assert ingestion._extract_mysql(
        full, _spec(load_type="FULL", cursor=None), "old"
    ) == (
        None,
        rows,
    )
    assert "WHERE" not in full.connections[0].calls[-1][0]


def test_extract_mongo_projects_only_contract_columns() -> None:
    """Mongo values stay strings and the latest document sets the upper bound."""
    collection = FakeCollection(
        [
            {"id": "1", "amount": "10", "updated_at": "2026-01-01"},
            {"id": "2", "amount": "20", "updated_at": "2026-01-03"},
        ]
    )
    mongo = FakeMongoClient({"fleet_operations": {"loads": collection}})
    spec = _spec(system="mongo", table="loads")
    upper, rows = ingestion._extract_mongo(mongo, spec, "2026-01-01")

    assert upper == "2026-01-03"
    assert rows == [{"id": "2", "amount": "20", "updated_at": "2026-01-03"}]
    _, projection = collection.find_calls[-1]
    assert projection == {"id": 1, "amount": 1, "updated_at": 1, "_id": 0}

    _, all_rows = ingestion._extract_mongo(mongo, spec, None)
    assert len(all_rows) == 2

    empty = FakeMongoClient({"fleet_operations": {"loads": FakeCollection([])}})
    assert ingestion._extract_mongo(empty, spec, None) == (None, [])


def test_write_bronze_upserts_or_replaces_by_load_type() -> None:
    """INCREMENTAL uses ON CONFLICT; FULL deletes before chunked inserts."""
    pg = FakeEngine(lambda _sql, _params: FakeResult(rows=[]))
    rows = [{"id": i, "amount": i, "updated_at": "x"} for i in range(3)]
    assert ingestion._write_bronze(pg, _spec(), rows) == 3
    statements = [sql for sql, _ in pg.connections[0].calls]
    assert all("DELETE" not in sql for sql in statements)
    assert any(
        "ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount" in sql
        for sql in statements
    )

    full = FakeEngine(lambda _sql, _params: FakeResult(rows=[]))
    ingestion._write_bronze(full, _spec(load_type="FULL", cursor=None), rows)
    assert "DELETE FROM bronze.mysql__loads" in full.connections[0].calls[0][0]

    key_only = FakeEngine(lambda _sql, _params: FakeResult(rows=[]))
    ingestion._write_bronze(
        key_only, _spec(columns=("id",), pk=("id",), cursor=None), rows[:1]
    )
    assert "DO NOTHING" in key_only.connections[0].calls[0][0]


def test_chunks_are_bounded() -> None:
    """Large arrays are split into insert-sized slices."""
    rows = [{"id": i} for i in range(5)]
    assert [len(chunk) for chunk in ingestion._chunks(rows, 2)] == [2, 2, 1]


def test_counts_and_reconciliation_are_recorded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bronze/source counts feed one reconciliation row per table."""
    pg = FakeEngine(_router(("COUNT(*)", FakeResult(scalar=9.0))))
    mysql = FakeEngine(_router(("COUNT(*)", FakeResult(scalar=10.0))))
    assert ingestion._count_bronze(pg, _spec()) == 9
    assert ingestion._count_mysql(mysql, _spec()) == 10

    seen: list[dict[str, Any]] = []
    monkeypatch.setattr(
        ingestion.audit,
        "log_reconciliation",
        lambda _batch, rows: seen.extend(rows),
    )
    ingestion._log_recon(pg, 4, _spec(), 10, 9, 8, "2026-01-02")
    assert seen[0]["status"] == "MISMATCH"
    assert seen[0]["source_row_count"] == 10
    assert "extracted=8" in seen[0]["message"]


def test_ingest_one_runs_extract_write_and_audit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One table produces a Stats row and marks its audit task with the row count."""
    spec = _spec()
    seen: list[Any] = []
    monkeypatch.setattr(ingestion, "_watermark", lambda *_a: "2026-01-01 00:00:00")
    monkeypatch.setattr(
        ingestion, "_extract_mysql", lambda *_a: ("2026-01-02 00:00:00", [{"id": 1}])
    )
    monkeypatch.setattr(ingestion, "_column_types", lambda *_a: {"id": "integer"})
    monkeypatch.setattr(ingestion, "_count_mysql", lambda *_a: 3)
    monkeypatch.setattr(ingestion, "_ensure_bronze", lambda *_a: seen.append("ensure"))
    monkeypatch.setattr(
        ingestion,
        "_write_bronze",
        lambda *_a: seen.append("write") or 1,
    )
    monkeypatch.setattr(
        ingestion, "_advance_watermark", lambda *_a: seen.append("advance")
    )
    monkeypatch.setattr(ingestion, "_count_bronze", lambda *_a: 3)
    monkeypatch.setattr(ingestion, "_log_recon", lambda *_a: seen.append("recon"))

    class Task:
        rows_processed: int | None = None

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

    task = Task()
    monkeypatch.setattr(ingestion.audit, "task_run", lambda *_a, **_kw: task)

    stats = ingestion._ingest_one(None, None, None, spec, 5, "run", "dag")

    assert seen == ["ensure", "write", "advance", "recon"]
    assert task.rows_processed == 1
    assert stats == ingestion.Stats(
        "mysql__loads", "INCREMENTAL", 1, 3, 3, "2026-01-02 00:00:00"
    )


def test_databricks_status_never_fails_the_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A connectivity probe reports the error type and keeps running."""

    class Cursor:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def execute(self, _sql: str) -> None:
            return None

        def fetchone(self) -> tuple[int]:
            return (1,)

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

    monkeypatch.setattr(ingestion, "get_databricks_connection", lambda: Connection())
    assert ingestion._databricks_status() == "ok"

    def fail() -> None:
        raise PyMongoError("no endpoint")

    monkeypatch.setattr(ingestion, "get_databricks_connection", fail)
    assert ingestion._databricks_status() == "failed (PyMongoError)"


def test_report_summarises_audit_and_control(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The end-of-run report reads audit counts and current watermarks."""
    pg = FakeEngine(
        _router(
            ("audit.task_run_log", FakeResult(rows=[("SUCCEEDED", 2)])),
            ("audit.reconciliation_results", FakeResult(rows=[("MATCH", 2)])),
            ("audit.pipeline_run_log", FakeResult(rows=[("STARTED",), ("SUCCEEDED",)])),
            ("control.watermark", FakeResult(rows=[("mysql__loads", "2026-01-02")])),
        )
    )
    monkeypatch.setattr(ingestion, "_databricks_status", lambda: "ok")
    caplog.set_level(logging.INFO)
    stats = [ingestion.Stats("mysql__loads", "INCREMENTAL", 2, 3, 3, "2026-01-02")]

    ingestion._report(pg, 11, stats)

    text = caplog.text
    assert "extracted=2" in text
    assert "audit.task_run_log SUCCEEDED: 2" in text
    assert "audit.reconciliation_results MATCH: 2" in text
    assert "control.watermark mysql__loads = 2026-01-02" in text
    assert "databricks connectivity: ok" in text


def test_arguments_and_selection_rules() -> None:
    """CLI flags reject invalid batches and select by source or table."""
    with pytest.raises(SystemExit):
        ingestion._arguments(["--batch-id", "0"])
    args = ingestion._arguments(
        [
            "--source",
            "mongo",
            "--table",
            "events",
            "--batch-id",
            "4",
            "--airflow-run-id",
            "r",
        ]
    )
    assert (args.source, args.table, args.batch_id, args.airflow_run_id) == (
        "mongo",
        "events",
        4,
        "r",
    )

    contracts = [
        {"source": "mysql.fleet_registry.drivers"},
        {"source": "mongodb.fleet_operations.events"},
    ]
    assert ingestion._selected_contracts(contracts, None, None) == contracts
    assert len(ingestion._selected_contracts(contracts, "mongo", None)) == 1
    with pytest.raises(ValueError, match="no contract matches"):
        ingestion._selected_contracts(contracts, "mongo", "drivers")


def test_main_completes_a_owned_batch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A successful pass finishes its own batch and disposes every engine."""
    calls: list[str] = []
    pg = FakeEngine(lambda _sql, _params: FakeResult(scalar=1))
    mysql = FakeEngine(lambda _sql, _params: FakeResult())
    mongo = FakeMongoClient({})
    contract = _contract(
        tmp_path / "mysql__trucks.yml",
        "source: mysql.fleet_registry.trucks\n"
        "landing_table: bronze.mysql__trucks\n"
        "primary_key:\n- truck_id\n"
        "extraction:\n  mode: full_replace\n"
        "columns:\n- name: truck_id\n  type: string\n",
    )
    monkeypatch.setattr(ingestion, "CONTRACTS_DIR", contract.parent)
    monkeypatch.setattr(ingestion, "get_postgres_engine", lambda: pg)
    monkeypatch.setattr(ingestion, "get_mysql_engine", lambda: mysql)
    monkeypatch.setattr(ingestion, "get_mongo_db", lambda: mongo)
    monkeypatch.setattr(ingestion, "_start_batch", lambda *_a: 99)
    monkeypatch.setattr(
        ingestion,
        "_ingest_one",
        lambda *_a: (
            calls.append("ingest") or ingestion.Stats("t", "FULL", 1, 1, 1, None)
        ),
    )
    monkeypatch.setattr(ingestion, "_finish_batch", lambda *_a: calls.append("finish"))
    monkeypatch.setattr(ingestion, "_report", lambda *_a: calls.append("report"))

    assert ingestion.main([]) == 0

    assert calls == ["ingest", "finish", "report"]
    assert mongo.closed and pg.disposed and mysql.disposed


def test_main_fails_and_logs_without_closing_a_shared_batch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A failed task logs an error but leaves the Airflow-owned batch open."""
    calls: list[str] = []
    pg = FakeEngine(lambda _sql, _params: FakeResult(scalar=1))
    mysql = FakeEngine(lambda _sql, _params: FakeResult())
    mongo = FakeMongoClient({})
    contract = _contract(
        tmp_path / "mysql__trucks.yml",
        "source: mysql.fleet_registry.trucks\n"
        "landing_table: bronze.mysql__trucks\n"
        "primary_key:\n- truck_id\n"
        "extraction:\n  mode: full_replace\n"
        "columns:\n- name: truck_id\n  type: string\n",
    )
    monkeypatch.setattr(ingestion, "CONTRACTS_DIR", contract.parent)
    monkeypatch.setattr(ingestion, "get_postgres_engine", lambda: pg)
    monkeypatch.setattr(ingestion, "get_mysql_engine", lambda: mysql)
    monkeypatch.setattr(ingestion, "get_mongo_db", lambda: mongo)
    monkeypatch.setattr(
        ingestion,
        "_ingest_one",
        lambda *_a: (_ for _ in ()).throw(SQLAlchemyError("x")),
    )
    monkeypatch.setattr(
        ingestion,
        "audit",
        type(
            "Audit",
            (),
            {
                "error_summary": staticmethod(lambda error, op: "summarised"),
                "log_error": staticmethod(lambda **_kw: calls.append("error")),
            },
        )(),
    )
    monkeypatch.setattr(ingestion, "_finish_batch", lambda *_a: calls.append("finish"))

    assert ingestion.main(["--batch-id", "12"]) == 1

    assert calls == ["error"]
    assert mongo.closed and pg.disposed and mysql.disposed


def test_main_rejects_an_unknown_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bad --table filter exits before any connection is opened."""
    monkeypatch.setattr(ingestion, "CONTRACTS_DIR", Path("contracts/sources"))
    monkeypatch.setattr(
        ingestion, "get_postgres_engine", lambda: pytest.fail("must not connect")
    )
    assert ingestion.main(["--source", "mongo", "--table", "missing"]) == 1
