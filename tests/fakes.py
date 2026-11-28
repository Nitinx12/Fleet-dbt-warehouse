"""In-memory SQLAlchemy-shaped fakes for exercising script code paths."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import nullcontext
from typing import Any, Self

Handler = Callable[[str, dict[str, Any]], "FakeResult"]


class FakeRow(dict[str, Any]):
    """Mapping that also supports positional row access."""

    def __getitem__(self, key: str | int) -> Any:
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


class FakeResult:
    """Minimal result object supporting the accessors the scripts use."""

    def __init__(
        self,
        rows: list[Any] | None = None,
        *,
        scalar: Any = None,
    ) -> None:
        self.rows = rows if rows is not None else []
        self.scalar_value = scalar

    def __iter__(self) -> Iterator[Any]:
        """Allow mapping rows to be consumed directly."""
        return iter(self.rows)

    def scalar(self) -> Any:
        """Return a single value from `SELECT MAX(...)`-style queries."""
        return self.scalar_value

    def scalar_one(self) -> Any:
        """Return one scalar, preferring the first cell of the first row."""
        if self.rows:
            first = self.rows[0]
            return first[0] if isinstance(first, (tuple, list)) else first
        return self.scalar_value

    def one(self) -> Any:
        """Return the first row as a tuple."""
        return self.rows[0]

    def mappings(self) -> Self:
        """Return a view whose `.all()` yields dictionaries."""
        return self

    def all(self) -> list[dict[str, Any]]:
        """Return all rows as dictionaries."""
        return [FakeRow(row) if isinstance(row, dict) else row for row in self.rows]

    def scalars(self) -> Iterator[Any]:
        """Return the first cell of each row."""
        return (row[0] if isinstance(row, (tuple, list)) else row for row in self.rows)

    def fetchall(self) -> list[Any]:
        """Return all rows."""
        return list(self.rows)

    def fetchmany(self, _size: int) -> list[Any]:
        """Return the remaining rows; scripts loop until this is empty."""
        rows, self.rows = list(self.rows), []
        return rows

    def fetchone(self) -> Any | None:
        """Return and remove the first row."""
        return self.rows.pop(0) if self.rows else None


class FakeConnection:
    """Capture executed SQL and delegate results to a scripted handler."""

    def __init__(self, handler: Handler) -> None:
        self.handler = handler
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def begin(self) -> Any:
        """Return a no-op transaction context."""
        return nullcontext(self)

    def execution_options(self, **_options: Any) -> Self:
        """Ignore streaming options on the fake connection."""
        return self

    def execute(self, statement: Any, params: Any = None) -> FakeResult:
        """Record the statement and return the handler's response."""
        sql = str(statement)
        bound = params if isinstance(params, list) else dict(params or {})
        self.calls.append((sql, bound))
        return self.handler(sql, bound)


class FakeEngine:
    """Engine-shaped object whose connections share one handler."""

    def __init__(self, handler: Handler) -> None:
        self.handler = handler
        self.connections: list[FakeConnection] = []
        self.disposed = False
        self.url = type(
            "Url",
            (),
            {"host": "db", "port": 5432, "database": "fleet_it_test"},
        )()

    def connect(self) -> FakeConnection:
        """Return a fresh recorded connection."""
        connection = FakeConnection(self.handler)
        self.connections.append(connection)
        return connection

    def begin(self) -> FakeConnection:
        """Return a connection usable as a transaction context manager."""
        return self.connect()

    def dispose(self) -> None:
        """Record engine cleanup."""
        self.disposed = True


class FakeCollection:
    """Small stand-in for a PyMongo collection."""

    def __init__(
        self, documents: list[dict[str, Any]], counts: int | None = None
    ) -> None:
        self.documents = documents
        self.counts = len(documents) if counts is None else counts
        self.find_calls: list[tuple[dict[str, Any], dict[str, Any]]] = []

    def find(self, query: dict[str, Any], projection: dict[str, Any]) -> FakeCursor:
        """Return documents matching a simple equality/range query."""
        self.find_calls.append((query, projection))
        documents = [doc for doc in self.documents if _matches(doc, query)]
        return FakeCursor(documents)

    def count_documents(self, _query: dict[str, Any]) -> int:
        """Return the scripted source row count."""
        return self.counts


class FakeCursor:
    """Chainable stand-in for a PyMongo cursor."""

    def __init__(self, documents: list[dict[str, Any]]) -> None:
        self.documents = documents

    def sort(self, field: str, direction: int) -> FakeCursor:
        """Sort documents by one field."""
        self.documents = sorted(
            self.documents,
            key=lambda doc: (doc.get(field) is None, doc.get(field)),
            reverse=direction < 0,
        )
        return self

    def limit(self, size: int) -> FakeCursor:
        """Keep only the first `size` documents."""
        self.documents = self.documents[:size]
        return self

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self.documents)


class FakeDBCursor:
    """Capture Databricks SQL and return scripted result rows."""

    def __init__(
        self, responses: dict[str, list[tuple[Any, ...]]] | None = None
    ) -> None:
        self.responses = responses or {}
        self.calls: list[str] = []
        self.rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, statement: str, _params: Any = None) -> None:
        """Record a statement and select rows by SQL fragment."""
        self.calls.append(statement)
        self.rows = []
        for fragment, rows in self.responses.items():
            if fragment in statement:
                self.rows = list(rows)
                break

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return all rows from the last statement."""
        return list(self.rows)

    def fetchone(self) -> tuple[Any, ...] | None:
        """Return the first row from the last statement."""
        return self.rows[0] if self.rows else None


class FakeDBConnection:
    """Databricks connection-shaped object with one reusable cursor."""

    def __init__(self, cursor: FakeDBCursor) -> None:
        self.cursor_obj = cursor
        self.closed = False

    def cursor(self) -> FakeDBCursor:
        """Return the scripted cursor."""
        return self.cursor_obj

    def close(self) -> None:
        """Record connection cleanup."""
        self.closed = True


class FakeMongoClient:
    """Map ``client[database][collection]`` onto fake collections."""

    def __init__(self, collections: dict[str, FakeCollection]) -> None:
        self.collections = collections
        self.client = self
        self.name = "fleet_it_test"
        self.closed = False

    def __getitem__(self, database: str) -> dict[str, FakeCollection]:
        return self.collections[database]

    def close(self) -> None:
        """Record client cleanup."""
        self.closed = True


def _matches(document: dict[str, Any], query: dict[str, Any]) -> bool:
    """Match the `$gt`/`$lte` query shapes the extractor builds."""
    for field, expected in query.items():
        actual = document.get(field)
        if isinstance(expected, dict):
            if "$gt" in expected and (actual is None or actual <= expected["$gt"]):
                return False
            if "$lte" in expected and (actual is None or actual > expected["$lte"]):
                return False
        elif actual != expected:
            return False
    return True
