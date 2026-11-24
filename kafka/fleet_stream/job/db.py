"""Minimal Postgres access for stream job metadata and checkpoints."""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any

import psycopg2
from psycopg2.extras import execute_values


class StreamDbError(RuntimeError):
    """Raised when a stream database operation fails."""


@contextmanager
def connection() -> Iterator[Any]:
    """Open one short-lived Postgres connection from environment settings."""
    params: dict[str, Any] = {
        "host": os.getenv("PG_HOST", "localhost"),
        "port": int(os.getenv("PG_PORT", "5432")),
        "dbname": os.getenv("PG_DBNAME", "fleet_dev"),
        "user": os.getenv("PG_USER", "postgres"),
        "password": os.getenv("PG_PASSWORD", ""),
        "connect_timeout": 10,
    }
    if os.getenv("PG_SSLMODE"):
        params["sslmode"] = os.environ["PG_SSLMODE"]
    conn = psycopg2.connect(**params)
    try:
        yield conn
    finally:
        conn.close()


def jdbc_url() -> str:
    """Return the Postgres JDBC URL for Spark's read/write calls."""
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    database = os.getenv("PG_DBNAME", "fleet_dev")
    return f"jdbc:postgresql://{host}:{port}/{database}"


def jdbc_properties() -> dict[str, str]:
    """Return JDBC driver properties from environment settings."""
    return {
        "user": os.getenv("PG_USER", "postgres"),
        "password": os.getenv("PG_PASSWORD", ""),
        "driver": "org.postgresql.Driver",
    }


def execute(sql: str, params: Sequence[Any] | Mapping[str, Any] = ()) -> None:
    """Execute one write statement in its own transaction."""
    with connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
        conn.commit()


def fetchone(sql: str, params: Sequence[Any] | Mapping[str, Any] = ()) -> tuple | None:
    """Execute one read-only statement and return its first row."""
    with connection() as conn, conn.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchone()


def fetchall(sql: str, params: Sequence[Any] | Mapping[str, Any] = ()) -> list[tuple]:
    """Execute one read-only statement and return all rows."""
    with connection() as conn, conn.cursor() as cursor:
        cursor.execute(sql, params)
        return list(cursor.fetchall())


def insert_many(
    table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]
) -> None:
    """Insert many rows through psycopg2's batched VALUES form."""
    if not rows:
        return
    column_list = ", ".join(columns)
    sql = f"INSERT INTO {table} ({column_list}) VALUES %s"
    with connection() as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, sql, rows, page_size=1_000)
        conn.commit()
