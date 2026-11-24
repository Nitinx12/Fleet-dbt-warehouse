"""Throwaway Postgres fixture for integration tests."""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine

from utils import engine as config

_SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "scripts"
_CREATE_BLOCK = re.compile(r"\\else\b.*?\\endif\b", re.DOTALL)


def _ssl_query() -> dict[str, str]:
    """Return the optional SSL arguments used by the runtime connection."""
    return {
        name: value
        for name, value in {
            "sslmode": config.PG_SSLMODE,
            "channel_binding": config.PG_CHANNEL_BINDING,
        }.items()
        if value
    }


def _admin_connection() -> psycopg2.extensions.connection:
    """Connect to the maintenance database with the configured Postgres user."""
    return psycopg2.connect(
        dbname="postgres",
        user=config.PG_USER,
        password=config.PG_PASSWORD,
        host=config.PG_HOST,
        port=config.PG_PORT,
    )


def _database_url(database: str) -> URL:
    """Build a SQLAlchemy URL for the scratch database."""
    return URL.create(
        "postgresql+psycopg2",
        username=config.PG_USER,
        password=config.PG_PASSWORD,
        host=config.PG_HOST,
        port=config.PG_PORT,
        database=database,
        query=_ssl_query(),
    )


def _init_sql(sql: str, database: str) -> str:
    """Retarget the psql bootstrap script to the scratch database."""
    sql = _CREATE_BLOCK.sub("", sql)
    sql = sql.replace("\\gset", ";")
    sql = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("\\")
    )
    sql = sql.replace("fleet_warehouse", database)
    return sql.replace("AUTHORIZATION postgres", f"AUTHORIZATION {config.PG_USER}")


def _apply_scripts(database: str) -> None:
    """Apply scripts 01..07 to the scratch database in order."""
    connection = psycopg2.connect(
        dbname=database,
        user=config.PG_USER,
        password=config.PG_PASSWORD,
        host=config.PG_HOST,
        port=config.PG_PORT,
    )
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET client_min_messages TO warning")
            for path in sorted(_SQL_DIR.glob("[0-8][0-9]_*.sql")):
                sql = path.read_text(encoding="utf-8")
                if path.name.startswith("01_"):
                    sql = _init_sql(sql, database)
                cursor.execute(sql)
    finally:
        connection.close()


def _drop_database(database: str) -> None:
    """Remove the scratch database after the integration suite finishes."""
    connection = _admin_connection()
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
    finally:
        connection.close()


@pytest.fixture(scope="session")
def postgres_engine() -> Iterator[Engine]:
    """Return an engine bound to a throwaway database built from SQL scripts."""
    try:
        admin = _admin_connection()
    except psycopg2.OperationalError:
        pytest.skip("Postgres not reachable")

    database = f"fleet_it_{uuid4().hex[:12]}"
    admin.autocommit = True
    try:
        with admin.cursor() as cursor:
            cursor.execute(f'CREATE DATABASE "{database}"')
    finally:
        admin.close()

    engine = None
    try:
        _apply_scripts(database)
        engine = create_engine(_database_url(database))
        yield engine
    finally:
        if engine is not None:
            engine.dispose()
        _drop_database(database)
