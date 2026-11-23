"""Database helpers for read-only gold reporting."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from utils.connection import get_postgres_engine

ROOT = Path(__file__).resolve().parents[2]
FINDINGS_DIR = ROOT / "reports" / "findings"


def get_engine() -> Engine:
    """Return the cached Postgres engine."""
    return get_postgres_engine()


def query_df(sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    """Run a read-only SQL query and return a DataFrame."""
    with get_engine().connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)


def write_findings(script_name: str, findings: dict[str, Any]) -> Path:
    """Write script findings as JSON for INSIGHT.md."""
    FINDINGS_DIR.mkdir(parents=True, exist_ok=True)
    path = FINDINGS_DIR / f"{script_name}.json"
    path.write_text(json.dumps(findings, indent=2, default=str), encoding="utf-8")
    return path
