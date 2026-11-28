"""Close control batches without mutating append-only audit tables."""

from __future__ import annotations

import math
import os
from datetime import timedelta

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from utils.connection import get_postgres_engine

from .redact import redact


def stale_cleanup_enabled() -> bool:
    """Return whether scheduled stale-batch cleanup is enabled."""
    return os.getenv("FLEET_STALE_CLEANUP_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def stale_batch_max_age() -> timedelta:
    """Return the configured maximum age for a RUNNING batch."""
    raw_hours = os.getenv("FLEET_STALE_BATCH_MAX_AGE_HOURS", "6")
    try:
        hours = float(raw_hours)
    except ValueError as error:
        raise ValueError("FLEET_STALE_BATCH_MAX_AGE_HOURS must be numeric") from error
    if not math.isfinite(hours) or hours < 0:
        raise ValueError(
            "FLEET_STALE_BATCH_MAX_AGE_HOURS must be finite and non-negative"
        )
    return timedelta(hours=hours)


def fail_stale_batches(max_age: timedelta | None = None) -> int:
    """Fail batches older than max_age and return how many were failed."""
    age = stale_batch_max_age() if max_age is None else max_age
    with get_postgres_engine().begin() as connection:
        return int(
            connection.execute(
                text("SELECT control.fail_stale_batches(:max_age)"),
                {"max_age": age},
            ).scalar_one()
        )


def start_batch(*args: object) -> int:
    """Open or reopen the idempotent daily batch and return its identifier."""
    if len(args) == 4:
        pipeline, dag_id, run_id, logical_date = args
    elif len(args) == 3:
        pipeline, dag_id, run_id, logical_date = "fleet_daily", *args
    else:
        raise TypeError("start_batch expects dag_id, run_id, logical_date")
    with get_postgres_engine().begin() as connection:
        return int(
            connection.execute(
                text(
                    "SELECT control.start_batch(:pipeline, :dag_id, :run_id, :logical_date, 'airflow')"
                ),
                {
                    "pipeline": str(pipeline),
                    "dag_id": str(dag_id),
                    "run_id": str(run_id),
                    "logical_date": logical_date,
                },
            ).scalar_one()
        )


def finalize_batch(batch_id: int, status: str, message: str | None = None) -> bool:
    """Finish a running batch; return false when a prior callback closed it."""
    if status not in {"SUCCEEDED", "FAILED", "CANCELLED"}:
        raise ValueError("invalid final batch status")
    try:
        with get_postgres_engine().begin() as connection:
            connection.execute(
                text("SELECT control.finish_batch(:batch_id, :status, :message)"),
                {
                    "batch_id": batch_id,
                    "status": status,
                    "message": redact(message or ""),
                },
            )
    except DBAPIError as error:
        if "not RUNNING" in str(error):
            return False
        raise
    return True
