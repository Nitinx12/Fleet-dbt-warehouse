"""Build safe argv lists and run warehouse scripts with redacted output."""

from __future__ import annotations

import logging
import os
import re
import subprocess
from collections.abc import Sequence
from pathlib import Path

from .redact import redact

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[3]
IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
SILVER_SELECTOR = "path:models/silver"
GOLD_DIM_SELECTOR = "dim_*"
GOLD_FACT_SELECTOR = "fct_*"
MART_SELECTOR = "mart_*"
MART_RECONCILE_TEST = "assert_marts_reconcile_to_facts"
SILVER_SELECTORS = (SILVER_SELECTOR,)
GOLD_DIM_SELECTORS = (GOLD_DIM_SELECTOR,)
GOLD_FACT_SELECTORS = (GOLD_FACT_SELECTOR,)
GOLD_TEST_SELECTORS = (GOLD_DIM_SELECTOR, GOLD_FACT_SELECTOR)
MARTS_SELECTORS = (MART_SELECTOR,)
MARTS_TEST_SELECTORS = (MART_SELECTOR, MART_RECONCILE_TEST)


class CommandError(RuntimeError):
    """Raised when a script exits unsuccessfully."""


def _identifier(value: str, name: str = "identifier") -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"invalid {name}")
    return value


def _base(script: str) -> list[str]:
    return ["uv", "run", "--no-sync", "python", str(ROOT / "scripts" / script)]


def ingest_argv(
    source: str, table: str, batch_id: int, dag_id: str, run_id: str
) -> list[str]:
    """Return the argv for one source-table ingestion task."""
    return _base("ingestion.py") + [
        "--source",
        _identifier(source, "source"),
        "--table",
        _identifier(table, "table"),
        "--batch-id",
        str(batch_id),
        "--airflow-dag-id",
        dag_id,
        "--airflow-run-id",
        run_id,
    ]


def dbt_argv(
    command: str,
    selectors: Sequence[str],
    batch_id: int,
    dag_id: str,
    run_id: str,
    target: str | None = None,
    exclude: Sequence[str] = (),
) -> list[str]:
    """Return the argv for one dbt invocation through the audited runner."""
    if command not in {"run", "test"}:
        raise ValueError("invalid dbt command")
    argv = _base("run_dbt.py") + [
        command,
        "--target",
        _identifier(target or os.getenv("DBT_TARGET", "dev"), "target"),
    ]
    if command == "test":
        argv.extend(["--indirect-selection", "cautious"])
    if selectors:
        argv.extend(["--select", *selectors])
    if exclude:
        argv.extend(["--exclude", *exclude])
    argv.extend(
        [
            "--batch-id",
            str(batch_id),
            "--airflow-dag-id",
            dag_id,
            "--airflow-run-id",
            run_id,
        ]
    )
    return argv


def publish_argv(batch_id: int, dag_id: str, run_id: str) -> list[str]:
    """Return the argv for the marts publisher."""
    return _base("publish_marts.py") + [
        "--batch-id",
        str(batch_id),
        "--airflow-dag-id",
        dag_id,
        "--airflow-run-id",
        run_id,
    ]


def monitor_argv() -> list[str]:
    """Return the argv for one watchdog stream probe."""
    return _base("stream_monitor.py")


def reconcile_argv(batch_id: int, dag_id: str, run_id: str) -> list[str]:
    """Return the argv for one observational stream reconciliation."""
    return _base("stream_reconcile.py") + [
        "--batch-id",
        str(batch_id),
        "--airflow-dag-id",
        dag_id,
        "--airflow-run-id",
        run_id,
    ]


def run_command(argv: Sequence[str], timeout: int = 3_600) -> None:
    """Run an argv list without a shell and surface only redacted diagnostics."""
    completed = subprocess.run(
        list(argv),
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    output = redact((completed.stdout or "") + (completed.stderr or ""))
    if output:
        log.info("%s", output.rstrip())
    if completed.returncode:
        raise CommandError(f"command failed with exit code {completed.returncode}")


build_ingest_command = ingest_argv
build_dbt_command = dbt_argv
build_publish_command = publish_argv
build_stream_monitor_command = monitor_argv
build_stream_reconcile_command = reconcile_argv
