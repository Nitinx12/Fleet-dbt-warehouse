"""Test safe argv construction for Airflow pipeline stages."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

AIRFLOW_ROOT = Path(__file__).resolve().parents[1] / "airflow"
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from helpers.scripts.commads import (
    MART_RECONCILE_TEST,
    build_dbt_command,
    build_publish_command,
    build_stream_monitor_command,
    build_stream_reconcile_command,
)


@pytest.mark.unit
def test_dbt_command_has_target_batch_and_exclusion() -> None:
    """Gold tests carry the batch linkage and late mart-test exclusion."""
    argv = build_dbt_command(
        "test",
        ("dim_*", "fct_*"),
        42,
        "fleet_daily_load",
        "manual__1",
        "ci",
        (MART_RECONCILE_TEST,),
    )

    assert (
        argv[-10:]
        == [
            "--select",
            "dim_*",
            "fct_*",
            "--exclude",
            MART_RECONCILE_TEST,
            "--batch-id",
            "42",
            "--airflow-dag-id",
            "fleet_daily_load",
            "--airflow-run-id",
            "manual__1",
        ][-10:]
    )
    assert "--target" in argv and argv[argv.index("--target") + 1] == "ci"
    assert argv[argv.index("--indirect-selection") + 1] == "cautious"


@pytest.mark.unit
def test_publish_command_contains_no_shell_program() -> None:
    """Publication is invoked as a direct executable argument list."""
    argv = build_publish_command(4, "fleet_daily_load", "run-4")

    assert argv[:4] == ["uv", "run", "--no-sync", "python"]
    assert "publish_marts.py" in argv[4]
    assert all(";" not in item and "|" not in item for item in argv)


@pytest.mark.unit
def test_stream_monitor_command_is_a_thin_argv() -> None:
    """The watchdog probe runs one script with no shell interpolation."""
    argv = build_stream_monitor_command()

    assert argv[:4] == ["uv", "run", "--no-sync", "python"]
    assert "stream_monitor.py" in argv[4]
    assert len(argv) == 5


@pytest.mark.unit
def test_stream_reconcile_command_carries_batch_linkage() -> None:
    """Reconciliation is observational but keeps the same run identifiers."""
    argv = build_stream_reconcile_command(9, "fleet_daily_load", "run-9")

    assert "stream_reconcile.py" in argv[4]
    assert argv[-6:] == [
        "--batch-id",
        "9",
        "--airflow-dag-id",
        "fleet_daily_load",
        "--airflow-run-id",
        "run-9",
    ]
