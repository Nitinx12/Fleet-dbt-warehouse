"""Fail control batches left RUNNING after a crashed worker."""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import DAG, task

AIRFLOW_DIRECTORY = Path(__file__).resolve().parents[1]
if str(AIRFLOW_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_DIRECTORY))

from helpers.scripts.finalize_audit import (
    fail_stale_batches as run_stale_cleanup,
)
from helpers.scripts.finalize_audit import (
    stale_cleanup_enabled,
)


@task(task_id="fail_stale_batches", retries=0, execution_timeout=timedelta(minutes=5))
def fail_stale_batches() -> int:
    """Fail stale control batches only when the maintenance switch is enabled."""
    if not stale_cleanup_enabled():
        return 0
    return run_stale_cleanup()


with DAG(
    dag_id="fleet_batch_cleanup",
    description="Fail stale control batches left running by crashed workers.",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    tags=["fleet", "maintenance"],
    doc_md="""# Fleet batch cleanup\n\nFails RUNNING control batches older than the configured maximum age when enabled.""",
    default_args={"owner": "fleet-data-eng"},
) as dag:
    fail_stale_batches()
