"""Probe the Kafka speed layer and alert on lag or freshness breaches."""

from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import DAG, task

AIRFLOW_DIRECTORY = Path(__file__).resolve().parents[1]
if str(AIRFLOW_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_DIRECTORY))

from helpers.scripts.commads import build_stream_monitor_command, run_command
from helpers.scripts.notify import notify
from helpers.scripts.stream_health import latest_watchdog_health


@task(task_id="watch_stream_health", retries=0, execution_timeout=timedelta(minutes=5))
def watch_stream_health() -> None:
    """Probe Kafka lag and freshness, then send one redacted breach alert."""
    run_command(build_stream_monitor_command(), 300)
    health = latest_watchdog_health()
    if health is None or not (health.message or "").startswith("BREACH"):
        return
    notify(
        "[fleet] STREAM BREACH fleet_watchdog",
        {
            "batch_id": "-",
            "task_id": "watch_stream_health",
            "errors": [health.message],
            "airflow_url": os.getenv("AIRFLOW_BASE_URL", ""),
        },
        failure=True,
    )


with DAG(
    dag_id="fleet_watchdog",
    description="Probe Kafka stream lag and freshness every 15 minutes.",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    tags=["fleet", "watchdog"],
    doc_md="""# Fleet watchdog\n\nProbes Kafka lag and event freshness, writing append-only health rows and alerts.""",
    default_args={"owner": "fleet-data-eng"},
) as dag:
    watch_stream_health()
