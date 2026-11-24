"""Orchestrate the daily Fleet landing, dbt, and mart publication pipeline."""

from __future__ import annotations

import logging
import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.sdk import DAG, TaskGroup, TriggerRule, get_current_context, task

DAG_DIRECTORY = Path(__file__).resolve().parent
AIRFLOW_DIRECTORY = DAG_DIRECTORY.parent
if str(AIRFLOW_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_DIRECTORY))

from helpers.scripts.auidt_reader import assert_ingestion_complete
from helpers.scripts.callbacks import on_dag_failure, on_task_failure
from helpers.scripts.commads import (
    GOLD_DIM_SELECTORS,
    GOLD_FACT_SELECTORS,
    GOLD_TEST_SELECTORS,
    MART_RECONCILE_TEST,
    MARTS_SELECTORS,
    MARTS_TEST_SELECTORS,
    SILVER_SELECTORS,
    CommandError,
    build_dbt_command,
    build_ingest_command,
    build_publish_command,
    build_stream_reconcile_command,
    run_command,
)
from helpers.scripts.finalize_audit import finalize_batch, start_batch
from helpers.scripts.notify import send_run_summary
from helpers.scripts.prefilght import run_preflight

INGEST_TIERS: dict[str, tuple[tuple[str, ...], ...]] = {
    "mysql": (
        ("drivers", "trucks", "trailers", "customers", "routes", "facilities"),
        ("loads",),
        ("trips",),
        ("fuel_purchases",),
    ),
    "mongo": (("delivery_events", "maintenance_records", "safety_incidents"),),
}
MYSQL_TIER_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "loads": ("drivers", "trucks", "trailers", "customers", "routes", "facilities"),
    "trips": (
        "drivers",
        "trucks",
        "trailers",
        "customers",
        "routes",
        "facilities",
        "loads",
    ),
    # The fuel contract references trips.trip_id.
    "fuel_purchases": (
        "drivers",
        "trucks",
        "trailers",
        "customers",
        "routes",
        "facilities",
        "trips",
    ),
}

INGESTION_TIMEOUT = timedelta(minutes=45)
DBT_TIMEOUT = timedelta(minutes=60)
PUBLISH_TIMEOUT = timedelta(minutes=90)
DAG_TIMEOUT = timedelta(hours=6)
INGEST_RETRY_DELAY = timedelta(minutes=5)
INGEST_MAX_RETRY_DELAY = timedelta(minutes=30)
INGEST_AUDIT_TASKS = frozenset(
    f"ingest_{source}__{table}"
    for source, tiers in INGEST_TIERS.items()
    for tier in tiers
    for table in tier
)
RECONCILE_TIMEOUT = timedelta(minutes=30)
log = logging.getLogger(__name__)


def _runtime_ids() -> tuple[str, str, Any]:
    """Return Airflow identifiers from the task runtime context."""
    context = get_current_context()
    return context["dag"].dag_id, context["run_id"], context["logical_date"]


def _active_batch(starter_batch_id: int) -> tuple[int, str, str, Any]:
    """Reopen a cleared run's batch instead of consuming a stale XCom value."""
    dag_id, run_id, logical_date = _runtime_ids()
    batch_id = start_batch(dag_id, run_id, logical_date)
    if batch_id != starter_batch_id:
        raise RuntimeError("control.start_batch returned a different batch")
    return batch_id, dag_id, run_id, logical_date


@task(task_id="preflight", retries=1, retry_delay=timedelta(minutes=2))
def preflight() -> None:
    """Check configuration and dependencies before opening a warehouse batch."""
    run_preflight()


@task(task_id="start_batch", retries=1)
def open_batch() -> int:
    """Open or recover the one control batch for this Airflow run."""
    dag_id, run_id, logical_date = _runtime_ids()
    return start_batch(dag_id, run_id, logical_date)


@task(
    retries=2,
    retry_delay=INGEST_RETRY_DELAY,
    retry_exponential_backoff=True,
    max_retry_delay=INGEST_MAX_RETRY_DELAY,
    execution_timeout=INGESTION_TIMEOUT,
    on_failure_callback=on_task_failure,
)
def ingest_table(source: str, table: str, batch_id: int) -> None:
    """Land one source table through the existing ingestion script."""
    batch_id, dag_id, run_id, _ = _active_batch(batch_id)
    argv = build_ingest_command(source, table, batch_id, dag_id, run_id)
    run_command(argv, int(INGESTION_TIMEOUT.total_seconds()))


@task(
    task_id="ingest_done",
    retries=0,
    execution_timeout=timedelta(minutes=5),
    on_failure_callback=on_task_failure,
)
def ingest_done(batch_id: int) -> None:
    """Require a successful audit record for every contract source table."""
    batch_id, _, _, _ = _active_batch(batch_id)
    assert_ingestion_complete(batch_id, set(INGEST_AUDIT_TASKS))


@task(retries=0, execution_timeout=DBT_TIMEOUT, on_failure_callback=on_task_failure)
def dbt_stage(
    command: str,
    selectors: tuple[str, ...],
    batch_id: int,
    exclude: tuple[str, ...] = (),
) -> None:
    """Run one intentionally separate dbt build or test stage."""
    batch_id, dag_id, run_id, _ = _active_batch(batch_id)
    target = os.environ["DBT_TARGET"]
    argv = build_dbt_command(
        command, selectors, batch_id, dag_id, run_id, target, exclude
    )
    run_command(argv, int(DBT_TIMEOUT.total_seconds()))


@task(
    task_id="publish_marts",
    retries=1,
    retry_delay=timedelta(minutes=10),
    execution_timeout=PUBLISH_TIMEOUT,
    on_failure_callback=on_task_failure,
)
def publish_marts(batch_id: int) -> None:
    """Publish only the mart allow-list through the existing sync script."""
    batch_id, dag_id, run_id, _ = _active_batch(batch_id)
    run_command(
        build_publish_command(batch_id, dag_id, run_id),
        int(PUBLISH_TIMEOUT.total_seconds()),
    )


@task(
    task_id="reconcile_stream",
    retries=0,
    execution_timeout=RECONCILE_TIMEOUT,
)
def reconcile_stream(batch_id: int) -> None:
    """Record stream-vs-gold drift without failing the authoritative batch."""
    batch_id, dag_id, run_id, _ = _active_batch(batch_id)
    try:
        run_command(
            build_stream_reconcile_command(batch_id, dag_id, run_id),
            int(RECONCILE_TIMEOUT.total_seconds()),
        )
    except CommandError as error:
        log.warning("stream reconciliation command failed: %s", error)


@task(
    task_id="notify_summary",
    retries=2,
    trigger_rule=TriggerRule.ALL_DONE,
    on_failure_callback=on_task_failure,
)
def notify_summary(batch_id: int) -> None:
    """Close the batch successfully before best-effort notification delivery."""
    batch_id, dag_id, run_id, logical_date = _active_batch(batch_id)
    finalize_batch(batch_id, "SUCCEEDED", None)
    send_run_summary(batch_id, dag_id, run_id, logical_date)


with DAG(
    dag_id="fleet_daily_load",
    description="Daily Fleet warehouse ingestion, transformation, tests, and publishing.",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=DAG_TIMEOUT,
    tags=["fleet", "daily"],
    doc_md="""# Fleet daily load\n\nLands all source contracts, tests each dbt layer, then publishes approved marts and observes stream drift.""",
    default_args={"owner": "fleet-data-eng"},
    on_failure_callback=on_dag_failure,
) as dag:
    preflight_task = preflight()
    batch = open_batch()
    preflight_task >> batch

    ingest_tasks: dict[str, Any] = {}
    for source, tiers in INGEST_TIERS.items():
        pool = f"{source}_pool"
        source_tasks: dict[str, Any] = {}
        with TaskGroup(group_id=f"ingest_{source}"):
            for tier in tiers:
                for table in tier:
                    table_task = ingest_table.override(task_id=table, pool=pool)(
                        source, table, batch
                    )
                    dependencies = MYSQL_TIER_DEPENDENCIES.get(table, ())
                    upstream = (
                        [source_tasks[name] for name in dependencies]
                        if source == "mysql" and dependencies
                        else [batch]
                    )
                    upstream >> table_task
                    ingest_tasks[f"{source}.{table}"] = table_task
                    source_tasks[table] = table_task

    ingest_barrier = ingest_done(batch)
    list(ingest_tasks.values()) >> ingest_barrier
    silver_run = dbt_stage.override(task_id="silver_run")(
        "run", SILVER_SELECTORS, batch
    )
    silver_test = dbt_stage.override(task_id="silver_test")(
        "test", SILVER_SELECTORS, batch
    )
    gold_dims_run = dbt_stage.override(task_id="gold_dims_run")(
        "run", GOLD_DIM_SELECTORS, batch
    )
    gold_facts_run = dbt_stage.override(task_id="gold_facts_run")(
        "run", GOLD_FACT_SELECTORS, batch
    )
    gold_test = dbt_stage.override(task_id="gold_test")(
        "test", GOLD_TEST_SELECTORS, batch, (MART_RECONCILE_TEST,)
    )
    marts_run = dbt_stage.override(task_id="marts_run")("run", MARTS_SELECTORS, batch)
    marts_test = dbt_stage.override(task_id="marts_test")(
        "test", MARTS_TEST_SELECTORS, batch
    )
    published = publish_marts(batch)
    reconciled = reconcile_stream(batch)
    notification = notify_summary(batch)

    (
        ingest_barrier
        >> silver_run
        >> silver_test
        >> gold_dims_run
        >> gold_facts_run
        >> gold_test
        >> marts_run
        >> marts_test
    )
    marts_test >> [published, reconciled]
    [published, reconciled] >> notification
