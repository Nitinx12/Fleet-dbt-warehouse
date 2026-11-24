"""Assert the critical ordering and operational settings of the daily DAG."""

from __future__ import annotations

import importlib.util
from itertools import pairwise
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = ROOT / "dags" / "fleet_dbt_daliy.py"


def _dag():
    """Load the daily DAG from its deliberately misspelled filename."""
    if importlib.util.find_spec("airflow.sdk") is None:
        pytest.skip("Airflow Task SDK is installed in the Airflow image, not this venv")
    spec = importlib.util.spec_from_file_location("fleet_daily_dag", DAG_FILE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


@pytest.mark.unit
def test_daily_dag_has_exact_stage_tasks_and_settings() -> None:
    """Keep task identities stable for operators and audit correlation."""
    dag = _dag()
    expected = {
        "preflight",
        "start_batch",
        "ingest_done",
        "silver_run",
        "silver_test",
        "gold_dims_run",
        "gold_facts_run",
        "gold_test",
        "marts_run",
        "marts_test",
        "publish_marts",
        "reconcile_stream",
        "notify_summary",
        "ingest_mysql.drivers",
        "ingest_mysql.trucks",
        "ingest_mysql.trailers",
        "ingest_mysql.customers",
        "ingest_mysql.routes",
        "ingest_mysql.facilities",
        "ingest_mysql.loads",
        "ingest_mysql.fuel_purchases",
        "ingest_mysql.trips",
        "ingest_mongo.delivery_events",
        "ingest_mongo.maintenance_records",
        "ingest_mongo.safety_incidents",
    }

    assert set(dag.task_ids) == expected
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert str(dag.schedule) == "0 2 * * *"
    assert dag.timezone.name == "Asia/Kolkata"
    assert dag.tags == ["fleet", "daily"]
    assert dag.doc_md
    assert dag.owner == "fleet-data-eng"


@pytest.mark.unit
def test_daily_dag_preserves_contract_gating_and_stage_order() -> None:
    """Ensure no later layer can run before its predecessor succeeds."""
    dag = _dag()
    task = dag.get_task

    assert task("preflight").downstream_task_ids == {"start_batch"}
    assert task("ingest_mysql.trips").upstream_task_ids >= {
        "ingest_mysql.loads",
        "ingest_mysql.drivers",
        "ingest_mysql.trucks",
        "ingest_mysql.trailers",
        "ingest_mysql.customers",
        "ingest_mysql.routes",
        "ingest_mysql.facilities",
    }
    assert task("ingest_mysql.fuel_purchases").upstream_task_ids >= {
        "ingest_mysql.trips",
        "ingest_mysql.drivers",
        "ingest_mysql.trucks",
        "ingest_mysql.trailers",
        "ingest_mysql.customers",
        "ingest_mysql.routes",
        "ingest_mysql.facilities",
    }
    for name in (
        "ingest_mongo.delivery_events",
        "ingest_mongo.maintenance_records",
        "ingest_mongo.safety_incidents",
    ):
        assert task(name).upstream_task_ids == {"start_batch"}
        assert task(name).pool == "mongo_pool"
    for name in (
        "ingest_mysql.drivers",
        "ingest_mysql.trucks",
        "ingest_mysql.trailers",
        "ingest_mysql.customers",
        "ingest_mysql.routes",
        "ingest_mysql.facilities",
        "ingest_mysql.loads",
        "ingest_mysql.fuel_purchases",
        "ingest_mysql.trips",
    ):
        assert task(name).pool == "mysql_pool"
    assert task("ingest_done").upstream_task_ids == {
        task_id
        for task_id in dag.task_ids
        if task_id.startswith("ingest_") and task_id != "ingest_done"
    }
    stages = (
        "ingest_done",
        "silver_run",
        "silver_test",
        "gold_dims_run",
        "gold_facts_run",
        "gold_test",
        "marts_run",
        "marts_test",
    )
    for upstream, downstream in pairwise(stages):
        assert downstream in task(upstream).downstream_task_ids
    assert task("publish_marts").upstream_task_ids == {"marts_test"}
    assert task("reconcile_stream").upstream_task_ids == {"marts_test"}
    assert task("notify_summary").upstream_task_ids == {
        "publish_marts",
        "reconcile_stream",
    }
    rule = task("notify_summary").trigger_rule
    assert getattr(rule, "value", str(rule)) == "all_done"
    for name in (
        "silver_run",
        "silver_test",
        "gold_dims_run",
        "gold_facts_run",
        "gold_test",
        "marts_run",
        "marts_test",
        "reconcile_stream",
    ):
        assert task(name).retries == 0
