"""pipeline_stats reads audit and catalog data from throwaway Postgres."""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from scripts import pipeline_stats as stats


@pytest.mark.integration
def test_build_report_reads_audit_and_catalog(postgres_engine: Engine) -> None:
    """Successful runs, ingest rows and relation sizes come from Postgres."""
    suffix = uuid4().hex[:12]
    pipeline = f"stats_test_{suffix}"
    dag_id = f"dag_stats_{suffix}"
    run_id = f"run_stats_{suffix}"

    with postgres_engine.begin() as connection:
        connection.execute(
            text(
                "insert into control.pipeline (pipeline_name, airflow_dag_id)"
                " values (:pipeline, :dag_id)"
            ),
            {"pipeline": pipeline, "dag_id": dag_id},
        )
        batch_id = int(
            connection.execute(
                text(
                    "select control.start_batch(:pipeline, :dag_id, :run_id, null, 'test')"
                ),
                {"pipeline": pipeline, "dag_id": dag_id, "run_id": run_id},
            ).scalar_one()
        )
        connection.execute(
            text(
                "insert into audit.pipeline_run_log"
                " (batch_id, pipeline_name, airflow_dag_id, airflow_run_id,"
                "  event_type, event_at)"
                " values (:batch_id, :pipeline, :dag_id, :run_id,"
                "  'SUCCEEDED', now() + interval '120 seconds')"
            ),
            {
                "batch_id": batch_id,
                "pipeline": pipeline,
                "dag_id": dag_id,
                "run_id": run_id,
            },
        )
        for task_id, rows in (("ingest_loads", 900), ("ingest_trucks", 100)):
            connection.execute(
                text(
                    "insert into audit.task_run_log"
                    " (batch_id, airflow_dag_id, airflow_run_id, task_id,"
                    "  status, started_at, ended_at, rows_processed)"
                    " values (:batch_id, :dag_id, :run_id, :task_id,"
                    "  'SUCCEEDED', now() - interval '5 seconds', now(), :rows)"
                ),
                {
                    "batch_id": batch_id,
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "task_id": task_id,
                    "rows": rows,
                },
            )
        connection.execute(
            text(
                "insert into audit.task_run_log"
                " (batch_id, airflow_dag_id, airflow_run_id, task_id,"
                "  status, started_at, ended_at, rows_processed)"
                " values (:batch_id, :dag_id, :run_id, 'silver_run',"
                "  'SUCCEEDED', now(), now() + interval '1 second', 99999)"
            ),
            {"batch_id": batch_id, "dag_id": dag_id, "run_id": run_id},
        )

    report = stats.build_report(
        pipeline=pipeline, days=1, limit=5, engine=postgres_engine
    )

    assert report["durations"]["successful_runs"] == 1
    assert 119.0 <= report["durations"]["median_seconds"] <= 121.0
    assert 119.0 <= report["durations"]["p95_seconds"] <= 121.0
    assert [task["task_id"] for task in report["largest_ingest_tasks"]] == [
        "ingest_loads",
        "ingest_trucks",
    ]
    assert report["largest_relations"]
    assert "table_name" in report["largest_relations"][0]
