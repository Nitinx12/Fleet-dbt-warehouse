"""Load Airflow DAG modules when the pinned Airflow runtime is available."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
DAGS_DIRECTORY = ROOT / "dags"


def _load_dag(name: str) -> Any:
    """Load one DAG module from the Airflow DAGs directory."""
    spec = importlib.util.spec_from_file_location(name, DAGS_DIRECTORY / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


@pytest.mark.unit
def test_dag_modules_import_without_errors() -> None:
    """Keep the phase-one and phase-two DAG modules syntactically importable."""
    if importlib.util.find_spec("airflow.sdk") is None:
        pytest.skip("Airflow Task SDK is installed in the Airflow image, not this venv")
    for dag_file in sorted(DAGS_DIRECTORY.glob("*.py")):
        dag = _load_dag(dag_file.stem)
        assert dag.doc_md
        assert dag.owner == "fleet-data-eng"
        assert dag.tags


@pytest.mark.unit
def test_batch_cleanup_dag_shape() -> None:
    """Keep the stale-batch safety net isolated and frequent."""
    if importlib.util.find_spec("airflow.sdk") is None:
        pytest.skip("Airflow Task SDK is installed in the Airflow image, not this venv")
    dag = _load_dag("fleet_batch_cleanup")

    assert dag.dag_id == "fleet_batch_cleanup"
    assert str(dag.schedule) == "*/15 * * * *"
    assert dag.timezone.name == "Asia/Kolkata"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.tags == ["fleet", "maintenance"]
    assert set(dag.task_ids) == {"fail_stale_batches"}
