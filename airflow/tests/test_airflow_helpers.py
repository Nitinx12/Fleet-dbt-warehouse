"""Exercise pure Airflow helper boundaries without external services."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
AIRFLOW_ROOT = ROOT / "airflow"
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from helpers.scripts.commads import build_dbt_command, build_ingest_command
from helpers.scripts.dbt_assets import (
    GROUPS,
    asset_groups_from_manifest,
    load_asset_keys,
)
from helpers.scripts.redact import redact


@pytest.mark.unit
def test_redact_removes_environment_and_url_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Never expose a configured secret or credential-bearing URL."""
    monkeypatch.setenv("TEST_TOKEN", "very-secret")

    actual = redact("Bearer very-secret https://user:pass@example.test/path")

    assert "very-secret" not in actual
    assert "user:pass" not in actual
    assert "[REDACTED]" in actual


@pytest.mark.unit
def test_command_builders_use_argv_and_airflow_identifiers() -> None:
    """Commands contain the batch and Airflow run linkage without a shell."""
    ingest = build_ingest_command("mysql", "drivers", 7, "fleet_daily_load", "run-1")
    dbt = build_dbt_command("test", ("dim_*",), 7, "fleet_daily_load", "run-1")

    assert "--batch-id" in ingest and ingest[ingest.index("--batch-id") + 1] == "7"
    assert "--airflow-run-id" in ingest
    assert "--target" in dbt
    assert not any(item in {"|", ";", "&&"} for item in [*ingest, *dbt])


@pytest.mark.unit
def test_invalid_table_identifier_is_rejected() -> None:
    """A table cannot introduce shell syntax into a constructed argv."""
    with pytest.raises(ValueError, match="invalid table"):
        build_ingest_command("mysql", "drivers; echo unsafe", 7, "dag", "run")


@pytest.mark.unit
def test_asset_groups_from_manifest_group_and_sort_models() -> None:
    """Only real dbt models become sorted, layer-scoped asset keys."""
    manifest = {
        "nodes": {
            "model.fleet_dbt.customers": {
                "resource_type": "model",
                "name": "customers",
                "original_file_path": "models/silver/customers.sql",
            },
            "model.fleet_dbt.drivers": {
                "resource_type": "model",
                "name": "drivers",
                "original_file_path": "models/silver/drivers.sql",
            },
            "model.fleet_dbt.dim_trucks": {
                "resource_type": "model",
                "name": "dim_trucks",
                "original_file_path": "models/gold/dim_trucks.sql",
            },
            "model.fleet_dbt.fct_trips": {
                "resource_type": "model",
                "name": "fct_trips",
                "original_file_path": "models/gold/fct_trips.sql",
            },
            "model.fleet_dbt.mart_sales_daily": {
                "resource_type": "model",
                "name": "mart_sales_daily",
                "original_file_path": "models/marts/mart_sales_daily.sql",
            },
            "model.fleet_dbt.assert_marts_reconcile_to_facts": {
                "resource_type": "model",
                "name": "assert_marts_reconcile_to_facts",
                "original_file_path": "models/marts/assert_marts_reconcile_to_facts.sql",
            },
            "test.fleet_dbt.not_null_drivers_id": {
                "resource_type": "test",
                "name": "not_null_drivers_id",
                "original_file_path": "models/silver/drivers.sql",
            },
        }
    }

    assert asset_groups_from_manifest(manifest) == {
        "silver": ("model.fleet_dbt.customers", "model.fleet_dbt.drivers"),
        "gold_dim": ("model.fleet_dbt.dim_trucks",),
        "gold_fact": ("model.fleet_dbt.fct_trips",),
        "mart": ("model.fleet_dbt.mart_sales_daily",),
    }


@pytest.mark.unit
def test_asset_groups_from_manifest_accepts_schema_only_nodes() -> None:
    """Manifests without file paths still resolve grouped model schemas."""
    manifest = {
        "nodes": {
            "model.fleet_dbt.customers": {
                "resource_type": "model",
                "schema": "silver",
                "name": "customers",
            },
            "model.fleet_dbt.dim_trucks": {
                "resource_type": "model",
                "schema": "gold",
                "name": "dim_trucks",
            },
            "model.fleet_dbt.fct_trips": {
                "resource_type": "model",
                "schema": "gold",
                "name": "fct_trips",
            },
            "model.fleet_dbt.mart_sales_daily": {
                "resource_type": "model",
                "schema": "marts",
                "name": "mart_sales_daily",
            },
            "model.fleet_dbt.assert_marts_reconcile_to_facts": {
                "resource_type": "model",
                "schema": "marts",
                "name": "assert_marts_reconcile_to_facts",
            },
            "model.fleet_dbt.staging_helper": {
                "resource_type": "model",
                "schema": "staging",
                "name": "staging_helper",
            },
        }
    }

    assert asset_groups_from_manifest(manifest) == {
        "silver": ("model.fleet_dbt.customers",),
        "gold_dim": ("model.fleet_dbt.dim_trucks",),
        "gold_fact": ("model.fleet_dbt.fct_trips",),
        "mart": ("model.fleet_dbt.mart_sales_daily",),
    }


@pytest.mark.unit
def test_asset_groups_reject_conflicting_unique_ids() -> None:
    """A node cannot claim a different dbt identity than its manifest key."""
    manifest = {
        "nodes": {
            "model.fleet_dbt.drivers": {
                "resource_type": "model",
                "unique_id": "model.fleet_dbt.duplicate",
                "name": "drivers",
                "original_file_path": "models/silver/drivers.sql",
            }
        }
    }

    with pytest.raises(ValueError, match="mismatched unique_id"):
        asset_groups_from_manifest(manifest)


@pytest.mark.unit
def test_checked_in_asset_keys_are_complete_and_sorted() -> None:
    """The checked-in index is the manifest-free source of asset identities."""
    groups = load_asset_keys()

    assert tuple(groups) == GROUPS
    for keys in groups.values():
        assert keys == tuple(sorted(set(keys)))
        assert all(key.startswith("model.fleet_dbt.") for key in keys)
        assert "assert_marts_reconcile_to_facts" not in keys


@pytest.mark.unit
def test_load_asset_keys_rejects_duplicates(tmp_path: Path) -> None:
    """The same dbt unique id cannot back two producer tasks."""
    payload = {
        group: ["model.fleet_dbt.drivers"] if group == "silver" else []
        for group in GROUPS
    }
    payload["silver"] = ["model.fleet_dbt.drivers", "model.fleet_dbt.drivers"]
    path = tmp_path / "dbt_asset_keys.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate"):
        load_asset_keys(path)
