"""Generate_dbt_asset_keys maps only grouped dbt models to asset keys."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from scripts import generate_dbt_asset_keys as asset_keys

GROUPED_NODES: dict[str, dict[str, Any]] = {
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
    "model.fleet_dbt.fct_loads": {
        "resource_type": "model",
        "schema": "gold",
        "name": "fct_loads",
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
    "test.fleet_dbt.not_null_customers": {
        "resource_type": "test",
        "schema": "dbt_test__audit",
        "name": "not_null_customers",
    },
    "model.fleet_dbt.staging_helper": {
        "resource_type": "model",
        "schema": "staging",
        "name": "staging_helper",
    },
}


def _manifest(tmp_path: Path, nodes: dict[str, dict[str, Any]]) -> Path:
    """Write a small manifest for one test."""
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"nodes": nodes}), encoding="utf-8")
    return path


@pytest.mark.unit
def test_build_key_map_groups_and_sorts_models(tmp_path: Path) -> None:
    """Only grouped models survive, with reconciliation excluded."""
    manifest = _manifest(
        tmp_path,
        {
            **GROUPED_NODES,
            "model.fleet_dbt.dim_customers": {
                "resource_type": "model",
                "schema": "gold",
                "name": "dim_customers",
            },
        },
    )

    key_map = asset_keys.build_key_map(manifest)

    assert key_map == {
        "silver": ["model.fleet_dbt.customers"],
        "gold_dim": ["model.fleet_dbt.dim_customers", "model.fleet_dbt.dim_trucks"],
        "gold_fact": ["model.fleet_dbt.fct_loads"],
        "mart": ["model.fleet_dbt.mart_sales_daily"],
    }


@pytest.mark.unit
def test_build_key_map_falls_back_to_unique_id_name(tmp_path: Path) -> None:
    """A missing name still yields a grouped model."""
    manifest = _manifest(
        tmp_path,
        {
            "model.fleet_dbt.mart_orders": {
                "resource_type": "model",
                "schema": "marts",
            }
        },
    )

    assert asset_keys.build_key_map(manifest)["mart"] == ["model.fleet_dbt.mart_orders"]


@pytest.mark.unit
def test_write_key_map_creates_parent_and_stable_json(tmp_path: Path) -> None:
    """Write deterministic JSON even when the assets directory is absent."""
    path = tmp_path / "airflow" / "assets" / "dbt_asset_keys.json"
    key_map = {"silver": ["b", "a"], "gold_dim": [], "gold_fact": [], "mart": []}

    asset_keys.write_key_map(key_map, path)

    assert json.loads(path.read_text(encoding="utf-8")) == key_map
    assert path.read_text(encoding="utf-8").startswith('{\n  "gold_dim"')


@pytest.mark.unit
def test_check_key_map_reports_missing_invalid_and_stale(tmp_path: Path) -> None:
    """Check fails closed for missing, unreadable and stale maps."""
    path = tmp_path / "dbt_asset_keys.json"
    key_map = {"silver": ["a"], "gold_dim": [], "gold_fact": [], "mart": []}

    assert asset_keys.check_key_map(key_map, path) == [f"{path}: missing"]

    path.write_text("not json", encoding="utf-8")
    assert "invalid JSON" in asset_keys.check_key_map(key_map, path)[0]

    asset_keys.write_key_map(key_map, path)
    assert asset_keys.check_key_map(key_map, path) == []

    asset_keys.write_key_map({**key_map, "silver": ["b"]}, path)
    assert asset_keys.check_key_map(key_map, path) == [
        "silver: manifest and asset-key map differ"
    ]


@pytest.mark.unit
def test_main_write_and_check_report_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """CLI writes from the manifest and validates what it wrote."""
    manifest = _manifest(tmp_path, GROUPED_NODES)
    target = tmp_path / "assets" / "dbt_asset_keys.json"
    monkeypatch.setattr(asset_keys, "MANIFEST_PATH", manifest)
    monkeypatch.setattr(asset_keys, "ASSET_KEYS_PATH", target)

    assert asset_keys.main(["--write"]) == 0
    assert "wrote" in capsys.readouterr().out

    assert asset_keys.main(["--check"]) == 0
    assert capsys.readouterr().out == "dbt asset keys OK\n"

    target.write_text("{}\n", encoding="utf-8")
    assert asset_keys.main(["--check"]) == 1
    assert "differ" in capsys.readouterr().err
