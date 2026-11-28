"""Smoke checks that the shipped modules can be imported."""

from __future__ import annotations

import importlib

import pytest

_MODULES = (
    "scripts.ingestion",
    "scripts.generate_dbt_asset_keys",
    "scripts.pipeline_stats",
    "scripts.publish_marts",
    "scripts.run_dbt",
    "scripts.run_notebooks",
    "utils.audit",
    "utils.connection",
    "utils.dbt_audit",
    "utils.engine",
)


@pytest.mark.parametrize("module_name", _MODULES)
def test_module_imports(module_name: str) -> None:
    """Import each production module in a clean process namespace."""
    importlib.import_module(module_name)
