"""Resolve dbt unique ids used as Airflow asset identities."""

from __future__ import annotations

import json
from pathlib import Path

GROUPS = ("silver", "gold_dim", "gold_fact", "mart")
PACKAGE_PREFIX = "model.fleet_dbt."
EXCLUDED_MODELS = frozenset({"assert_marts_reconcile_to_facts"})
DEFAULT_ASSET_KEYS_PATH = (
    Path(__file__).resolve().parents[2] / "assets" / "dbt_asset_keys.json"
)


def _group_for(node: dict[str, object], name: str) -> str | None:
    if name in EXCLUDED_MODELS:
        return None
    path = str(node.get("original_file_path", "")).replace("\\", "/")
    schema = str(node.get("schema", "")).lower()
    if path.startswith("silver/") or "/silver/" in f"/{path}" or schema == "silver":
        return "silver"
    if name.startswith("dim_"):
        return "gold_dim"
    if name.startswith("fct_"):
        return "gold_fact"
    if name.startswith("mart_"):
        return "mart"
    return None


def asset_groups_from_manifest(
    manifest: dict[str, object],
) -> dict[str, tuple[str, ...]]:
    """Return sorted dbt unique ids grouped by warehouse layer."""
    raw_nodes = manifest.get("nodes")
    if raw_nodes is None:
        raw_nodes = {
            key: value
            for key, value in manifest.items()
            if isinstance(key, str) and key.startswith(PACKAGE_PREFIX)
        }
    if not isinstance(raw_nodes, dict):
        raise TypeError("manifest nodes must be an object")

    groups: dict[str, list[str]] = {group: [] for group in GROUPS}
    seen: set[str] = set()
    for unique_id, raw_node in raw_nodes.items():
        if not isinstance(unique_id, str) or not unique_id.startswith(PACKAGE_PREFIX):
            continue
        if not isinstance(raw_node, dict):
            raise TypeError(f"manifest node {unique_id} must be an object")
        declared_id = raw_node.get("unique_id")
        if declared_id is not None and declared_id != unique_id:
            raise ValueError(f"manifest node {unique_id} has a mismatched unique_id")
        if raw_node.get("resource_type") != "model":
            continue
        name = str(raw_node.get("name", unique_id.removeprefix(PACKAGE_PREFIX)))
        group = _group_for(raw_node, name)
        if group is None:
            continue
        if unique_id in seen:
            raise ValueError(f"duplicate dbt asset key: {unique_id}")
        seen.add(unique_id)
        groups[group].append(unique_id)
    return {group: tuple(sorted(keys)) for group, keys in groups.items()}


def load_asset_keys(path: Path | None = None) -> dict[str, tuple[str, ...]]:
    """Load and validate the checked-in dbt asset key index."""
    asset_path = path or DEFAULT_ASSET_KEYS_PATH
    payload = json.loads(asset_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("asset key file must contain an object")

    groups: dict[str, list[str]] = {}
    seen: set[str] = set()
    for group, raw_keys in payload.items():
        if group not in GROUPS or not isinstance(raw_keys, list):
            raise ValueError(f"invalid asset group: {group}")
        keys: list[str] = []
        for key in raw_keys:
            if not isinstance(key, str) or not key.startswith(PACKAGE_PREFIX):
                raise ValueError(f"invalid dbt asset key: {key}")
            if key.removeprefix(PACKAGE_PREFIX) in EXCLUDED_MODELS:
                raise ValueError(f"excluded dbt asset key: {key}")
            if key in seen:
                raise ValueError(f"duplicate dbt asset key: {key}")
            seen.add(key)
            keys.append(key)
        groups[group] = keys

    missing = [group for group in GROUPS if group not in groups]
    if missing:
        raise ValueError(f"missing asset groups: {', '.join(missing)}")
    return {group: tuple(sorted(groups[group])) for group in GROUPS}
