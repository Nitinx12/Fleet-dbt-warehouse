"""Generate the Airflow asset-key map from the dbt manifest."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "dbt/target/manifest.json"
ASSET_KEYS_PATH = ROOT / "airflow/assets/dbt_asset_keys.json"
GROUPS = ("silver", "gold_dim", "gold_fact", "mart")
EXCLUDED_MODELS = {"assert_marts_reconcile_to_facts"}


def _model_name(unique_id: str, node: dict[str, Any]) -> str:
    """Return the dbt model name, falling back to the manifest unique id."""
    return str(node.get("name") or unique_id.rsplit(".", 1)[-1])


def _group(name: str, node: dict[str, Any]) -> str | None:
    """Return the asset group for one dbt model, or None when ungrouped."""
    path = str(node.get("original_file_path") or "").replace("\\", "/")
    schema = str(node.get("schema") or "")
    if path.startswith("silver/") or "/silver/" in f"/{path}" or schema == "silver":
        return "silver"
    if name.startswith("dim_"):
        return "gold_dim"
    if name.startswith("fct_"):
        return "gold_fact"
    if name.startswith("mart_"):
        return "mart"
    return None


def build_key_map(manifest_path: Path | None = None) -> dict[str, list[str]]:
    """Build sorted dbt unique ids for each Airflow asset group."""
    path = manifest_path or MANIFEST_PATH
    manifest = json.loads(path.read_text(encoding="utf-8"))
    grouped: dict[str, list[str]] = {group: [] for group in GROUPS}

    for unique_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        name = _model_name(unique_id, node)
        if name in EXCLUDED_MODELS:
            continue
        group = _group(name, node)
        if group is not None:
            grouped[group].append(unique_id)

    return {group: sorted(keys) for group, keys in grouped.items()}


def write_key_map(key_map: dict[str, list[str]], path: Path | None = None) -> None:
    """Write the asset-key map as stable JSON."""
    destination = path or ASSET_KEYS_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(key_map, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def check_key_map(key_map: dict[str, list[str]], path: Path | None = None) -> list[str]:
    """Return drift messages; an empty list means the map matches the manifest."""
    source = path or ASSET_KEYS_PATH
    if not source.exists():
        return [f"{source}: missing"]

    try:
        actual = json.loads(source.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"{source}: invalid JSON ({exc.msg})"]

    if actual == key_map:
        return []
    issues = []
    for group in sorted(set(key_map) | set(actual)):
        if key_map.get(group) != actual.get(group):
            issues.append(f"{group}: manifest and asset-key map differ")
    return issues


def main(argv: list[str] | None = None) -> int:
    """Write or validate the generated asset-key map."""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true", help="write the asset-key map")
    mode.add_argument("--check", action="store_true", help="check the asset-key map")
    args = parser.parse_args(argv)

    key_map = build_key_map()
    if args.write:
        write_key_map(key_map)
        sys.stdout.write(f"wrote {ASSET_KEYS_PATH}\n")
        return 0

    issues = check_key_map(key_map)
    if issues:
        for issue in issues:
            sys.stderr.write(f"{issue}\n")
        return 1
    sys.stdout.write("dbt asset keys OK\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
