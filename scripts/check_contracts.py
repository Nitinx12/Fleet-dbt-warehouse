"""Validate source and stream contracts against their consumers."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "kafka"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from fleet_stream.contract import alignment_issues, load_contract

from scripts.ingestion import read_contract


def _source_issues() -> list[str]:
    index = json.loads((ROOT / "contracts/sources_index.json").read_text("utf-8"))
    indexed = {item["file"]: item["source"] for item in index["contracts"]}
    paths = {path.name for path in (ROOT / "contracts/sources").glob("*.yml")}
    contracts = {
        path.name: read_contract(path)
        for path in (ROOT / "contracts/sources").glob("*.yml")
    }
    dbt_lines = (ROOT / "dbt/models/bronze/sources.yml").read_text("utf-8").splitlines()
    dbt_tables = {
        line.strip().removeprefix("- name: ").strip()
        for line in dbt_lines
        if line.strip().startswith("- name:")
    }
    dbt_tables.discard("bronze")
    contract_tables = {
        str(contract["landing_table"]).split(".", 1)[1]
        for contract in contracts.values()
    }
    issues = []
    if paths != set(indexed):
        issues.append("contract index files differ from contracts/sources")
    if {name: contract["source"] for name, contract in contracts.items()} != indexed:
        issues.append("contract index sources differ from contract YAML")
    if contract_tables != dbt_tables:
        issues.append("dbt bronze source tables differ from contract landing tables")
    return issues


def main() -> None:
    """Fail with a concise drift report when any contract boundary disagrees."""
    issues = list(_source_issues())
    for issue in alignment_issues(load_contract()):
        issues.append(f"stream: {issue}")
    if issues:
        detail = "\n".join(f"  - {issue}" for issue in issues)
        raise SystemExit(f"contracts FAILED:\n{detail}")
    print("contracts OK: 12 sources, 1 stream")


if __name__ == "__main__":
    main()
