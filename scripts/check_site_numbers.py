"""Check every number on the project site against generated facts."""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "index.html"
FACTS_FILE = ROOT / "site" / "facts.json"
CONFIG_FILE = ROOT / "scripts" / "site_numbers_allowlist.yml"


def _libs() -> tuple[Any, Any, Any]:
    sys.path.insert(0, str(ROOT / "scripts"))
    from site_lib import facts as fact_lib
    from site_lib import numbers, views

    return fact_lib, numbers, views


def _load_config() -> dict[str, Any]:
    import yaml

    return yaml.safe_load(CONFIG_FILE.read_text(encoding="utf-8"))


def _normalise(text: str) -> str:
    return text.replace(",", "").strip().lower()


def _same_display(actual: str, expected: str) -> bool:
    return _normalise(actual) == _normalise(expected)


def _same_raw(actual: str, expected: Any) -> bool:
    try:
        return Decimal(_normalise(actual)) == Decimal(str(expected))
    except InvalidOperation:
        return False


def _allows(rule: dict[str, Any], token: Any) -> bool:
    kinds = rule.get("kinds")
    if kinds and token.kind not in kinds:
        return False
    pattern = rule.get("pattern")
    return not pattern or re.search(pattern, token.context, re.IGNORECASE) is not None


def check_bindings(
    errors: list[str],
    tokens: list[Any],
    facts: dict[str, Any],
    config: dict[str, Any],
    strict: bool,
) -> tuple[int, int]:
    fact_lib, _, _ = _libs()
    formats: dict[str, str] = config["formats"]
    rules: list[dict[str, Any]] = config["rules"]
    checked = allowed = 0
    formatted = {
        fact_lib.format_fact(key, fact_lib.fact_value(facts, key), rule)
        for key, rule in formats.items()
        if key in facts
        and re.search(
            r"[$%,]|\d{3}",
            fact_lib.format_fact(key, fact_lib.fact_value(facts, key), rule),
        )
    }
    for token in tokens:
        if token.kind == "css":
            continue
        if token.illustrative:
            if (
                strict
                and token.kind in {"text", "attr"}
                and re.search(r"[.$%,]|\.\d", token.token)
            ):
                errors.append(
                    f"index.html:{token.line}: exact-looking illustrative number "
                    f"{token.token!r}; use a fact instead"
                )
            allowed += 1
            continue
        if token.binding:
            key = token.binding
            if key not in facts:
                errors.append(f"index.html:{token.line}: unknown fact {key!r}")
                continue
            if key not in formats:
                errors.append(f"allowlist: no format rule for {key!r}")
                continue
            value = fact_lib.fact_value(facts, key)
            if token.kind == "json":
                ok = _same_raw(token.token, value)
                expected = str(value)
            else:
                expected = fact_lib.format_fact(key, value, formats[key])
                ok = _same_display(token.token, expected)
            if not ok:
                errors.append(
                    f"index.html:{token.line}: {token.token!r} != {expected!r} "
                    f"({key}; {facts[key].source})"
                )
            checked += 1
            continue
        if token.kind == "js" and any(
            _same_display(token.token, value) for value in formatted
        ):
            errors.append(
                f"index.html:{token.line}: hardcoded fact {token.token!r} in JavaScript; "
                f"read FACTS instead"
            )
            continue
        if any(_allows(rule, token) for rule in rules):
            allowed += 1
            continue
        errors.append(
            f"index.html:{token.line}: unbound number {token.token!r} in "
            f"{token.kind} context {token.context[:100]!r}"
        )
    return checked, allowed


def check_placeholders(errors: list[str], html: str) -> None:
    for placeholder in ("__LIVE_DATA__", "__SITE_FACTS__"):
        count = html.count(placeholder)
        if count != 1:
            errors.append(f"index.html: expected one {placeholder}, found {count}")


def _display_path(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


def _doc_paths() -> list[Path]:
    paths = [ROOT / "README.md", ROOT / "ARCHITECTURE.md", ROOT / "DEPLOYMENT.md"]
    paths.extend(sorted((ROOT / "docs").glob("*.md")))
    paths.extend(sorted((ROOT / "airflow").glob("README.md")))
    paths.extend(sorted((ROOT / "reports").glob("*.md")))
    return [path for path in paths if path.exists()]


def check_docs(
    errors: list[str], facts: dict[str, Any], config: dict[str, Any]
) -> None:
    fact_lib, _, _ = _libs()
    formats: dict[str, str] = config["formats"]

    def expected(key: str) -> str:
        return fact_lib.format_fact(
            key, fact_lib.fact_value(facts, key), formats.get(key, "count")
        )

    claims: list[tuple[str, list[str], list[str]]] = [
        (r"Source tables / collections\s*\|\s*(\d+)", ["contracts.total"], []),
        (r"dbt models\s*\|\s*(\d+)", ["dbt.models.total"], []),
        (r"dbt tests\s*\|\s*(\d+)", ["dbt.tests.total"], []),
        (r"\((\d+) error, (\d+) warn\)", ["dbt.tests.error", "dbt.tests.warn"], []),
        (r"Published marts\s*\|\s*(\d+)", ["publish.marts.count"], []),
        (
            r"(\d+) collected \((\d+) \+ (\d+)\)",
            [
                "tests.collected.combined",
                "tests.collected.total",
                "airflow.tests.total",
            ],
            [],
        ),
        (
            r"CI jobs\s*\|\s*(\d+), plus (\d+)",
            ["ci.jobs.total", "ci.jobs.secret_scan"],
            [],
        ),
        (
            r"(\d+) dbt tests split (\d+) silver, (\d+) gold and (\d+) marts",
            [
                "dbt.tests.total",
                "dbt.tests.silver",
                "dbt.tests.gold",
                "dbt.tests.marts",
            ],
            [],
        ),
        (r"(\d+) contracts are checked", ["contracts.total"], []),
        (r"CI has (\d+) jobs", ["ci.jobs.total"], []),
        (
            r"Schedule: `([^`]+)` in `([^`]+)`",
            ["airflow.schedule.cron", "airflow.schedule.timezone"],
            [],
        ),
        (r"catchup=(False|True)", ["airflow.catchup"], []),
        (r"max_active_runs=(\d+)", ["airflow.max_active_runs"], []),
    ]
    for path in _doc_paths():
        text = path.read_text(encoding="utf-8")
        for pattern, keys, _ in claims:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                line = text.count("\n", 0, match.start()) + 1
                for index, key in enumerate(keys):
                    found = match.group(index + 1)
                    want = expected(key)
                    if not _same_display(found, want):
                        errors.append(
                            f"{_display_path(path)}:{line}: {found!r} "
                            f"!= {want!r} ({key})"
                        )


def _payload_keys() -> set[str]:
    path = ROOT / ".streamlit" / "streamlit_state.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    keys: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "live_payload":
            for child in ast.walk(node):
                if isinstance(child, ast.Dict):
                    keys.update(
                        item.value
                        for item in child.keys
                        if isinstance(item, ast.Constant)
                        and isinstance(item.value, str)
                    )
    return keys


def check_streamlit(errors: list[str], facts: dict[str, Any]) -> None:
    _, _, views_lib = _libs()
    want_keys = set(facts["streamlit.payload.keys"].value)
    got_keys = _payload_keys()
    if got_keys != want_keys:
        errors.append(
            f"streamlit_state.py: live_payload keys {sorted(got_keys)} != {sorted(want_keys)}"
        )
    sql = (ROOT / "sql" / "scripts" / "06_audit_relationships.sql").read_text(
        encoding="utf-8"
    )
    view_map = {
        "batches": "v_batch_summary",
        "dbt": "v_dbt_results",
        "dq": "v_data_quality_results",
    }
    for label, view in view_map.items():
        want = list(facts[f"streamlit.views.{label}.columns"].value)
        try:
            got = views_lib.parse_view_columns(sql, view)
        except ValueError as exc:
            errors.append(f"06_audit_relationships.sql: {exc}")
            continue
        if got != want:
            errors.append(
                f"06_audit_relationships.sql: {view} columns changed; "
                f"regenerate site/facts.json"
            )


def check_fact_shape(
    errors: list[str], facts: dict[str, Any], config: dict[str, Any]
) -> None:
    formats = config["formats"]
    for key, item in facts.items():
        if not {"value", "source", "how"} <= set(vars(item)):
            errors.append(f"site/facts.json:{key}: missing value, source or how")
        if key not in formats:
            errors.append(f"site/facts.json:{key}: no format rule")


def run(strict: bool = False, as_json: bool = False) -> int:
    errors: list[str] = []
    fact_lib, numbers_lib, _ = _libs()
    facts = fact_lib.load_facts(FACTS_FILE)
    config = _load_config()
    html = INDEX.read_text(encoding="utf-8")
    tokens = numbers_lib.extract_tokens(INDEX)
    check_fact_shape(errors, facts, config)
    check_placeholders(errors, html)
    checked, allowed = check_bindings(errors, tokens, facts, config, strict)
    check_docs(errors, facts, config)
    check_streamlit(errors, facts)
    if as_json:
        print(
            json.dumps(
                {
                    "ok": not errors,
                    "facts": len(facts),
                    "bound_numbers": checked,
                    "allowed_numbers": allowed,
                    "errors": errors,
                },
                indent=2,
            )
        )
    else:
        for error in errors:
            print(error)
        print(
            f"checked {checked} bound numbers; {allowed} allowed; "
            f"{len(errors)} error(s)"
        )
    return 1 if errors else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", dest="as_json", action="store_true")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()
    return run(strict=args.strict, as_json=args.as_json)


if __name__ == "__main__":
    raise SystemExit(main())
