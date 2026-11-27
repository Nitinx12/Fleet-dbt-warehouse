"""Collect every number shown on the project site from repository sources."""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "site" / "facts.json"
DAG_FILE = ROOT / "airflow" / "dags" / "fleet_dbt_daliy.py"
SQL_VIEWS = ROOT / "sql" / "scripts" / "06_audit_relationships.sql"


def _fact(value: Any, source: str, how: str) -> dict[str, Any]:
    return {"value": value, "source": source, "how": how}


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _literal(node: ast.AST) -> Any:
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError, SyntaxError):
        return None


def _keyword(call: ast.Call, name: str) -> Any:
    for keyword in call.keywords:
        if keyword.arg == name:
            return _literal(keyword.value)
    return None


def _call_name(func: ast.AST) -> str:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Call):
        return _call_name(func.func)
    return ""


def collect_contracts(facts: dict[str, Any]) -> None:
    source = ROOT / "contracts" / "sources_index.json"
    entries = json.loads(source.read_text(encoding="utf-8"))["contracts"]
    mysql = sum(1 for item in entries if item["source"].startswith("mysql."))
    mongo = sum(1 for item in entries if item["source"].startswith("mongodb."))
    how = "count contracts/sources_index.json entries by source prefix"
    facts["contracts.total"] = _fact(len(entries), _rel(source), how)
    facts["contracts.mysql"] = _fact(mysql, _rel(source), how)
    facts["contracts.mongodb"] = _fact(mongo, _rel(source), how)


def _schema_test_counts() -> dict[str, int]:
    counts = {"silver": 0, "gold": 0, "marts": 0}
    files = {
        "silver": ROOT / "dbt" / "models" / "silver" / "schema.yml",
        "gold": ROOT / "dbt" / "models" / "gold" / "schema.yml",
        "marts": ROOT / "dbt" / "models" / "marts" / "schema_marts.yml",
    }
    for layer, path in files.items():
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        for model in data.get("models", []):
            for column in model.get("columns", []):
                tests = column.get("tests", column.get("data_tests", []))
                counts[layer] += len(tests)
    return counts


def _sql_model_counts() -> dict[str, int]:
    return {
        layer: len(list((ROOT / "dbt" / "models" / layer).glob("*.sql")))
        for layer in ("silver", "gold", "marts")
    }


def collect_dbt(facts: dict[str, Any], inputs: list[str]) -> None:
    manifest = ROOT / "dbt" / "target" / "manifest.json"
    project = ROOT / "dbt" / "dbt_project.yml"
    if manifest.exists():
        data = json.loads(manifest.read_text(encoding="utf-8"))
        nodes = data["nodes"]
        models = [node for node in nodes.values() if node["resource_type"] == "model"]
        tests = [node for node in nodes.values() if node["resource_type"] == "test"]
        layers = {layer: 0 for layer in ("silver", "gold", "marts")}
        model_ids: dict[str, str] = {}
        for node in models:
            layer = node["fqn"][1]
            layers[layer] += 1
            model_ids[node["unique_id"]] = layer
        test_layers = {layer: 0 for layer in layers}
        errors = warns = 0
        for node in tests:
            layer = layers and model_ids.get(
                node.get("attached_node", ""), node["fqn"][1]
            )
            test_layers[layer] += 1
            severity = node.get("config", {}).get("severity", "ERROR").lower()
            errors += severity == "error"
            warns += severity == "warn"
        sources = (
            _rel(source)
            if (source := ROOT / "dbt" / "models" / "bronze" / "sources.yml")
            else ""
        )
        inputs.append(_rel(manifest))
        how = "count manifest.json nodes by resource_type, fqn layer and severity"
    else:
        model_counts = _sql_model_counts()
        test_counts = _schema_test_counts()
        layers = model_counts
        test_layers = test_counts
        errors = sum(test_layers.values())
        warns = 0
        for path in (ROOT / "dbt" / "models").glob("*/schema*.yml"):
            text = path.read_text(encoding="utf-8")
            warns += len(re.findall(r"severity:\s*warn", text, re.IGNORECASE))
        errors -= warns
        source = project
        sources = "dbt/models/bronze/sources.yml"
        inputs.extend(
            [
                _rel(project),
                _rel(ROOT / "dbt" / "models" / "silver" / "schema.yml"),
                _rel(ROOT / "dbt" / "models" / "gold" / "schema.yml"),
                _rel(ROOT / "dbt" / "models" / "marts" / "schema_marts.yml"),
            ]
        )
        how = "count model SQL files and schema.yml test declarations by layer"
    source_name = _rel(source)
    for layer, count in layers.items():
        facts[f"dbt.models.{layer}"] = _fact(count, source_name, how)
    for layer, count in test_layers.items():
        facts[f"dbt.tests.{layer}"] = _fact(count, source_name, how)
    facts["dbt.models.total"] = _fact(sum(layers.values()), source_name, how)
    facts["dbt.tests.total"] = _fact(sum(test_layers.values()), source_name, how)
    facts["dbt.tests.error"] = _fact(errors, source_name, how)
    facts["dbt.tests.warn"] = _fact(warns, source_name, how)
    facts["dbt.nodes.total"] = _fact(
        sum(layers.values()) + sum(test_layers.values()), source_name, how
    )
    facts["dbt.sources.total"] = _fact(
        len(
            yaml.safe_load(
                (ROOT / "dbt" / "models" / "bronze" / "sources.yml").read_text(
                    encoding="utf-8"
                )
            )["sources"][0]["tables"]
        ),
        sources,
        "count tables in the only dbt source declaration",
    )
    gold_sql = list((ROOT / "dbt" / "models" / "gold").glob("*.sql"))
    facts["dbt.gold.dims"] = _fact(
        sum(1 for path in gold_sql if path.stem.startswith("dim_")),
        "dbt/models/gold/*.sql",
        "count gold dim models",
    )
    facts["dbt.gold.facts"] = _fact(
        sum(1 for path in gold_sql if path.stem.startswith("fct_")),
        "dbt/models/gold/*.sql",
        "count gold fact models",
    )
    mart_sql = list((ROOT / "dbt" / "models" / "marts").glob("*.sql"))
    facts["dbt.marts.bi"] = _fact(
        sum(1 for path in mart_sql if path.stem.startswith("mart_")),
        "dbt/models/marts/*.sql",
        "count mart_* models",
    )
    facts["dbt.marts.reconcile"] = _fact(
        sum(1 for path in mart_sql if not path.stem.startswith("mart_")),
        "dbt/models/marts/*.sql",
        "count reconciliation models in the mart layer",
    )


def _dag_calls(path: Path) -> list[ast.Call]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [node for node in ast.walk(tree) if isinstance(node, ast.Call)]


def collect_airflow(facts: dict[str, Any], inputs: list[str]) -> None:
    dag_dir = ROOT / "airflow" / "dags"
    dag_files = sorted(dag_dir.glob("*.py"))
    dag_ids: list[str] = []
    for path in dag_files:
        for call in _dag_calls(path):
            if _call_name(call.func) == "DAG":
                dag_id = _keyword(call, "dag_id")
                if isinstance(dag_id, str):
                    dag_ids.append(dag_id)
    inputs.extend(_rel(path) for path in dag_files)

    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    dag_call = next(
        call
        for call in _dag_calls(DAG_FILE)
        if _keyword(call, "dag_id") == "fleet_daily_load"
    )
    cron = str(_keyword(dag_call, "schedule"))
    catchup = bool(_keyword(dag_call, "catchup"))
    max_active = int(_keyword(dag_call, "max_active_runs"))
    timezone = ""
    for call in _dag_calls(DAG_FILE):
        if _call_name(call.func).endswith("datetime"):
            timezone = str(_keyword(call, "tz") or "")
            if timezone:
                break
    zone = ZoneInfo(timezone)
    offset = datetime(2026, 1, 1, tzinfo=zone).utcoffset()
    if offset is None:
        raise ValueError("timezone has no UTC offset")
    offset_ms = int(offset.total_seconds() * 1000)
    minute, hour = (int(field) for field in cron.split()[:2])

    retries: dict[str, int] = {}
    task_ids: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for decorator in node.decorator_list:
                if (
                    isinstance(decorator, ast.Call)
                    and _call_name(decorator.func) == "task"
                ):
                    task_id = _keyword(decorator, "task_id") or node.name
                    if node.name not in {"ingest_table", "dbt_stage"}:
                        task_ids.add(str(task_id))
                    value = _keyword(decorator, "retries")
                    if isinstance(value, int):
                        retries[node.name] = value
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Call)
            and _call_name(node.func.func) == "override"
        ):
            override_id = _keyword(node, "task_id")
            if isinstance(override_id, str):
                task_ids.add(override_id)

    tiers_node = next(
        node.value
        for node in tree.body
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and node.target.id == "INGEST_TIERS"
    )
    tiers = ast.literal_eval(tiers_node)
    for source, groups in tiers.items():
        for group in groups:
            for table in group:
                task_ids.add(f"ingest_{source}__{table}")

    compose = ROOT / "airflow" / "compose.yml"
    version_match = re.search(
        r"AIRFLOW_VERSION:.*?\$\{AIRFLOW_VERSION:-([^}]+)\}",
        compose.read_text(encoding="utf-8"),
    )
    version = version_match.group(1) if version_match else ""
    inputs.append(_rel(compose))

    source_name = _rel(DAG_FILE)
    facts["airflow.version"] = _fact(
        version, _rel(compose), "read AIRFLOW_VERSION default"
    )
    facts["airflow.dag_ids"] = _fact(
        sorted(dag_ids), _rel(dag_dir), "parse DAG() dag_id in every DAG file"
    )
    facts["airflow.schedule.cron"] = _fact(
        cron, source_name, "read DAG schedule keyword"
    )
    facts["airflow.schedule.timezone"] = _fact(
        timezone, source_name, "read DAG start_date tz keyword"
    )
    facts["airflow.schedule.time"] = _fact(
        f"{hour:02d}:{minute:02d}", source_name, "format cron hour and minute"
    )
    facts["airflow.schedule.utc_offset_ms"] = _fact(
        offset_ms, source_name, "derive UTC offset from the DAG timezone"
    )
    facts["airflow.catchup"] = _fact(catchup, source_name, "read DAG catchup keyword")
    facts["airflow.max_active_runs"] = _fact(
        max_active, source_name, "read DAG max_active_runs keyword"
    )
    facts["airflow.retries.ingest"] = _fact(
        retries["ingest_table"], source_name, "read @task retries"
    )
    facts["airflow.retries.dbt"] = _fact(
        retries["dbt_stage"], source_name, "read @task retries"
    )
    facts["airflow.retries.publish"] = _fact(
        retries["publish_marts"], source_name, "read @task retries"
    )
    facts["airflow.retries.notify"] = _fact(
        retries["notify_summary"], source_name, "read @task retries"
    )
    facts["airflow.tasks.ingest"] = _fact(
        len(tiers["mysql"][0])
        + sum(len(group) for group in tiers["mysql"][1:])
        + sum(len(group) for group in tiers["mongo"]),
        source_name,
        "count tables in INGEST_TIERS",
    )
    facts["airflow.tasks.ids"] = _fact(
        sorted(task_ids), source_name, "parse @task ids, overrides and INGEST_TIERS"
    )


def collect_publish(facts: dict[str, Any], inputs: list[str]) -> None:
    source = ROOT / "scripts" / "publish_marts.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    approved = next(
        ast.literal_eval(node.value)
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "PUBLISHABLE_MARTS"
            for target in node.targets
        )
    )
    mart_models = {
        path.stem for path in (ROOT / "dbt" / "models" / "marts").glob("mart_*.sql")
    }
    blocked = sorted(mart_models - set(approved))
    inputs.extend([_rel(source), "dbt/models/marts/*.sql"])
    facts["publish.marts.approved"] = _fact(
        list(approved), _rel(source), "parse PUBLISHABLE_MARTS"
    )
    facts["publish.marts.blocked"] = _fact(
        blocked, "dbt/models/marts/*.sql", "mart models not in the allowlist"
    )
    facts["publish.marts.total"] = _fact(
        len(mart_models), "dbt/models/marts/*.sql", "count mart_* models"
    )
    facts["publish.marts.count"] = _fact(
        len(approved), _rel(source), "parse PUBLISHABLE_MARTS"
    )


def _collect_count(args: list[str], env: dict[str, str] | None = None) -> int:
    run_env = os.environ.copy()
    if env:
        run_env.update(env)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", *args],
        cwd=ROOT,
        env=run_env,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(result.stdout[-2000:] + result.stderr[-2000:])
    match = re.search(r"(\d+)(?:/\d+)? tests? collected", result.stdout)
    if not match:
        raise ValueError(f"could not parse pytest output: {result.stdout[-500:]}")
    return int(match.group(1))


def collect_tests(facts: dict[str, Any], inputs: list[str]) -> None:
    command = "python -m pytest --collect-only -q"
    counts = {
        "total": _collect_count(["tests"]),
        "unit": _collect_count(["tests", "-m", "unit"]),
        "integration": _collect_count(["tests", "-m", "integration"]),
        "smoke": _collect_count(["tests", "-m", "smoke"]),
    }
    airflow_count = _collect_count(["airflow/tests"], {"PYTHONPATH": "airflow"})
    inputs.extend(["tests", "airflow/tests", "pyproject.toml"])
    for name, count in counts.items():
        facts[f"tests.collected.{name}"] = _fact(
            count, command, "parse pytest collect-only totals"
        )
    facts["tests.collected.combined"] = _fact(
        counts["total"] + airflow_count,
        command,
        "add tests/ and airflow/tests collections",
    )
    facts["airflow.tests.total"] = _fact(
        airflow_count,
        f"PYTHONPATH=airflow {command} airflow/tests",
        "parse pytest collect-only totals",
    )


def collect_ci(facts: dict[str, Any], inputs: list[str]) -> None:
    ci = ROOT / ".github" / "workflows" / "ci.yml"
    leaks = ROOT / ".github" / "workflows" / "leaks.yml"
    ci_jobs = yaml.safe_load(ci.read_text(encoding="utf-8"))["jobs"]
    leaks_jobs = yaml.safe_load(leaks.read_text(encoding="utf-8"))["jobs"]
    secret_jobs = [
        name
        for name, body in leaks_jobs.items()
        if "gitleaks" in json.dumps(body).lower()
    ]
    inputs.extend([_rel(ci), _rel(leaks)])
    facts["ci.jobs.total"] = _fact(len(ci_jobs), _rel(ci), "count jobs keys in ci.yml")
    facts["ci.jobs.secret_scan"] = _fact(
        len(secret_jobs), _rel(leaks), "count gitleaks jobs in leaks.yml"
    )


def collect_reports(facts: dict[str, Any], inputs: list[str]) -> None:
    findings = ROOT / "reports" / "findings"

    def load(name: str) -> dict[str, Any]:
        path = findings / name
        inputs.append(_rel(path))
        return json.loads(path.read_text(encoding="utf-8"))

    delivery = load("02_delivery_service.json")
    safety = load("05_safety.json")
    revenue = load("01_revenue_concentration.json")
    sources = {
        "delivery": "02_delivery_service.json",
        "safety": "05_safety.json",
        "revenue": "01_revenue_concentration.json",
    }
    mapping = {
        "report.delivery.detention_cost_per_hour_usd": delivery[
            "detention_cost_per_hour_usd"
        ],
        "report.delivery.events": delivery["events"],
        "report.delivery.on_time_rate": delivery["overall_on_time_rate"],
        "report.delivery.total_estimated_detention_cost": delivery[
            "total_estimated_detention_cost"
        ],
        "report.delivery.worst_facility.on_time_rate": delivery["worst_facility"][
            "on_time_rate"
        ],
        "report.delivery.worst_facility.events": delivery["worst_facility"]["events"],
        "report.delivery.worst_facility.detention_cost": delivery["worst_facility"][
            "detention_cost"
        ],
        "report.delivery.top_detention_customer.detention_cost": delivery[
            "top_detention_customer"
        ]["detention_cost"],
        "report.delivery.top_detention_customer.events": delivery[
            "top_detention_customer"
        ]["events"],
        "report.safety.incidents": safety["incidents"],
        "report.safety.preventable_incidents": safety["preventable_incidents"],
        "report.safety.preventable_share": safety["preventable_share"],
        "report.safety.total_claim_amount": safety["total_claim_amount"],
        "report.revenue.top_customer_revenue": revenue["top_customer_revenue"],
        "report.revenue.top1_revenue_share": revenue["top1_revenue_share"],
        "report.revenue.top10_revenue_share": revenue["top10_revenue_share"],
    }
    for key, value in mapping.items():
        source = f"reports/findings/{sources[key.split('.')[1]]}"
        facts[key] = _fact(
            value, source, "read the named field from the committed findings JSON"
        )


def _view_columns(view: str) -> list[str]:
    sys.path.insert(0, str(ROOT))
    from scripts.site_lib.views import load_view_columns

    return load_view_columns(SQL_VIEWS, view)


def collect_streamlit(facts: dict[str, Any], inputs: list[str]) -> None:
    state = ROOT / ".streamlit" / "streamlit_state.py"
    tree = ast.parse(state.read_text(encoding="utf-8"))
    keys: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "live_payload":
            for child in ast.walk(node):
                if isinstance(child, ast.Dict):
                    keys.update(
                        key.value
                        for key in child.keys
                        if isinstance(key, ast.Constant) and isinstance(key.value, str)
                    )
    inputs.extend([_rel(state), _rel(SQL_VIEWS)])
    facts["streamlit.payload.keys"] = _fact(
        sorted(keys), _rel(state), "parse dict keys returned by live_payload"
    )
    for view in ("v_batch_summary", "v_dbt_results", "v_data_quality_results"):
        label = {
            "v_batch_summary": "batches",
            "v_dbt_results": "dbt",
            "v_data_quality_results": "dq",
        }[view]
        facts[f"streamlit.views.{label}.columns"] = _fact(
            _view_columns(view),
            _rel(SQL_VIEWS),
            "parse SELECT aliases of the audit view",
        )


def build_facts() -> dict[str, Any]:
    """Collect all facts in insertion order."""
    facts: dict[str, Any] = {}
    inputs: list[str] = [
        "index.html",
        "contracts/sources_index.json",
        "scripts/publish_marts.py",
        "scripts/site_facts.py",
    ]
    collect_contracts(facts)
    collect_dbt(facts, inputs)
    collect_airflow(facts, inputs)
    collect_publish(facts, inputs)
    collect_tests(facts, inputs)
    collect_ci(facts, inputs)
    collect_reports(facts, inputs)
    collect_streamlit(facts, inputs)
    facts["meta.inputs"] = _fact(
        sorted(set(inputs)),
        "scripts/site_facts.py",
        "files and commands read by this collector",
    )
    return facts


def write_facts() -> dict[str, Any]:
    facts = build_facts()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(facts, indent=2) + "\n", encoding="utf-8")
    return facts


def _git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def _last_commit(path: str) -> str:
    result = _git("log", "-1", "--format=%H", "--", path)
    return result.stdout.strip() if result.returncode == 0 else ""


def _changed_after(baseline: str, path: str) -> bool:
    candidate = _last_commit(path)
    if not candidate or candidate == baseline:
        return False
    return _git("merge-base", "--is-ancestor", baseline, candidate).returncode == 0


def check_stale() -> int:
    if not OUTPUT.exists():
        print(f"missing {_rel(OUTPUT)}; run python scripts/site_facts.py")
        return 1
    facts = json.loads(OUTPUT.read_text(encoding="utf-8"))
    inputs = facts.get("meta.inputs", {}).get("value", [])
    baseline = _last_commit(_rel(OUTPUT))
    if not baseline:
        print(f"no committed baseline for {_rel(OUTPUT)}")
        return 1
    stale = [item for item in inputs if _changed_after(baseline, item)]
    if stale:
        for item in stale:
            print(f"stale: {item} changed after {_rel(OUTPUT)}")
        return 1
    print(f"{_rel(OUTPUT)} is current ({len(inputs)} inputs)")
    return 0


def print_table(facts: dict[str, Any]) -> None:
    for key, item in facts.items():
        value = item["value"]
        if isinstance(value, list) and len(value) > 5:
            shown = (
                ", ".join(str(entry) for entry in value[:3])
                + f", … ({len(value)} total)"
            )
        else:
            shown = str(value)
        print(f"{key:55} {shown[:80]}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-stale", action="store_true")
    parser.add_argument("--print", dest="print_table", action="store_true")
    args = parser.parse_args()
    if args.check_stale:
        return check_stale()
    facts = write_facts()
    if args.print_table:
        print_table(facts)
    print(f"wrote {_rel(OUTPUT)} ({len(facts)} facts)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
