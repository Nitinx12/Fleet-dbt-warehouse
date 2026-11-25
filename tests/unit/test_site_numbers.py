"""Unit tests for the site's fact formatting and extraction helpers."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from scripts import check_site_numbers as checker
from scripts.site_lib import facts, numbers, views


def test_format_fact_uses_explicit_rounding() -> None:
    assert facts.format_fact("x", Decimal("1.25"), "decimal1") == "1.3"
    assert facts.format_fact("x", 0.556697, "percent1") == "55.7%"
    assert facts.format_fact("x", 19545578.25, "usd_m1") == "$19.5M"
    assert facts.format_fact("x", 2653171.82, "usd_m2") == "$2.65M"


def test_extraction_binds_the_whole_fact_text(tmp_path: Path) -> None:
    path = tmp_path / "index.html"
    path.write_text(
        '<span data-fact="airflow.schedule.time">02:00</span>'
        '<span data-fact="airflow.schedule.cron">0 2 * * *</span>',
        encoding="utf-8",
    )
    tokens = [token for token in numbers.extract_tokens(path) if token.binding]
    assert [token.token for token in tokens] == ["02:00", "0 2 * * *"]


def test_view_parser_keeps_nested_from_columns() -> None:
    sql = """
    CREATE OR REPLACE VIEW audit.v_batch_summary AS
    SELECT b.batch_id, ROUND(EXTRACT(EPOCH FROM (b.finished_at - b.started_at))) AS duration_seconds
    FROM audit.pipeline_batch b;
    """
    assert views.parse_view_columns(sql, "v_batch_summary") == [
        "batch_id",
        "duration_seconds",
    ]


def test_generated_facts_carry_provenance() -> None:
    loaded = facts.load_facts(Path("site/facts.json"))
    assert "dbt.models.total" in loaded
    assert all(item.source and item.how for item in loaded.values())


def _config() -> dict[str, object]:
    return {"formats": {"site.count": "count"}, "rules": []}


def _facts() -> dict[str, facts.Fact]:
    return {
        "site.count": facts.Fact(12, "unit", "test fact"),
        "dbt.models.total": facts.Fact(12, "unit", "test fact"),
    }


def test_wrong_bound_number_is_rejected() -> None:
    token = numbers.NumberToken(
        token="99", line=1, column=1, kind="text", context="99", binding="site.count"
    )
    errors: list[str] = []
    checker.check_bindings(errors, [token], _facts(), _config(), strict=False)
    assert errors and "'99' != '12'" in errors[0]


def test_missing_data_fact_is_rejected() -> None:
    token = numbers.NumberToken(
        token="99", line=2, column=1, kind="text", context="99 things"
    )
    errors: list[str] = []
    checker.check_bindings(errors, [token], _facts(), _config(), strict=False)
    assert errors and "unbound number '99'" in errors[0]


def test_strict_mode_rejects_exact_looking_illustration() -> None:
    token = numbers.NumberToken(
        token="$1.25M",
        line=3,
        column=1,
        kind="text",
        context="$1.25M",
        illustrative=True,
    )
    errors: list[str] = []
    checker.check_bindings(errors, [token], _facts(), _config(), strict=True)
    assert errors and "exact-looking illustrative number" in errors[0]


def test_stale_doc_claim_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    doc = tmp_path / "README.md"
    doc.write_text("| dbt models | 31 |", encoding="utf-8")
    monkeypatch.setattr(checker, "_doc_paths", lambda: [doc])
    errors: list[str] = []
    checker.check_docs(errors, _facts(), _config())
    assert errors and any("dbt.models.total" in error for error in errors)
