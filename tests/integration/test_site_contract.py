"""Integration tests for the committed site fact and render contracts."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _run(script: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, script, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )


def test_site_number_check_passes_json_contract() -> None:
    result = _run("scripts/check_site_numbers.py", "--json", "--strict")
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["bound_numbers"] > 0
    assert payload["errors"] == []


def test_generated_facts_are_current() -> None:
    result = _run("scripts/site_facts.py", "--check-stale")
    assert result.returncode == 0, result.stdout + result.stderr


def test_render_removes_both_placeholders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import render_site

    output = tmp_path / "site.html"
    monkeypatch.setattr(render_site, "OUTPUT", output)
    assert render_site.main() == 0
    html = output.read_text(encoding="utf-8")
    assert "__SITE_FACTS__" not in html
    assert "__LIVE_DATA__" not in html
    assert '"contracts.total":12' in html
