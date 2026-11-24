"""Validate the Makefile's safe public verification targets."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
MAKEFILE = ROOT / "Makefile"


def _make(*args: str) -> str:
    """Run make for a safe help or dry-run assertion."""
    if shutil.which("make") is None:
        pytest.skip("make is not installed")
    completed = subprocess.run(
        ["make", *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout


def test_help_documents_verify_target() -> None:
    """Expose the verification command in the generated help output."""
    help_output = _make("help")

    assert "verify" in help_output
    assert "Run all non-destructive local quality checks." in help_output


def test_help_documents_airflow_targets() -> None:
    """Expose the supported local Airflow commands in help."""
    help_output = _make("help")

    assert "airflow-test" in help_output
    assert "airflow-up" in help_output


def test_contracts_check_is_a_documented_read_only_target() -> None:
    """Keep the preflight contract check available through Make."""
    help_output = _make("help")
    dry_run = _make("--dry-run", "contracts-check")

    assert "contracts-check" in help_output
    assert "scripts/check_contracts.py" in dry_run
    assert "psql" not in dry_run


def test_verify_dry_run_contains_only_quality_checks() -> None:
    """Keep verification independent of database and dbt write targets."""
    dry_run = _make("--dry-run", "verify")

    assert "ruff check ." in dry_run
    assert "pytest" in dry_run
    assert "scripts/run_dbt.py" not in dry_run
    assert "psql" not in dry_run


def test_notebooks_dry_run_executes_the_runner() -> None:
    """Keep notebook execution behind its own non-mutating target."""
    dry_run = _make("--dry-run", "notebooks")

    assert "scripts/run_notebooks.py" in dry_run
    assert "--inplace" not in dry_run


def test_destructive_targets_keep_the_confirmation_guard() -> None:
    """Require an explicit confirmation before destructive operations."""
    makefile = MAKEFILE.read_text(encoding="utf-8")

    for target in ("db-init", "dbt-full-refresh", "distclean"):
        start = makefile.index(f"{target}:")
        end = makefile.find("\n\n", start)
        body = makefile[start:] if end == -1 else makefile[start:end]
        assert "$(call confirm_destructive)" in body
