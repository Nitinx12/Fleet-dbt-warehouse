"""Load .env, invoke dbt, then land the run results in the audit tables."""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
# run as a script, so the repo root is not on sys.path yet
sys.path.insert(0, str(ROOT))

DBT_DIR = "Scripts" if sys.platform == "win32" else "bin"
DBT_NAME = "dbt.exe" if sys.platform == "win32" else "dbt"
DBT = ROOT / ".venv" / DBT_DIR / DBT_NAME
if not DBT.exists():
    DBT = "dbt"


def _arguments(argv: list[str] | None = None) -> tuple[argparse.Namespace, list[str]]:
    """Extract optional Airflow context without passing it to dbt."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--batch-id", type=int)
    parser.add_argument("--airflow-dag-id")
    parser.add_argument("--airflow-run-id")
    args, dbt_argv = parser.parse_known_args(argv)
    if args.batch_id is not None and args.batch_id <= 0:
        parser.error("--batch-id must be positive")
    return args, dbt_argv


def main(argv: list[str] | None = None) -> int:
    """Run dbt, audit the invocation, and return dbt's own exit code."""
    args, dbt_argv = _arguments(sys.argv[1:] if argv is None else argv)
    started = datetime.now(UTC)
    exit_code = subprocess.call([str(DBT), *dbt_argv], cwd=ROOT / "dbt")
    audit_invocation(
        started,
        exit_code,
        dbt_target=_dbt_target(dbt_argv),
        batch_id=args.batch_id,
    )
    return exit_code


def audit_invocation(
    started: datetime,
    exit_code: int,
    dbt_target: str,
    batch_id: int | None = None,
) -> None:
    """Best effort: a broken audit trail must never hide dbt's exit code."""
    from utils import dbt_audit
    from utils.logger import get_logger

    log = get_logger("run_dbt")
    try:
        dbt_audit.audit_invocation(
            ROOT / "dbt",
            started_after=started,
            exit_code=exit_code,
            dbt_target=dbt_target,
            batch_id=batch_id,
        )
    except Exception as error:  # noqa: BLE001 - dbt's exit code must be preserved
        log.error("dbt audit load failed (%s)", type(error).__name__)


def _dbt_target(argv: list[str]) -> str:
    """dbt does not record --target in its artifacts, so read it from argv."""
    for index, arg in enumerate(argv):
        if arg in ("--target", "-t") and index + 1 < len(argv):
            return argv[index + 1]
        if arg.startswith("--target="):
            return arg.split("=", 1)[1]
    return "dev"


if __name__ == "__main__":
    sys.exit(main())
