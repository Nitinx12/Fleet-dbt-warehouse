"""Compare the stream speed layer with the authoritative gold facts."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "kafka"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from fleet_stream.monitor.reconcile import reconcile
from fleet_stream.settings import from_env

log = logging.getLogger(__name__)


def main() -> None:
    """Write observational stream reconciliation rows for one batch."""
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", required=True, type=int)
    parser.add_argument("--airflow-dag-id", required=True)
    parser.add_argument("--airflow-run-id", required=True)
    args = parser.parse_args()

    count = reconcile(args.batch_id, from_env())
    log.info("stream reconciliation comparisons=%s", count)


if __name__ == "__main__":
    main()
