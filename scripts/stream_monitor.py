"""Probe Kafka lag and freshness for the stream watchdog."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
try:
    from dotenv import load_dotenv
except ImportError:  # containers receive settings from Compose env_file
    pass
else:
    load_dotenv(ROOT / ".env")
for path in (ROOT, ROOT / "kafka"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from fleet_stream.contract import load_contract
from fleet_stream.monitor.lag import probe
from fleet_stream.settings import from_env

log = logging.getLogger(__name__)


def main() -> None:
    """Append one watchdog health row when the stream is registered."""
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    metrics = probe(from_env(), load_contract())
    if metrics is None:
        log.info("stream health skipped: no ENABLED stream")
        return
    log.info(
        "stream health: lag=%s age=%s rejected=%s late=%s",
        metrics.partition_lag,
        metrics.newest_event_age_seconds,
        metrics.rejected,
        metrics.late_dropped,
    )


if __name__ == "__main__":
    main()
