"""Replay delivery events from a fixture or the live source into Kafka."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "kafka"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from fleet_stream.contract import load_contract
from fleet_stream.producer.publish import KafkaPublisher
from fleet_stream.producer.replay import replay
from fleet_stream.producer.sources import read_fixture_rows, read_live_rows
from fleet_stream.settings import from_env

log = logging.getLogger(__name__)


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def main() -> None:
    """Run one deterministic replay into the prefixed Kafka topic."""
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("live", "fixture"))
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--from", dest="from_time", type=_timestamp)
    parser.add_argument("--to", dest="to_time", type=_timestamp)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    settings = from_env()
    contract = load_contract()
    source = args.source or settings.source
    if source == "fixture":
        source_rows = (
            read_fixture_rows(args.fixture) if args.fixture else read_fixture_rows()
        )
    else:
        source_rows = list(read_live_rows(args.limit))
    publisher = KafkaPublisher(
        settings.topic(contract.topic), settings.bootstrap_servers
    )
    stats = replay(
        source_rows,
        publisher,
        settings,
        contract,
        from_time=args.from_time,
        to_time=args.to_time,
    )
    log.info(
        "producer topic=%s records=%s", settings.topic(contract.topic), stats.published
    )


if __name__ == "__main__":
    main()
