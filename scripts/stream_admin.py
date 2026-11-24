"""Administer Kafka topics, stream registration, status and resets."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "kafka"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from fleet_stream.admin.guard import assert_safe_target
from fleet_stream.admin.registry import register_stream
from fleet_stream.admin.reset import reset_stream
from fleet_stream.admin.status import stream_status
from fleet_stream.admin.topics import create_topics
from fleet_stream.contract import load_contract
from fleet_stream.settings import from_env

log = logging.getLogger(__name__)


def main() -> None:
    """Run one guarded stream administration command."""
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("topics", help="create missing topics")
    subparsers.add_parser("register", help="enable the stream registry row")
    subparsers.add_parser("status", help="show stream health")
    subparsers.add_parser("reset", help="clear checkpoints and realtime tables")
    args = parser.parse_args()

    settings = from_env()
    contract = load_contract()
    assert_safe_target(settings)
    if args.command == "topics":
        create_topics(settings)
    elif args.command == "register":
        register_stream(settings, contract)
    elif args.command == "status":
        stream_status(contract.stream)
    else:
        reset_stream(settings)
        log.info("stream reset completed")


if __name__ == "__main__":
    main()
