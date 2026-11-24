"""Replay validated source rows through deterministic stream faults."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from fleet_stream.contract import Contract, validate_event
from fleet_stream.events import (
    EventExpansionError,
    dedupe_key,
    event_time,
    expand_source_row,
)
from fleet_stream.producer.faults import FaultInjector
from fleet_stream.settings import StreamSettings

log = logging.getLogger(__name__)


class Publisher(Protocol):
    """Minimal publisher interface used by the replay driver."""

    def publish(self, event: Mapping[str, Any]) -> None: ...

    def flush(self) -> None: ...


@dataclass
class ReplayStats:
    """Counts recorded for one replay run."""

    source_rows: int = 0
    events: int = 0
    published: int = 0
    duplicates: int = 0
    late: int = 0
    rejected: int = 0


def replay(
    source_rows: Sequence[Mapping[str, Any]],
    publisher: Publisher,
    settings: StreamSettings,
    contract: Contract,
    *,
    from_time: datetime | None = None,
    to_time: datetime | None = None,
) -> ReplayStats:
    """Expand, validate, seed faults and publish one replay batch."""
    stats = ReplayStats(source_rows=len(source_rows))
    expanded: list[dict[str, Any]] = []
    for row in source_rows:
        try:
            events = expand_source_row(row)
        except EventExpansionError:
            stats.rejected += 1
            log.warning("source row rejected: invalid value")
            continue
        for event in events:
            result = validate_event(event, contract)
            if not result.ok:
                stats.rejected += 1
                log.warning("event rejected: invalid contract")
                continue
            value = event_time(event)
            if from_time is not None and value < from_time:
                continue
            if to_time is not None and value > to_time:
                continue
            expanded.append(event)

    expanded.sort(key=lambda event: (event_time(event), dedupe_key(event)))
    stats.events = len(expanded)
    injector = FaultInjector(settings, contract.allowed_lateness_minutes)
    for seeded in injector.plan(expanded):
        result = validate_event(seeded.event, contract)
        if not result.ok:
            stats.rejected += 1
            log.warning("seeded event rejected: invalid contract")
            continue
        publisher.publish(seeded.event)
        stats.published += 1
        stats.duplicates += int(seeded.duplicate)
        stats.late += int(seeded.late)
    publisher.flush()

    log.info(
        "replay completed: rows=%s events=%s published=%s duplicates=%s late=%s rejected=%s",
        stats.source_rows,
        stats.events,
        stats.published,
        stats.duplicates,
        stats.late,
        stats.rejected,
    )
    return stats
