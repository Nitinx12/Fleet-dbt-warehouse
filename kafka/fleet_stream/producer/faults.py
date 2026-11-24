"""Seed deterministic duplicate and late-arrival faults for local replay."""

from __future__ import annotations

import random
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from fleet_stream.events import parse_source_timestamp
from fleet_stream.settings import StreamSettings


@dataclass(frozen=True)
class SeededEvent:
    """One outbound event and the fault it carries."""

    event: dict[str, Any]
    duplicate: bool
    late: bool


class FaultInjector:
    """Apply local-only replay faults with a deterministic seed."""

    def __init__(self, settings: StreamSettings, lateness_minutes: int) -> None:
        settings.require_faults_allowed()
        self._duplicate_fraction = settings.duplicate_fraction
        self._late_fraction = settings.late_fraction
        self._rng = random.Random(settings.seed)
        self._lateness = lateness_minutes + 1

    def plan(self, events: Sequence[dict[str, Any]]) -> list[SeededEvent]:
        """Return outbound events with seeded duplicates and late arrivals."""
        planned: list[SeededEvent] = []
        watermark = None
        for event in events:
            event_time = parse_source_timestamp(event["event_time"])
            draw = self._rng.random()
            duplicate = draw < self._duplicate_fraction
            late = (
                not duplicate and draw < self._duplicate_fraction + self._late_fraction
            )
            outbound = dict(event)
            if late and watermark is not None:
                outbound["event_time"] = (
                    watermark - timedelta(minutes=self._lateness)
                ).isoformat()
            planned.append(SeededEvent(outbound, duplicate, late))
            watermark = max(watermark, event_time) if watermark else event_time
            if duplicate:
                planned.append(SeededEvent(dict(outbound), True, False))
        return planned
