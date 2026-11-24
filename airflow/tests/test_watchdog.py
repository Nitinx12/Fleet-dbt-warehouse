"""Keep the watchdog focused on observational stream health alerts."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.unit
def test_watchdog_probes_stream_health_without_batch_control() -> None:
    """The watchdog may alert, but must never open or finalise a batch."""
    source = (
        Path(__file__).resolve().parents[1] / "dags" / "fleet_watchdog.py"
    ).read_text(encoding="utf-8")

    assert "watch_stream_health" in source
    assert "build_stream_monitor_command" in source
    assert "latest_watchdog_health" in source
    assert "start_batch" not in source
    assert "finalize_batch" not in source
    assert "NotImplementedError" not in source
