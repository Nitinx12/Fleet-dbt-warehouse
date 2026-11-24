"""Reset disposable stream state after explicit confirmation."""

from __future__ import annotations

import os
import shutil

from fleet_stream.admin.guard import assert_safe_target
from fleet_stream.job.db import execute
from fleet_stream.settings import StreamSettings

TRUNCATE_REALTIME = """
TRUNCATE TABLE
    realtime.delivery_events_raw,
    realtime.facility_service_window,
    realtime.detention_alert
"""


def reset_stream(settings: StreamSettings) -> None:
    """Truncate realtime tables and remove the checkpoint; never touch batch layers."""
    assert_safe_target(settings)
    if os.getenv("CONFIRM") != "yes":
        raise ValueError("stream reset requires CONFIRM=yes")
    checkpoint = settings.checkpoint_dir
    if not any("checkpoint" in part.lower() for part in checkpoint.parts):
        raise ValueError("STREAM_CHECKPOINT_DIR must point at a checkpoint directory")
    execute(TRUNCATE_REALTIME)
    if checkpoint.exists():
        shutil.rmtree(checkpoint)
