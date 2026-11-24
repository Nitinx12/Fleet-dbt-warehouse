"""Report stream registration and the latest append-only health evidence."""

from __future__ import annotations

import logging
from typing import Any

from fleet_stream.job.db import fetchone

log = logging.getLogger(__name__)

REGISTRATION = """
SELECT status, topic, dlq_topic, checkpoint_path
  FROM control.stream
 WHERE stream_name = %s
"""

LATEST_HEALTH = """
SELECT observed_at, rows_raw, rows_window, rows_alert, rejected, late_dropped, message
  FROM audit.stream_health
 WHERE stream_name = %s
 ORDER BY observed_at DESC
 LIMIT 1
"""


def stream_status(stream_name: str) -> dict[str, Any]:
    """Return registration and latest health values for one stream."""
    registration = fetchone(REGISTRATION, (stream_name,))
    health = fetchone(LATEST_HEALTH, (stream_name,))
    status = {
        "registered": registration is not None,
        "status": registration[0] if registration else None,
        "topic": registration[1] if registration else None,
        "dlq_topic": registration[2] if registration else None,
        "checkpoint_path": registration[3] if registration else None,
        "health": health,
    }
    log.info(
        "stream=%s registered=%s status=%s health=%s",
        stream_name,
        status["registered"],
        status["status"],
        "present" if health else "none",
    )
    return status
