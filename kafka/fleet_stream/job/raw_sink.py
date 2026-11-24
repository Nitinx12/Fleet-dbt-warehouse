"""Idempotent raw-table sink for validated delivery lifecycle events."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from psycopg2.extras import execute_values

from fleet_stream.job.db import connection

RAW_COLUMNS = (
    "event_id",
    "event_type",
    "stop_type",
    "facility_id",
    "scheduled_datetime",
    "actual_datetime",
    "event_time",
    "detention_minutes",
    "on_time_flag",
    "location_city",
    "location_state",
    "loaded_at",
)

CREATE_STAGE = """
CREATE TEMP TABLE stream_raw_stage (
    event_id text NOT NULL,
    event_type text NOT NULL,
    stop_type text NOT NULL,
    facility_id text NOT NULL,
    scheduled_datetime timestamptz NOT NULL,
    actual_datetime timestamptz NOT NULL,
    event_time timestamptz NOT NULL,
    detention_minutes integer NOT NULL,
    on_time_flag boolean NOT NULL,
    location_city text NOT NULL,
    location_state text NOT NULL,
    loaded_at timestamptz NOT NULL
) ON COMMIT DROP
"""

INSERT_STAGE = """
INSERT INTO stream_raw_stage (
    event_id, event_type, stop_type, facility_id, scheduled_datetime,
    actual_datetime, event_time, detention_minutes, on_time_flag,
    location_city, location_state, loaded_at
) VALUES %s
"""

MERGE_RAW = """
INSERT INTO realtime.delivery_events_raw (
    event_id, event_type, stop_type, facility_id, scheduled_datetime,
    actual_datetime, event_time, detention_minutes, on_time_flag,
    location_city, location_state, loaded_at
)
SELECT
    event_id, event_type, stop_type, facility_id, scheduled_datetime,
    actual_datetime, event_time, detention_minutes, on_time_flag,
    location_city, location_state, loaded_at
FROM stream_raw_stage
ON CONFLICT (event_id, event_type) DO NOTHING
"""


def upsert_raw(rows: Sequence[Mapping[str, Any]]) -> int:
    """Insert new lifecycle events and ignore duplicates by dedupe key."""
    if not rows:
        return 0
    values = [tuple(row[name] for name in RAW_COLUMNS) for row in rows]
    with connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_STAGE)
            execute_values(cursor, INSERT_STAGE, values, page_size=1_000)
            cursor.execute(MERGE_RAW)
            inserted = cursor.rowcount
        conn.commit()
    return max(inserted, 0)
