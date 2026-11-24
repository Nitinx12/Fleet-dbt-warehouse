"""Detention alert state derived from completed departure events."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fleet_stream.job.db import connection, execute
from fleet_stream.job.windows import read_raw

UPSERT_ALERTS = """
INSERT INTO realtime.detention_alert (
    event_id, facility_id, status, opened_at, resolved_at
) VALUES %s
ON CONFLICT (event_id) DO NOTHING
"""

RESOLVE_ALERTS = """
UPDATE realtime.detention_alert
   SET status = 'RESOLVED', resolved_at = %s, updated_at = now()
 WHERE status = 'OPEN'
   AND facility_id = %s
   AND opened_at < %s
"""


def open_alerts(raw_df: Any, threshold: int) -> Any:
    """Select departures whose detention crosses the contract threshold."""
    from pyspark.sql import functions as F

    return raw_df.filter(
        (F.col("event_type") == F.lit("DEPARTED"))
        & (F.col("detention_minutes") >= F.lit(threshold))
    ).select(
        F.col("event_id"),
        F.col("facility_id"),
        F.lit("OPEN").alias("status"),
        F.col("event_time").alias("opened_at"),
        F.lit(None).alias("resolved_at"),
    )


def clean_departures(raw_df: Any, threshold: int) -> Any:
    """Find each facility's latest departure below the detention threshold."""
    from pyspark.sql import functions as F

    return (
        raw_df.filter(
            (F.col("event_type") == F.lit("DEPARTED"))
            & (F.col("detention_minutes") < F.lit(threshold))
        )
        .groupBy("facility_id")
        .agg(F.max("event_time").alias("clean_at"))
    )


def process_detention(
    spark: Any,
    settings: Any,
    start: datetime,
    end: datetime,
    cutoff: datetime,
) -> int:
    """Open new alerts and resolve older ones for one micro-batch."""
    from psycopg2.extras import execute_values

    raw = read_raw(spark, start, end, cutoff)
    openings = [
        row.asDict()
        for row in open_alerts(raw, settings.detention_threshold_minutes).collect()
    ]
    columns = ("event_id", "facility_id", "status", "opened_at", "resolved_at")
    if openings:
        values = [tuple(row[name] for name in columns) for row in openings]
        with connection() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, UPSERT_ALERTS, values, page_size=500)
            conn.commit()

    resolutions = clean_departures(raw, settings.detention_threshold_minutes).collect()
    for row in resolutions:
        clean_at = row["clean_at"]
        execute(RESOLVE_ALERTS, (clean_at, row["facility_id"], clean_at))
    return len(openings)
