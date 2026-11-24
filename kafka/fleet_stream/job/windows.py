"""Facility service-window aggregation and idempotent upsert."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from fleet_stream.job.db import jdbc_properties, jdbc_url

UPSERT_WINDOWS = """
INSERT INTO realtime.facility_service_window (
    facility_id, window_start, window_end, events_count, completed_events,
    on_time_events, late_events, on_time_known_events, detention_events,
    detention_minutes, dwell_minutes
) VALUES %s
ON CONFLICT (facility_id, window_start) DO UPDATE SET
    window_end = EXCLUDED.window_end,
    events_count = EXCLUDED.events_count,
    completed_events = EXCLUDED.completed_events,
    on_time_events = EXCLUDED.on_time_events,
    late_events = EXCLUDED.late_events,
    on_time_known_events = EXCLUDED.on_time_known_events,
    detention_events = EXCLUDED.detention_events,
    detention_minutes = EXCLUDED.detention_minutes,
    dwell_minutes = EXCLUDED.dwell_minutes,
    updated_at = now()
"""


def aggregate_windows(raw_df: Any, settings: Any) -> Any:
    """Aggregate raw events into event-time facility windows."""
    from pyspark.sql import functions as F

    threshold = settings.detention_threshold_minutes
    windowed = raw_df.withColumn(
        "event_window",
        F.window(F.col("event_time"), f"{settings.window_minutes} minutes"),
    )
    return windowed.groupBy(
        F.col("facility_id"),
        F.col("event_window.start").alias("window_start"),
        F.col("event_window.end").alias("window_end"),
    ).agg(
        F.count(F.lit(1)).alias("events_count"),
        F.sum(F.when(F.col("event_type") == F.lit("DEPARTED"), 1).otherwise(0)).alias(
            "completed_events"
        ),
        F.sum(F.when(F.col("on_time_flag") == F.lit(True), 1).otherwise(0)).alias(
            "on_time_events"
        ),
        F.sum(F.when(F.col("on_time_flag") == F.lit(False), 1).otherwise(0)).alias(
            "late_events"
        ),
        F.sum(F.when(F.col("on_time_flag").isNotNull(), 1).otherwise(0)).alias(
            "on_time_known_events"
        ),
        F.sum(
            F.when(
                (F.col("event_type") == F.lit("DEPARTED"))
                & (F.col("detention_minutes") >= F.lit(threshold)),
                1,
            ).otherwise(0)
        ).alias("detention_events"),
        F.sum(
            F.when(
                F.col("event_type") == F.lit("DEPARTED"), F.col("detention_minutes")
            ).otherwise(0)
        )
        .cast("int")
        .alias("detention_minutes"),
        F.sum(
            F.when(
                (F.col("event_type") == F.lit("DEPARTED"))
                & (F.col("detention_minutes") >= F.lit(threshold)),
                F.col("detention_minutes"),
            ).otherwise(0)
        )
        .cast("int")
        .alias("dwell_minutes"),
    )


def read_raw(spark: Any, start: datetime, end: datetime, cutoff: datetime) -> Any:
    """Read raw rows in a batch's event-time range, excluding late arrivals."""
    where = (
        f"event_time >= '{start.isoformat()}' "
        f"AND event_time < '{end.isoformat()}' "
        f"AND event_time >= '{cutoff.isoformat()}'"
    )
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url())
        .option("dbtable", "realtime.delivery_events_raw")
        .option("where", where)
        .options(**jdbc_properties())
        .load()
    )


def upsert_windows(rows: list[dict[str, Any]]) -> int:
    """Replace affected windows from the complete raw-table view."""
    from psycopg2.extras import execute_values

    from fleet_stream.job.db import connection

    if not rows:
        return 0
    columns = (
        "facility_id",
        "window_start",
        "window_end",
        "events_count",
        "completed_events",
        "on_time_events",
        "late_events",
        "on_time_known_events",
        "detention_events",
        "detention_minutes",
        "dwell_minutes",
    )
    values = [tuple(row[name] for name in columns) for row in rows]
    with connection() as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, UPSERT_WINDOWS, values, page_size=500)
        conn.commit()
    return len(rows)


def process_windows(
    spark: Any,
    settings: Any,
    start: datetime,
    end: datetime,
    cutoff: datetime,
) -> int:
    """Compute and persist windows for one micro-batch."""
    raw = read_raw(spark, start, end, cutoff)
    rows = [row.asDict() for row in aggregate_windows(raw, settings).collect()]
    return upsert_windows(rows)


def batch_range(batch_max: datetime, settings: Any) -> tuple[datetime, datetime]:
    """Return the raw-table event-time range a batch can affect."""
    padding = timedelta(minutes=settings.window_minutes)
    return batch_max - padding, batch_max + padding
