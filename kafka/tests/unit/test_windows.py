"""Static Spark tests for windowing and detention logic."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fleet_stream.job.detention import clean_departures, open_alerts
from fleet_stream.job.windows import aggregate_windows
from fleet_stream.settings import from_env

pytestmark = pytest.mark.unit

START = datetime(2026, 1, 1, 6, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark() -> Any:
    """Create a tiny local Spark session for static DataFrame tests."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("fleet-stream-unit")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def _events(spark: Any) -> Any:
    from pyspark.sql.types import (
        BooleanType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("facility_id", StringType(), False),
            StructField("stop_type", StringType(), False),
            StructField("on_time_flag", BooleanType(), False),
            StructField("detention_minutes", IntegerType(), False),
            StructField("location_city", StringType(), False),
            StructField("location_state", StringType(), False),
            StructField("event_time", TimestampType(), False),
        ]
    )
    rows = [
        (
            "evt-1",
            "ARRIVED",
            "FAC-1",
            "PICKUP",
            True,
            0,
            "Pune",
            "MH",
            START,
        ),
        (
            "evt-2",
            "DEPARTED",
            "FAC-1",
            "PICKUP",
            False,
            10,
            "Pune",
            "MH",
            START + timedelta(minutes=10),
        ),
        (
            "evt-3",
            "DEPARTED",
            "FAC-2",
            "DELIVERY",
            True,
            0,
            "Mumbai",
            "MH",
            START + timedelta(minutes=12),
        ),
    ]
    return spark.createDataFrame(rows, schema)


def test_aggregate_windows_counts_stops_and_detention(spark: Any) -> None:
    """A facility/stop window aggregates one arrival and one departure."""
    settings = replace(from_env(), window_minutes=15, detention_threshold_minutes=1)
    windows = aggregate_windows(_events(spark), settings).collect()

    assert len(windows) == 2
    facility = next(row for row in windows if row["facility_id"] == "FAC-1")
    assert facility["events_count"] == 2
    assert facility["completed_events"] == 1
    assert facility["on_time_events"] == 1
    assert facility["detention_minutes"] == 10


def test_detention_alerts_open_and_clean(spark: Any) -> None:
    """Only positive detention opens an alert; a clean departure resolves it."""
    events = _events(spark)

    opened = [row["event_id"] for row in open_alerts(events, 1).collect()]
    cleaned = [row["facility_id"] for row in clean_departures(events, 1).collect()]

    assert opened == ["evt-2"]
    assert cleaned == ["FAC-2"]
