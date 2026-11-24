"""Read delivery rows from the fixture sample or the live source collection."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
FIXTURE_PATH = ROOT / "contracts" / "samples" / "mongo__delivery_events.sample.json"
SOURCE_FIELDS = (
    "event_id",
    "load_id",
    "trip_id",
    "event_type",
    "facility_id",
    "scheduled_datetime",
    "actual_datetime",
    "detention_minutes",
    "on_time_flag",
    "location_city",
    "location_state",
    "loaded_at",
)


class ReaderError(RuntimeError):
    """Raised when a source row cannot be read."""


def read_fixture_rows(path: Path = FIXTURE_PATH) -> list[dict[str, Any]]:
    """Return the raw delivery rows from a contract sample file."""
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = data.get("sample_rows_raw")
    if not isinstance(rows, list):
        raise ReaderError(f"{path.name}: sample_rows_raw must be a list")
    if any(not isinstance(row, dict) for row in rows):
        raise ReaderError(f"{path.name}: sample rows must be objects")
    return rows


def read_live_rows(limit: int | None = None) -> Iterator[dict[str, Any]]:
    """Stream raw delivery documents from the read-only source collection."""
    try:
        from pymongo import MongoClient
    except ImportError as exc:
        raise ReaderError("pymongo is required for STREAM_SOURCE=live") from exc

    uri = os.getenv("MONGO_URI")
    database = os.getenv("MONGO_DB_NAME")
    if not uri or not database:
        raise ReaderError("MONGO_URI and MONGO_DB_NAME are required for live replay")

    client = MongoClient(uri)
    try:
        cursor = client[database]["delivery_events"].find(
            {}, projection={field: 1 for field in SOURCE_FIELDS} | {"_id": 0}
        )
        for index, document in enumerate(cursor):
            if limit is not None and index >= limit:
                break
            yield document
    finally:
        client.close()
