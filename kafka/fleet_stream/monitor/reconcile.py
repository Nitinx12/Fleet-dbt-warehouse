"""Read-only reconciliation between the disposable speed layer and gold facts."""

from __future__ import annotations

import re
from typing import Any

from fleet_stream.contract import Contract, load_contract
from fleet_stream.job.db import fetchall, fetchone
from fleet_stream.monitor.lag import REGISTRATION
from fleet_stream.settings import StreamSettings

IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


def date_key_sql(column: str) -> str:
    """Return the gold-compatible integer date key for one safe column name."""
    if not IDENTIFIER.fullmatch(column):
        raise ValueError("invalid date column")
    return f"COALESCE(TO_CHAR(CAST({column} AS DATE), 'YYYYMMDD')::INTEGER, -1)"


def classify(
    check: str,
    source_count: int,
    target_count: int,
    tolerance_pct: float,
) -> dict[str, str]:
    """Classify one count comparison without changing the authoritative fact."""
    difference = source_count - target_count
    deviation = abs(difference) * 100 / max(target_count, 1)
    if difference == 0:
        status = "MATCH"
        message = f"{check}: counts match"
    elif deviation <= tolerance_pct:
        status = "WITHIN_TOLERANCE"
        message = f"DRIFT {check}: deviation {deviation:.2f}% within {tolerance_pct}%"
    else:
        status = "MISMATCH"
        message = f"DRIFT {check}: deviation {deviation:.2f}% exceeds {tolerance_pct}%"
    return {"status": status, "message": message}


def _by_key(rows: list[tuple]) -> dict[tuple[str, int], dict[str, int]]:
    result: dict[tuple[str, int], dict[str, int]] = {}
    for facility, date_key, events, on_time, detention in rows:
        result[(str(facility), int(date_key))] = {
            "events": int(events or 0),
            "on_time": int(on_time or 0),
            "detention": int(detention or 0),
        }
    return result


def raw_counts(contract: Contract, hours: int) -> dict[tuple[str, int], dict[str, int]]:
    """Aggregate ARRIVED lifecycle events to facility and business date."""
    date_column = str(contract.reconcile["date_column"])
    facility_column = str(contract.reconcile["facility_column"])
    key = date_key_sql(date_column)
    sql = f"""
    SELECT {facility_column}, {key} AS business_date,
           COUNT(DISTINCT event_id) AS events,
           SUM(CASE WHEN on_time_flag THEN 1 ELSE 0 END) AS on_time,
           SUM(detention_minutes) AS detention
      FROM realtime.delivery_events_raw
     WHERE event_type = 'ARRIVED'
       AND {date_column} < now() - (%s * INTERVAL '1 hour')
     GROUP BY 1, 2
    """
    return _by_key(fetchall(sql, (hours,)))


def window_counts(hours: int) -> dict[tuple[str, int], dict[str, int]]:
    """Aggregate service windows to facility and business date."""
    key = date_key_sql("window_start")
    sql = f"""
    SELECT facility_id, {key} AS business_date,
           SUM(events_count) AS events,
           SUM(on_time_events) AS on_time,
           SUM(detention_minutes) AS detention
      FROM realtime.facility_service_window
     WHERE window_start < now() - (%s * INTERVAL '1 hour')
     GROUP BY 1, 2
    """
    return _by_key(fetchall(sql, (hours,)))


def gold_counts(hours: int) -> dict[tuple[str, int], dict[str, int]]:
    """Aggregate the authoritative facts to facility and business date."""
    sql = """
    SELECT d.facility_id, f.scheduled_date_key AS business_date,
           COUNT(*) AS events,
           SUM(CASE WHEN f.on_time_flag THEN 1 ELSE 0 END) AS on_time,
           COALESCE(SUM(f.detention_minutes), 0) AS detention
      FROM gold.fct_delivery_events f
      LEFT JOIN gold.dim_facilities d ON d.facility_key = f.facility_key
     WHERE f.scheduled_datetime < now() - (%s * INTERVAL '1 hour')
     GROUP BY 1, 2
    """
    return _by_key(fetchall(sql, (hours,)))


def build_comparisons(
    contract: Contract, settings: StreamSettings
) -> list[dict[str, Any]]:
    """Build observational comparison rows for every stream/gold facility date."""
    hours = settings.min_window_age_hours
    raw = raw_counts(contract, hours)
    windows = window_counts(hours)
    gold = gold_counts(hours)
    comparisons: list[dict[str, Any]] = []
    for source_object, source in (
        (str(contract.reconcile["source_object"]), raw),
        ("realtime.facility_service_window", windows),
    ):
        for key in sorted(set(source) | set(gold)):
            facility, date_key = key
            source_values = source.get(key, {"events": 0, "on_time": 0, "detention": 0})
            target_values = gold.get(key, {"events": 0, "on_time": 0, "detention": 0})
            tolerance = (
                settings.raw_tolerance_pct
                if source_object == contract.reconcile["source_object"]
                else settings.window_tolerance_pct
            )
            outcome = classify(
                source_object,
                source_values["events"],
                target_values["events"],
                tolerance,
            )
            message = (
                f"{outcome['message']}; on_time={source_values['on_time']}/"
                f"{target_values['on_time']}; detention="
                f"{source_values['detention']}/{target_values['detention']}; "
                f"facility={facility}; date_key={date_key}"
            )
            comparisons.append(
                {
                    "source_system": "stream",
                    "source_object": source_object,
                    "target_layer": "gold",
                    "target_object": str(contract.reconcile["target_object"]),
                    "source_row_count": source_values["events"],
                    "target_row_count": target_values["events"],
                    "tolerance_pct": tolerance,
                    "status": outcome["status"],
                    "message": message,
                }
            )
    return comparisons


def reconcile(
    batch_id: int,
    settings: StreamSettings,
    *,
    airflow_dag_id: str = "fleet_daily_load",
    airflow_run_id: str = "-",
) -> int:
    """Log stream-vs-gold comparisons, or an audit note when no stream is enabled."""
    from utils.audit import log_error, log_reconciliation

    contract = load_contract()
    registration = fetchone(REGISTRATION, (contract.stream,))
    if registration is None or registration[0] != "ENABLED":
        log_error(
            component="CONTROL",
            error_message=(
                "stream reconciliation skipped: no ENABLED stream "
                f"(run_id={airflow_run_id})"
            ),
            batch_id=batch_id,
            severity="WARNING",
            error_code="STREAM_SKIPPED",
            airflow_dag_id=airflow_dag_id,
            task_id="reconcile_stream",
        )
        return 0
    comparisons = build_comparisons(contract, settings)
    for comparison in comparisons:
        comparison["message"] = (
            f"{comparison['message']}; airflow_run_id={airflow_run_id}"
        )
    return log_reconciliation(batch_id, comparisons)
