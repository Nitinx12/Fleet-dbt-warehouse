"""Build the live audit payload embedded in the Streamlit site."""

import json
import math
from collections.abc import Callable

from sqlalchemy.exc import SQLAlchemyError


def _json_safe(value: object) -> object:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def live_payload(
    read: Callable[[str, int], object], configured: Callable[[], bool]
) -> dict:
    """Return audit rows for the embedded page; empty when live status is off."""
    if not configured():
        return {"batches": [], "dbt": [], "dq": []}
    try:
        return {
            "batches": read("audit.v_batch_summary", 20).to_dict("records"),
            "dbt": read("audit.v_dbt_results", 100).to_dict("records"),
            "dq": read("audit.v_data_quality_results", 100).to_dict("records"),
        }
    except (SQLAlchemyError, KeyError, TypeError, ValueError):
        return {"batches": [], "dbt": [], "dq": []}


def inject_payload(html: str, payload: dict) -> str:
    """Replace the placeholder with escaped JSON for the embedded page."""
    data = json.dumps(_json_safe(payload), default=str, allow_nan=False).replace(
        "<", "\\u003c"
    )
    return html.replace("__LIVE_DATA__", data)
