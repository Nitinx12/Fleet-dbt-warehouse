"""Build the live audit payload embedded in the Streamlit site."""

import json
import math
from collections.abc import Callable
from pathlib import Path

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


def audit_error(exc: Exception) -> str:
    """Return a safe, actionable cause for an audit read failure."""
    if isinstance(exc, KeyError):
        return "Missing [postgres] secret key: check host, port, dbname, user and password."
    if isinstance(exc, (TypeError, ValueError)):
        return "Invalid [postgres] value: port must be numeric and secrets must be strings."
    if not isinstance(exc, SQLAlchemyError):
        return "Unexpected error; check Streamlit Cloud logs."
    detail = str(getattr(exc, "orig", exc)).lower()
    if (
        "name or service not known" in detail
        or "could not translate host name" in detail
    ):
        return "Network: the host in [postgres] cannot be resolved; check the exact hostname."
    if any(
        s in detail for s in ("connection refused", "timed out", "could not connect")
    ):
        return "Network: check the port and the DB allowlist/firewall; localhost will not work from Cloud."
    if (
        "password authentication" in detail
        or "no pg_hba.conf entry" in detail
        or ("does not exist" in detail and ("role " in detail or "user " in detail))
    ):
        return "Credentials: check the user, password and dbname in [postgres]."
    if (
        "permission denied" in detail
        or "must be owner" in detail
        or "insufficient_privilege" in detail
    ):
        return (
            "Grants: give the user USAGE ON SCHEMA audit and SELECT on the three views."
        )
    if "does not exist" in detail:
        return "Schema: the database or audit views are missing; check dbname."
    return "Database error; check Streamlit Cloud logs (details hidden here)."


def inject_payload(html: str, payload: dict) -> str:
    """Replace the placeholder with escaped JSON for the embedded page."""
    data = json.dumps(_json_safe(payload), default=str, allow_nan=False).replace(
        "<", "\\u003c"
    )
    return html.replace("__LIVE_DATA__", data)


def inject_facts(html: str, facts: dict | None = None) -> str:
    """Replace the site-facts placeholder with the committed fact JSON."""
    if facts is None:
        path = Path(__file__).resolve().parents[1] / "site" / "facts.json"
        facts = json.loads(path.read_text(encoding="utf-8"))
    data = json.dumps(_json_safe(facts), default=str, allow_nan=False).replace(
        "<", "\\u003c"
    )
    return html.replace("__SITE_FACTS__", data)
