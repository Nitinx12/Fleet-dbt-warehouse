"""Guard stream operations against unsafe environment and database targets."""

from __future__ import annotations

import os

from fleet_stream.settings import StreamSettings


class StreamGuardError(RuntimeError):
    """Raised when a stream operation targets an unsafe environment."""


def assert_safe_target(settings: StreamSettings) -> None:
    """Refuse dev/CI writes to unapproved databases or topics."""
    database = os.getenv("PG_DBNAME", "")
    prefix = settings.topic_prefix
    if settings.env in {"dev", "ci"}:
        allowed = ("fleet_dev", "fleet_it")
        if settings.env == "dev":
            allowed += ("fleet_warehouse",)
        if not database.startswith(allowed):
            raise StreamGuardError(
                f"ENV={settings.env} may only write {', '.join(allowed)} databases"
            )
        expected = f"{settings.env}."
        if not prefix.startswith(expected):
            raise StreamGuardError(
                f"ENV={settings.env} requires KAFKA_TOPIC_PREFIX={expected}"
            )
        return
    if os.getenv("STREAM_ALLOW_PROD") != "YES" or os.getenv("CONFIRM") != "yes":
        raise StreamGuardError(
            "production streaming requires STREAM_ALLOW_PROD=YES and CONFIRM=yes"
        )
    if prefix.startswith(("dev.", "ci.")):
        raise StreamGuardError(
            "production streaming cannot use a dev or ci topic prefix"
        )
