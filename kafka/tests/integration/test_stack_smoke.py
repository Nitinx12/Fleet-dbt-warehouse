"""Opt-in integration checks for the local Kafka stack."""

from __future__ import annotations

import os

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_STREAM_INTEGRATION") != "1",
        reason="set RUN_STREAM_INTEGRATION=1 with the stream stack running",
    ),
]


def test_topics_and_registration_are_ready() -> None:
    """The replay topic, DLQ and control registration must exist together."""
    from pathlib import Path

    import psycopg2
    from confluent_kafka.admin import AdminClient
    from dotenv import load_dotenv
    from fleet_stream.contract import load_contract
    from fleet_stream.settings import from_env

    root = Path(__file__).resolve().parents[3]
    load_dotenv(root / ".env")
    settings = from_env()
    contract = load_contract()
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    topics = admin.list_topics(timeout=10).topics

    assert settings.topic(contract.topic) in topics
    assert settings.topic(contract.dlq_topic) in topics

    connection = psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DBNAME", "fleet_dev"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "postgres"),
    )
    with connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT status FROM control.stream WHERE stream_name = %s",
            (contract.stream,),
        )
        row = cursor.fetchone()

    assert row is not None and row[0] == "ENABLED"
