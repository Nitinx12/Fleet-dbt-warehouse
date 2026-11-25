"""Static checks for Kafka runtime wiring."""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]

pytestmark = pytest.mark.unit


def test_broker_advertises_internal_and_host_listeners() -> None:
    """Container and host clients receive listeners they can resolve."""
    source = (ROOT / "kafka" / "config" / "broker.env").read_text(encoding="utf-8")

    assert "INTERNAL://kafka:9092" in source
    assert "EXTERNAL://localhost:9094" in source
    assert "KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL" in source
    assert "KAFKA_LOG_DIRS=/var/lib/kafka/data" in source


def test_compose_uses_external_port_and_reachable_postgres() -> None:
    """Host producers and stream containers use reachable endpoints."""
    source = (ROOT / "kafka" / "compose.yml").read_text(encoding="utf-8")

    assert '"9094:9094"' in source
    assert "PG_HOST: ${STREAM_PG_HOST:-host.docker.internal}" in source
    assert "host.docker.internal:host-gateway" in source
    assert "image: ${STREAM_IMAGE:-fleet-stream:local}" in source
    assert "restart: unless-stopped" in source


def test_stream_image_installs_live_source_clients_and_spark_config() -> None:
    """The stream image runs live replay and loads its Spark defaults."""
    dockerfile = (ROOT / "kafka" / "Dockerfile").read_text(encoding="utf-8")
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")

    assert "--only-group kafka --only-group spark" in dockerfile
    assert "UV_PYTHON=3.12" in dockerfile
    assert "UV_PYTHON_INSTALL_DIR=/opt/uv-python" in dockerfile
    assert "COPY kafka/config/spark-defaults.conf /opt/spark/conf/" in dockerfile
    assert "pymongo" in pyproject
    assert "pymysql" in pyproject


def test_host_stream_scripts_load_environment_file() -> None:
    """Host launchers read .env when dotenv is installed; containers skip it."""
    for name in (
        "stream_admin.py",
        "stream_producer.py",
        "stream_monitor.py",
        "stream_reconcile.py",
        "stream_job.py",
    ):
        source = (ROOT / "scripts" / name).read_text(encoding="utf-8")
        assert 'load_dotenv(ROOT / ".env")' in source, name
        assert "except ImportError" in source, name
