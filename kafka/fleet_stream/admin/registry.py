"""Register the supervised stream in control.stream."""

from __future__ import annotations

import logging

from fleet_stream.admin.guard import assert_safe_target
from fleet_stream.contract import CONTRACT_PATH, Contract
from fleet_stream.job.db import execute
from fleet_stream.settings import StreamSettings

log = logging.getLogger(__name__)

REGISTER = """
INSERT INTO control.stream (
    stream_name, topic, dlq_topic, contract_file, target_tables,
    checkpoint_path, owner, status
) VALUES (%s, %s, %s, %s, %s::text[], %s, %s, 'ENABLED')
ON CONFLICT (stream_name) DO UPDATE SET
    topic = EXCLUDED.topic,
    dlq_topic = EXCLUDED.dlq_topic,
    contract_file = EXCLUDED.contract_file,
    target_tables = EXCLUDED.target_tables,
    checkpoint_path = EXCLUDED.checkpoint_path,
    owner = EXCLUDED.owner,
    status = 'ENABLED'
"""

TARGET_TABLES = (
    "realtime.delivery_events_raw",
    "realtime.facility_service_window",
    "realtime.detention_alert",
)


def register_stream(settings: StreamSettings, contract: Contract) -> None:
    """Enable one stream for the current safe environment."""
    assert_safe_target(settings)
    execute(
        REGISTER,
        (
            contract.stream,
            settings.topic(contract.topic),
            settings.topic(contract.dlq_topic),
            str(CONTRACT_PATH.relative_to(CONTRACT_PATH.parents[2])),
            list(TARGET_TABLES),
            str(settings.checkpoint_dir),
            contract.owner,
        ),
    )
    log.info(
        "stream %s ENABLED for topic %s",
        contract.stream,
        settings.topic(contract.topic),
    )
