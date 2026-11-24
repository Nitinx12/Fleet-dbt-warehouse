"""Spark Structured Streaming application for the delivery speed layer."""

from __future__ import annotations

import json
import logging
import os
import signal
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from fleet_stream.admin.guard import assert_safe_target
from fleet_stream.contract import Contract, load_contract
from fleet_stream.events import parse_source_timestamp
from fleet_stream.job.db import fetchone
from fleet_stream.job.detention import process_detention
from fleet_stream.job.metrics import HealthMetrics, write_health
from fleet_stream.job.raw_sink import upsert_raw
from fleet_stream.job.validate import payload_schema, validation_udf
from fleet_stream.job.windows import batch_range, process_windows
from fleet_stream.settings import StreamSettings, from_env

log = logging.getLogger(__name__)


class StreamJobError(RuntimeError):
    """Raised when the stream job is not registered or cannot start."""


def read_watermark(path: Path) -> datetime | None:
    """Read the last committed event-time watermark from the checkpoint volume."""
    target = path / "watermark.json"
    if not target.exists():
        return None
    data = json.loads(target.read_text(encoding="utf-8"))
    return datetime.fromisoformat(data["watermark"]).astimezone(UTC)


def write_watermark(path: Path, value: datetime) -> None:
    """Atomically persist a watermark after every sink commits."""
    path.mkdir(parents=True, exist_ok=True)
    target = path / "watermark.json"
    temporary = path / "watermark.json.tmp"
    temporary.write_text(
        json.dumps({"watermark": value.astimezone(UTC).isoformat()}), encoding="utf-8"
    )
    temporary.replace(target)


def kafka_source_options(settings: StreamSettings, topic: str) -> dict[str, str]:
    """Return the Kafka source options for the prefixed topic."""
    return {
        "kafka.bootstrap.servers": settings.bootstrap_servers,
        "subscribe": topic,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "10000",
    }


def build_session(settings: StreamSettings) -> Any:
    """Create the local Spark session used by the supervised job."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("fleet-delivery-stream")
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.checkpointLocation", str(settings.checkpoint_dir))
        .getOrCreate()
    )


def assert_registered(settings: StreamSettings, contract: Contract) -> None:
    """Refuse to run until the stream is explicitly enabled in control."""
    row = fetchone(
        "SELECT status FROM control.stream WHERE stream_name = %s",
        (contract.stream,),
    )
    if row is None or row[0] != "ENABLED":
        raise StreamJobError(f"stream {contract.stream} is not ENABLED")


def _write_dlq(invalid_df: Any, settings: StreamSettings, contract: Contract) -> None:
    from pyspark.sql import functions as F

    value = F.to_json(
        F.struct(
            F.concat_ws("; ", F.col("_validation")).alias("reason"),
            F.col("value").cast("string").alias("original"),
            F.col("topic").alias("topic"),
            F.col("partition").alias("partition"),
            F.col("offset").alias("offset"),
        )
    )
    (
        invalid_df.select(F.col("key").alias("key"), value.alias("value"))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("topic", settings.topic(contract.dlq_topic))
        .save()
    )


def process_batch(
    batch_df: Any,
    batch_id: int,
    spark: Any,
    settings: StreamSettings,
    contract: Contract,
    state: dict[str, datetime | None],
) -> None:
    """Process one micro-batch: validate, sink raw, window, alert and record health."""
    from pyspark.sql import functions as F

    batch_df.persist()
    try:
        invalid_df = batch_df.filter(
            F.col("_validation").isNotNull() & (F.size(F.col("_validation")) > 0)
        )
        rejected = invalid_df.count()
        if rejected:
            _write_dlq(invalid_df, settings, contract)

        payload = F.from_json(F.col("value").cast("string"), payload_schema(contract))
        valid = (
            batch_df.filter(F.size(F.col("_validation")) == 0)
            .withColumn("payload", payload)
            .select(
                F.col("payload.*"),
                F.to_timestamp(F.col("payload.event_time")).alias("_event_time"),
                F.col("topic"),
                F.col("partition"),
                F.col("offset"),
            )
            .filter(F.col("_event_time").isNotNull())
        )
        rows = [row.asDict() for row in valid.collect()]
        event_times = [row["_event_time"] for row in rows]
        if not event_times:
            write_health(
                contract.stream,
                settings.topic(contract.topic),
                "JOB",
                HealthMetrics(rejected=rejected, message=f"batch {batch_id}"),
            )
            return

        batch_max = max(event_times)
        previous = state["watermark"]
        effective = max(previous, batch_max) if previous else batch_max
        cutoff = effective - timedelta(minutes=settings.watermark_minutes)
        late = sum(1 for value in event_times if value < cutoff)

        upsert_raw(rows)
        start, end = batch_range(batch_max, settings)
        window_rows = process_windows(spark, settings, start, end, cutoff)
        alert_rows = process_detention(spark, settings, start, end, cutoff)

        loaded_at = max(parse_source_timestamp(row["loaded_at"]) for row in rows)
        age = max((datetime.now(UTC) - loaded_at).total_seconds(), 0.0)
        write_health(
            contract.stream,
            settings.topic(contract.topic),
            "JOB",
            HealthMetrics(
                rows_raw=len(rows),
                rows_window=window_rows,
                rows_alert=alert_rows,
                rejected=rejected,
                late_dropped=late,
                newest_event_age_seconds=age,
                message=f"batch {batch_id}",
            ),
        )
        write_watermark(settings.checkpoint_dir, effective)
        state["watermark"] = effective
    finally:
        batch_df.unpersist()


def run() -> None:
    """Start the supervised streaming query and stop cleanly on signals."""
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    settings = from_env()
    assert_safe_target(settings)
    contract = load_contract()
    assert_registered(settings, contract)
    spark = build_session(settings)
    state: dict[str, datetime | None] = {
        "watermark": read_watermark(settings.checkpoint_dir)
    }

    raw = (
        spark.readStream.format("kafka")
        .options(**kafka_source_options(settings, settings.topic(contract.topic)))
        .load()
        .withColumn("_validation", validation_udf(contract)(value_column()))
    )
    query = (
        raw.writeStream.foreachBatch(
            lambda batch_df, batch_id: process_batch(
                batch_df, batch_id, spark, settings, contract, state
            )
        )
        .option("checkpointLocation", str(settings.checkpoint_dir))
        .trigger(processingTime=f"{settings.trigger_seconds} seconds")
        .start()
    )

    stopping = False

    def request_stop(signum: int, frame: Any) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        while not stopping:
            query.awaitTermination(5)
    finally:
        query.stop()
        spark.stop()
    log.info("stream job stopped")


def value_column() -> Any:
    """Return the Kafka value column without importing Spark at module import."""
    from pyspark.sql import functions as F

    return F.col("value").cast("string")
