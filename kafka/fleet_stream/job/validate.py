"""Contract validation and dead-letter routing for Spark micro-batches."""

from __future__ import annotations

import json
from typing import Any

from fleet_stream.contract import Contract, validate_event


def validation_reasons(value: str | None, contract: Contract) -> list[str]:
    """Return non-sensitive rejection reasons for one Kafka value."""
    if value is None:
        return ["missing payload"]
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return ["invalid json"]
    if not isinstance(payload, dict):
        return ["payload is not an object"]
    return list(validate_event(payload, contract).reasons)


def payload_schema(contract: Contract) -> Any:
    """Build the Spark schema implied by the stream contract."""
    from pyspark.sql.types import (
        BooleanType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    types = {
        "string": StringType(),
        "timestamp": StringType(),
        "int": IntegerType(),
        "boolean": BooleanType(),
    }
    fields = [
        StructField(column.name, types[column.staged_type], column.nullable)
        for column in contract.columns
    ]
    return StructType(fields)


def validation_udf(contract: Contract) -> Any:
    """Return a Spark UDF that applies the same strict validator as the producer."""
    from pyspark.sql.functions import udf
    from pyspark.sql.types import ArrayType, StringType

    def _reasons(value: str | None) -> list[str]:
        return validation_reasons(value, contract)

    return udf(_reasons, ArrayType(StringType()))


def dlq_value(
    value: str | None,
    reasons: list[str] | tuple[str, ...],
    topic: str,
    partition: int,
    offset: int,
) -> str:
    """Serialise a rejected record with its origin and reason list."""
    return json.dumps(
        {
            "reason": "; ".join(reasons) or "rejected",
            "original": value,
            "topic": topic,
            "partition": partition,
            "offset": offset,
        },
        separators=(",", ":"),
        sort_keys=True,
    )
