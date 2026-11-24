"""Load and validate the delivery stream contract without Kafka or Spark imports."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = ROOT / "contracts" / "streams" / "delivery_events.v1.yml"
TOPICS_PATH = ROOT / "kafka" / "config" / "topics.yml"
DDL_PATH = ROOT / "sql" / "scripts" / "08_streaming.sql"
DBT_SOURCES_PATH = ROOT / "dbt" / "models" / "bronze" / "sources.yml"
REALTIME_TABLES = frozenset(
    {"delivery_events_raw", "facility_service_window", "detention_alert"}
)
FORBIDDEN_COLUMNS = frozenset(
    {
        "license_number",
        "date_of_birth",
        "first_name",
        "last_name",
        "fuel_card_number",
        "description",
    }
)


class ContractError(ValueError):
    """Raised when a contract or its alignment is invalid."""


@dataclass(frozen=True)
class Column:
    """One typed field in the stream value."""

    name: str
    staged_type: str
    nullable: bool
    accepted_values: tuple[str, ...]
    min_value: float | None

    @property
    def python_type(self) -> type:
        """Return the expected Python type for a JSON value."""
        if self.staged_type == "string":
            return str
        if self.staged_type == "timestamp":
            return str
        if self.staged_type == "int":
            return int
        if self.staged_type == "boolean":
            return bool
        raise ContractError(f"unsupported staged_type: {self.staged_type}")


@dataclass(frozen=True)
class Contract:
    """The complete delivery event stream contract."""

    contract_version: str
    stream: str
    topic: str
    dlq_topic: str
    key: tuple[str, ...]
    dedupe_key: tuple[str, ...]
    event_time_field: str
    allowed_lateness_minutes: int
    value_format: str
    owner: str
    columns: tuple[Column, ...]
    reconcile: Mapping[str, Any]

    @property
    def column_names(self) -> frozenset[str]:
        """Return the exact allowed value keys."""
        return frozenset(column.name for column in self.columns)

    def column(self, name: str) -> Column:
        """Return one column by name."""
        for column in self.columns:
            if column.name == name:
                return column
        raise ContractError(f"unknown column: {name}")


@dataclass(frozen=True)
class ValidationResult:
    """Validation outcome and human-readable reasons."""

    ok: bool
    reasons: tuple[str, ...]


def _required_text(mapping: Mapping[str, Any], key: str, where: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise ContractError(f"{where}.{key} is required")
    return value


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp must include a UTC offset")
    return parsed


def _column(raw: Mapping[str, Any], where: str) -> Column:
    name = _required_text(raw, "name", where)
    staged_type = _required_text(raw, "staged_type", where)
    nullable = bool(raw.get("nullable", False))
    accepted = tuple(str(value) for value in raw.get("accepted_values", ()))
    minimum = raw.get("min")
    min_value = float(minimum) if minimum is not None else None
    column = Column(name, staged_type, nullable, accepted, min_value)
    _ = column.python_type
    return column


def load_contract(path: Path = CONTRACT_PATH) -> Contract:
    """Load and structurally validate a stream contract."""
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ContractError(f"{path.name}: root must be a mapping")

    columns_raw = data.get("columns")
    if not isinstance(columns_raw, list) or not columns_raw:
        raise ContractError(f"{path.name}: columns must be a non-empty list")
    columns = tuple(
        _column(item, f"{path.name}.columns[{index}]")
        for index, item in enumerate(columns_raw)
    )
    names = [column.name for column in columns]
    if len(names) != len(set(names)):
        raise ContractError(f"{path.name}: duplicate column names")

    contract = Contract(
        contract_version=_required_text(data, "contract_version", path.name),
        stream=_required_text(data, "stream", path.name),
        topic=_required_text(data, "topic", path.name),
        dlq_topic=_required_text(data, "dlq_topic", path.name),
        key=tuple(str(value) for value in data.get("key", ())),
        dedupe_key=tuple(str(value) for value in data.get("dedupe_key", ())),
        event_time_field=_required_text(data, "event_time_field", path.name),
        allowed_lateness_minutes=int(data.get("allowed_lateness_minutes", 0)),
        value_format=_required_text(data, "value_format", path.name),
        owner=_required_text(data, "owner", path.name),
        columns=columns,
        reconcile=data.get("reconcile", {}),
    )
    if contract.value_format != "json":
        raise ContractError(f"{path.name}: value_format must be json")
    if contract.event_time_field not in contract.column_names:
        raise ContractError(f"{path.name}: event_time_field is not a column")
    if not set(contract.key).issubset(contract.column_names):
        raise ContractError(f"{path.name}: key references unknown columns")
    if not set(contract.dedupe_key).issubset(contract.column_names):
        raise ContractError(f"{path.name}: dedupe_key references unknown columns")
    if FORBIDDEN_COLUMNS & contract.column_names:
        raise ContractError(f"{path.name}: contract contains a forbidden column")
    return contract


def validate_event(
    event: Mapping[str, Any], contract: Contract | None = None
) -> ValidationResult:
    """Validate one decoded event against the contract's strict schema."""
    contract = contract or load_contract()
    reasons: list[str] = []
    actual = set(event)
    expected = set(contract.column_names)
    for name in sorted(expected - actual):
        reasons.append(f"missing field: {name}")
    for name in sorted(actual - expected):
        reasons.append(f"unknown field: {name}")

    for column in contract.columns:
        if column.name not in event:
            continue
        value = event[column.name]
        if value is None:
            if not column.nullable:
                reasons.append(f"null value: {column.name}")
            continue
        if type(value) is not column.python_type:
            reasons.append(f"wrong type: {column.name} expected {column.staged_type}")
            continue
        if column.staged_type == "timestamp":
            try:
                _parse_timestamp(value)
            except ValueError:
                reasons.append(f"invalid timestamp: {column.name}")
        if column.accepted_values and str(value) not in column.accepted_values:
            reasons.append(f"value outside accepted set: {column.name}")
        if column.min_value is not None and value < column.min_value:
            reasons.append(f"value below minimum: {column.name}")

    return ValidationResult(not reasons, tuple(reasons))


def load_topics(path: Path = TOPICS_PATH) -> list[dict[str, Any]]:
    """Load the topic infrastructure declaration."""
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    topics = data.get("topics") if isinstance(data, dict) else None
    if not isinstance(topics, list) or not topics:
        raise ContractError(f"{path.name}: topics must be a non-empty list")
    return topics


def alignment_issues(
    contract: Contract | None = None,
    *,
    topics_path: Path = TOPICS_PATH,
    ddl_path: Path = DDL_PATH,
    dbt_sources_path: Path = DBT_SOURCES_PATH,
) -> list[str]:
    """Return drift messages between the stream contract, topics, DDL and dbt sources."""
    contract = contract or load_contract()
    issues: list[str] = []
    topics = load_topics(topics_path)
    by_name = {str(topic.get("name")): topic for topic in topics}
    for name in (contract.topic, contract.dlq_topic):
        topic = by_name.get(name)
        if topic is None:
            issues.append(f"topics.yml: missing {name}")
            continue
        expected = {
            "partitions": 3,
            "replication_factor": 1,
            "retention_ms": 604800000,
            "cleanup_policy": "delete",
        }
        for key, value in expected.items():
            if topic.get(key) != value:
                issues.append(f"topics.yml: {name}.{key} != {value}")
    if by_name.get(contract.topic, {}).get("dlq") is not False:
        issues.append(f"topics.yml: {contract.topic}.dlq must be false")
    if by_name.get(contract.dlq_topic, {}).get("dlq") is not True:
        issues.append(f"topics.yml: {contract.dlq_topic}.dlq must be true")

    ddl = ddl_path.read_text(encoding="utf-8")
    ddl_tables = set(re.findall(r"CREATE TABLE IF NOT EXISTS realtime\.([a-z_]+)", ddl))
    for name in sorted(REALTIME_TABLES - ddl_tables):
        issues.append(f"08_streaming.sql: missing realtime.{name}")
    for name in sorted(ddl_tables - REALTIME_TABLES):
        issues.append(f"08_streaming.sql: unexpected realtime.{name}")

    dbt_sources = dbt_sources_path.read_text(encoding="utf-8")
    if re.search(r"(^|\s)(name|schema):\s*realtime\s*$", dbt_sources, re.MULTILINE):
        issues.append("dbt bronze sources must not declare realtime")
    return issues
