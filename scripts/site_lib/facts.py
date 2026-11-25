"""Load and format site facts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Fact:
    """One derived site fact with its provenance."""

    value: Any
    source: str
    how: str


def load_facts(path: Path) -> dict[str, Fact]:
    """Read the generated facts JSON into typed records."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    facts: dict[str, Fact] = {}
    for key, item in raw.items():
        if not isinstance(item, dict) or not {"value", "source", "how"} <= set(item):
            raise ValueError(f"{path}:{key}: expected value, source and how")
        facts[key] = Fact(item["value"], item["source"], item["how"])
    return facts


def fact_value(facts: dict[str, Fact], key: str) -> Any:
    """Return one fact value or raise a named error."""
    if key not in facts:
        raise KeyError(f"unknown fact: {key}")
    return facts[key].value


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _group(value: int | Decimal) -> str:
    sign = "-" if value < 0 else ""
    digits = str(abs(int(value)))
    parts = []
    while digits:
        parts.append(digits[-3:])
        digits = digits[:-3]
    return sign + ",".join(reversed(parts))


def _quantize(value: Decimal, places: int) -> Decimal:
    step = Decimal(1).scaleb(-places)
    return value.quantize(step, rounding=ROUND_HALF_UP)


def format_fact(key: str, value: Any, rule: str) -> str:
    """Format a fact with an explicit, deterministic rounding rule."""
    if rule == "string":
        return str(value)
    if rule == "bool":
        return "true" if value else "false"
    if rule == "list":
        return ", ".join(str(item) for item in value)
    number = _decimal(value)
    if rule == "count":
        return _group(number)
    if rule == "decimal1":
        return f"{_quantize(number, 1):.1f}"
    if rule == "decimal2":
        return f"{_quantize(number, 2):.2f}"
    if rule == "percent1":
        return f"{_quantize(number * 100, 1):.1f}%"
    if rule == "percent2":
        return f"{_quantize(number * 100, 2):.2f}%"
    if rule == "usd_int":
        return f"${_group(number)}"
    if rule == "usd_k0":
        return f"${_group(_quantize(number / 1000, 0))}K"
    if rule == "usd_m1":
        return f"${_quantize(number / Decimal(1_000_000), 1):.1f}M"
    if rule == "usd_m2":
        return f"${_quantize(number / Decimal(1_000_000), 2):.2f}M"
    raise ValueError(f"{key}: unknown format rule {rule!r}")
