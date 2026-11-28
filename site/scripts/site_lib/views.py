"""Parse audit view columns from SQL without a database."""

from __future__ import annotations

import re
from pathlib import Path


def parse_view_columns(sql: str, view: str) -> list[str]:
    """Return the output column names of one audit view."""
    match = re.search(
        rf"CREATE\s+OR\s+REPLACE\s+VIEW\s+audit\.{re.escape(view)}\s+AS\s*(.*?);",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        raise ValueError(f"view audit.{view} not found")
    select = match.group(1)
    depth = 0
    end = None
    for found in re.finditer(r"\bFROM\b", select, re.IGNORECASE):
        depth = select.count("(", 0, found.start()) - select.count(
            ")", 0, found.start()
        )
        if depth == 0:
            end = found
            break
    if end:
        select = select[: end.start()]
    after_select = re.split(r"\bSELECT\b", select, maxsplit=1, flags=re.IGNORECASE)
    body = after_select[-1]
    columns: list[str] = []
    depth = 0
    item: list[str] = []
    for char in body:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            columns.append("".join(item))
            item = []
        else:
            item.append(char)
    columns.append("".join(item))
    names: list[str] = []
    for column in columns:
        alias = re.search(r"\bAS\s+([A-Za-z_]\w*)", column, re.IGNORECASE)
        name = alias.group(1) if alias else column.strip().split(".")[-1]
        if name and name not in {"AS", "SELECT"}:
            names.append(name)
    return names


def load_view_columns(path: Path, view: str) -> list[str]:
    """Read and parse one view definition from a SQL bootstrap file."""
    return parse_view_columns(path.read_text(encoding="utf-8"), view)
