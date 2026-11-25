"""Extract numeric tokens and their context from index.html."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

_NUMBER_RE = re.compile(r"\$?\d[\d,]*(?:\.\d+)?(?:[KMB%]|e[+-]?\d+)?", re.IGNORECASE)
_HEX_RE = re.compile(r"#[0-9a-fA-F]{3,8}\b")


@dataclass(frozen=True)
class NumberToken:
    """A numeric literal with its source location and binding metadata."""

    token: str
    line: int
    column: int
    kind: str
    context: str
    binding: str | None = None
    illustrative: bool = False


@dataclass
class _Parser(HTMLParser):
    tokens: list[NumberToken] = field(default_factory=list)
    scripts: list[tuple[int, str, str, str]] = field(default_factory=list)
    stack: list[tuple[str, dict[str, str]]] = field(default_factory=list)
    style_depth: int = 0
    script: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        super().__init__()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_map = {name: value or "" for name, value in attrs}
        self.stack.append((tag, attrs_map))
        if tag == "style":
            self.style_depth += 1
        if tag == "script":
            self.script = {
                "line": self.getpos()[0],
                "id": attrs_map.get("id", ""),
                "type": attrs_map.get("type", ""),
                "text": [],
            }
        if tag not in {"style", "script"}:
            for name, value in attrs_map.items():
                self._add_numbers(
                    value,
                    self.getpos()[0],
                    self.getpos()[1],
                    kind="attr",
                    context=f"<{tag} {name}={value!r}>",
                    illustrative="data-illustrative" in attrs_map,
                )

    def handle_endtag(self, tag: str) -> None:
        if tag == "style" and self.style_depth:
            self.style_depth -= 1
        if tag == "script" and self.script is not None:
            text = "".join(self.script["text"])
            self.scripts.append(
                (
                    self.script["line"],
                    self.script["id"],
                    self.script["type"],
                    text,
                )
            )
            self.script = None
        if self.stack:
            self.stack.pop()

    def handle_data(self, data: str) -> None:
        if self.script is not None:
            self.script["text"].append(data)
            return
        tag, attrs = self.stack[-1] if self.stack else ("", {})
        binding = attrs.get("data-fact")
        illustrative = "data-illustrative" in attrs
        kind = "css" if self.style_depth or tag == "style" else "text"
        if binding:
            self.tokens.append(
                NumberToken(
                    token=data.strip(),
                    line=self.getpos()[0],
                    column=self.getpos()[1],
                    kind=kind,
                    context=data.strip()[:160],
                    binding=binding,
                    illustrative=illustrative,
                )
            )
            return
        self._add_numbers(
            data,
            self.getpos()[0],
            self.getpos()[1],
            kind=kind,
            context=data.strip()[:160],
            binding=binding,
            illustrative=illustrative,
        )

    def _add_numbers(
        self,
        text: str,
        line: int,
        column: int,
        *,
        kind: str,
        context: str,
        binding: str | None = None,
        illustrative: bool = False,
    ) -> None:
        safe = _HEX_RE.sub(lambda match: " " * len(match.group()), text)
        for match in _NUMBER_RE.finditer(safe):
            before = safe[: match.start()]
            self.tokens.append(
                NumberToken(
                    token=match.group(),
                    line=line + before.count("\n"),
                    column=column + len(before.split("\n")[-1]),
                    kind=kind,
                    context=context,
                    binding=binding,
                    illustrative=illustrative,
                )
            )


def _json_tokens(start_line: int, script_id: str, text: str) -> list[NumberToken]:
    try:
        payload = json.loads(text)
    except (TypeError, ValueError):
        return []
    tokens: list[NumberToken] = []

    def walk(value: Any, path: str) -> None:
        if isinstance(value, bool) or value is None:
            return
        if isinstance(value, (int, float)):
            needle = f'"{path.split(".")[-1]}"'
            offset = text.find(needle)
            line = start_line + (text[: offset + 1].count("\n") if offset >= 0 else 0)
            tokens.append(
                NumberToken(
                    token=str(value),
                    line=line,
                    column=1,
                    kind="json",
                    context=path,
                    binding=path,
                )
            )
        elif isinstance(value, dict):
            for key, item in value.items():
                walk(item, f"{path}.{key}" if path else str(key))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                walk(item, f"{path}.{index}")

    walk(payload, "")
    return tokens


def _js_tokens(start_line: int, script_id: str, text: str) -> list[NumberToken]:
    tokens: list[NumberToken] = []
    for offset, line in enumerate(text.splitlines()):
        context = line.strip()
        illustrative = "data-illustrative" in line
        safe = _HEX_RE.sub(lambda match: " " * len(match.group()), line)
        for match in _NUMBER_RE.finditer(safe):
            tokens.append(
                NumberToken(
                    token=match.group(),
                    line=start_line + offset + 1,
                    column=match.start() + 1,
                    kind="js",
                    context=context[:160],
                    illustrative=illustrative,
                )
            )
    return tokens


def extract_tokens(path: Path) -> list[NumberToken]:
    """Return every numeric token in an HTML file, in document order."""
    parser = _Parser()
    parser.feed(path.read_text(encoding="utf-8"))
    parser.close()
    tokens = list(parser.tokens)
    for start_line, script_id, script_type, text in parser.scripts:
        if script_type == "application/json":
            tokens.extend(_json_tokens(start_line, script_id, text))
        else:
            tokens.extend(_js_tokens(start_line, script_id, text))
    return sorted(tokens, key=lambda item: (item.line, item.column, item.kind))


def extract_script(path: Path, script_id: str) -> str:
    """Return the contents of one script tag by id."""
    parser = _Parser()
    parser.feed(path.read_text(encoding="utf-8"))
    parser.close()
    for _, found_id, _, text in parser.scripts:
        if found_id == script_id:
            return text
    return ""
