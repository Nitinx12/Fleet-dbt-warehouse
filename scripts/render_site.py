"""Render index.html as a standalone build/site.html with no live DB."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "index.html"
FACTS = ROOT / "site" / "facts.json"
OUTPUT = ROOT / "build" / "site.html"


def render() -> str:
    """Replace both placeholders and make relative links work from build/."""
    html = SOURCE.read_text(encoding="utf-8")
    facts = json.loads(FACTS.read_text(encoding="utf-8"))
    data = json.dumps(facts, allow_nan=False).replace("<", "\\u003c")
    html = html.replace("__SITE_FACTS__", data).replace("__LIVE_DATA__", "{}")
    for prefix in ("assets", "reports"):
        html = html.replace(f'"{prefix}/', f'"../{prefix}/')
        html = html.replace(f"'{prefix}/", f"'../{prefix}/")
    for placeholder in ("__SITE_FACTS__", "__LIVE_DATA__"):
        if placeholder in html:
            raise ValueError(f"unresolved placeholder: {placeholder}")
    return html


def main() -> int:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(render(), encoding="utf-8")
    print(f"wrote {OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
