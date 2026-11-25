"""Start the Spark Structured Streaming job for delivery events."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
try:
    from dotenv import load_dotenv
except ImportError:  # containers receive settings from Compose env_file
    pass
else:
    load_dotenv(ROOT / ".env")
for path in (ROOT, ROOT / "kafka"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from fleet_stream.job.app import run


def main() -> None:
    """Run the supervised stream job until it is stopped."""
    run()


if __name__ == "__main__":
    main()
