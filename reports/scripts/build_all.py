"""Run all report scripts in order."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SCRIPTS = [
    "reports.scripts.01_revenue_concentration",
    "reports.scripts.02_delivery_service",
    "reports.scripts.03_fuel_efficiency",
    "reports.scripts.04_fleet_maintenance",
    "reports.scripts.05_safety",
    "reports.scripts.06_seasonality",
    "reports.scripts.07_data_quality",
    "reports.scripts.08_streaming",
]

ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    """Run report scripts sequentially and fail loudly on error."""
    for module in SCRIPTS:
        cmd = [sys.executable, "-m", module]
        print(f"==> {' '.join(cmd)}")
        proc = subprocess.run(cmd, cwd=ROOT, check=False)
        if proc.returncode != 0:
            raise SystemExit(f"report build failed: {module} exited {proc.returncode}")
    print("report build complete")


if __name__ == "__main__":
    main()
