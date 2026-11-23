"""Shared plotting theme and formatting helpers."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

ROOT = Path(__file__).resolve().parents[2]
IMG_DIR = ROOT / "reports" / "img"


def set_theme() -> None:
    """Apply the shared report theme."""
    sns.set_theme(style="whitegrid", context="talk", palette="colorblind")
    plt.rcParams.update(
        {
            "figure.figsize": (11, 6),
            "axes.titlesize": 14,
            "axes.titleweight": "bold",
            "axes.labelsize": 12,
        }
    )


def save_fig(fig: plt.Figure, script_name: str, chart_name: str) -> Path:
    """Save a figure to reports/img and close it."""
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    path = IMG_DIR / f"{script_name}_{chart_name}.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def pct(value: float, decimals: int = 1) -> str:
    """Format a ratio as a percent string."""
    return f"{value * 100:.{decimals}f}%"


def currency(value: float, decimals: int = 0) -> str:
    """Format a number as USD currency."""
    return f"${value:,.{decimals}f}"


def pct_axis(ax: plt.Axes, decimals: int = 0) -> None:
    """Format y-axis ticks as percentages."""
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=decimals))
