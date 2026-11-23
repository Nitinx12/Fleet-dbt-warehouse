"""Data quality checks for *_consistent flags."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from reports.helpers.db import query_df, write_findings
from reports.helpers.plotting import pct, save_fig, set_theme

SCRIPT = "07_data_quality"


def _quality(table: str, column: str) -> dict[str, object]:
    row = query_df(
        f"""
        select count(*) as rows,
               sum(case when {column} is true then 1 else 0 end) as pass_rows,
               sum(case when {column} is false then 1 else 0 end) as fail_rows,
               sum(case when {column} is null then 1 else 0 end) as unknown_rows
        from gold.{table}
        """
    ).iloc[0]
    rows = int(row["rows"])
    fail_rows = int(row["fail_rows"])
    unknown_rows = int(row["unknown_rows"])
    return {
        "table": table,
        "check": column,
        "rows": rows,
        "pass_rows": int(row["pass_rows"]),
        "fail_rows": fail_rows,
        "unknown_rows": unknown_rows,
        "fail_share": fail_rows / rows if rows else 0.0,
        "unknown_share": unknown_rows / rows if rows else 0.0,
    }


def main() -> None:
    """Build data quality chart and findings."""
    set_theme()
    checks = [
        _quality("fct_trips", "is_mpg_consistent"),
        _quality("fct_fuel_purchases", "is_total_cost_consistent"),
        _quality("fct_maintenance", "is_total_cost_consistent"),
        _quality("dim_drivers", "is_employment_consistent"),
    ]
    df = pd.DataFrame(checks)
    worst = df.sort_values(["fail_share", "unknown_share"], ascending=False).iloc[0]

    plot_df = df.melt(
        id_vars=["table", "check", "rows"],
        value_vars=["pass_rows", "fail_rows", "unknown_rows"],
        var_name="status",
        value_name="count",
    )
    plot_df["share"] = plot_df["count"] / plot_df["rows"]
    status_labels = {
        "pass_rows": "Pass",
        "fail_rows": "Fail",
        "unknown_rows": "Unknown",
    }
    plot_df["status"] = plot_df["status"].map(status_labels)

    fig, ax = plt.subplots()
    sns.barplot(
        data=plot_df,
        y="check",
        x="share",
        hue="status",
        ax=ax,
        palette={"Pass": "#55A868", "Fail": "#C44E52", "Unknown": "#8172B3"},
    )
    ax.set_title(
        f"{worst['check']} has the highest quality risk ({pct(float(worst['fail_share'] + worst['unknown_share']))} not passing)"
    )
    ax.set_xlabel("Share of rows (%)")
    ax.set_ylabel("Consistency check")
    ax.xaxis.set_major_formatter(lambda x, _: f"{x * 100:.0f}%")
    ax.legend(title="Status", loc="lower right")
    save_fig(fig, SCRIPT, "consistency_checks")

    findings = {
        "script": SCRIPT,
        "checks": checks,
        "worst_check": {
            "check": str(worst["check"]),
            "table": str(worst["table"]),
            "fail_share": float(worst["fail_share"]),
            "unknown_share": float(worst["unknown_share"]),
        },
        "consistent_checks": "This script reports *_consistent flags; downstream scripts filter to passing rows where applicable.",
    }
    write_findings(SCRIPT, findings)
    for check in checks:
        print(
            f"{check['table']}.{check['check']} fail={pct(float(check['fail_share']))} unknown={pct(float(check['unknown_share']))}"
        )


if __name__ == "__main__":
    main()
