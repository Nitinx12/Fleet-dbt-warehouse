"""Revenue and load seasonality by month and weekday."""

from __future__ import annotations

import matplotlib.pyplot as plt
import seaborn as sns

from reports.helpers.db import query_df, write_findings
from reports.helpers.plotting import currency, save_fig, set_theme

SCRIPT = "06_seasonality"


def main() -> None:
    """Build seasonality charts and findings."""
    set_theme()
    month = query_df(
        """
        select d.month_number,
               d.month_name,
               count(*) as loads,
               sum(f.revenue) as revenue
        from gold.fct_loads f
        join gold.dim_date d on d.date_key = f.load_date_key
        where d.date_key <> -1
        group by d.month_number, d.month_name
        order by d.month_number
        """
    )
    weekday = query_df(
        """
        select d.day_of_week_number,
               d.day_name,
               count(*) as loads,
               sum(f.revenue) as revenue
        from gold.fct_loads f
        join gold.dim_date d on d.date_key = f.load_date_key
        where d.date_key <> -1
        group by d.day_of_week_number, d.day_name
        order by d.day_of_week_number
        """
    )
    if month.empty or weekday.empty:
        raise SystemExit("06_seasonality: no rows returned")

    month["revenue"] = month["revenue"].astype(float)
    weekday["revenue"] = weekday["revenue"].astype(float)
    peak_month = month.loc[month["revenue"].idxmax()]
    low_month = month.loc[month["revenue"].idxmin()]
    peak_weekday = weekday.loc[weekday["revenue"].idxmax()]
    low_weekday = weekday.loc[weekday["revenue"].idxmin()]

    fig, ax = plt.subplots()
    sns.lineplot(
        data=month, x="month_name", y="revenue", marker="o", ax=ax, color="#4C72B0"
    )
    ax.set_title(
        f"{peak_month['month_name']} is the peak revenue month; {low_month['month_name']} is the trough"
    )
    ax.set_xlabel("Month")
    ax.set_ylabel("Revenue (USD)")
    ax.yaxis.set_major_formatter(lambda x, _: f"${x / 1_000_000:.1f}M")
    ax.tick_params(axis="x", rotation=30)
    save_fig(fig, SCRIPT, "revenue_by_month")

    fig, ax = plt.subplots()
    sns.barplot(data=weekday, x="day_name", y="loads", ax=ax, color="#55A868")
    ax.set_title(
        f"{peak_weekday['day_name']} carries the highest load volume in the week"
    )
    ax.set_xlabel("Weekday")
    ax.set_ylabel("Loads")
    ax.tick_params(axis="x", rotation=20)
    save_fig(fig, SCRIPT, "loads_by_weekday")

    findings = {
        "script": SCRIPT,
        "peak_month": {
            "month_name": peak_month["month_name"],
            "revenue": float(peak_month["revenue"]),
            "loads": int(peak_month["loads"]),
        },
        "low_month": {
            "month_name": low_month["month_name"],
            "revenue": float(low_month["revenue"]),
            "loads": int(low_month["loads"]),
        },
        "peak_weekday": {
            "day_name": peak_weekday["day_name"],
            "revenue": float(peak_weekday["revenue"]),
            "loads": int(peak_weekday["loads"]),
        },
        "low_weekday": {
            "day_name": low_weekday["day_name"],
            "revenue": float(low_weekday["revenue"]),
            "loads": int(low_weekday["loads"]),
        },
        "month_rows": month.to_dict(orient="records"),
        "weekday_rows": weekday.to_dict(orient="records"),
        "consistent_checks": "N/A - fct_loads has no *_consistent columns; all rows included",
    }
    write_findings(SCRIPT, findings)
    print(
        f"peak_month={peak_month['month_name']} revenue={currency(float(peak_month['revenue']))}"
    )
    print(f"peak_weekday={peak_weekday['day_name']} loads={int(peak_weekday['loads'])}")


if __name__ == "__main__":
    main()
