"""Delivery service by facility and customer."""

from __future__ import annotations

import matplotlib.pyplot as plt
import seaborn as sns

from reports.helpers.db import query_df, write_findings
from reports.helpers.plotting import currency, pct, save_fig, set_theme

SCRIPT = "02_delivery_service"
DETENTION_COST_PER_HOUR = 75.0


def main() -> None:
    """Build delivery service charts and findings."""
    set_theme()
    base = query_df(
        """
        select e.event_id,
               e.on_time_flag,
               e.detention_hours,
               e.detention_minutes,
               coalesce(e.detention_hours, 0) * :cost_per_hour as detention_cost,
               f.facility_name,
               c.customer_name
        from gold.fct_delivery_events e
        left join gold.dim_facilities f on f.facility_key = e.facility_key
        left join gold.dim_customers c on c.customer_key = e.customer_key
        where e.event_type in ('PICKUP', 'DELIVERY')
        """,
        {"cost_per_hour": DETENTION_COST_PER_HOUR},
    )
    if base.empty:
        raise SystemExit("02_delivery_service: no rows returned")

    base["on_time_flag"] = base["on_time_flag"].astype("boolean")
    known = base[base["on_time_flag"].notna()].copy()
    overall_on_time = float(known["on_time_flag"].mean())
    unknown_rate = float(base["on_time_flag"].isna().mean())
    total_detention_cost = float(base["detention_cost"].fillna(0).sum())

    facility = (
        known.groupby("facility_name", dropna=False)
        .agg(
            events=("event_id", "count"),
            on_time_rate=("on_time_flag", "mean"),
            detention_cost=("detention_cost", "sum"),
        )
        .reset_index()
    )
    facility["detention_cost"] = facility["detention_cost"].astype(float)
    facility = facility[facility["events"] >= 100].sort_values("on_time_rate")
    worst_facility = facility.iloc[0]
    best_facility = facility.iloc[-1]

    fig, ax = plt.subplots()
    plot_fac = facility.head(10).sort_values("on_time_rate", ascending=False)
    sns.barplot(
        data=plot_fac, y="facility_name", x="on_time_rate", ax=ax, color="#4C72B0"
    )
    ax.set_title(
        f"{worst_facility['facility_name']} has the lowest on-time rate among high-volume facilities"
    )
    ax.set_xlabel("On-time rate (%)")
    ax.set_ylabel("Facility")
    ax.xaxis.set_major_formatter(lambda x, _: f"{x * 100:.0f}%")
    save_fig(fig, SCRIPT, "on_time_by_facility")

    customer = (
        known.groupby("customer_name", dropna=False)
        .agg(
            events=("event_id", "count"),
            on_time_rate=("on_time_flag", "mean"),
            detention_cost=("detention_cost", "sum"),
        )
        .reset_index()
    )
    customer["detention_cost"] = customer["detention_cost"].astype(float)
    customer = customer[customer["events"] >= 100].sort_values(
        "detention_cost", ascending=False
    )
    top_detention_customer = customer.iloc[0]

    fig, ax = plt.subplots()
    sns.barplot(
        data=customer.head(10).sort_values("detention_cost", ascending=True),
        y="customer_name",
        x="detention_cost",
        ax=ax,
        color="#DD8452",
    )
    ax.set_title(
        f"{top_detention_customer['customer_name']} drives the highest estimated detention cost"
    )
    ax.set_xlabel("Estimated detention cost (USD, $75/hour)")
    ax.set_ylabel("Customer")
    ax.xaxis.set_major_formatter(lambda x, _: f"${x / 1000:.0f}K")
    save_fig(fig, SCRIPT, "detention_cost_by_customer")

    findings = {
        "script": SCRIPT,
        "detention_cost_per_hour_usd": DETENTION_COST_PER_HOUR,
        "events": len(base),
        "known_on_time_events": len(known),
        "unknown_on_time_rate": unknown_rate,
        "overall_on_time_rate": overall_on_time,
        "total_estimated_detention_cost": total_detention_cost,
        "worst_facility": {
            "facility_name": worst_facility["facility_name"],
            "on_time_rate": float(worst_facility["on_time_rate"]),
            "events": int(worst_facility["events"]),
            "detention_cost": float(worst_facility["detention_cost"]),
        },
        "best_facility": {
            "facility_name": best_facility["facility_name"],
            "on_time_rate": float(best_facility["on_time_rate"]),
            "events": int(best_facility["events"]),
        },
        "top_detention_customer": {
            "customer_name": top_detention_customer["customer_name"],
            "detention_cost": float(top_detention_customer["detention_cost"]),
            "events": int(top_detention_customer["events"]),
            "on_time_rate": float(top_detention_customer["on_time_rate"]),
        },
        "consistent_checks": "N/A - fct_delivery_events has no *_consistent columns; null on_time_flag excluded from rate denominators and reported",
    }
    write_findings(SCRIPT, findings)
    print(f"overall_on_time={pct(overall_on_time)} unknown_on_time={pct(unknown_rate)}")
    print(
        f"worst_facility={worst_facility['facility_name']} on_time={pct(float(worst_facility['on_time_rate']))}"
    )
    print(
        f"top_detention_customer={top_detention_customer['customer_name']} detention_cost={currency(float(top_detention_customer['detention_cost']))}"
    )


if __name__ == "__main__":
    main()
