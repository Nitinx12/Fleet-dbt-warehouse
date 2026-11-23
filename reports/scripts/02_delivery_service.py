"""Delivery service by facility and customer."""

from __future__ import annotations

import matplotlib.pyplot as plt
import seaborn as sns

from reports.helpers.db import query_df, write_findings
from reports.helpers.plotting import currency, pct, save_fig, set_theme

SCRIPT = "02_delivery_service"
DETENTION_COST_PER_HOUR = 75.0

SUMMARY_SQL = """
select count(*) as events,
       count(*) filter (where e.on_time_flag is not null) as known_on_time_events,
       coalesce(avg(case when e.on_time_flag is null then 1.0 else 0.0 end), 0) as unknown_on_time_rate,
       avg(case when e.on_time_flag is null then null when e.on_time_flag then 1.0 else 0.0 end) as overall_on_time_rate,
       coalesce(sum(coalesce(e.detention_hours, 0) * :cost_per_hour), 0) as total_estimated_detention_cost
from gold.fct_delivery_events e
where e.event_type in ('PICKUP', 'DELIVERY')
"""

GROUP_SQL = """
select {dimension},
       count(*) as events,
       avg(case when e.on_time_flag then 1.0 else 0.0 end) as on_time_rate,
       sum(coalesce(e.detention_hours, 0) * :cost_per_hour) as detention_cost
from gold.fct_delivery_events e
left join gold.{lookup} on {join_key} = e.{key}
 where e.event_type in ('PICKUP', 'DELIVERY')
   and e.on_time_flag is not null
 group by {dimension}
having count(*) >= 100
"""


def main() -> None:
    """Build delivery service charts and findings."""
    set_theme()
    params = {"cost_per_hour": DETENTION_COST_PER_HOUR}
    summary = query_df(SUMMARY_SQL, params)
    facility = query_df(
        GROUP_SQL.format(
            dimension="f.facility_name",
            lookup="dim_facilities f",
            join_key="f.facility_key",
            key="facility_key",
        ),
        params,
    )
    customer = query_df(
        GROUP_SQL.format(
            dimension="c.customer_name",
            lookup="dim_customers c",
            join_key="c.customer_key",
            key="customer_key",
        ),
        params,
    )
    if summary.empty or facility.empty or customer.empty:
        raise SystemExit("02_delivery_service: no rows returned")

    for frame in (facility, customer):
        frame["detention_cost"] = frame["detention_cost"].astype(float)
    facility = facility.sort_values("on_time_rate")
    customer = customer.sort_values("detention_cost", ascending=False)
    worst_facility = facility.iloc[0]
    best_facility = facility.iloc[-1]
    top_detention_customer = customer.iloc[0]

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

    row = summary.iloc[0]
    events = int(row["events"])
    known_events = int(row["known_on_time_events"])
    unknown_rate = float(row["unknown_on_time_rate"])
    overall_on_time = float(row["overall_on_time_rate"])
    findings = {
        "script": SCRIPT,
        "detention_cost_per_hour_usd": DETENTION_COST_PER_HOUR,
        "events": events,
        "known_on_time_events": known_events,
        "unknown_on_time_rate": unknown_rate,
        "overall_on_time_rate": overall_on_time,
        "total_estimated_detention_cost": float(row["total_estimated_detention_cost"]),
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
