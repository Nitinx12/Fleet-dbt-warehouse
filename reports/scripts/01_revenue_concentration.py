"""Revenue concentration by customer and lane."""

from __future__ import annotations

import matplotlib.pyplot as plt
import seaborn as sns

from reports.helpers.db import query_df, write_findings
from reports.helpers.plotting import currency, pct, save_fig, set_theme

SCRIPT = "01_revenue_concentration"


def main() -> None:
    """Build revenue concentration charts and findings."""
    set_theme()
    customers = query_df(
        """
        select c.customer_name,
               sum(f.revenue) as revenue,
               count(*) as loads,
               avg(case when f.is_cancelled then 1.0 else 0.0 end) as cancel_rate
        from gold.fct_loads f
        join gold.dim_customers c on c.customer_key = f.customer_key
        where c.customer_id <> '-1'
        group by c.customer_name
        order by revenue desc
        """
    )
    lanes = query_df(
        """
        select r.lane_name,
               sum(f.revenue) as revenue,
               count(*) as loads
        from gold.fct_loads f
        join gold.dim_routes r on r.route_key = f.route_key
        where r.route_id <> '-1'
        group by r.lane_name
        order by revenue desc
        limit 10
        """
    )
    booking = query_df(
        """
        select booking_type,
               count(*) as loads,
               sum(case when is_cancelled then 1 else 0 end) as cancelled_loads,
               avg(case when is_cancelled then 1.0 else 0.0 end) as cancel_rate,
               sum(revenue) as revenue
        from gold.fct_loads
        group by booking_type
        order by revenue desc
        """
    )
    if customers.empty or lanes.empty:
        raise SystemExit("01_revenue_concentration: no rows returned")

    customers["revenue"] = customers["revenue"].astype(float)
    customers["cum_share"] = customers["revenue"].cumsum() / customers["revenue"].sum()
    top_n = min(10, len(customers))
    top_share = float(
        customers["revenue"].head(top_n).sum() / customers["revenue"].sum()
    )
    top1_share = float(customers["revenue"].iloc[0] / customers["revenue"].sum())

    fig, ax = plt.subplots()
    top = customers.head(10).copy()
    sns.barplot(data=top, y="customer_name", x="revenue", ax=ax, color="#4C72B0")
    ax.set_title(f"Top 10 customers capture {pct(top_share)} of load revenue")
    ax.set_xlabel("Revenue (USD)")
    ax.set_ylabel("Customer")
    ax.xaxis.set_major_formatter(lambda x, _: f"${x / 1_000_000:.0f}M")
    save_fig(fig, SCRIPT, "top_customers")

    fig, ax = plt.subplots()
    pareto = customers.reset_index(drop=True)
    ax.bar(pareto.index + 1, pareto["revenue"] / 1_000_000, color="#4C72B0", alpha=0.7)
    ax2 = ax.twinx()
    ax2.plot(pareto.index + 1, pareto["cum_share"], color="#DD8452", linewidth=2)
    ax2.axhline(0.8, color="#555555", linestyle="--", linewidth=1)
    ax2.yaxis.set_major_formatter(lambda x, _: f"{x * 100:.0f}%")
    ax.set_title("Revenue is concentrated in a small customer set (Pareto curve)")
    ax.set_xlabel("Customers ranked by revenue")
    ax.set_ylabel("Revenue (USD millions)")
    ax2.set_ylabel("Cumulative revenue share (%)")
    save_fig(fig, SCRIPT, "pareto")

    lanes["revenue"] = lanes["revenue"].astype(float)
    top_lane_share = float(lanes["revenue"].iloc[0] / customers["revenue"].sum())
    fig, ax = plt.subplots()
    sns.barplot(data=lanes.head(10), y="lane_name", x="revenue", ax=ax, color="#55A868")
    ax.set_title(f"Top lane generates {pct(top_lane_share)} of total load revenue")
    ax.set_xlabel("Revenue (USD)")
    ax.set_ylabel("Lane")
    ax.xaxis.set_major_formatter(lambda x, _: f"${x / 1_000_000:.1f}M")
    save_fig(fig, SCRIPT, "top_lanes")

    booking["cancel_rate"] = booking["cancel_rate"].astype(float)
    booking["revenue"] = booking["revenue"].astype(float)
    fig, ax = plt.subplots()
    sns.barplot(data=booking, x="booking_type", y="cancel_rate", ax=ax, color="#C44E52")
    ax.set_title(
        "Cancellation rate by booking type shows where service risk concentrates"
    )
    ax.set_xlabel("Booking type")
    ax.set_ylabel("Cancel rate (%)")
    ax.yaxis.set_major_formatter(lambda x, _: f"{x * 100:.1f}%")
    save_fig(fig, SCRIPT, "cancel_rate_by_booking")

    findings = {
        "script": SCRIPT,
        "top_customer": top["customer_name"].iloc[0],
        "top_customer_revenue": float(top["revenue"].iloc[0]),
        "top1_revenue_share": top1_share,
        "top10_revenue_share": top_share,
        "top_lane": lanes["lane_name"].iloc[0],
        "top_lane_revenue": float(lanes["revenue"].iloc[0]),
        "top_lane_share_of_total_revenue": top_lane_share,
        "booking_cancel_rates": booking[
            ["booking_type", "loads", "cancelled_loads", "cancel_rate", "revenue"]
        ].to_dict(orient="records"),
        "consistent_checks": "N/A - fct_loads has no *_consistent columns; all rows included",
    }
    write_findings(SCRIPT, findings)
    print(f"top1_share={pct(top1_share)} top10_share={pct(top_share)}")
    print(
        f"top_lane={findings['top_lane']} top_lane_revenue={currency(findings['top_lane_revenue'])}"
    )
    for row in findings["booking_cancel_rates"]:
        print(
            f"booking={row['booking_type']} cancel_rate={pct(float(row['cancel_rate']))} loads={row['loads']}"
        )


if __name__ == "__main__":
    main()
