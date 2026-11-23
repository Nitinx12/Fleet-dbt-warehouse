"""Maintenance cost and downtime by truck age band."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from reports.helpers.db import query_df, write_findings
from reports.helpers.plotting import currency, save_fig, set_theme

SCRIPT = "04_fleet_maintenance"
REPLACEMENT_COST_THRESHOLD = 15000.0
REPLACEMENT_DOWNTIME_THRESHOLD = 80.0


def main() -> None:
    """Build maintenance cost charts and findings."""
    set_theme()
    quality = query_df(
        """
        select count(*) as rows,
               sum(case when is_total_cost_consistent is true then 1 else 0 end) as consistent_rows,
               sum(case when is_total_cost_consistent is false then 1 else 0 end) as inconsistent_rows,
               sum(case when is_total_cost_consistent is null then 1 else 0 end) as unknown_rows
        from gold.fct_maintenance
        """
    )
    by_age = query_df(
        """
        select tr.age_band,
               count(distinct tr.truck_id) as trucks,
               count(*) as maintenance_events,
               sum(m.total_cost) as total_cost,
               avg(m.total_cost) as avg_event_cost,
               sum(m.downtime_hours) as total_downtime_hours,
               avg(m.downtime_hours) as avg_downtime_hours,
               sum(m.total_cost) / nullif(count(distinct tr.truck_id), 0) as cost_per_truck,
               sum(m.downtime_hours) / nullif(count(distinct tr.truck_id), 0) as downtime_per_truck
        from gold.fct_maintenance m
        join gold.dim_trucks tr on tr.truck_key = m.truck_key
        where m.is_total_cost_consistent is true
          and tr.age_band <> 'Unknown'
        group by tr.age_band
        order by tr.age_band
        """
    )
    if by_age.empty:
        raise SystemExit("04_fleet_maintenance: no rows returned")

    for col in [
        "total_cost",
        "avg_event_cost",
        "total_downtime_hours",
        "avg_downtime_hours",
        "cost_per_truck",
        "downtime_per_truck",
    ]:
        by_age[col] = by_age[col].astype(float)

    order = ["0-2 years", "3-5 years", "6-10 years", "11+ years"]
    by_age["age_band"] = pd.Categorical(
        by_age["age_band"], categories=order, ordered=True
    )
    by_age = by_age.sort_values("age_band")
    by_age["age_band"] = by_age["age_band"].astype(str)

    oldest = by_age.loc[by_age["age_band"] == "11+ years"].iloc[0]
    baseline_candidates = by_age[by_age["age_band"].isin(["0-2 years", "3-5 years"])]
    baseline = (
        baseline_candidates.iloc[0] if not baseline_candidates.empty else by_age.iloc[0]
    )
    cost_ratio = (
        float(oldest["cost_per_truck"] / baseline["cost_per_truck"])
        if baseline["cost_per_truck"]
        else 0.0
    )

    fig, ax = plt.subplots()
    sns.barplot(data=by_age, x="age_band", y="cost_per_truck", ax=ax, color="#4C72B0")
    ax.set_title(
        f"Trucks {oldest['age_band']} cost {cost_ratio:.1f}x more maintenance per truck than {baseline['age_band']}"
    )
    ax.set_xlabel("Truck age band")
    ax.set_ylabel("Maintenance cost per truck (USD)")
    ax.yaxis.set_major_formatter(lambda x, _: f"${x:,.0f}")
    save_fig(fig, SCRIPT, "cost_per_truck_by_age_band")

    fig, ax = plt.subplots()
    sns.barplot(
        data=by_age, x="age_band", y="downtime_per_truck", ax=ax, color="#DD8452"
    )
    ax.set_title("Older truck bands accumulate more downtime hours per truck")
    ax.set_xlabel("Truck age band")
    ax.set_ylabel("Downtime hours per truck")
    save_fig(fig, SCRIPT, "downtime_per_truck_by_age_band")

    by_age["replacement_flag"] = (
        by_age["cost_per_truck"] >= REPLACEMENT_COST_THRESHOLD
    ) | (by_age["downtime_per_truck"] >= REPLACEMENT_DOWNTIME_THRESHOLD)
    flagged = by_age[by_age["replacement_flag"]]["age_band"].tolist()

    q = quality.iloc[0]
    findings = {
        "script": SCRIPT,
        "rows": int(q["rows"]),
        "consistent_rows": int(q["consistent_rows"]),
        "inconsistent_rows": int(q["inconsistent_rows"]),
        "unknown_rows": int(q["unknown_rows"]),
        "replacement_cost_threshold_usd_per_truck": REPLACEMENT_COST_THRESHOLD,
        "replacement_downtime_threshold_hours_per_truck": REPLACEMENT_DOWNTIME_THRESHOLD,
        "baseline_age_band": baseline["age_band"],
        "baseline_cost_per_truck": float(baseline["cost_per_truck"]),
        "oldest_age_band": oldest["age_band"],
        "oldest_cost_per_truck": float(oldest["cost_per_truck"]),
        "oldest_downtime_per_truck": float(oldest["downtime_per_truck"]),
        "oldest_to_baseline_cost_ratio": cost_ratio,
        "replacement_flagged_age_bands": flagged,
        "age_band_rows": by_age.to_dict(orient="records"),
        "consistent_checks": "Filtered to fct_maintenance.is_total_cost_consistent=true; inconsistent/unknown rows excluded from aggregates and counted in findings",
    }
    write_findings(SCRIPT, findings)
    print(f"cost_ratio_11plus_vs_{baseline['age_band']}={cost_ratio:.2f}x")
    print(f"oldest_cost_per_truck={currency(float(oldest['cost_per_truck']))}")
    print(f"replacement_flagged={flagged}")


if __name__ == "__main__":
    main()
