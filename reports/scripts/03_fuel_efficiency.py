"""Fuel efficiency by truck age, idle, and fuel price by state."""

from __future__ import annotations

import matplotlib.pyplot as plt
import seaborn as sns

from reports.helpers.db import query_df, write_findings
from reports.helpers.plotting import currency, save_fig, set_theme

SCRIPT = "03_fuel_efficiency"


def main() -> None:
    """Build fuel efficiency charts and findings."""
    set_theme()
    mpg = query_df(
        """
        select tr.age_band,
               count(*) as trips,
               sum(t.actual_distance_miles) as miles,
               sum(t.fuel_gallons_used) as gallons,
               sum(t.actual_distance_miles)::numeric / nullif(sum(t.fuel_gallons_used), 0) as mpg,
               avg(t.idle_share_of_duration) as idle_share,
               sum(case when t.is_mpg_consistent then 1 else 0 end) as consistent_trips,
               sum(case when not t.is_mpg_consistent then 1 else 0 end) as inconsistent_trips
        from gold.fct_trips t
        join gold.dim_trucks tr on tr.truck_key = t.truck_key
        where t.is_mpg_consistent is true
          and t.fuel_gallons_used > 0
          and t.actual_distance_miles is not null
          and tr.age_band <> 'Unknown'
        group by tr.age_band
        order by tr.age_band
        """
    )
    quality = query_df(
        """
        select count(*) as trips,
               sum(case when is_mpg_consistent is true then 1 else 0 end) as consistent_trips,
               sum(case when is_mpg_consistent is false then 1 else 0 end) as inconsistent_trips,
               sum(case when is_mpg_consistent is null then 1 else 0 end) as unknown_trips
        from gold.fct_trips
        """
    )
    fuel_price = query_df(
        """
        select location_state,
               count(*) as purchases,
               sum(total_cost) / nullif(sum(gallons), 0) as avg_price_per_gallon,
               sum(case when is_total_cost_consistent then 1 else 0 end) as consistent_purchases,
               sum(case when not is_total_cost_consistent then 1 else 0 end) as inconsistent_purchases
        from gold.fct_fuel_purchases
        where is_total_cost_consistent is true
          and gallons > 0
          and location_state is not null
        group by location_state
        having count(*) >= 100
        order by avg_price_per_gallon desc
        """
    )
    if mpg.empty or fuel_price.empty:
        raise SystemExit("03_fuel_efficiency: no rows returned")

    for col in ["mpg", "idle_share", "miles", "gallons"]:
        mpg[col] = mpg[col].astype(float)
    fuel_price["avg_price_per_gallon"] = fuel_price["avg_price_per_gallon"].astype(
        float
    )

    best = mpg.loc[mpg["mpg"].idxmax()]
    worst = mpg.loc[mpg["mpg"].idxmin()]
    mpg_gap_pct = float((best["mpg"] - worst["mpg"]) / best["mpg"])

    fig, ax = plt.subplots()
    sns.barplot(data=mpg, x="age_band", y="mpg", ax=ax, color="#4C72B0")
    ax.set_title(
        f"{worst['age_band']} trucks get {mpg_gap_pct:.0%} lower MPG than {best['age_band']} trucks"
    )
    ax.set_xlabel("Truck age band")
    ax.set_ylabel("Miles per gallon (MPG)")
    save_fig(fig, SCRIPT, "mpg_by_age_band")

    fig, ax = plt.subplots()
    sns.barplot(data=mpg, x="age_band", y="idle_share", ax=ax, color="#DD8452")
    ax.set_title("Idle time share of trip duration by truck age band")
    ax.set_xlabel("Truck age band")
    ax.set_ylabel("Idle share of duration (%)")
    ax.yaxis.set_major_formatter(lambda x, _: f"{x * 100:.0f}%")
    save_fig(fig, SCRIPT, "idle_share_by_age_band")

    fig, ax = plt.subplots()
    top_states = fuel_price.head(10).sort_values("avg_price_per_gallon", ascending=True)
    sns.barplot(
        data=top_states,
        y="location_state",
        x="avg_price_per_gallon",
        ax=ax,
        color="#55A868",
    )
    ax.set_title(
        f"{fuel_price.iloc[0]['location_state']} has the highest average fuel price among high-volume states"
    )
    ax.set_xlabel("Average price per gallon (USD)")
    ax.set_ylabel("State")
    ax.xaxis.set_major_formatter(lambda x, _: f"${x:.2f}")
    save_fig(fig, SCRIPT, "fuel_price_by_state")

    q = quality.iloc[0]
    findings = {
        "script": SCRIPT,
        "trip_rows": int(q["trips"]),
        "consistent_trips": int(q["consistent_trips"]),
        "inconsistent_trips": int(q["inconsistent_trips"]),
        "unknown_consistency_trips": int(q["unknown_trips"]),
        "best_age_band_mpg": {
            "age_band": best["age_band"],
            "mpg": float(best["mpg"]),
            "idle_share": float(best["idle_share"]),
        },
        "worst_age_band_mpg": {
            "age_band": worst["age_band"],
            "mpg": float(worst["mpg"]),
            "idle_share": float(worst["idle_share"]),
        },
        "mpg_gap_pct_best_vs_worst": mpg_gap_pct,
        "highest_fuel_price_state": {
            "state": fuel_price.iloc[0]["location_state"],
            "avg_price_per_gallon": float(fuel_price.iloc[0]["avg_price_per_gallon"]),
            "purchases": int(fuel_price.iloc[0]["purchases"]),
        },
        "age_band_rows": mpg.to_dict(orient="records"),
        "consistent_checks": "Filtered to fct_trips.is_mpg_consistent=true and fct_fuel_purchases.is_total_cost_consistent=true; inconsistent/unknown rows excluded from aggregates and counted in findings",
    }
    write_findings(SCRIPT, findings)
    print(
        f"mpg_best={best['age_band']}:{best['mpg']:.2f} mpg_worst={worst['age_band']}:{worst['mpg']:.2f}"
    )
    print(
        f"inconsistent_trips={int(q['inconsistent_trips'])} unknown_trips={int(q['unknown_trips'])}"
    )
    print(
        f"highest_fuel_state={fuel_price.iloc[0]['location_state']} price={currency(float(fuel_price.iloc[0]['avg_price_per_gallon']), 2)}"
    )


if __name__ == "__main__":
    main()
