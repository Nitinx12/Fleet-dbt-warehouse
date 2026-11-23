"""Safety incidents and claim cost by driver experience and tenure."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from reports.helpers.db import query_df, write_findings
from reports.helpers.plotting import currency, pct, save_fig, set_theme

SCRIPT = "05_safety"
SUMMARY_SQL = """
select count(*) as incidents,
       coalesce(sum(coalesce(i.claim_amount, 0)), 0) as total_claim_amount,
       sum(case when coalesce(i.preventable_flag, false) then 1 else 0 end) as preventable_incidents,
       avg(case when coalesce(i.preventable_flag, false) then 1.0 else 0.0 end) as preventable_share
from gold.fct_safety_incidents i
join gold.dim_drivers d on d.driver_key = i.driver_key
"""
BAND_SQL = """
select d.{band} as {band},
       count(*) as incidents,
       sum(case when coalesce(i.preventable_flag, false) then 1 else 0 end) as preventable_incidents,
       sum(coalesce(i.claim_amount, 0)) as claim_cost,
       avg(coalesce(i.claim_amount, 0)) as avg_claim,
       avg(case when coalesce(i.preventable_flag, false) then 1.0 else 0.0 end) as preventable_share
from gold.fct_safety_incidents i
join gold.dim_drivers d on d.driver_key = i.driver_key
group by d.{band}
"""
EXP_ORDER = ["<1 year", "1-4 years", "5-9 years", "10-19 years", "20+ years", "Unknown"]
TEN_ORDER = ["<1 year", "1-2 years", "3-5 years", "6-10 years", "10+ years", "Unknown"]


def _ordered(df: pd.DataFrame, column: str, order: list[str]) -> pd.DataFrame:
    """Order a SQL-aggregated band table without changing its values."""
    out = df.copy()
    for name in ("claim_cost", "avg_claim", "preventable_share"):
        out[name] = out[name].astype(float)
    out[column] = pd.Categorical(out[column], categories=order, ordered=True)
    out = out.sort_values(column)
    out[column] = out[column].astype(str)
    return out


def main() -> None:
    """Build safety charts and findings."""
    set_theme()
    summary = query_df(SUMMARY_SQL)
    exp = query_df(BAND_SQL.format(band="experience_band"))
    ten = query_df(BAND_SQL.format(band="tenure_band"))
    if summary.empty or exp.empty or ten.empty:
        raise SystemExit("05_safety: no rows returned")

    exp = _ordered(exp, "experience_band", EXP_ORDER)
    ten = _ordered(ten, "tenure_band", TEN_ORDER)
    worst_exp = exp.loc[exp["claim_cost"].idxmax()]
    worst_ten = ten.loc[ten["claim_cost"].idxmax()]

    row = summary.iloc[0]
    preventable_share = float(row["preventable_share"])
    total_claim = float(row["total_claim_amount"])

    fig, ax = plt.subplots()
    sns.barplot(data=exp, x="experience_band", y="claim_cost", ax=ax, color="#C44E52")
    ax.set_title(
        f"{worst_exp['experience_band']} drivers account for {currency(float(worst_exp['claim_cost']))} in claim cost"
    )
    ax.set_xlabel("Driver experience band")
    ax.set_ylabel("Total claim amount (USD)")
    ax.yaxis.set_major_formatter(lambda x, _: f"${x / 1000:.0f}K")
    ax.tick_params(axis="x", rotation=20)
    save_fig(fig, SCRIPT, "claim_cost_by_experience")

    fig, ax = plt.subplots()
    sns.barplot(data=ten, x="tenure_band", y="claim_cost", ax=ax, color="#4C72B0")
    ax.set_title("Claim cost concentration by driver tenure band")
    ax.set_xlabel("Driver tenure band")
    ax.set_ylabel("Total claim amount (USD)")
    ax.yaxis.set_major_formatter(lambda x, _: f"${x / 1000:.0f}K")
    ax.tick_params(axis="x", rotation=20)
    save_fig(fig, SCRIPT, "claim_cost_by_tenure")

    prev = exp[["experience_band", "preventable_share", "incidents"]].copy()
    fig, ax = plt.subplots()
    sns.barplot(
        data=prev, x="experience_band", y="preventable_share", ax=ax, color="#DD8452"
    )
    ax.set_title(
        f"{pct(preventable_share)} of all incidents are preventable across the fleet"
    )
    ax.set_xlabel("Driver experience band")
    ax.set_ylabel("Preventable share (%)")
    ax.yaxis.set_major_formatter(lambda x, _: f"{x * 100:.0f}%")
    ax.tick_params(axis="x", rotation=20)
    save_fig(fig, SCRIPT, "preventable_share_by_experience")

    findings = {
        "script": SCRIPT,
        "incidents": int(row["incidents"]),
        "total_claim_amount": total_claim,
        "preventable_incidents": int(row["preventable_incidents"]),
        "preventable_share": preventable_share,
        "worst_experience_band": {
            "experience_band": str(worst_exp["experience_band"]),
            "claim_cost": float(worst_exp["claim_cost"]),
            "incidents": int(worst_exp["incidents"]),
            "preventable_share": float(worst_exp["preventable_share"]),
        },
        "worst_tenure_band": {
            "tenure_band": str(worst_ten["tenure_band"]),
            "claim_cost": float(worst_ten["claim_cost"]),
            "incidents": int(worst_ten["incidents"]),
        },
        "experience_rows": exp.to_dict(orient="records"),
        "tenure_rows": ten.to_dict(orient="records"),
        "consistent_checks": "N/A - fct_safety_incidents has no *_consistent columns; null preventable_flag treated as not preventable for share and reported via counts",
    }
    write_findings(SCRIPT, findings)
    print(
        f"incidents={int(row['incidents'])} total_claim={currency(total_claim)} preventable_share={pct(preventable_share)}"
    )
    print(
        f"worst_experience={worst_exp['experience_band']} claim={currency(float(worst_exp['claim_cost']))}"
    )
    print(
        f"worst_tenure={worst_ten['tenure_band']} claim={currency(float(worst_ten['claim_cost']))}"
    )


if __name__ == "__main__":
    main()
