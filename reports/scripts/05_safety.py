"""Safety incidents and claim cost by driver experience and tenure."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from reports.helpers.db import query_df, write_findings
from reports.helpers.plotting import currency, pct, save_fig, set_theme

SCRIPT = "05_safety"


def _agg(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    out = (
        df.groupby(group_col, dropna=False)
        .agg(
            incidents=("incident_id", "count"),
            preventable_incidents=(
                "preventable_flag",
                lambda s: int(s.fillna(False).astype(bool).sum()),
            ),
            claim_cost=("claim_amount", "sum"),
            avg_claim=("claim_amount", "mean"),
        )
        .reset_index()
    )
    out["claim_cost"] = out["claim_cost"].astype(float)
    out["avg_claim"] = out["avg_claim"].astype(float)
    out["preventable_share"] = out["preventable_incidents"] / out["incidents"]
    return out


def main() -> None:
    """Build safety charts and findings."""
    set_theme()
    df = query_df(
        """
        select i.incident_id,
               i.preventable_flag,
               i.claim_amount,
               d.experience_band,
               d.tenure_band
        from gold.fct_safety_incidents i
        join gold.dim_drivers d on d.driver_key = i.driver_key
        """
    )
    if df.empty:
        raise SystemExit("05_safety: no rows returned")

    df["claim_amount"] = df["claim_amount"].fillna(0).astype(float)
    preventable_share = float(df["preventable_flag"].fillna(False).astype(bool).mean())
    total_claim = float(df["claim_amount"].sum())

    exp = _agg(df, "experience_band")
    ten = _agg(df, "tenure_band")
    exp_order = [
        "<1 year",
        "1-4 years",
        "5-9 years",
        "10-19 years",
        "20+ years",
        "Unknown",
    ]
    ten_order = [
        "<1 year",
        "1-2 years",
        "3-5 years",
        "6-10 years",
        "10+ years",
        "Unknown",
    ]
    exp["experience_band"] = pd.Categorical(
        exp["experience_band"], categories=exp_order, ordered=True
    )
    ten["tenure_band"] = pd.Categorical(
        ten["tenure_band"], categories=ten_order, ordered=True
    )
    exp = exp.sort_values("experience_band")
    ten = ten.sort_values("tenure_band")
    exp["experience_band"] = exp["experience_band"].astype(str)
    ten["tenure_band"] = ten["tenure_band"].astype(str)

    worst_exp = exp.loc[exp["claim_cost"].idxmax()]
    worst_ten = ten.loc[ten["claim_cost"].idxmax()]

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
        "incidents": len(df),
        "total_claim_amount": total_claim,
        "preventable_incidents": int(
            df["preventable_flag"].fillna(False).astype(bool).sum()
        ),
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
        f"incidents={len(df)} total_claim={currency(total_claim)} preventable_share={pct(preventable_share)}"
    )
    print(
        f"worst_experience={worst_exp['experience_band']} claim={currency(float(worst_exp['claim_cost']))}"
    )
    print(
        f"worst_tenure={worst_ten['tenure_band']} claim={currency(float(worst_ten['claim_cost']))}"
    )


if __name__ == "__main__":
    main()
