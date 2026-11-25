"""Kafka speed-layer throughput, quality and alert health."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from reports.helpers.db import query_df, write_findings
from reports.helpers.plotting import pct, save_fig, set_theme

SCRIPT = "08_streaming"

JOB_SQL = """
select split_part(h.message, ' ', 2)::int as batch,
       h.rows_raw,
       h.rows_window,
       h.rows_alert,
       h.rejected,
       h.late_dropped,
       h.observed_at
from audit.stream_health h
where h.source = 'JOB'
order by h.observed_at
"""

RAW_SUMMARY_SQL = """
select count(*) as raw_rows,
       count(*) filter (where event_type = 'ARRIVED') as arrived_rows,
       count(*) filter (where event_type = 'DEPARTED') as departed_rows,
       count(*) filter (where on_time_flag) as on_time_rows,
       count(*) filter (where not on_time_flag) as late_flag_rows,
       min(event_time) as min_event_time,
       max(event_time) as max_event_time
from realtime.delivery_events_raw
"""

ALERT_SQL = """
select status, count(*) as alerts
from realtime.detention_alert
group by status
"""

MONTHLY_SQL = """
select date_trunc('month', event_time)::date as month,
       count(*) as events
from realtime.delivery_events_raw
group by 1
order by 1
"""

WATCHDOG_SQL = """
select count(*) as probes,
       max(partition_lag) as max_lag,
       max(newest_event_age_seconds) as max_age_seconds
from audit.stream_health
where source = 'WATCHDOG' and partition_lag is not null
"""

REGISTRY_SQL = """
select stream_name, topic, status
from control.stream
"""


def main() -> None:
    """Build streaming charts and findings."""
    set_theme()
    job = query_df(JOB_SQL)
    raw = query_df(RAW_SUMMARY_SQL)
    alerts = query_df(ALERT_SQL)
    monthly = query_df(MONTHLY_SQL)
    watchdog = query_df(WATCHDOG_SQL)
    registry = query_df(REGISTRY_SQL)
    if job.empty or raw.empty or alerts.empty or monthly.empty:
        raise SystemExit("08_streaming: no rows returned")

    job = job.sort_values("batch").reset_index(drop=True)
    raw_row = raw.iloc[0]
    raw_rows = int(raw_row["raw_rows"])
    arrived = int(raw_row["arrived_rows"])
    departed = int(raw_row["departed_rows"])
    on_time = int(raw_row["on_time_rows"])
    late_flag = int(raw_row["late_flag_rows"])
    total_batches = len(job)
    total_rejected = int(job["rejected"].sum())
    total_late_dropped = int(job["late_dropped"].sum())
    sum_raw = int(job["rows_raw"].sum())

    fig, ax = plt.subplots()
    ax.plot(
        job["batch"], job["rows_raw"], marker="o", label="Raw batch", color="#4C72B0"
    )
    ax.plot(
        job["batch"],
        job["rows_window"],
        marker="s",
        label="Window rows (cumulative)",
        color="#55A868",
    )
    ax.plot(
        job["batch"],
        job["rows_alert"],
        marker="^",
        label="Alert rows (cumulative)",
        color="#DD8452",
    )
    ax.set_title(
        f"Speed layer drained {raw_rows:,} raw events across {total_batches} micro-batches"
    )
    ax.set_xlabel("Micro-batch id")
    ax.set_ylabel("Row count")
    ax.legend(loc="upper left")
    ax.xaxis.set_major_formatter(lambda x, _: f"{x:.0f}")
    save_fig(fig, SCRIPT, "batch_throughput")

    quality = pd.DataFrame(
        [
            {
                "bucket": "Accepted (late-dropped)",
                "count": raw_rows - total_late_dropped,
            },
            {"bucket": "Late-dropped", "count": total_late_dropped},
            {"bucket": "Contract-rejected", "count": total_rejected},
        ]
    )
    quality["share"] = quality["count"] / max(quality["count"].sum(), 1)
    fig, ax = plt.subplots()
    sns.barplot(
        data=quality,
        y="bucket",
        x="share",
        hue="bucket",
        hue_order=["Accepted (late-dropped)", "Late-dropped", "Contract-rejected"],
        palette={
            "Accepted (late-dropped)": "#55A868",
            "Late-dropped": "#CCB974",
            "Contract-rejected": "#C44E52",
        },
        legend=False,
        ax=ax,
    )
    ax.set_title(
        f"Contract rejects are {pct(total_rejected / max(sum_raw, 1))} of micro-batch reads; watermark dropped {total_late_dropped:,} late events"
    )
    ax.set_xlabel("Share of micro-batch reads")
    ax.set_ylabel("Quality bucket")
    ax.xaxis.set_major_formatter(lambda x, _: f"{x * 100:.1f}%")
    for patch, share in zip(ax.patches, quality["share"], strict=False):
        ax.annotate(
            pct(float(share), 2),
            (patch.get_width(), patch.get_y() + patch.get_height() / 2),
            xytext=(4, 0),
            textcoords="offset points",
            va="center",
        )
    save_fig(fig, SCRIPT, "event_quality")

    fig, ax = plt.subplots()
    lifecycle = pd.DataFrame(
        [
            {"event_type": "ARRIVED", "count": arrived},
            {"event_type": "DEPARTED", "count": departed},
        ]
    )
    sns.barplot(data=lifecycle, y="event_type", x="count", ax=ax, color="#4C72B0")
    ax.set_title(
        f"Lifecycle expansion produced a balanced ARRIVED/DEPARTED pair set ({arrived:,} each)"
    )
    ax.set_xlabel("Raw rows")
    ax.set_ylabel("Event type")
    ax.xaxis.set_major_formatter(lambda x, _: f"{x / 1000:.0f}K")
    save_fig(fig, SCRIPT, "lifecycle_split")

    fig, ax = plt.subplots()
    ax_pie = ax
    sizes = [on_time, late_flag]
    labels = [
        f"On time {pct(on_time / max(raw_rows, 1))}",
        f"Late {pct(late_flag / max(raw_rows, 1))}",
    ]
    ax_pie.pie(
        sizes,
        labels=labels,
        autopct=lambda p: f"{p:.1f}%",
        colors=["#55A868", "#C44E52"],
        startangle=90,
        wedgeprops={"linewidth": 1, "edgecolor": "white"},
    )
    ax_pie.set_title(
        f"Stream on-time share is {pct(on_time / max(raw_rows, 1))} across {raw_rows:,} lifecycle events"
    )
    save_fig(fig, SCRIPT, "on_time_share")

    fig, ax = plt.subplots()
    alert_plot = alerts.sort_values("alerts", ascending=True)
    sns.barplot(
        data=alert_plot,
        y="status",
        x="status",
        hue="status",
        hue_order=list(alert_plot["status"]),
        palette={"OPEN": "#C44E52", "RESOLVED": "#55A868"},
        legend=False,
        ax=ax,
    )
    # seaborn needs x numeric; plot via count proxy
    ax.clear()
    colors = ["#C44E52" if s == "OPEN" else "#55A868" for s in alert_plot["status"]]
    ax.barh(alert_plot["status"], alert_plot["alerts"], color=colors)
    top_alert = alert_plot.iloc[-1]
    ax.set_title(
        f"{int(top_alert['alerts']):,} detention alerts are {top_alert['status']} after replay"
    )
    ax.set_xlabel("Detention alerts")
    ax.set_ylabel("Alert status")
    ax.xaxis.set_major_formatter(lambda x, _: f"{x / 1000:.0f}K")
    save_fig(fig, SCRIPT, "detention_alert_status")

    fig, ax = plt.subplots()
    monthly = monthly.copy()
    monthly["month"] = pd.to_datetime(monthly["month"])
    ax.plot(monthly["month"], monthly["events"], marker="o", color="#4C72B0")
    peak = monthly.loc[monthly["events"].idxmax()]
    trough = monthly.loc[monthly["events"].idxmin()]
    ax.scatter([peak["month"]], [peak["events"]], s=80, color="#C44E52", zorder=5)
    ax.scatter([trough["month"]], [trough["events"]], s=80, color="#55A868", zorder=5)
    ax.set_title(
        f"Replayed event volume peaked in {peak['month']:%b %Y} ({int(peak['events']):,}) and troughed in {trough['month']:%b %Y} ({int(trough['events']):,})"
    )
    ax.set_xlabel("Event month")
    ax.set_ylabel("Lifecycle events")
    ax.yaxis.set_major_formatter(lambda x, _: f"{x / 1000:.0f}K")
    fig.autofmt_xdate(rotation=45)
    save_fig(fig, SCRIPT, "monthly_event_volume")

    reg = registry.iloc[0] if not registry.empty else None
    max_lag = int(watchdog.iloc[0]["max_lag"]) if not watchdog.empty else None
    findings = {
        "script": SCRIPT,
        "registry": {
            "stream_name": str(reg["stream_name"]) if reg is not None else None,
            "topic": str(reg["topic"]) if reg is not None else None,
            "status": str(reg["status"]) if reg is not None else None,
        },
        "batches": total_batches,
        "micro_batch_reads": sum_raw,
        "raw_rows": raw_rows,
        "arrived_rows": arrived,
        "departed_rows": departed,
        "on_time_rows": on_time,
        "late_flag_rows": late_flag,
        "on_time_share": on_time / max(raw_rows, 1),
        "rejected_total": total_rejected,
        "rejected_share": total_rejected / max(sum_raw, 1),
        "late_dropped_total": total_late_dropped,
        "window_rows_latest": int(job.iloc[-1]["rows_window"]),
        "alert_rows_latest": int(job.iloc[-1]["rows_alert"]),
        "detention_alerts": {
            str(row["status"]): int(row["alerts"]) for _, row in alerts.iterrows()
        },
        "event_time_range": {
            "min": str(raw_row["min_event_time"]),
            "max": str(raw_row["max_event_time"]),
        },
        "watchdog": {
            "probes": int(watchdog.iloc[0]["probes"]) if not watchdog.empty else 0,
            "max_lag": max_lag,
            "max_age_seconds": float(watchdog.iloc[0]["max_age_seconds"])
            if not watchdog.empty
            else None,
        },
        "notes": "Replay fixture consumed from Kafka topic; Spark writes realtime.* only. Late-dropped events stay in raw but are excluded from windows and alerts past the watermark cutoff.",
    }
    write_findings(SCRIPT, findings)
    print(
        f"batches={total_batches} raw_rows={raw_rows} rejected={pct(total_rejected / max(sum_raw, 1))}"
    )
    print(
        f"on_time={pct(on_time / max(raw_rows, 1))} late_dropped={total_late_dropped:,}"
    )
    print(
        f"alerts_open={int(alerts.loc[alerts['status'] == 'OPEN', 'alerts'].iloc[0]) if (alerts['status'] == 'OPEN').any() else 0}"
    )


if __name__ == "__main__":
    main()
