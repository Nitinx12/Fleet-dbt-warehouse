# Architecture guide

**Purpose:** Explain the current data flow, ownership boundaries, idempotency model, and failure path of the Fleet dbt Warehouse.

**Audience:** Engineers, reviewers, and analysts who need to understand where a value is created and who may change it.

[Back to README](../README.md) · [Authoritative architecture](../ARCHITECTURE.md)

This guide is a short companion to the root architecture document. If they disagree, the root document wins.

## Data flow

```mermaid
flowchart LR
    classDef source fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
    classDef bronze fill:#fff3e0,stroke:#ef6c00,stroke-width:2px,color:#e65100
    classDef silver fill:#f3e5f5,stroke:#6a1b9a,stroke-width:2px,color:#4a148c
    classDef gold fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef marts fill:#e0f7fa,stroke:#00838f,stroke-width:2px,color:#006064
    classDef serving fill:#fff8e1,stroke:#f9a825,stroke-width:2px,color:#f57f17
    classDef control fill:#fce4ec,stroke:#c62828,stroke-width:2px,color:#b71c1c
    classDef orchestration fill:#e1f5fe,stroke:#0277bd,stroke-width:2px,color:#01579b

    AF["Airflow<br/>fleet_daily_load"]:::orchestration
    SRC["MySQL + MongoDB<br/>12 source tables"]:::source
    ING["Python extractor<br/>contract + watermark"]:::bronze
    BR[("bronze<br/>immutable landing")]:::bronze
    SI[("silver<br/>12 typed models")]:::silver
    GD[("gold<br/>7 dims + 6 facts")]:::gold
    MA[("marts<br/>6 BI + reconcile")]:::marts
    DBX[("Databricks<br/>5 approved marts")]:::serving
    BI["BI consumers"]:::serving
    CTRL[("control + audit<br/>batch and evidence")]:::control

    AF -->|"12 tasks"| ING
    SRC -->|"read-only"| ING
    ING --> BR
    BR --> SI --> GD --> MA
    MA -->|"allowlist"| DBX --> BI
    AF -.-> CTRL
    ING -.-> CTRL
    MA -.-> CTRL
```

| Colour | Layer | Writer |
|---|---|---|
| Blue (`#e3f2fd`) | Source | MySQL and MongoDB |
| Orange (`#fff3e0`) | Bronze | `scripts/ingestion.py`, `scripts/load_fixtures.py` |
| Purple (`#f3e5f5`) | Silver | dbt |
| Green (`#e8f5e9`) | Gold | dbt |
| Teal (`#e0f7fa`) | Marts | dbt |
| Yellow (`#fff8e1`) | Serving | `scripts/publish_marts.py`, BI tools |
| Pink (`#fce4ec`) | Control / audit | SQL functions, Python, Airflow |
| Light blue (`#e1f5fe`) | Orchestration | Airflow |

## Ownership

| Schema | Written only by | Reason |
|---|---|---|
| `bronze` | Ingestion and fixture loader | Raw landed rows must remain unchanged before silver. |
| `silver` | dbt | One typed model per bronze table; no business joins. |
| `gold` | dbt | Business keys, conformed dimensions, facts, and PII rules. |
| `snapshots` | dbt | Daily history for customers, drivers and trucks; facts and marts are not wired to it. |
| `marts` | dbt | Denormalised grains for BI and publication. |
| Databricks `marts` copy | `scripts/publish_marts.py` | Read-only serving copy. |
| `control`, `audit` | `control.*` functions, Python, SQL triggers, Airflow | Run state and evidence cannot be edited by dbt. |

## Design choices and trade-offs

| Choice | Trade-off |
|---|---|
| dbt owns transformations; Python only moves data | One testable lineage; more data lands in `bronze` before it is shaped. |
| Watermark micro-batches with lookback | Handles late and changed rows without full reloads; watermark resets and reprocessing windows need monitoring. |
| Current-state SCD1 dimensions with snapshots | Simple, fast BI; snapshots capture customer, driver and truck history without rewiring fact/mart grain. |
| Published-mart allowlist | Prevents PII or unreviewed models reaching Databricks; new grains require a reviewed PR. |
| Airflow decides when, scripts decide what | Local, CI, and DAG runs use the same commands; DAG code holds no transformation logic. |

## Sensitive-data boundaries

| Value | Handling |
|---|---|
| `drivers.license_number` | Hashed into a derived value; raw value does not reach marts. |
| `drivers.date_of_birth` | Converted to an age attribute in silver/gold; not in marts. |
| `drivers.first_name`, `last_name` | Kept in Postgres only; the driver scorecard mart is not published. |
| `fuel_purchases.fuel_card_number` | Masked to the last four digits. |
| `safety_incidents.description` | Excluded from gold and published marts. |

## Replay and failure behaviour

- Ingestion upserts on contract primary keys and advances watermarks only after a successful write and reconciliation.
- Silver and facts reprocess a lookback window keyed on load timestamps; rerunning the same input is safe.
- Failed contract or data tests block downstream stages; retries are for transient connection errors only.
- `control.batch` keeps run state, while `audit.*` stores task, dbt, quality, reconciliation, and error evidence.
- Failed batches emit `pg_notify('pipeline_failure', …)` and a redacted operator notification.

## Current state and gaps

All 32 dbt models enforce contracts, mart columns declare `data_type`, incremental models use `on_schema_change: fail`, and every relationships test is error severity. The manifest has 401 test nodes with 0 warn, including 391 schema tests (138 silver, 202 gold, 51 marts) and 3 unit tests. Watchdog and opt-in stale-batch cleanup are implemented; dbt run tasks publish manifest-keyed assets.

Remaining gaps:

- Snapshots exist for customers, drivers and trucks, but facts and marts still read current-state SCD1 dimensions.
- `fleet_report_weekly` remains a skeleton; no approved weekly payload exists yet.
- There is no production data rollback beyond code redeploy, and no scheduled backup/restore with a tested restore.
- CI uses Python 3.12 while the image uses Python 3.13.
- `SOURCE_MODE` is not a unified switch; fixture loading is a separate guarded script.

Related: [deployment guide](DEPLOYMENT.md), [dbt guide](DBT.md), [testing guide](TESTING.md), [decision records](adr/).
