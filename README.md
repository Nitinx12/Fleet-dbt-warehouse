<p align="center"><img src="assets/logo/logo_1.png" alt="Fleet dbt Warehouse logo" width="120"></p>

<h1 align="center">Fleet dbt Warehouse</h1>

A daily analytics warehouse for fleet logistics operations. Contract-validated extracts from MySQL `fleet_registry` and MongoDB `fleet_operations` land in PostgreSQL `bronze`, dbt transforms them through `silver` and `gold` into BI marts, and five approved non-PII marts are published to Databricks. Airflow orchestrates the daily run, and a read-only Streamlit site shows live run, dbt and data-quality audit status.

<p align="center">
  <a href="https://github.com/Nitinx12/Fleet-dbt-warehouse/actions/workflows/ci.yml"><img src="https://github.com/Nitinx12/Fleet-dbt-warehouse/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/licence-MIT-blue.svg" alt="Licence: MIT"></a>
  <a href="https://github.com/Nitinx12/Fleet-dbt-warehouse/commits/main"><img src="https://img.shields.io/github/last-commit/Nitinx12/Fleet-dbt-warehouse.svg" alt="Last commit"></a>
  <a href="https://github.com/Nitinx12/Fleet-dbt-warehouse/issues"><img src="https://img.shields.io/github/issues/Nitinx12/Fleet-dbt-warehouse.svg" alt="Open issues"></a>
  <a href="https://github.com/Nitinx12/Fleet-dbt-warehouse"><img src="https://img.shields.io/github/repo-size/Nitinx12/Fleet-dbt-warehouse.svg" alt="Repo size"></a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.12%20%7C%203.13-3776AB?logo=python&logoColor=white" alt="Python 3.12 | 3.13">
  <img src="https://img.shields.io/badge/dbt-Core-FF694B?logo=dbt&logoColor=white" alt="dbt Core">
  <img src="https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white" alt="PostgreSQL 16">
  <img src="https://img.shields.io/badge/Airflow-3.3.2-017CEE?logo=apacheairflow&logoColor=white" alt="Airflow 3.3.2">
  <img src="https://img.shields.io/badge/Databricks-Delta-FF3621?logo=databricks&logoColor=white" alt="Databricks Delta">
  <img src="https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white" alt="Streamlit">
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white" alt="Docker Compose">
</p>

<p align="center">
  <a href="https://docs.astral.sh/ruff/"><img src="https://img.shields.io/badge/lint-ruff-D7FF64?logo=ruff&logoColor=black" alt="Ruff"></a>
  <a href="https://docs.astral.sh/uv/"><img src="https://img.shields.io/badge/packaging-uv-DE5FE9?logo=uv&logoColor=white" alt="uv"></a>
  <img src="https://img.shields.io/badge/secret%20scan-gitleaks-C8302F?logo=git&logoColor=white" alt="Gitleaks">
  <img src="https://img.shields.io/badge/tests-pytest%20%2B%20dbt-0A9EDC?logo=pytest&logoColor=white" alt="pytest and dbt tests">
</p>

**Live demo:** [fleet-dbt-warehouse.streamlit.app](https://fleet-dbt-warehouse-vvdnyjnjo3z4k8gkrjqrhf.streamlit.app/) is a read-only view of the latest pipeline runs, dbt results and data-quality checks.

## Tech stack

| Layer | Tools |
|---|---|
| **Sources** | ![MySQL](https://img.shields.io/badge/MySQL-4479A1?logo=mysql&logoColor=white) ![MongoDB](https://img.shields.io/badge/MongoDB-47A248?logo=mongodb&logoColor=white) |
| **Ingestion** | ![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white) ![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-D71F00?logo=sqlalchemy&logoColor=white) ![PyMySQL](https://img.shields.io/badge/PyMySQL-4479A1?logo=mysql&logoColor=white) ![PyMongo](https://img.shields.io/badge/PyMongo-47A248?logo=mongodb&logoColor=white) |
| **Warehouse** | ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white) |
| **Transformation** | ![dbt Core](https://img.shields.io/badge/dbt%20Core-FF694B?logo=dbt&logoColor=white) ![dbt-postgres](https://img.shields.io/badge/dbt--postgres-FF694B?logo=dbt&logoColor=white) ![dbt_utils](https://img.shields.io/badge/dbt__utils-FF694B?logo=dbt&logoColor=white) |
| **Serving copy** | ![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white) ![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD4?logo=deltalake&logoColor=white) |
| **Orchestration** | ![Airflow](https://img.shields.io/badge/Airflow-3.3.2-017CEE?logo=apacheairflow&logoColor=white) ![Celery](https://img.shields.io/badge/Celery-37814A?logo=celery&logoColor=white) ![Redis](https://img.shields.io/badge/Redis-7.4-DC382D?logo=redis&logoColor=white) |
| **Presentation** | ![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white) ![pandas](https://img.shields.io/badge/pandas-150458?logo=pandas&logoColor=white) |
| **Infra & dev tooling** | ![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white) ![Docker Compose](https://img.shields.io/badge/Compose-2496ED?logo=docker&logoColor=white) ![uv](https://img.shields.io/badge/uv-DE5FE9?logo=uv&logoColor=white) ![Ruff](https://img.shields.io/badge/Ruff-D7FF64?logo=ruff&logoColor=black) ![Make](https://img.shields.io/badge/Makefile-6D00CC?logo=gnu&logoColor=white) ![Shell](https://img.shields.io/badge/Shell-4EAA25?logo=gnubash&logoColor=white) |
| **CI, delivery & quality** | ![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?logo=githubactions&logoColor=white) ![GHCR](https://img.shields.io/badge/GHCR-181717?logo=github&logoColor=white) ![pytest](https://img.shields.io/badge/pytest-0A9EDC?logo=pytest&logoColor=white) ![Gitleaks](https://img.shields.io/badge/Gitleaks-8.30.1-C8302F?logo=git&logoColor=white) |

---

## Highlights

- 12 source tables (9 MySQL, 3 MongoDB) feed 32 dbt models: 12 silver, 13 gold and 7 marts.
- 5 approved marts publish to Databricks; `mart_driver_scorecard_monthly` is excluded because it carries a driver display name.
- 391 dbt tests and 131 collected Python/Airflow tests; 12 source contracts are checked by `make contracts-check`.
- `fleet_daily_load` runs at `0 2 * * *` in `Asia/Kolkata`, with batches and watermarks in `control.*` and `audit.*`.
- PII is reduced early: licence numbers hashed, dates of birth banded, fuel cards masked, one driver mart excluded.
- The opt-in Kafka/Spark speed layer writes only `realtime.*` and `audit.stream_health`.

## Architecture

```mermaid
flowchart TD
  classDef src fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
  classDef bronze fill:#fff3e0,stroke:#ef6c00,stroke-width:2px,color:#e65100
  classDef silver fill:#f3e5f5,stroke:#6a1b9a,stroke-width:2px,color:#4a148c
  classDef gold fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
  classDef mart fill:#e0f7fa,stroke:#00838f,stroke-width:2px,color:#006064
  classDef bi fill:#fff8e1,stroke:#f9a825,stroke-width:2px,color:#f57f17
  classDef ctrl fill:#fce4ec,stroke:#c62828,stroke-width:2px,color:#b71c1c
  classDef orch fill:#e1f5fe,stroke:#0277bd,stroke-width:2px,color:#01579b
  classDef speed fill:#ede7f6,stroke:#4527a0,stroke-width:2px,color:#311b92
  classDef blocked fill:#fce4ec,stroke:#c62828,stroke-width:2px,stroke-dasharray:5 4,color:#b71c1c

  AF["Airflow<br/>fleet_daily_load"]:::orch

  subgraph sources["Sources"]
    MYSQL[("MySQL<br/>fleet_registry")]:::src
    MONGO[("MongoDB<br/>fleet_operations")]:::src
  end

  ING["scripts/ingestion.py<br/>contract + watermark"]:::bronze

  subgraph warehouse["PostgreSQL warehouse"]
    BRONZE[("bronze<br/>12 landed tables")]:::bronze
    subgraph dbt["dbt build + test"]
      SILVER[("silver<br/>12 models")]:::silver
      GOLD[("gold<br/>7 dims + 6 facts")]:::gold
      MARTS[("marts<br/>6 BI marts + reconcile")]:::mart
    end
  end

  PUB["publish_marts.py<br/>five-mart allowlist"]:::mart

  subgraph databricks["Databricks"]
    DATABRICKS[("approved marts<br/>Delta copy for BI")]:::bi
    BLOCKED["excluded: PII<br/>mart_driver_scorecard_monthly"]:::blocked
  end

  subgraph control_audit["Control and audit"]
    CTRL[("control.* + audit.*<br/>batches + evidence")]:::ctrl
  end

  STREAMLIT["Streamlit site<br/>reads 3 audit views"]:::orch

  subgraph speed["Speed layer (opt-in)"]
    SPEED["Kafka + Spark<br/>fixture replay"]:::speed
    REALTIME[("realtime.*")]:::speed
    STREAMHEALTH[("audit.stream_health")]:::ctrl
  end

  MYSQL --> ING
  MONGO --> ING
  ING --> BRONZE
  BRONZE --> SILVER --> GOLD --> MARTS
  MARTS --> PUB --> DATABRICKS
  MARTS -. "not published" .-> BLOCKED

  AF -. triggers .-> ING
  AF -. triggers .-> dbt
  AF -. triggers .-> PUB

  ING -. watermarks .-> CTRL
  dbt -. invocations .-> CTRL
  PUB -. reconciliation .-> CTRL
  CTRL -. read only .-> STREAMLIT

  SPEED --> REALTIME
  SPEED -. health .-> STREAMHEALTH

  style speed stroke-dasharray:6 4
```

The batch path above is the governed warehouse. Two observers run alongside it: the opt-in Kafka/Spark speed layer writes only `realtime.*` plus append-only health rows, and the Streamlit site reads only three `audit` views. Neither gates the daily batch. See the [Kafka runbook](kafka/README.md) and [architecture section 8](ARCHITECTURE.md#8-streamlit-project-site).

### Layer colours

| Colour | Class | Used for |
|---|---|---|
| blue | `src` | MySQL and MongoDB sources |
| orange | `bronze` | Landed tables and ingestion |
| purple | `silver` | Silver models |
| green | `gold` | Seven dimensions, including `dim_date`, and six facts |
| teal | `mart` | Six BI marts, one reconciliation model and the publisher |
| amber | `bi` | Databricks Delta copy |
| pink | `ctrl` | Control, audit and excluded PII paths |
| light blue | `orch` | Airflow and Streamlit |
| violet | `speed` | Kafka, Spark and `realtime.*` |

## Daily run

```mermaid
flowchart TD
  classDef orch fill:#e1f5fe,stroke:#0277bd,stroke-width:2px,color:#01579b
  classDef ctrl fill:#fce4ec,stroke:#c62828,stroke-width:2px,color:#b71c1c
  classDef bronze fill:#fff3e0,stroke:#ef6c00,stroke-width:2px,color:#e65100
  classDef silver fill:#f3e5f5,stroke:#6a1b9a,stroke-width:2px,color:#4a148c
  classDef gold fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
  classDef mart fill:#e0f7fa,stroke:#00838f,stroke-width:2px,color:#006064

  PF["preflight<br/>env, connectivity, contracts"]:::orch
  SB["start_batch"]:::ctrl

  subgraph INGEST["Ingestion: 12 tables"]
    M1["MySQL tier 1<br/>drivers, trucks, trailers,<br/>customers, routes, facilities"]:::bronze
    M2["MySQL tier 2-4<br/>loads, trips, fuel_purchases"]:::bronze
    MG["MongoDB tier 1<br/>delivery_events, maintenance_records,<br/>safety_incidents"]:::bronze
    M1 --> M2
  end

  ID["ingest_done<br/>assert 12 task rows"]:::ctrl

  subgraph DBT["dbt: run, then test, per stage"]
    S["silver"]:::silver --> G["gold<br/>dims, then facts"]:::gold --> M["marts"]:::mart
  end

  PUB["publish_marts<br/>5 approved marts to Databricks"]:::mart
  NS["notify_summary<br/>finish batch, send summary"]:::orch

  ANY(("any task<br/>fails")):::ctrl
  FAIL["on_dag_failure<br/>finish FAILED, pg_notify,<br/>redacted alert"]:::ctrl

  PF --> SB
  SB --> M1
  SB --> MG
  M2 --> ID
  MG --> ID
  ID --> S
  M --> PUB --> NS
  ANY -.-> FAIL
```

DAG file: `airflow/dags/fleet_dbt_daliy.py`. Schedule: `0 2 * * *` in `Asia/Kolkata`; `catchup=False`, `max_active_runs=1`, 6-hour timeout. Ingest: 2 retries, 5–30 minute backoff, 45-minute timeout, pools `mysql_pool`/`mongo_pool`; dbt: no retries; publish: 1 retry after 10 minutes, 90-minute timeout. `on_dag_failure` closes the batch as `FAILED` and sends one redacted alert; the database trigger emits `pg_notify`.

## Project at a glance

| Metric | Value | Source |
|---|---|---|
| Source tables / collections | 12 (9 MySQL, 3 MongoDB) | `contracts/sources/*.yml` |
| dbt models | 32 (12 silver, 13 gold, 7 marts) | `dbt/target/manifest.json` |
| dbt tests | 391 schema tests (401 manifest nodes, 0 warn), including 3 unit tests | `dbt/target/manifest.json` |
| Published marts | 5 | `scripts/publish_marts.py` |
| Python/Airflow tests | 230 collected (186 + 44) | pytest collection |
| CI jobs | 9, plus 1 secret-scan job | `.github/workflows/` |

## Data quality and testing

391 dbt schema tests split 138 silver, 202 gold and 51 marts, expanded to 401 manifest nodes with 0 warn; 3 dbt unit tests and 3 snapshots are committed, all contracts are enforced and all relationships are error severity. Python has 186 collected tests and Airflow 44; coverage is gated at `COV_MIN=80`. 12 source contracts are checked and CI has 9 jobs plus Gitleaks. Full gate table: [data quality](docs/DATA_QUALITY.md); inventory: [testing](docs/TESTING.md); site numbers: [site facts](site/SITE.md).

## Key design decisions

dbt owns transformation and Python only moves data. Watermark lookback, one writer per layer, current-state SCD1 dimensions and a five-mart publication allowlist keep replay deterministic and PII out of Databricks. Full trade-offs: [design decisions](docs/DESIGN_DECISIONS.md).

## Mart layer

| Mart | Grain | Published |
|---|---|---|
| `mart_sales_daily` | Load date × customer × route | Yes |
| `mart_equipment_utilization_daily` | Dispatch date × truck × trailer | Yes |
| `mart_delivery_performance_daily` | Scheduled date × facility × event type | Yes |
| `mart_driver_scorecard_monthly` | Month × driver | No — driver display name |
| `mart_customer_service_monthly` | Month × customer | Yes |
| `mart_truck_cost_monthly` | Month × truck | Yes |

`assert_marts_reconcile_to_facts` is the seventh model in `dbt/models/marts/`; it is a reconciliation table, not a BI mart.

## Business insights

Full analysis: [reports/INSIGHT.md](reports/INSIGHT.md). Charts and findings are regenerated from `gold` tables by `make report`. Figures come from a populated synthetic dataset behind the committed report artefacts; `make demo` loads only small `contracts/samples` fixtures and full `data/` extracts are not tracked.

| Question | Finding | Source |
| --- | --- | --- |
| Where is revenue concentrated? | First Group generated $9.14M (3.5% of load revenue); the top 10 customers generated 19.8%. | `reports/findings/01_revenue_concentration.json` |
| Where does service perform worst? | Atlanta Distribution Center had the lowest high-volume on-time rate at 53.4%. | `reports/findings/02_delivery_service.json` |
| Where is cost risk concentrated? | 170 safety incidents produced $2.65M in claims; 37.6% were preventable. | `reports/findings/05_safety.json` |
| Did the Kafka speed layer hold up? | 45 micro-batches drained 341,640 lifecycle events with 0 contract rejects; on-time share 55.7% matched gold. | `reports/findings/08_streaming.json` |

### Detailed insight: delivery on-time rate and detention cost

Overall on-time performance was **55.7%** across **170,820** delivery events, with estimated detention cost of **$19.5M** at $75/hour. Atlanta Distribution Center was the weakest high-volume facility at **53.4% on time** across 3,278 events and **$379K** estimated detention. First Group was the highest detention-cost customer at **$689K** across 6,002 events.

![Bar chart of on-time rate for the ten lowest-performing high-volume facilities](reports/img/02_delivery_service_on_time_by_facility.png)

![Bar chart of estimated detention cost for the top detention-cost customers](reports/img/02_delivery_service_detention_cost_by_customer.png)

**So what:** Low on-time rates and long dwells compound: missed appointments consume capacity, add detention charges, and weaken customer trust. Start with a root-cause review at Atlanta Distribution Center and First Group dwell limits, then track on-time rate and detention cost per customer after changes.

Source: `gold.fct_delivery_events` joined to `gold.dim_facilities` and `gold.dim_customers`; chart generated by `reports/scripts/02_delivery_service.py`.

### Detailed insight: Kafka speed layer held the contract end-to-end

A full fixture replay into `dev.fleet.delivery_events.v1` was consumed by Spark in **45 micro-batches**, landing **341,640** lifecycle rows (170,820 ARRIVED / 170,820 DEPARTED) with **0 contract rejects**. On-time share in `realtime.delivery_events_raw` was **55.7%**, matching gold. The watchdog peaked at **147,517** messages of backlog while draining (expected for a large replay at `maxOffsetsPerTrigger=10000`), and detention alerts settled at **147,978 RESOLVED / 370 OPEN**.

![Line chart of raw, window and alert row counts across stream micro-batches](reports/img/08_streaming_batch_throughput.png)

![Bar chart of accepted, late-dropped and contract-rejected share of micro-batch reads](reports/img/08_streaming_event_quality.png)

**So what:** Zero rejects and a balanced lifecycle split show the strict contract, producer and DLQ path stayed aligned under load. Treat lag as transient drain signal (not broker failure) until the watchdog returns `OK topic-high-low`; the OPEN detention alerts are the live operator queue.

Source: `audit.stream_health`, `realtime.*`, `control.stream`; chart generated by `reports/scripts/08_streaming.py`.

## Limitations and known gaps

- Coverage is gated at `COV_MIN=80` with `make py-cov`; `make pipeline-stats ENV=dev` reports typical run time and largest loaded table read-only from `audit.*` and catalog metadata.
- All 32 dbt models enforce contracts; 3 unit tests and 3 snapshots are committed.
- All dbt `relationships` tests are error severity; Airflow dbt run tasks publish manifest-keyed assets and `fleet_batch_cleanup` fails stale batches when enabled.

## Repository map

<details>
<summary>Repo Tree</summary>

```text
├── AGENTS.md / CLAUDE.md                   # agent rules and Claude-specific notes
├── CHANGELOG.md                            # Keep a Changelog release history
├── Makefile                                # single entry point; run make help
├── app.py / index.html                     # Streamlit host and project site
├── pyproject.toml / uv.lock                # dependencies, pytest and Ruff config
├── requirements.txt                        # Streamlit runtime dependencies
├── .env.example                            # every config knob, placeholders only
├── .githooks/                              # commit-msg and pre-push guards
├── .streamlit/                             # Streamlit config (secrets template; secrets.toml ignored)
├── site/                                   # project-site facts and scripts
│   ├── facts.json                          # generated site facts with provenance
│   └── scripts/                            # site render/check/facts + embedded audit JSON
├── assets/                                 # logo and committed screenshots
│   ├── logo/
│   └── screenshots/pipeline_flow.svg
├── contracts/                              # source-of-truth schemas: 12 sources, 1 stream, samples
├── scripts/                                # runnable pipeline steps
│   ├── ingestion.py                        # MySQL + MongoDB → bronze, watermarks, audit
│   ├── run_dbt.py                          # dbt runner + audit artifact loader
│   ├── publish_marts.py                    # approved Postgres marts → Databricks
│   ├── run_notebooks.py                    # reproducible notebook runner
│   └── stream_*.py                         # thin Kafka/Spark/stream-admin launchers
├── utils/                                  # shared plumbing: config, connections, audit, logger
│
├── dbt/                                    # transformation layer
│   ├── models/                             # bronze sources, silver (12), gold (13), marts (7)
│   ├── macros/                             # cleaning, incremental, gold and mart helpers
│   └── tests/generic/silver/               # project-specific generic tests
│
├── sql/
│   ├── scripts/                            # bootstrap 01..08 applied by make db-init
│   └── metadata/                           # read-only inspection queries
│
├── airflow/                                # orchestration stack (Dockerfile, compose, README)
│   ├── dags/                               # fleet_dbt_daliy, fleet_report_weekly, fleet_watchdog
│   ├── helpers/                            # preflight, callbacks, notify, templates
│   └── tests/                              # DAG unit tests
│
├── kafka/                                  # opt-in speed layer: contract, package, image, tests
│   ├── fleet_stream/                       # producer, Spark job, admin and monitor modules
│   ├── Dockerfile/compose.yml              # streaming image and `streaming` profile
│   └── README.md                           # runbook for replay, health and reset
│
├── docs/                                   # DBT.md, FINDING.md, TESTING.md
├── tests/                                  # unit / integration / smoke
├── helpers/                                # shell utilities (health, backup, logs, env protection)
└── notebooks/                              # EDA notebooks
```

</details>

## Makefile

`make help` is the authoritative target list. Full list: [Make targets](docs/MAKEFILE.md). Configuration: `ENV=dev|ci|prod`, `SELECT=<dbt selector>`, `EXCLUDE=<resource type>`, `CONFIRM=yes`, `V=1`.

<details>
<summary>Common commands</summary>

```bash
# Setup and local checks
make install                                                    # create/update .venv from uv.lock
make check-tools                                                # verify bash, uv and awk
make env-check                                                  # verify required environment variables
make contracts-check                                            # validate contract index and dbt bronze sources
make lint-fast                                                  # Ruff lint and format check
make py-test                                                    # run Python test suite
make verify                                                     # lint-fast + py-test; aliases: make ci, make check

# Zero-credential fixture demo in fleet_dev
make demo                                                       # build fixture demo in fleet_dev

# Warehouse bootstrap and ingestion
make db-init CONFIRM=yes                                        # create database and apply bootstrap scripts
make ingest ENV=dev                                             # load MySQL/Mongo sources into bronze

# dbt
make dbt-deps                                                   # install dbt packages
make dbt-parse ENV=dev                                          # parse dbt project
make dbt-build ENV=dev                                          # run dbt models and tests
make dbt-test ENV=dev                                           # run dbt tests
make dbt-docs ENV=dev                                           # generate dbt docs
make dbt-build ENV=dev SELECT=my_model+                         # build selected model and downstream

# One layer at a time
make dbt-silver ENV=dev && make dbt-silver-tests ENV=dev        # run and test silver
make dbt-gold ENV=dev && make dbt-gold-tests ENV=dev            # run and test gold
make dbt-marts ENV=dev && make dbt-marts-tests ENV=dev          # run and test marts

# Publishing and Airflow
make publish-marts ENV=dev                                      # publish approved marts to Databricks
make airflow-test                                               # run Airflow orchestration tests
make airflow-up                                                 # start local Airflow stack
make airflow-down                                               # stop local Airflow stack

# Streamlit project site
make streamlit                                                  # serve app.py with the live audit panel

# Opt-in Kafka speed layer
make stream-up                                                  # start Kafka, topics, registry and supervised job
make stream-produce                                             # replay fixture or live delivery_events
make stream-status                                              # show registration and latest health evidence
make stream-monitor                                             # probe Kafka lag and freshness once
make stream-reconcile BATCH_ID=42                               # compare realtime with gold observationally
make stream-demo                                                # zero-credential fixture replay into fleet_dev
make stream-reset CONFIRM=yes                                   # drop checkpoints and truncate realtime only
make stream-test                                                # run broker-free streaming unit tests

# Reports and cleanup
make report                                                     # build charts and findings JSON
make report-clean                                               # remove generated report artifacts
make clean                                                      # remove caches and generated output
```

</details>

## Author, contact and licence

Nitin — [GitHub @Nitinx12](https://github.com/Nitinx12) · nitin321x@gmail.com

Released under the [MIT License](LICENSE).
