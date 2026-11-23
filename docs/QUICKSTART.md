# Quickstart

Get the Fleet dbt Warehouse running locally, load sample data, build dbt, and generate reports.

## Prerequisites

- Git, `uv`, Docker + Docker Compose, PostgreSQL client (`psql`, `createdb`)
- Access to a local/dev Postgres and (for live ingest) MySQL/MongoDB sources
- Copy `.env.example` to `.env` and fill placeholders; never commit `.env`

## 1. Install and verify

```bash
cp .env.example .env
make install
make env-check
make contracts-check
```

`make help` is the authoritative target list. Use `ENV=dev` for local dbt runs.

## 2. Bootstrap Postgres

Creates warehouse schemas, control/audit objects, and bootstrap scripts in the configured Postgres DB.

```bash
make db-init CONFIRM=yes
```

For CI-style audit bootstrap only: `make PG_DBNAME=fleet_dev DB_NAME=fleet_dev CONFIRM=yes db-init-audit`.

## 3. Load data

**Sample fixtures (safe local path):** loads `contracts/samples/*.json` into `bronze`. Guarded to `fleet_dev` / `fleet_it_*` and requires `FIXTURE_LOAD=1`.

```bash
make PG_DBNAME=fleet_dev load-fixtures ENV=ci
```

**Live ingestion:** reads MySQL `fleet_registry` and MongoDB `fleet_operations`, writes to Postgres `bronze` with watermarks/audit.

```bash
make ingest
```

## 4. Build dbt

```bash
make dbt-deps
make dbt-parse ENV=dev
make dbt-build ENV=dev
make dbt-test ENV=dev
```

Layer targets: `dbt-silver`, `dbt-gold`, `dbt-marts` (and matching `*-tests`). Destructive/full rebuilds require confirmation: `make dbt-full-refresh ENV=dev CONFIRM=yes SELECT=...`.

## 5. Analytics reports

Reports read only Postgres `gold` tables, aggregate in SQL, and write images/findings.

```bash
make report
make report-clean   # remove generated reports/img/*.png and reports/findings/*.json
```

Outputs: `reports/img/*.png`, `reports/findings/*.json`, `reports/INSIGHT.md`.

## 6. Airflow (optional)

Runs the local orchestration stack. Use `CONFIRM=yes` for destructive/trigger/clear DAG actions.

```bash
make airflow-up
make airflow-dag-list
make airflow-dag-status AIRFLOW_DAG_ID=fleet_daily_load AIRFLOW_RUN_ID=<run_id>
make airflow-down
```

## Common checks

```bash
make lint-fast
make py-test
make airflow-test
make verify
```

## Notes

- Run local work against dev/CI databases only; never target production without explicit approval.
- `db-init`, `load-fixtures`, full refresh, deploy, and DAG clear/trigger have safety guards; read the Make target before bypassing.
- For details: `docs/DBT.md`, `docs/TESTING.md`, `airflow/README.md`, `contracts/README.md`, `DEPLOYMENT.md`.
