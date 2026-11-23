# dbt project

**Purpose:** Summarise the dbt project layout, rules, and local commands.

**Audience:** Analytics engineers and reviewers changing models or tests.

[Root README](../README.md) · [dbt guide](../docs/DBT.md)

dbt owns all transformations: Postgres `bronze` (read-only) → `silver` → `gold` → `marts`. Airflow orchestrates; approved non-PII marts are synced to Databricks.

## Layout

| Path | Purpose |
|---|---|
| `dbt_project.yml` | Project name, layer schemas, vars (`silver_lookback_hours`, `gold_lookback_hours`, `gold_date_start/end`). |
| `profiles.yml` | Postgres targets `dev`, `ci`, `prod`. |
| `packages.yml` | `dbt_utils`; lockfile is `package-lock.yml`. |
| `models/bronze/sources.yml` | Only source: 12 landed bronze tables. |
| `models/silver/` | 12 staging models + `schema.yml`. |
| `models/gold/` | 7 SCD1 dimensions (including `dim_date`) and 6 incremental facts + `schema.yml`. |
| `models/marts/` | 6 BI marts, reconciliation model + `schema_marts.yml`. |
| `macros/` | Cleaning, incremental, schema, gold and mart helpers. |
| `tests/generic/silver/` | 11 custom generic tests. |

Generated and not committed: `target/`, `logs/`, `dbt_packages/`.

## Rules

- Always use `{{ source() }}` and `{{ ref() }}`; never write to `bronze`.
- Every model and column has a description and `meta` (`owner`, `tier`, `serve`, `contains_pii`); keys have `unique`/`not_null` and logical FKs have `relationships` tests.
- PII is hashed/masked in `gold`; `mart_driver_scorecard_monthly` stays in Postgres.
- Thresholds are vars, not literals; money is `decimal`; timestamps are UTC.
- `silver` and facts are incremental; use the lookback vars and watermark macros.

## Commands

Run from the repo root; `make help` is authoritative. All dbt targets call `scripts/run_dbt.py` for audit logging.

```bash
make dbt-deps
make dbt-parse ENV=dev
make dbt-compile ENV=dev SELECT="silver"
make dbt-silver ENV=dev && make dbt-silver-tests ENV=dev
make dbt-gold ENV=dev && make dbt-gold-tests ENV=dev
make dbt-marts ENV=dev && make dbt-marts-tests ENV=dev
make dbt-build ENV=dev
make dbt-test ENV=dev
make dbt-docs ENV=dev
```

`dev` writes to `DBT_DEV_SCHEMA` in `fleet_warehouse`; `ci` prefixes schemas with `DBT_SCHEMA_PREFIX` in `fleet_dev`. `prod` requires `ENV=prod CONFIRM=yes` and explicit approval. Full refresh/backfills require `CONFIRM=yes`.

See the [dbt guide](../docs/DBT.md) for macros, variables, gotchas and the CI fixture flow.
