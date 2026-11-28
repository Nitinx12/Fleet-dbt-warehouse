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
| `snapshots/` | `snap_customers`, `snap_drivers`, `snap_trucks` + `schema.yml`. |
| `macros/` | Cleaning, incremental, schema, gold and mart helpers. |
| `tests/generic/silver/` | 11 custom generic tests. |

Generated and not committed: `target/`, `logs/`, `dbt_packages/`.

Three dbt unit tests are declared in the gold and mart YAML; three snapshots
record history but are not wired into fact or mart joins.

## Rules

- Always use `{{ source() }}` and `{{ ref() }}`; never write to `bronze`.
- Every model and column has a description and `meta` (`owner`, `tier`, `serve`, `contains_pii`); keys have `unique`/`not_null` and FKs have error-severity `relationships` tests.
- All 32 models enforce contracts; mart columns have explicit `data_type` and incremental models use `on_schema_change: fail`.
- PII is hashed/masked in `gold`; `mart_driver_scorecard_monthly` stays in Postgres.
- Thresholds are vars, not literals; money is `decimal`; timestamps are UTC.
- `silver` and facts are incremental; use the lookback vars and watermark macros.

## Commands

Run from the repo root; `make help` is authoritative. All dbt targets call `scripts/run_dbt.py` for audit logging.

```bash
make dbt-deps
make dbt-parse ENV=dev
make dbt-asset-keys-check
make dbt-compile ENV=dev SELECT="silver"
make dbt-silver ENV=dev && make dbt-silver-tests ENV=dev
make dbt-gold ENV=dev && make dbt-gold-tests ENV=dev
make dbt-marts ENV=dev && make dbt-marts-tests ENV=dev
make dbt-snapshots ENV=dev
make dbt-unit ENV=dev
make dbt-build ENV=dev
make dbt-test ENV=dev
make dbt-docs ENV=dev
```

`ci` prefixes schemas with `DBT_SCHEMA_PREFIX` in `fleet_dev`. Outside CI, custom schemas resolve verbatim (`silver`, `gold`, `marts`); `DBT_DEV_SCHEMA` only applies to nodes without a custom schema, so use a throwaway `PG_DBNAME` for local experiments. `prod` requires `ENV=prod CONFIRM=yes` and explicit approval. Full refresh/backfills require `CONFIRM=yes`.

The parsed manifest has 32 models, 401 test nodes with 0 warn (391 schema tests split 138 silver / 202 gold / 51 marts, plus 3 unit tests), and 3 snapshots. See the [dbt guide](../docs/DBT.md) for macros, variables, gotchas and the CI fixture flow.
