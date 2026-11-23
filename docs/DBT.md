# dbt guide

**Purpose:** Explain how dbt sources, layers, macros, variables, and commands are structured in this warehouse.

**Audience:** Analytics engineers and reviewers changing models, tests, or contracts.

[Back to README](../README.md) · [Architecture](ARCHITECTURE.md)

dbt owns every transformation in this warehouse. Python and dlt-style ingestion
only land data in Postgres `bronze`; dbt reads `bronze` and writes `silver`,
`gold` and `marts`. The synced Databricks `gold` copy is read-only outside the
publish job.

## Layout

| Path | Purpose |
|---|---|
| `dbt/dbt_project.yml` | Project name, layer schemas, default materialisations and shared vars. |
| `dbt/profiles.yml` | Postgres targets `dev`, `ci` and `prod`; `dev` uses `DBT_DEV_SCHEMA`, `ci` uses `DBT_SCHEMA_PREFIX`. |
| `dbt/packages.yml` | `dbt_utils` dependency; lockfile is `dbt/package-lock.yml`. |
| `dbt/models/bronze/sources.yml` | The only dbt source: `bronze` with 12 landed tables. |
| `dbt/models/silver/` | 12 staging models and `schema.yml`. |
| `dbt/models/gold/` | 7 SCD1 dimensions (including `dim_date`), 6 incremental facts and `schema.yml`. |
| `dbt/models/marts/` | 6 BI marts, one reconciliation table model and `schema_marts.yml`. |
| `dbt/macros/` | Cleaning, schema, incremental, gold and mart helper macros. |
| `dbt/tests/generic/silver/` | 11 project-specific generic tests. |
| `dbt/target/` | Generated manifest, compiled SQL and run artifacts; not committed. |

`make help` is the authoritative command list. All dbt Make targets call
`scripts/run_dbt.py`, which records invocations and artifacts in the audit
tables.

## Sources and contracts

`bronze` is immutable to dbt. Source tables are declared once in
`dbt/models/bronze/sources.yml` and must always be referenced with
`{{ source('bronze', '<landing_table>') }}`.

- 9 MySQL sources: `mysql__customers`, `drivers`, `facilities`,
  `fuel_purchases`, `loads`, `routes`, `trailers`, `trips`, `trucks`.
- 3 MongoDB sources: `mongo__delivery_events`, `maintenance_records`,
  `safety_incidents`.
- Source contracts live in `contracts/sources/*.yml`, with sample payloads in
  `contracts/samples/`.
- `make contracts-check` validates the contract index, YAML source names and
  bronze landing-table names. It does not yet check column-level drift.

## Layers

| Layer | Models | Materialisation | Responsibility |
|---|---:|---|---|
| `silver` | 12 | Incremental, `delete+insert` | Rename, cast, dedupe, validate and normalise one bronze table per model. Use `bronze_incremental_filter()` and `silver_lookback_hours`. |
| `gold` | 13 | Dimensions are tables; facts are incremental `delete+insert` | Business keys, Unknown members, SCD1 dimensions, facts, PII hashing/masking and watermarks. Facts use `gold_watermark()` with `gold_lookback_hours`. |
| `marts` | 7 | 6 incremental marts plus one table model | Denormalised BI grains with `mart_key`, additive measures, guarded ratios and `mart_watermark()`. Only approved non-PII marts are published to Databricks. |

`dim_date` is generated from `gold_date_start` and `gold_date_end`.
Dimensions are SCD1: current values overwrite previous values; there is no SCD2
history in the current project.

`assert_marts_reconcile_to_facts` lives under `models/marts/` and is a normal
table model, not a dbt singular test. It returns rows where a mart and its fact
source disagree; query it as a table or select it explicitly in a downstream
check.

Every model and column should have a description and `meta` with `owner`,
`tier`, `serve` and `contains_pii`. Primary keys carry `unique` and `not_null`;
logical foreign keys carry `relationships` tests.

## Macros

| Macro file | Macros | Use |
|---|---|---|
| `cleaning.sql` | `clean_string`, `clean_date`, `clean_timestamp` | Normalise blank strings and empty text dates. |
| `clean_boolean.sql` | `clean_boolean` | Normalise common true/false string spellings. |
| `bronze_incremental_filter.sql` | `bronze_incremental_filter` | Apply the silver lookback window on `_ingested_at`. |
| `generate_schema.sql` | `generate_schema_name` | Use the configured schema verbatim; `ci` prefixes custom layer schemas with `DBT_SCHEMA_PREFIX`. |
| `gold_helpers.sql` | `gold_unknown_key`, `gold_key`, `gold_date_key`, `gold_hash_pii`, `gold_mask_tail`, `gold_watermark` | Surrogate keys, Unknown members, PII handling and fact watermarks. |
| `marts_helpers.sql` | `mart_watermark`, `mart_key`, `mart_hash`, `mart_ratio`, `mart_month_key` | Incremental marts, dimension-fingerprint checks and safe division. |

Custom generic tests in `dbt/tests/generic/silver/` cover freshness, ranges,
non-negative values, state codes, clean text, flag values, date order, scale,
null rate and cleaning loss. The current YAML uses most of them;
`null_rate_below`, `cleaning_loss_below` and `matches_source_key_count` are
defined but unused.

## Configuration and variables

`dbt_project.yml` sets:

- `silver_lookback_hours: 24` for bronze incremental filters.
- `gold_lookback_hours: 3` for fact reprocessing.
- `gold_date_start: '2015-01-01'` and `gold_date_end: '2035-12-31'` for
  `dim_date`.

`profiles.yml` has `dev`, `ci` and `prod`. `dev` writes to
`${DBT_DEV_SCHEMA:-dbt_dev}`; `ci` writes to the `${DBT_SCHEMA_PREFIX:-ci_}`
prefix in `fleet_dev`; `prod` writes to `silver`. Never run `prod` without
explicit approval, and use `ENV=dev` for local work.

## Common commands

```bash
make dbt-deps
make dbt-parse ENV=dev
make dbt-debug ENV=dev
make dbt-compile ENV=dev SELECT="silver"

make dbt-silver ENV=dev
make dbt-silver-tests ENV=dev
make dbt-gold ENV=dev
make dbt-gold-tests ENV=dev
make dbt-marts ENV=dev
make dbt-marts-tests ENV=dev

make dbt-test ENV=dev
make dbt-build ENV=dev
make dbt-docs ENV=dev

make dbt-parse ENV=ci
make load-fixtures ENV=ci
make dbt-build ENV=ci EXCLUDE=unit_test
```

`load-fixtures` requires `FIXTURE_LOAD=1` and refuses any `PG_DBNAME` outside
`fleet_dev` or `fleet_it_*`; CI sets both guards. When `.env` points at another
database, pass `make PG_DBNAME=fleet_dev load-fixtures ENV=ci` locally. The `ci`
target prefixes every custom layer schema so CI never writes to production layer
schemas.

`dbt-full-refresh` and backfills require `CONFIRM=yes` and explicit approval.
Production commands additionally require `ENV=prod CONFIRM=yes`.

## Current state and gotchas

- The parsed manifest currently has 12 sources, 32 models and 383 tests:
  348 at `error` severity and 35 at `warn` severity.
- All 35 gold/mart `relationships` tests are `severity: warn`; silver
  relationships use the default error severity.
- No model has `contract.enforced: true` yet. Incremental models use
  `on_schema_change: append_new_columns`.
- There are no dbt unit tests or snapshots committed yet.
- `contracts-check` is an index/name check, not a full column-contract gate.
- Airflow runs dbt in separate `run` then `test` stages and does not derive
  asset keys from the dbt manifest yet.
