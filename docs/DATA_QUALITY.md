# Data quality and testing

**Purpose:** Hold the full verification-gate inventory referenced by the README.

**Audience:** Contributors checking what CI, dbt and Python tests cover.

[Back to README](../README.md) · [Testing inventory](TESTING.md)

| Gate | Verified state | Command |
|---|---|---|
| dbt tests | 391 schema tests (silver 138, gold 202, marts 51); manifest has 401 test nodes with 0 warn, including 3 unit tests | `make dbt-build ENV=ci`, `make dbt-unit ENV=dev` |
| dbt contracts | All 32 models enforce contracts; mart columns have explicit `data_type`; incremental models use `on_schema_change: fail`; all relationships are error severity | `make dbt-parse ENV=ci` |
| dbt snapshots | 3 snapshots: `snap_customers`, `snap_drivers`, `snap_trucks`; facts and marts remain current-state SCD1 | `make dbt-snapshots ENV=dev` |
| Python tests | 186 collected, coverage TOTAL 84.36% on `scripts` and `utils` with `COV_MIN=80` | `make py-test`, `make py-cov` |
| Airflow tests | 44 collected (230 Python/Airflow combined) | `PYTHONPATH=airflow uv run --no-sync pytest airflow/tests` |
| Airflow assets | Manifest-keyed keys committed in `airflow/assets/dbt_asset_keys.json`; `fleet_batch_cleanup` is opt-in with a 6-hour max age | `make dbt-asset-keys-check` |
| Contracts | 12 sources aligned by index, YAML and dbt source names | `make contracts-check` |
| CI | 9 jobs in `ci.yml`, plus 1 secret-scan job in `leaks.yml`; coverage gate, asset-key check and dbt unit tests are included | `.github/workflows/ci.yml`, `.github/workflows/leaks.yml` |

A failed contract or data test blocks downstream work, and retries are for transient connection errors only. `make pipeline-stats ENV=dev` reports median and p95 run time plus the largest loaded table read-only from `audit.*` and `sql/metadata/09_list_table_sizes.sql`.

Current gaps: the weekly report DAG is still a skeleton; snapshots are not wired into SCD2 fact/mart joins; production data rollback, tested backup/restore and Python version alignment remain open.
