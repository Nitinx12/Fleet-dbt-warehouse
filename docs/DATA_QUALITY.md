# Data quality and testing

**Purpose:** Hold the full verification-gate inventory referenced by the README.

**Audience:** Contributors checking what CI, dbt and Python tests cover.

[Back to README](../README.md) · [Testing inventory](TESTING.md)

| Gate | Verified state | Command |
|---|---|---|
| dbt tests | 391 nodes: silver 138, gold 202, marts 51; 357 error and 35 warn | `make dbt-build ENV=ci` |
| Python tests | 97 collected | `make py-test` |
| Airflow tests | 34 collected | `PYTHONPATH=airflow uv run --no-sync pytest airflow/tests` |
| Contracts | 12 sources aligned by index, YAML and dbt source names | `make contracts-check` |
| CI | 9 jobs in `ci.yml`, plus 1 secret-scan job in `leaks.yml` | `.github/workflows/ci.yml`, `.github/workflows/leaks.yml` |

The 35 `warn` tests are gold and mart `relationships` checks; silver relationships are error severity. A failed contract or data test blocks downstream work, and retries are for transient connection errors only.

Current gaps: dbt model contracts are not enforced (0 of 32), and there are no dbt unit tests or snapshots. Coverage is not configured.
