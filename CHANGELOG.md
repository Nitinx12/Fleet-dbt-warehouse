# Changelog

All notable changes to Fleet-dbt-warehouse are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Versions match `pyproject.toml`; releases are cut by pushing a `vX.Y.Z` tag that
the `release` workflow verifies before publishing. No tag has been cut yet, so
all current work is listed under `Unreleased`.

## [Unreleased]

### Added

- Airflow orchestration stack: Docker image, compose files, DAGs, helpers,
  callbacks and operational docs under `airflow/`.
- Airflow and alert environment settings, plus batch-context support in the
  ingestion and dbt runner scripts so CLI and DAG runs share one audit trail.
- Gold layer: 13 models (7 SCD1 dimensions, `dim_date` and 6 incremental facts),
  `gold_helpers` macros, column descriptions and silver-style tests.
- Silver layer: seven models, `schema.yml` tests and three generic tests.
- Databricks publishing for approved non-PII Postgres marts, with schema
  validation, parallel workers, bounded literal `INSERT` batches, swap recovery
  tables and audit reconciliation.
- Audit loaders for dbt run and test results, `audit.dbt_invocation`, foreign
  keys from every audit table to `control.batch`, and the `v_batch_summary`,
  `v_dbt_results` and `v_data_quality_results` dashboard views.
- Read-only `sql/metadata/01..10` scripts for schemas, tables, columns,
  constraints, indexes, views, sequences, sizes and extensions.
- `sql/scripts/07_pipeline_event_triggers.sql`: task failures and retries write
  deduplicated `audit.error_log` rows, while failed or cancelled batches write
  CONTROL error rows and publish `pg_notify('pipeline_failure', json)` on failure.
- `tests/test_pipeline_triggers.py` covering both failure triggers end to end.
- Reproducible notebook runner and generated executed-notebook handling.
- Detailed `README.md` and `airflow/README.md` covering setup, DAG triggering,
  the daily task graph, audit linkage and troubleshooting.
- `docs/DBT.md` and `docs/TESTING.md` guides covering the dbt project and the
  full current test/check inventory.
- Reproducible `reports/` analytics package: read-only Postgres `gold` SQL
  scripts, shared plotting/db helpers, generated PNGs, findings JSON and
  `reports/INSIGHT.md`; Makefile targets `report` and `report-clean`.
- Folder-based pytest organization under `tests/unit/`, `tests/integration/` and
  `tests/smoke/`, with folder-applied markers and a throwaway Postgres fixture
  that applies `sql/scripts/01..07`.
- Smoke coverage for production imports, required environment settings, Postgres
  reachability and safe Makefile targets, plus `make py-unit`,
  `make py-integration` and `make py-smoke`.
- GitHub Actions CI and deploy workflows with SHA-pinned actions, fixture-backed
  dbt builds, image smoke tests, GHCR `sha-<commit>` images, digest-based
  SSH/Compose staging deploys and manually approved production deploys.
- `leaks.yml` runs checksum-verified Gitleaks over full history with a narrow
  `.gitleaks.toml` allowlist; it replaces the duplicate CI secret-scan job.
- `.github/CODEOWNERS`, pull request and issue templates; `scripts/load_fixtures.py`
  and `scripts/deploy.sh`; a `ci` dbt target with prefixed layer schemas.
- `make db-init-audit` to bootstrap audit/control tables in a CI database without
  applying the production-named `01_init_schema.sql`.

### Changed

- Rewrote the root `README.md` as a concise project front door with an updated
  repository map and links to `docs/DBT.md`, `docs/TESTING.md` and
  `airflow/README.md`.
- `make db-init` now applies `sql/scripts/01..07` in order.
- `make airflow-up` and `make airflow-down` pass `--env-file .env` while keeping
  compose-relative build contexts and mounts intact.
- The Airflow image uses uv `0.5.0`, the Python 3.13 constraints file and a
  full Dockerfile-owned `PYTHONPATH` covering the project venv and Airflow user
  site; `airflow/scripts/*.sh` are marked executable at build time.
- `airflow/compose.yml` accepts `AIRFLOW_IMAGE` so CI and deployments can pin a
  digest while local builds retain the `fleet-airflow:local` default.
- `dbt/models/bronze/sources.yml` no longer hard-codes the warehouse database,
  allowing the `ci` profile to resolve sources in `fleet_dev`.
- The root `.dockerignore` excludes local Airflow state, secrets, caches and
  generated artifacts; `airflow/.gitignore` ignores runtime databases, config
  and Python caches.
- `make clean` removes the generated `out/` directory.
- Makefile dbt targets cover seed, silver, gold, marts, snapshots, build and
  test runs; marts models target the `marts` schema.
- Databricks connections use tuned retry settings and the configured merge
  character budget.

### Removed

- Stale references to the placeholder `AIRFLOW.md` and `PROJECT.md` documents
  from the Airflow README.

### Fixed

- Helper scripts now read the project's env names (`PG_DBNAME`, `PG_PASSWORD`,
  `MYSQL_DATABASE`), find `.exe` clients on Windows/WSL and probe with native
  clients before falling back to port checks.
- Renamed `helpers/check_heakth.sh` to `helpers/check_health.sh`; dry-run backups
  no longer require dump tools to be installed.
- `helpers/protect_env.sh` uses anchored path patterns, allows `.env.example`,
  and installs a pre-commit hook pointing at its real location.
- `helpers/monitor_logs.sh` runs under `set -euo pipefail` with guarded probes.
- `.githooks/pre-push` uses LF endings so `set -euo pipefail` works in bash.
- `make airflow-test` runs the orchestration suite from `airflow/tests/` with
  `PYTHONPATH=airflow`.
- DAG integrity, daily structure, ingestion tier and watchdog test paths now
  resolve correctly inside the Airflow image.
- The Airflow image copies the correctly named `requirments-airflow.txt` file,
  and `airflow/compose.yml` no longer overrides `PYTHONPATH`.
- `utils.audit` validates audit labels, target layers, counts and tolerances
  before writing; task audit rows close on `BaseException`.
- Earlier Makefile bootstrap and pre-push hook path breakages.
- Ingestion audit logging and verification were hardened.
- Fixture loading now adds missing referenced rows so the full dbt fixture build
  passes its relationship tests, and CI bootstraps audit/control before dbt so
  the best-effort audit loader can record the invocation.
- CI supplies `DATABRICKS_SCHEMA`, and Makefile psql bootstrap targets connect
  with `PG_HOST`, `PG_PORT` and `PG_PASSWORD` instead of the local Unix socket.
