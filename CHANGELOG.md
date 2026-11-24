# Changelog

**Purpose:** Record notable changes and breaking-contract decisions for each release.

**Audience:** Maintainers, reviewers, and operators upgrading the warehouse.

[Root README](README.md)

All notable changes to Fleet-dbt-warehouse are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Versions match `pyproject.toml`; no release tag has been cut, and no `release.yml`
or `cleanup-ci.yml` workflow is tracked yet. All current work is listed under
`Unreleased`.

## [Unreleased]

### Added

- Streaming foundation: strict `delivery_events.v1` contract, Kafka topic config,
  `realtime`/`control.stream`/`audit.stream_health` DDL, environment placeholders,
  locked Kafka/Spark dependency groups, Make targets and contract alignment checks.
- Kafka-free streaming unit tests for environment guards, strict schema validation,
  contract alignment and ARRIVED/DEPARTED lifecycle expansion.
- Replay producer with fixture/live readers, strict JSON publishing by idempotency key,
  deterministic duplicate and late faults, and a `scripts/stream_producer.py` launcher.
- Spark speed-layer job and opt-in stack: strict value validation with DLQ routing,
  idempotent `realtime` raw upserts, watermarks, 15-minute service windows, detention
  alerts, append-only health records, guarded admin scripts, `kafka/Dockerfile`,
  `kafka/compose.yml` and Kafka-free/static Spark unit tests.
- Airflow stream observability: watchdog lag/freshness probes, redacted breach
  alerts, an observational `reconcile_stream` task that records drift without
  failing the daily batch, thin launchers and the Airflow image's Kafka client.
- CI gates for the speed layer: a broker-free `stream-tests` job and a pinned
  `sha-<commit>-stream` image build with an in-image import smoke check.
- Airflow orchestration stack: Docker image, compose files, DAGs, helpers,
  callbacks and operational docs under `airflow/`.
- Airflow and alert environment settings, plus batch-context support in the
  ingestion and dbt runner scripts so CLI and DAG runs share one audit trail.
- Gold layer: 13 models (7 SCD1 dimensions, `dim_date` and 6 incremental facts),
  `gold_helpers` macros, column descriptions and silver-style tests.
- Silver layer: 12 models, `schema.yml` tests and three generic tests.
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
- `make demo` target for a zero-credential fixture build: contracts check,
  audit/control bootstrap, 12 sample payloads and a full dbt build in `ci_demo_*`
  schemas on `fleet_dev`.
- Report scripts aggregate entirely in Postgres SQL and use pandas only for
  plotting and layout, with `make report` regenerating charts and findings JSON.
- Focused documentation corrections in `contracts/README.md`, `dbt/README.md`,
  `docs/FINDING.md`, `reports/INSIGHT.md`, `ARCHITECTURE.md` and `DEPLOYMENT.md`.

### Changed

- Moved the Streamlit audit payload helper into `.streamlit/streamlit_state.py`; the app and tests now load it from the Streamlit config directory.
- Replaced the static landing-page pipeline cards with a detailed Mermaid diagram
  covering the Airflow-orchestrated batch path, source contracts, PostgreSQL
  warehouse layers, Databricks publication, audit/control plane and the planned
  independent streaming path.
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
- Deploy digest resolution prints registry errors and falls back to `docker pull`
  when `docker buildx imagetools inspect` cannot resolve a pushed `sha-<commit>`
  image.

### Removed

- Stale references to the placeholder `AIRFLOW.md` and `PROJECT.md` documents
  from the Airflow README.

### Fixed

- Streaming wiring now uses one advertised broker listener per network, a local
  `fleet-stream:local` image, the external `9094` port, reachable Postgres hosts in
  Compose, the contract stream name in watchdog probes, and Airflow run ids in
  reconciliation audit rows.
- Producer rejects now reach the prefixed DLQ with a reason and original payload;
  the stream image installs live-source clients, loads Spark defaults, records its
  commit SHA and carries the files its import smoke check needs.
- CI installs stream-test dependencies before running the Kafka-free suite, and
  deploys resolve and export the immutable stream image digest alongside Airflow.
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
