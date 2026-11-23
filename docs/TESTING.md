# Testing guide

**Purpose:** List the current tests, checks, gates, and known coverage gaps in the repository.

**Audience:** Engineers adding code, reviewing a PR, or running the local quality gates.

[Back to README](../README.md) · [Deployment](DEPLOYMENT.md)

This is the current inventory of tests and checks in the repository. Use
`make help` for the authoritative target list. Run local checks against the
development database only; never run smoke or test commands against production
without explicit approval.

## Quick commands

```bash
make lint-fast                 # Ruff lint + format check
make py-test                   # full root pytest suite
make py-unit                   # fast isolated unit tests
make py-integration            # throwaway Postgres integration tests
make py-smoke                  # post-build liveness checks
make contracts-check           # contract index / source name alignment
make PG_DBNAME=fleet_dev DB_NAME=fleet_dev CONFIRM=yes db-init-audit  # bootstrap audit/control
make PG_DBNAME=fleet_dev load-fixtures ENV=ci  # land sample rows in bronze
make verify                    # lint-fast + py-test
make airflow-test              # orchestration tests with PYTHONPATH=airflow
make dbt-parse ENV=dev         # static dbt parse, no warehouse queries
make dbt-test ENV=dev          # DB-backed dbt tests
```

`make ci` still aliases `verify` for local use. The full pipeline lives in
`.github/workflows/ci.yml` and `.github/workflows/deploy.yml`; see “GitHub
Actions” below.

## Python suite — `tests/`

`make py-test` runs pytest from the repo root. Current collection is 73 tests:
52 `unit`, 5 `integration` and 16 `smoke`. Markers are declared in
`pyproject.toml` and applied automatically from the folder by `tests/conftest.py`.

```text
tests/
├── conftest.py            shared env defaults and folder markers
├── unit/                  no network, no DB, no Docker
├── integration/           real Postgres in a throwaway database
└── smoke/                 fast post-build or post-deploy liveness checks
```

`tests/integration/conftest.py` creates a uniquely named scratch database,
applies `sql/scripts/01..07` in order, yields a SQLAlchemy engine, then drops the
database with `WITH (FORCE)`. Integration tests skip when Postgres is unreachable.

| File | Coverage |
|---|---|
| `conftest.py` | Default env vars so modules import without a real `.env`; folder marker hook. |
| `unit/test_audit.py` | Audit labels, required columns, `TaskHandle` and sanitised errors. |
| `unit/test_connection.py` | Connection getters, quiet logger and Databricks retry configuration. |
| `unit/test_dbt_audit.py` | dbt artifact parsing, invocation rows, stale/missing artifacts and batch linkage. |
| `unit/test_engine.py` | Required env names, fail-closed settings, port casting and loaded config. |
| `unit/test_ingestion.py` | Ingestion CLI defaults and contract selection. |
| `unit/test_load_fixtures.py` | Fixture type mapping, fail-closed guard and source-spec construction. |
| `unit/test_publish_marts.py` | PII exclusion, Databricks type mapping, SQL escaping, batching, char budgets and handled HTTP errors. |
| `unit/test_run_notebooks.py` | Missing data directories before a kernel starts. |
| `integration/conftest.py` | Throwaway Postgres database built from `sql/scripts/01..07`. |
| `integration/test_audit_db.py` | Audit helpers write only allowed rows and roll back cleanly. |
| `integration/test_ingestion_fixture.py` | Bronze primary keys and fixture rows through the bronze write path. |
| `integration/test_notebook_execution.py` | A small notebook runs through a real Jupyter kernel. |
| `integration/test_pipeline_triggers.py` | Failure triggers write one deduplicated `audit.error_log` row per failed task/batch. |
| `smoke/test_env_and_connectivity.py` | Required env settings and a Postgres reachability query. |
| `smoke/test_imports.py` | Production modules import cleanly. |
| `smoke/test_makefile.py` | `help`, `verify`, `contracts-check`, notebook and Airflow targets, plus destructive-target confirmation guards. |

## Airflow suite — `airflow/tests/`

`make airflow-test` runs 14 files with `PYTHONPATH=airflow`: 29 collected,
24 passing and 5 skipped locally. The skips are DAG tests that require the
Airflow Task SDK installed in the Airflow image, not in the local venv.
`airflow/tests/test_run_dbt.py` exists but is not in the Make target; running
`PYTHONPATH=airflow pytest airflow/tests` collects all 31.

| File | Coverage |
|---|---|
| `test_airflow_helpers.py` | Credential/URL redaction, argv builders and identifier validation. |
| `test_audit_reader.py` | Audit readers are SELECT-only; ingestion gate reports missing tasks. |
| `test_callbacks.py` | Failure callbacks and batch finalisation before notification. |
| `test_commads.py` | dbt argv construction and shell-free publish command. |
| `test_dag_integrity.py` | DAG module import and DAG metadata when Airflow SDK is available. |
| `test_daily_dag_structure.py` | Daily task IDs, pools, retries, contract gating and stage order. |
| `test_finalize_audit.py` | Finalizer calls only `control.finish_batch`; rerun start-batch shape. |
| `test_ingest_tiers.py` | Contract coverage, source scoping and acyclic tier map. |
| `test_notify.py` | SMTP STARTTLS multipart email and non-blocking delivery errors. |
| `test_prefilght.py` | Missing setting names and contract failure reporting without secrets. |
| `test_redact.py` | Bearer token and credential URL redaction. |
| `test_run_dbt.py` | Shared-runner argv stripping and batch id propagation. |
| `test_summary.py` | Bounded name lists and failure context from aggregate readers. |
| `test_templates.py` | Text and HTML summary/failure templates. |
| `test_watchdog.py` | Watchdog remains a documented phase-two skeleton. |

## dbt tests — `dbt/`

The parsed manifest currently has 12 sources, 32 models and 383 tests:
348 at `error` severity and 35 at `warn` severity.

| Layer | Tests | Main test types |
|---|---:|---|
| `silver` | 134 | `unique`, `not_null`, `accepted_values`, `relationships`, `non_negative`, `within_range`, `not_in_future`, `max_scale`, `is_us_state_code`, `is_clean_text`, `is_true`, `date_not_before`. |
| `gold` | 198 | Same family as silver, with 24 `relationships` tests at `warn` severity. |
| `marts` | 51 | `unique`, `not_null`, `relationships`, `non_negative`; all relationships are `severity: warn`. |

Project-specific generic tests live in `dbt/tests/generic/silver/`. There are no
dbt unit tests committed yet. `assert_marts_reconcile_to_facts` is a table model
that returns mart/fact mismatches, not a dbt test node; `dbt test` will not
execute it as a test.

Run static parse checks with `make dbt-parse ENV=dev`. Run warehouse-backed
checks with `make dbt-silver-tests ENV=dev`, `make dbt-gold-tests ENV=dev`,
`make dbt-marts-tests ENV=dev` or `make dbt-test ENV=dev`.

## Contracts and fixtures

- 12 source contracts are in `contracts/sources/`.
- 12 sample payloads are in `contracts/samples/`.
- `contracts/sources_index.json` maps 9 MySQL and 3 MongoDB contracts.
- `make contracts-check` verifies that contract filenames match the index,
  YAML source names match the index and contract landing tables match
  `dbt/models/bronze/sources.yml`. It does not compare columns, types or
  sample rows yet.
- `scripts/load_fixtures.py` only writes to `fleet_dev` or `fleet_it_*` databases,
  expands sample rows with missing referenced keys, and requires `FIXTURE_LOAD=1`.

## Operational and helper checks

These are not pytest tests, but they are part of current verification:

```bash
bash helpers/check_health.sh --quick
bash helpers/check_health.sh --only postgres
bash helpers/db_backup.sh --dry-run --db all
bash helpers/monitor_logs.sh summary
bash helpers/monitor_logs.sh clean --dry-run
bash helpers/protect_env.sh --check
make env-check
```

`helpers/check_health.sh` prefers native clients and falls back to port probes;
`helpers/protect_env.sh` scans staged/committed paths and file content for
secrets. None of these helpers is wired into `make verify` yet.

## Git hooks

- `.githooks/commit-msg` enforces Conventional Commits and a 72-character hard
  subject cap.
- `.githooks/pre-push` compiles `utils`, `scripts` and `tests`, then parses all
  `contracts/**/*.json`. It does not run pytest, Ruff or `make contracts-check`.
- `helpers/protect_env.sh --install` can install a pre-commit secret guard; it
  is not installed by default.

## GitHub Actions

`ci.yml` runs on pull requests and pushes to `main`. PR and push runs perform
lint, unit tests, contract checks, `dbt parse`, Airflow tests, integration tests
against a throwaway Postgres service, audit/control bootstrap, and a dbt fixture
build in `ci_pr_<n>_*` schemas. The image build job waits for all checks, builds
the Airflow image with `GIT_SHA`, runs the in-image smoke script and pushes
`ghcr.io/<owner>/<repo>:sha-<commit>` only on `main`.

`leaks.yml` runs Gitleaks over the full history on pull requests, pushes to
`main`, a weekly schedule and manual dispatch. It downloads a checksum-verified
binary and uses `.gitleaks.toml`, which extends the default rules with narrow
allowlists for documented placeholders. No Gitleaks licence secret is required.

`deploy.yml` runs on successful CI completion for `main` and deploys staging,
then runs read-only smoke checks. Production uses `workflow_dispatch` with a
commit SHA, requires the `production` environment approval, and resolves the
same GHCR tag to a digest before deploying that digest. Staging and production
deployments call `scripts/deploy.sh` over SSH and pull the image by digest; no
`latest` tag is used.

Configure these repository secrets before deployment:

| Secret | Use |
|---|---|
| `STAGING_SSH_KEY` | SSH private key for the staging host. |
| `STAGING_DEPLOY_HOST` | Staging host name or address. |
| `STAGING_DEPLOY_USER` | SSH user with Docker Compose access. |
| `STAGING_DEPLOY_PATH` | Repository root on the staging host. |
| `PRODUCTION_SSH_KEY` | SSH private key for the production host. |
| `PRODUCTION_DEPLOY_HOST` | Production host name or address. |
| `PRODUCTION_DEPLOY_USER` | SSH user with Docker Compose access. |
| `PRODUCTION_DEPLOY_PATH` | Repository root on the production host. |

Create GitHub environments named `staging` and `production`. Add required
reviewers to `production` so the manual approval gate is enforced by GitHub.
The remote hosts need Docker Compose, a populated `.env`, and network access to
GHCR. The compose file reads `AIRFLOW_IMAGE` so the deploy can pin a digest.

## Current known gaps

- `make ci` is a local alias for `verify`; the full pipeline runs in GitHub Actions.
- `smoke` tests exist under `tests/smoke/`; no `smoke_prod` tests exist yet.
- No coverage plugin, `[tool.coverage.*]` config or enforced `COV_MIN` gate is
  implemented.
- No dbt unit tests, snapshots or full column-level contract enforcement yet.
- No release or cleanup workflow is tracked yet.

## Adding tests

- Add Python tests to `tests/unit/`, `tests/integration/` or `tests/smoke/` and
  Airflow tests to `airflow/tests/`.
- Folder markers are applied automatically; use explicit markers only for
  `contract` or `smoke_prod`, and never log credentials or row values.
- Add dbt column tests in the layer `schema.yml` files and generic tests under
  `dbt/tests/generic/silver/`.
- Never weaken, disable or delete a test to make a run pass; fix the data or
  the code instead.
