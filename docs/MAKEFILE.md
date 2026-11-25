# Makefile commands

**Purpose:** Keep the full Make target list out of the README while preserving the authoritative local entry point.

**Audience:** Contributors setting up, testing or operating the warehouse locally.

[Back to README](../README.md)

`make help` is the authoritative target list. The output below was generated from that target.

Configuration: `ENV=dev|ci|prod` (default `dev`), `SELECT=<dbt selector>`, `EXCLUDE=<resource type>`, `CONFIRM=yes`, `V=1`, `AIRFLOW_DAG_ID=fleet_daily_load`, `AIRFLOW_RUN_ID=<run id>`.

```text
Airflow:
  airflow-dag-clear    Clear one DAG run; requires AIRFLOW_RUN_ID and CONFIRM=yes.
  airflow-dag-imports  List Airflow DAG import errors.
  airflow-dag-list     List Airflow DAGs.
  airflow-dag-runs     List DAG runs for AIRFLOW_DAG_ID.
  airflow-dag-status   Show DAG run and task states for AIRFLOW_RUN_ID.
  airflow-dag-trigger  Trigger a DAG run; requires CONFIRM=yes.
  airflow-down         Stop the local Airflow stack.
  airflow-test         Run Airflow orchestration tests.
  airflow-up           Start the local Airflow stack.

Clean:
  clean                Remove Python, dbt and generated output artifacts.
  distclean            Remove caches and .venv; requires CONFIRM=yes.

Contracts:
  contracts-check      Validate source, stream and dbt contract alignment.

Database and ingestion:
  db-init              Create the warehouse database and apply SQL bootstrap scripts.
  db-init-audit        Apply audit/control bootstrap scripts to DB_NAME for CI.
  ingest               Load MySQL and MongoDB sources into the bronze layer.
  load-fixtures        Load contract samples into bronze; requires FIXTURE_LOAD=1.
  publish-marts        Incrementally publish approved Postgres marts to Databricks.

Demo:
  demo                 Build a zero-credential fixture demo in fleet_dev.

Deployment:
  deploy               Deploy IMAGE_REF by digest; requires CONFIRM=yes.
  deploy-smoke         Run read-only post-deploy smoke checks.

Environment:
  env-check            Verify application-required environment variables.

Help:
  help                 Show this help and available configuration.

Notebooks:
  notebooks            Execute EDA notebooks into out/notebooks.

Quality:
  check                Alias for verify.
  ci                   Run the checks supported by the tracked repository.
  fmt                  Apply Ruff formatting and safe fixes.
  format               Alias for fmt.
  lint                 Alias for lint-fast.
  lint-fast            Run Ruff lint and formatting checks.
  py-integration       Run tests against a throwaway Postgres database.
  py-smoke             Run post-build liveness checks.
  py-test              Run the Python test suite.
  py-unit              Run fast isolated unit tests.
  test                 Alias for py-test.
  verify               Run all non-destructive local quality checks.

Reports:
  report               Build the analytics report into reports/img and reports/findings.
  report-clean         Remove generated report images and findings.

Setup:
  check-tools          Verify the base command-line tools are available.
  install              Create or update .venv from uv.lock.
  setup                Alias for install.

Streaming:
  stream-build         Build the local stream image.
  stream-demo          Build a zero-credential fixture batch and drive the streaming speed layer.
  stream-down          Stop the local Kafka stream stack without deleting checkpoints.
  stream-monitor       Probe Kafka lag and freshness once.
  stream-produce       Replay delivery events through the stream producer.
  stream-reconcile     Compare realtime with gold for BATCH_ID.
  stream-reset         Drop checkpoints and truncate realtime; requires CONFIRM=yes.
  stream-status        Show stream registration and health.
  stream-test          Run Kafka-free streaming unit tests.
  stream-test-integration Run streaming tests that need the local Kafka stack.
  stream-up            Start the local Kafka stream stack and register topics.

Streamlit:
  streamlit            Serve app.py with Streamlit using requirements.txt.

dbt:
  build                Alias for dbt-build.
  dbt-build            Build dbt resources in ENV; optionally set SELECT.
  dbt-compile          Compile dbt SQL; optionally set SELECT.
  dbt-debug            Validate dbt connectivity for ENV.
  dbt-deps             Install dbt packages from dbt/package-lock.yml.
  dbt-docs             Generate local dbt documentation for ENV.
  dbt-full-refresh     Rebuild selected dbt resources; requires CONFIRM=yes.
  dbt-gold             Run gold models in ENV.
  dbt-gold-tests       Test gold models in ENV.
  dbt-marts            Run mart models in ENV.
  dbt-marts-tests      Test mart models in ENV.
  dbt-parse            Parse the dbt project without querying the warehouse.
  dbt-run              Run all dbt models in ENV; optionally set SELECT.
  dbt-seed             Load dbt seed files into ENV.
  dbt-silver           Run silver models in ENV.
  dbt-silver-tests     Test silver models in ENV.
  dbt-snapshots        Run dbt snapshots in ENV.
  dbt-test             Test all dbt resources in ENV; optionally set SELECT.
```
