# Fleet dbt Warehouse build entry point. Run `make help`; add targets as `name: ## summary`.
# Configuration: ENV=dev|ci|prod SELECT=<dbt selector> EXCLUDE=<resource type> CONFIRM=yes V=1.

.DEFAULT_GOAL := help
.SUFFIXES:
.DELETE_ON_ERROR:

SHELL := bash
ifneq ($(wildcard C:/PROGRA~1/Git/bin/bash.exe),)
SHELL := C:/PROGRA~1/Git/bin/bash.exe
endif
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables --no-builtin-rules

# Configuration
UV ?= uv
PSQL ?= psql
ENV ?= dev
SELECT ?=
EXCLUDE ?=
CONFIRM ?=
V ?= 0
DB_USER ?= postgres
DB_NAME ?= fleet_warehouse
PG_HOST ?= localhost
PG_PORT ?= 5432
PG_PASSWORD ?=
PYTHON_VERSION_FILE ?= .python-version
VENV_MARKER ?= .venv/pyvenv.cfg
VENV_STAMP ?= .venv/.installed

-include .env

DBT_SELECT = $(if $(strip $(SELECT)),--select "$(SELECT)",)
DBT_EXCLUDE = $(if $(strip $(EXCLUDE)),--exclude-resource-type "$(EXCLUDE)",)
DBT = "$(UV)" run --no-sync python scripts/run_dbt.py
PSQL_DB = PGPASSWORD="$(PG_PASSWORD)" "$(PSQL)" -h "$(PG_HOST)" -p "$(PG_PORT)" -U "$(DB_USER)"
AIRFLOW_COMPOSE = docker compose --env-file .env -f airflow/compose.yml -f airflow/compose.dev.yml
KAFKA_COMPOSE = docker compose --profile streaming --env-file .env -f kafka/compose.yml
STREAM_IMAGE ?= fleet-stream:local
export STREAM_IMAGE
AIRFLOW_DAG_ID ?= fleet_daily_load
BATCH_ID ?=
AIRFLOW_RUN_ID ?=
AIRFLOW_RUN_ID_ARG = $(if $(strip $(AIRFLOW_RUN_ID)),--run-id "$(AIRFLOW_RUN_ID)",)

ifeq ($(V),1)
Q :=
else
Q := @
endif

define require_cmd
command -v "$(1)" >/dev/null 2>&1 || { printf '%s\n' "make: *** '$(1)' not found. $(2)" >&2; exit 1; }
endef

define require_venv
test -f "$(VENV_MARKER)" || { printf '%s\n' "make: *** virtual environment not found. Run 'make install'." >&2; exit 1; }
endef

define require_environment
case "$(ENV)" in dev|ci|prod) ;; *) printf '%s\n' "make: *** ENV must be dev, ci or prod, got '$(ENV)'." >&2; exit 1 ;; esac
endef

define confirm_production
if [ "$(ENV)" = "prod" ] && [ "$(CONFIRM)" != "yes" ]; then printf '%s\n' "make: *** refusing production operation; rerun with ENV=prod CONFIRM=yes." >&2; exit 1; fi
endef

define confirm_destructive
if [ "$(CONFIRM)" != "yes" ]; then printf '%s\n' "make: *** refusing destructive operation; rerun with CONFIRM=yes." >&2; exit 1; fi
endef

define require_airflow_run_id
test -n "$(AIRFLOW_RUN_ID)" || { printf '%s\n' "make: *** AIRFLOW_RUN_ID is required." >&2; exit 1; }
endef

## Help
.PHONY: help
help: ## Show this help and available configuration.
	$(Q)awk '\
		/^## / { group = substr($$0, 4); seen[group] = 1; next } \
		/^[A-Za-z0-9][A-Za-z0-9_-]*:.*## / { \
			target = $$1; sub(/:.*/, "", target); \
			description = $$0; sub(/^.*## /, "", description); \
			key = group SUBSEP target; groups[key] = group; targets[key] = target; descriptions[key] = description; \
		} \
		function next_group(  candidate, value) { \
			candidate = ""; for (value in seen) if (!(value in printed_group) && (candidate == "" || value < candidate)) candidate = value; return candidate; \
		} \
		function print_group(name,  i, j, key, count, temp) { \
			count = 0; for (key in targets) if (groups[key] == name) ordered[++count] = key; \
			for (i = 1; i <= count; i++) for (j = i + 1; j <= count; j++) if (targets[ordered[j]] < targets[ordered[i]]) { temp = ordered[i]; ordered[i] = ordered[j]; ordered[j] = temp; } \
			if (count) { printf "\\n%s:\\n", name; for (i = 1; i <= count; i++) { key = ordered[i]; printf "  %-20s %s\\n", targets[key], descriptions[key]; } } \
		} \
		END { while ((group = next_group()) != "") { printed_group[group] = 1; print_group(group); } printf "\\nConfiguration: ENV=dev|ci|prod (default dev), SELECT=<dbt selector>, EXCLUDE=<resource type>, CONFIRM=yes, V=1, AIRFLOW_DAG_ID=fleet_daily_load, AIRFLOW_RUN_ID=<run id>\\n"; }' "$(firstword $(MAKEFILE_LIST))"

## Demo
.PHONY: demo
DEMO_DB ?= fleet_dev
DEMO_SCHEMA_PREFIX ?= ci_demo_
demo: check-tools install ## Build a zero-credential fixture demo in fleet_dev.
	$(Q)printf '%s\n' "==> Fixture demo: bronze in $(DEMO_DB), dbt schemas $(DEMO_SCHEMA_PREFIX)*"
	$(Q)env \
		PG_HOST=localhost PG_PORT=5432 PG_USER=postgres PG_PASSWORD=postgres \
		PG_DBNAME="$(DEMO_DB)" DB_NAME="$(DEMO_DB)" \
		MONGO_URI=mongodb://localhost:27017/fleet_operations MONGO_DB_NAME=fleet_operations \
		MYSQL_HOST=localhost MYSQL_PORT=3306 MYSQL_USER=root MYSQL_PASSWORD=postgres \
		MYSQL_DATABASE=fleet_registry MYSQL_SERVER_TZ=UTC MYSQL_SSL_MODE=PREFERRED \
		DATABRICKS_HOST=https://example.databricks.com \
		DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/example \
		DATABRICKS_SCHEMA=marts DATABRICKS_TOKEN=placeholder \
		SMTP_HOST=localhost SMTP_PORT=25 ALERT_EMAIL_FROM=ci@example.com ALERT_EMAIL_TO=ci@example.com \
		DBT_TARGET=ci DBT_SCHEMA_PREFIX="$(DEMO_SCHEMA_PREFIX)" FIXTURE_LOAD=1 \
		$(MAKE) -e ENV=ci CONFIRM=yes EXCLUDE=unit_test \
		env-check contracts-check db-init-audit load-fixtures dbt-deps dbt-build

## Setup
.PHONY: check-tools install setup
check-tools: ## Verify the base command-line tools are available.
	$(Q)$(call require_cmd,bash,Install Git Bash or another Bash implementation.)
	$(Q)$(call require_cmd,$(UV),Install uv from https://docs.astral.sh/uv/.)
	$(Q)$(call require_cmd,awk,Install a POSIX-compatible awk implementation.)
	$(Q)printf '%s\n' "Tool check passed."

$(VENV_STAMP): uv.lock pyproject.toml
	$(Q)$(call require_cmd,$(UV),Install uv from https://docs.astral.sh/uv/.)
	$(Q)printf '%s\n' "==> Syncing locked Python dependencies"
	$(Q)"$(UV)" sync --frozen
	$(Q)touch "$(VENV_STAMP)"

install: $(VENV_STAMP) ## Create or update .venv from uv.lock.

setup: install ## Alias for install.

.PHONY: check-venv check-version
check-venv:
	$(Q)$(call require_venv)

check-version: check-tools check-venv
	$(Q)test -f "$(PYTHON_VERSION_FILE)" || { printf '%s\n' "make: *** $(PYTHON_VERSION_FILE) is missing." >&2; exit 1; }
	$(Q)actual="$$("$(UV)" run --no-sync python -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')"; expected="$$(tr -d '\r\n' < "$(PYTHON_VERSION_FILE)")"; if [ "$$actual" != "$$expected" ]; then printf '%s\n' "warning: Python $$actual is installed; $(PYTHON_VERSION_FILE) pins $$expected." >&2; fi

## Quality
.PHONY: lint-fast lint fmt format py-test py-unit py-integration py-smoke test check ci verify
lint-fast: check-version ## Run Ruff lint and formatting checks.
	$(Q)printf '%s\n' "==> Running Ruff checks"
	$(Q)"$(UV)" run --no-sync ruff check .
	$(Q)"$(UV)" run --no-sync ruff format --check .

lint: lint-fast ## Alias for lint-fast.

fmt: check-version ## Apply Ruff formatting and safe fixes.
	$(Q)printf '%s\n' "==> Formatting Python sources"
	$(Q)"$(UV)" run --no-sync ruff format .
	$(Q)"$(UV)" run --no-sync ruff check --fix .

format: fmt ## Alias for fmt.

py-test: check-version ## Run the Python test suite.
	$(Q)printf '%s\n' "==> Running Python tests"
	$(Q)"$(UV)" run --no-sync pytest

py-unit: check-version ## Run fast isolated unit tests.
	$(Q)"$(UV)" run --no-sync pytest tests/unit

py-integration: check-version ## Run tests against a throwaway Postgres database.
	$(Q)"$(UV)" run --no-sync pytest tests/integration

py-smoke: check-version ## Run post-build liveness checks.
	$(Q)"$(UV)" run --no-sync pytest tests/smoke

test: py-test ## Alias for py-test.

verify: lint-fast py-test site-browser-smoke ## Run all non-destructive local quality checks.

check: verify ## Alias for verify.

ci: verify ## Run the checks supported by the tracked repository.

## Site
.PHONY: site-facts site-check site-render site-browser-smoke
site-facts: check-version ## Regenerate site/facts.json from repository sources.
	$(Q)"$(UV)" run --no-sync python scripts/site_facts.py

site-check: check-version ## Check every number on index.html against facts.
	$(Q)"$(UV)" run --no-sync python scripts/check_site_numbers.py --strict

site-render: site-check ## Render the standalone site into build/site.html.
	$(Q)"$(UV)" run --no-sync python scripts/render_site.py

site-browser-smoke: site-render ## Run the zero-dependency browser smoke check.
	$(Q)node tests/browser/site_smoke.mjs

## Contracts
.PHONY: contracts-check
contracts-check: check-version ## Validate source, stream and dbt contract alignment.
	$(Q)"$(UV)" run --no-sync python scripts/check_contracts.py

## Airflow
.PHONY: airflow-test airflow-up airflow-down airflow-dag-list airflow-dag-imports airflow-dag-runs airflow-dag-status airflow-dag-clear airflow-dag-trigger
airflow-test: check-version ## Run Airflow orchestration tests.
	$(Q)PYTHONPATH=airflow "$(UV)" run --no-sync pytest \
		airflow/tests/test_dag_integrity.py airflow/tests/test_daily_dag_structure.py \
		airflow/tests/test_ingest_tiers.py airflow/tests/test_airflow_helpers.py \
		airflow/tests/test_commads.py airflow/tests/test_notify.py airflow/tests/test_redact.py \
		airflow/tests/test_templates.py airflow/tests/test_prefilght.py \
		airflow/tests/test_finalize_audit.py airflow/tests/test_audit_reader.py \
		airflow/tests/test_callbacks.py airflow/tests/test_summary.py airflow/tests/test_watchdog.py

airflow-up: ## Start the local Airflow stack.
	$(Q)$(AIRFLOW_COMPOSE) up -d --build

airflow-down: ## Stop the local Airflow stack.
	$(Q)$(AIRFLOW_COMPOSE) down

airflow-dag-list: ## List Airflow DAGs.
	$(Q)$(AIRFLOW_COMPOSE) exec -T airflow-worker airflow dags list

airflow-dag-imports: ## List Airflow DAG import errors.
	$(Q)$(AIRFLOW_COMPOSE) exec -T airflow-worker airflow dags list-import-errors

airflow-dag-runs: ## List DAG runs for AIRFLOW_DAG_ID.
	$(Q)$(AIRFLOW_COMPOSE) exec -T airflow-worker airflow dags list-runs "$(AIRFLOW_DAG_ID)" -o json

airflow-dag-status: ## Show DAG run and task states for AIRFLOW_RUN_ID.
	$(Q)$(call require_airflow_run_id)
	$(Q)$(AIRFLOW_COMPOSE) exec -T airflow-worker airflow dags state "$(AIRFLOW_DAG_ID)" "$(AIRFLOW_RUN_ID)"
	$(Q)$(AIRFLOW_COMPOSE) exec -T airflow-worker airflow tasks states-for-dag-run "$(AIRFLOW_DAG_ID)" "$(AIRFLOW_RUN_ID)"

airflow-dag-clear: ## Clear one DAG run; requires AIRFLOW_RUN_ID and CONFIRM=yes.
	$(Q)$(call require_airflow_run_id)
	$(Q)$(call confirm_destructive)
	$(Q)$(AIRFLOW_COMPOSE) exec -T airflow-worker airflow dags clear "$(AIRFLOW_DAG_ID)" --run-id "$(AIRFLOW_RUN_ID)" --yes

airflow-dag-trigger: ## Trigger a DAG run; requires CONFIRM=yes.
	$(Q)$(call confirm_destructive)
	$(Q)$(AIRFLOW_COMPOSE) exec -T airflow-worker airflow dags trigger "$(AIRFLOW_DAG_ID)" $(AIRFLOW_RUN_ID_ARG) -o json

## Streaming
.PHONY: stream-build stream-up stream-down stream-status stream-monitor stream-reconcile stream-produce stream-reset stream-test stream-test-integration stream-demo
stream-build: check-tools ## Build the local stream image.
	$(Q)$(call require_cmd,docker,Install Docker and Docker Compose.)
	$(Q)$(KAFKA_COMPOSE) build stream-job

stream-up: check-tools stream-build ## Start the local Kafka stream stack and register topics.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(call require_venv)
	$(Q)$(call require_cmd,docker,Install Docker and Docker Compose.)
	$(Q)printf '%s\n' "==> Starting streaming stack for ENV=$(ENV)"
	$(Q)$(KAFKA_COMPOSE) up -d kafka kafka-init
	$(Q)"$(UV)" run --no-sync python scripts/stream_admin.py register
	$(Q)$(KAFKA_COMPOSE) up -d stream-job

stream-down: ## Stop the local Kafka stream stack without deleting checkpoints.
	$(Q)$(call require_cmd,docker,Install Docker and Docker Compose.)
	$(Q)$(KAFKA_COMPOSE) down

stream-status: check-version ## Show stream registration and health.
	$(Q)$(call require_environment)
	$(Q)"$(UV)" run --no-sync python scripts/stream_admin.py status

stream-monitor: check-version ## Probe Kafka lag and freshness once.
	$(Q)$(call require_environment)
	$(Q)"$(UV)" run --frozen --group kafka python scripts/stream_monitor.py

stream-reconcile: check-version ## Compare realtime with gold for BATCH_ID.
	$(Q)$(call require_environment)
	$(Q)"$(UV)" run --frozen --group kafka python scripts/stream_reconcile.py --batch-id "$(BATCH_ID)" --airflow-dag-id "$(AIRFLOW_DAG_ID)" --airflow-run-id "$(AIRFLOW_RUN_ID)"

stream-produce: check-version ## Replay delivery events through the stream producer.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)printf '%s\n' "==> Replaying delivery events from STREAM_SOURCE=$${STREAM_SOURCE:-fixture}"
	$(Q)"$(UV)" run --frozen --group kafka python scripts/stream_producer.py

stream-reset: check-version ## Drop checkpoints and truncate realtime; requires CONFIRM=yes.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_destructive)
	$(Q)$(call confirm_production)
	$(Q)$(call require_cmd,docker,Install Docker and Docker Compose.)
	$(Q)CONFIRM="$(CONFIRM)" $(KAFKA_COMPOSE) run --rm stream-job python scripts/stream_admin.py reset

stream-test: check-version ## Run Kafka-free streaming unit tests.
	$(Q)PYTHONPATH=kafka "$(UV)" run --frozen --group kafka --group spark pytest kafka/tests/unit -m unit

stream-test-integration: check-version ## Run streaming tests that need the local Kafka stack.
	$(Q)PYTHONPATH=kafka "$(UV)" run --frozen --group kafka --group spark pytest kafka/tests/integration -m integration

stream-demo: check-tools install ## Build a zero-credential fixture batch and drive the streaming speed layer.
	$(Q)printf '%s\n' "==> Streaming fixture demo in $(DEMO_DB), stream schemas realtime and audit"
	$(Q)env \
		PG_HOST=localhost PG_PORT=5432 PG_USER=postgres PG_PASSWORD=postgres \
		PG_DBNAME="$(DEMO_DB)" DB_NAME="$(DEMO_DB)" \
		MONGO_URI=mongodb://localhost:27017/fleet_operations MONGO_DB_NAME=fleet_operations \
		MYSQL_HOST=localhost MYSQL_PORT=3306 MYSQL_USER=root MYSQL_PASSWORD=postgres \
		MYSQL_DATABASE=fleet_registry MYSQL_SERVER_TZ=UTC MYSQL_SSL_MODE=PREFERRED \
		DATABRICKS_HOST=https://example.databricks.com \
		DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/example \
		DATABRICKS_SCHEMA=marts DATABRICKS_TOKEN=placeholder \
		SMTP_HOST=localhost SMTP_PORT=25 ALERT_EMAIL_FROM=ci@example.com ALERT_EMAIL_TO=ci@example.com \
		DBT_TARGET=ci DBT_SCHEMA_PREFIX="$(DEMO_SCHEMA_PREFIX)" FIXTURE_LOAD=1 \
		KAFKA_BOOTSTRAP_SERVERS=localhost:9094 KAFKA_TOPIC_PREFIX=ci. \
		STREAM_PG_HOST=host.docker.internal WAREHOUSE_NETWORK=fleet-airflow_airflow \
		STREAM_SOURCE=fixture \
		STREAM_REPLAY_SPEED=0 STREAM_LATE_FRACTION=0 STREAM_DUPLICATE_FRACTION=0 \
		$(MAKE) -e ENV=ci CONFIRM=yes EXCLUDE=unit_test \
		env-check contracts-check db-init-audit load-fixtures dbt-deps dbt-build \
		stream-up stream-produce stream-status

## Streamlit
.PHONY: streamlit
streamlit: check-version ## Serve app.py with Streamlit using requirements.txt.
	$(Q)"$(UV)" run --no-sync --with-requirements requirements.txt python -m streamlit run app.py --server.headless true

## Notebooks
.PHONY: notebooks
notebooks: check-version ## Execute EDA notebooks into out/notebooks.
	$(Q)"$(UV)" run --no-sync python scripts/run_notebooks.py

## Environment
.PHONY: env-check
env-check: check-version ## Verify application-required environment variables.
	$(Q)"$(UV)" run --no-sync python -c "import utils.engine; print('env OK: all required variables set')"

## Database and ingestion
.PHONY: db-init db-init-audit ingest load-fixtures publish-marts
db-init: check-tools ## Create the warehouse database and apply SQL bootstrap scripts.
	$(Q)$(call require_cmd,$(PSQL),Install the PostgreSQL client and put psql on PATH.)
	$(Q)$(call confirm_destructive)
	$(Q)$(call confirm_production)
	$(Q)printf '%s\n' "==> Initialising database $(DB_NAME) for ENV=$(ENV)"
	$(Q)$(PSQL_DB) -d postgres -v ON_ERROR_STOP=1 -f "sql/scripts/01_init_schema.sql"
	$(Q)$(PSQL_DB) -d "$(DB_NAME)" -v ON_ERROR_STOP=1 -f "sql/scripts/02_audit_tables.sql"
	$(Q)$(PSQL_DB) -d "$(DB_NAME)" -v ON_ERROR_STOP=1 -f "sql/scripts/03_control_pipeline_registry.sql"
	$(Q)$(PSQL_DB) -d "$(DB_NAME)" -v ON_ERROR_STOP=1 -f "sql/scripts/04_control_batch_tracking.sql"
	$(Q)$(PSQL_DB) -d "$(DB_NAME)" -v ON_ERROR_STOP=1 -f "sql/scripts/05_control_watermarks.sql"
	$(Q)$(PSQL_DB) -d "$(DB_NAME)" -v ON_ERROR_STOP=1 -f "sql/scripts/06_audit_relationships.sql"
	$(Q)$(PSQL_DB) -d "$(DB_NAME)" -v ON_ERROR_STOP=1 -f "sql/scripts/07_pipeline_event_triggers.sql"
	$(Q)$(PSQL_DB) -d "$(DB_NAME)" -v ON_ERROR_STOP=1 -f "sql/scripts/08_streaming.sql"

db-init-audit: check-tools ## Apply audit/control bootstrap scripts to DB_NAME for CI.
	$(Q)$(call require_cmd,$(PSQL),Install the PostgreSQL client and put psql on PATH.)
	$(Q)$(call confirm_destructive)
	$(Q)$(PSQL_DB) -d "$(DB_NAME)" -v ON_ERROR_STOP=1 -c "CREATE SCHEMA IF NOT EXISTS audit; CREATE SCHEMA IF NOT EXISTS control;"
	$(Q)for script in 02_audit_tables.sql 03_control_pipeline_registry.sql 04_control_batch_tracking.sql 05_control_watermarks.sql 06_audit_relationships.sql 07_pipeline_event_triggers.sql 08_streaming.sql; do $(PSQL_DB) -d "$(DB_NAME)" -v ON_ERROR_STOP=1 -f "sql/scripts/$$script"; done
	$(Q)$(PSQL_DB) -d "$(DB_NAME)" -v ON_ERROR_STOP=1 -c "INSERT INTO control.pipeline (pipeline_name, airflow_dag_id, description, schedule_cron, owner) VALUES ('fleet_daily', 'fleet_daily_load', 'Daily fleet telemetry and operations load', '0 2 * * *', 'data-engineering') ON CONFLICT (pipeline_name) DO NOTHING"

ingest: check-version ## Load MySQL and MongoDB sources into the bronze layer.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)printf '%s\n' "==> Ingesting sources into ENV=$(ENV)"
	$(Q)"$(UV)" run --no-sync python scripts/ingestion.py

load-fixtures: check-version ## Load contract samples into bronze; requires FIXTURE_LOAD=1.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)printf '%s\n' "==> Loading contract fixtures into ENV=$(ENV)"
	$(Q)"$(UV)" run --no-sync python scripts/load_fixtures.py

publish-marts: check-version ## Incrementally publish approved Postgres marts to Databricks.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)printf '%s\n' "==> Publishing Postgres marts to Databricks for ENV=$(ENV)"
	$(Q)"$(UV)" run --no-sync python scripts/publish_marts.py

## dbt
.PHONY: dbt-debug dbt-deps dbt-parse dbt-compile dbt-seed dbt-silver dbt-silver-tests dbt-gold dbt-gold-tests dbt-marts dbt-marts-tests dbt-snapshots dbt-build dbt-test dbt-run dbt-docs dbt-full-refresh build
dbt-debug: check-version ## Validate dbt connectivity for ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) debug --target "$(ENV)"

dbt-deps: check-version ## Install dbt packages from dbt/package-lock.yml.
	$(Q)$(DBT) deps

dbt-parse: check-version ## Parse the dbt project without querying the warehouse.
	$(Q)$(call require_environment)
	$(Q)$(DBT) parse --target "$(ENV)"

dbt-compile: check-version ## Compile dbt SQL; optionally set SELECT.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) compile --target "$(ENV)" $(DBT_SELECT)

dbt-seed: check-version ## Load dbt seed files into ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) seed --target "$(ENV)" $(DBT_SELECT)

dbt-silver: check-version ## Run silver models in ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) run --target "$(ENV)" --select silver $(DBT_SELECT)

dbt-silver-tests: check-version ## Test silver models in ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) test --target "$(ENV)" --select silver $(DBT_SELECT)

dbt-gold: check-version ## Run gold models in ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) run --target "$(ENV)" --select gold $(DBT_SELECT)

dbt-gold-tests: check-version ## Test gold models in ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) test --target "$(ENV)" --select gold $(DBT_SELECT)

dbt-marts: check-version ## Run mart models in ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) run --target "$(ENV)" --select marts $(DBT_SELECT)

dbt-marts-tests: check-version ## Test mart models in ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) test --target "$(ENV)" --select marts $(DBT_SELECT)

dbt-snapshots: check-version ## Run dbt snapshots in ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) snapshot --target "$(ENV)" $(DBT_SELECT)

dbt-build: check-version ## Build dbt resources in ENV; optionally set SELECT.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) build --target "$(ENV)" $(DBT_SELECT) $(DBT_EXCLUDE)

build: dbt-build ## Alias for dbt-build.

dbt-test: check-version ## Test all dbt resources in ENV; optionally set SELECT.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) test --target "$(ENV)" $(DBT_SELECT)

dbt-run: check-version ## Run all dbt models in ENV; optionally set SELECT.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) run --target "$(ENV)" $(DBT_SELECT)

dbt-docs: check-version ## Generate local dbt documentation for ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) docs generate --target "$(ENV)"

dbt-full-refresh: ## Rebuild selected dbt resources; requires CONFIRM=yes.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_destructive)
	$(Q)$(call confirm_production)
	$(Q)$(call require_cmd,$(UV),Install uv from https://docs.astral.sh/uv/.)
	$(Q)$(call require_venv)
	$(Q)printf '%s\n' "==> Full-refreshing dbt resources in ENV=$(ENV)"
	$(Q)$(DBT) build --target "$(ENV)" --full-refresh $(DBT_SELECT)

## Deployment
.PHONY: deploy deploy-smoke
deploy: check-tools ## Deploy IMAGE_REF by digest; requires CONFIRM=yes.
	$(Q)$(call confirm_destructive)
	$(Q)bash scripts/deploy.sh deploy

deploy-smoke: check-tools ## Run read-only post-deploy smoke checks.
	$(Q)bash scripts/deploy.sh smoke

## Reports
.PHONY: report report-clean
report: check-venv ## Build the analytics report into reports/img and reports/findings.
	$(Q)printf '%s\n' "==> Building analytics report"
	$(Q)"$(UV)" run --no-sync python -m reports.scripts.build_all

report-clean: ## Remove generated report images and findings.
	$(Q)printf '%s\n' "==> Cleaning report artifacts"
	$(Q)rm -f -- reports/img/*.png reports/findings/*.json

## Clean
.PHONY: clean distclean
clean: ## Remove Python, dbt and generated output artifacts.
	$(Q)printf '%s\n' "==> Removing local generated artifacts"
	$(Q)rm -rf -- ".pytest_cache" ".ruff_cache" "dbt/target" "dbt/logs" "out"
	$(Q)find "utils" "tests" "scripts" -type d -name "__pycache__" -prune -exec rm -rf -- {} +

distclean: ## Remove caches and .venv; requires CONFIRM=yes.
	$(Q)$(call confirm_destructive)
	$(Q)$(MAKE) clean
	$(Q)rm -rf -- ".venv"
