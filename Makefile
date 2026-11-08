SHELL := /bin/bash
.DEFAULT_GOAL := help

VENV    ?= .venv
PY      ?= $(VENV)/bin/python
ENV     ?= dev
AIRFLOW_HOME_LOCAL := $(CURDIR)/.airflow_local
LOCAL_AIRFLOW_ENV := AIRFLOW_HOME="$(AIRFLOW_HOME_LOCAL)" \
	AIRFLOW__CORE__DAGS_FOLDER="$(CURDIR)/airflow/dags" \
	AIRFLOW__CORE__PLUGINS_FOLDER="$(CURDIR)/airflow/plugins" \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	PYTHONPATH="$(CURDIR):$(CURDIR)/airflow/plugins" \
	PIPELINE_ENV=local BQ_BACKEND=duckdb RAW_FORMAT=jsonl CDC_MAX_REJECTED_RATIO=0.5 \
	CDC_TASK_RETRIES=0

.PHONY: help venv install lint format typecheck unit-test test local-test local-airflow \
	local-postgres local-postgres-events sql build deploy-aws deploy-gcp verify clean

help: ## Show targets
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-22s %s\n", $$1, $$2}'

venv: ## Create .venv (Python 3.11) with uv
	uv venv --python 3.11 $(VENV)

AIRFLOW_CONSTRAINTS := https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt

install: ## Install Airflow (with official constraints), then the pipeline + dev deps
	uv pip install --python $(PY) -r requirements-airflow.txt --constraint $(AIRFLOW_CONSTRAINTS)
	uv pip install --python $(PY) -r requirements.txt

lint: ## ruff lint + format check + mypy
	$(VENV)/bin/ruff check .
	$(VENV)/bin/ruff format --check .
	$(VENV)/bin/mypy

format: ## Auto-format
	$(VENV)/bin/ruff format .
	$(VENV)/bin/ruff check --fix .

typecheck:
	$(VENV)/bin/mypy

unit-test: ## Fast unit tests (no Spark)
	$(PY) -m pytest -m "not spark"

test: ## All tests incl. local PySpark
	$(PY) -m pytest

local-test: ## Full local pipeline (simulated DMS -> Spark -> local GCS -> DuckDB) + verification
	$(PY) scripts/run_local_pipeline.py --reset --batches 2 --rerun

local-airflow: ## Run the real Airflow DAG locally (airflow dags test, LocalExecutor-free)
	rm -rf local_data
	$(LOCAL_AIRFLOW_ENV) $(VENV)/bin/airflow db migrate > /dev/null
	$(LOCAL_AIRFLOW_ENV) $(PY) bigquery/scripts/deploy_bigquery.py --apply
	$(LOCAL_AIRFLOW_ENV) $(PY) scripts/generate_test_cdc_events.py --batch 1
	$(LOCAL_AIRFLOW_ENV) $(VENV)/bin/airflow dags test aws_rds_cdc_to_bigquery 2026-11-05T10:00:00+00:00
	$(LOCAL_AIRFLOW_ENV) $(PY) scripts/generate_test_cdc_events.py --batch 2
	$(LOCAL_AIRFLOW_ENV) $(VENV)/bin/airflow dags test aws_rds_cdc_to_bigquery 2026-11-05T11:00:00+00:00
	$(LOCAL_AIRFLOW_ENV) $(PY) scripts/verify_bigquery_results.py

local-postgres: ## Start local PostgreSQL (logical replication) via Docker
	docker compose --env-file .env -f source_db/docker-compose.postgres.yml up -d --wait

local-postgres-events: ## Generate CDC events from real DML on local PostgreSQL
	set -a; source .env; set +a; $(PY) scripts/generate_test_cdc_events.py --mode postgres --batch 1

sql: ## Regenerate bigquery/sql/*.sql from config
	$(PY) bigquery/scripts/deploy_bigquery.py --render

build: ## Build Glue artifacts into dist/ (zip for Spark job, wheel for Python Shell)
	rm -rf dist build *.egg-info && mkdir -p dist
	zip -qr dist/pipeline_libs.zip cdc_common glue transfer -x '*/tests/*' '*/__pycache__/*'
	$(PY) -m pip wheel --no-deps -w dist . -q || uv build --wheel -o dist
	@ls -1 dist

deploy-gcp: build ## Terraform apply GCP, then create BigQuery tables
	cd infra/gcp && terraform init -backend-config=envs/$(ENV).backend.hcl && \
	  terraform apply -var-file=envs/$(ENV).tfvars
	PIPELINE_ENV=cloud BQ_BACKEND=bigquery $(PY) bigquery/scripts/deploy_bigquery.py --apply

deploy-aws: build ## Terraform apply AWS
	cd infra/aws && terraform init -backend-config=envs/$(ENV).backend.hcl && \
	  terraform apply -var-file=envs/$(ENV).tfvars

tf-validate: build ## terraform fmt + validate (no backend)
	cd infra/aws && terraform fmt -check && terraform init -backend=false -input=false && terraform validate
	cd infra/gcp && terraform fmt -check && terraform init -backend=false -input=false && terraform validate

verify: ## Verify warehouse state against the reference model
	$(PY) scripts/verify_bigquery_results.py

clean:
	rm -rf local_data dist build *.egg-info .airflow_local spark-warehouse metastore_db derby.log
