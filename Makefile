.PHONY: help setup infra-up infra-down download upload spark dbt-run dbt-test run composer-deploy composer-trigger clean

include .env
export

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Install Python dependencies
	pip install -r requirements.txt
	mkdir -p data/raw data/processed keys

infra-up: ## Provision GCP resources with Terraform
	cd terraform && terraform init && terraform apply -auto-approve

infra-down: ## Tear down GCP resources
	cd terraform && terraform destroy -auto-approve

download: ## Download NYC TLC parquet files
	python scripts/download_data.py

upload: ## Upload raw Parquet files to GCS
	python scripts/upload_to_gcs.py

spark: ## Run PySpark transformation
	python spark/transform_events.py

load-bq: ## Load processed Parquet into BigQuery
	python scripts/load_to_bigquery.py

dbt-run: ## Run dbt models (via Docker)
	docker compose run --rm dbt run

dbt-test: ## Run dbt tests (via Docker)
	docker compose run --rm dbt test

run: download upload spark load-bq dbt-run dbt-test ## Run full pipeline end-to-end

composer-deploy: ## Deploy DAG and runtime files to Cloud Composer and sync Airflow Variables
	powershell -ExecutionPolicy Bypass -File scripts/deploy_to_composer.ps1

composer-trigger: ## Trigger Cloud Composer DAG run
	powershell -ExecutionPolicy Bypass -File scripts/trigger_composer_dag.ps1

clean: ## Remove local data files
	rm -rf data/raw/* data/processed/*
