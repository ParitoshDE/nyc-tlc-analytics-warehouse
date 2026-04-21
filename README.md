# nyc-tlc-analytics-warehouse

End-to-end batch data pipeline for NYC TLC trip data using GCP, Airflow, Spark, BigQuery, and dbt.

## Business Questions

- How do trip volume and revenue vary by pickup date and hour?
- Which pickup/dropoff zones and payment types drive the most revenue?
- How do distance and duration patterns change over time?

## Dataset

| Property | Value |
|---|---|
| Source | NYC TLC trip records (official monthly parquet files) |
| URL pattern | https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_YYYY-MM.parquet |
| Taxi types | yellow, green, fhv, fhvhv |
| Default range | TLC_START_MONTH=2023-01 to TLC_END_MONTH=2023-12 |

## Architecture

```
NYC TLC monthly parquet
        |
        v
   +-----------+     +-----------+     +-----------+     +-----------+     +-------------+
   |  Airflow  | --> |    GCS    | --> |  PySpark  | --> | BigQuery  | --> | Looker Studio|
   | (Composer)|     | (raw zone)|     | transform |     |   (DWH)   |     |  dashboard   |
   +-----------+     +-----------+     +-----------+     +-----+-----+     +-------------+
                                                             |
                                                           +---+
                                                           |dbt|
                                                           +---+
```

Airflow DAG flow:

```
download_from_tlc -> upload_raw_to_gcs -> spark_transform -> load_to_bigquery -> dbt_run -> dbt_test
```

## Tech Stack

| Layer | Technology |
|---|---|
| IaC | Terraform |
| Cloud | Google Cloud Platform |
| Orchestration | Cloud Composer (Airflow 2.x) |
| Data Lake | Google Cloud Storage |
| Batch Processing | PySpark |
| Data Warehouse | BigQuery (partitioned by pickup_date) |
| Transformations | dbt Core (Dockerized) |
| Dashboard | Looker Studio |
| Containerization | Docker + Docker Compose |

## Project Structure

```
nyc-tlc-analytics-warehouse/
+-- airflow/dags/
�   +-- nyc_tlc_pipeline_dag.py
+-- dbt/
�   +-- models/
�   �   +-- staging/
�   �   +-- dimensions/
�   �   +-- facts/
�   �   +-- aggregations/
+-- spark/
�   +-- transform_events.py
+-- scripts/
�   +-- download_data.py
�   +-- upload_to_gcs.py
�   +-- load_to_bigquery.py
+-- terraform/
+-- docker-compose.yml
+-- Makefile
+-- requirements.txt
+-- .env.example
+-- README.md
```

## Quick Start

### Prerequisites

- Python 3.10+
- Docker and Docker Compose
- Terraform >= 1.5
- GCP project with billing enabled
- GCP service-account key JSON

### 1) Configure environment

```bash
git clone https://github.com/ParitoshDE/nyc-tlc-analytics-warehouse.git
cd nyc-tlc-analytics-warehouse
cp .env.example .env
```

Update .env values (project, bucket, taxi type/month range, credentials path).

### 2) Install dependencies

```bash
make setup
```

### 3) Provision infrastructure

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
make infra-up
```

### 4) Run pipeline

```bash
make run
```

This executes: download -> upload -> spark -> load-bq -> dbt-run -> dbt-test.

### 5) Run steps individually

```bash
make download
make upload
make spark
make load-bq
make dbt-run
make dbt-test
```

### 6) Tear down

```bash
make infra-down
make clean
```

## Cloud Automation (Composer)

Use this flow to move from local/manual execution to Composer-orchestrated runs.

Runtime behavior in cloud mode:

- `download_from_tlc`, `upload_raw_to_gcs`, `load_to_bigquery`, `dbt_run`, and `dbt_test` run on Composer workers.
- `spark_transform` runs as a Dataproc Serverless batch job (not on Composer worker VM).

1) Provision cloud resources (includes Composer):

```bash
make infra-up
```

1. Deploy DAG + runtime files to Composer and sync Airflow Variables from `.env`:

```bash
make composer-deploy
```

1. Trigger DAG in Composer:

```bash
make composer-trigger
```

1. One-command production execution (deploy + trigger + wait for success/failure):

```bash
make prod-run
```

Notes:

- Deployment script: `scripts/deploy_to_composer.ps1`
- Trigger script: `scripts/trigger_composer_dag.ps1`
- DAG ID: `nyc_tlc_analytics_pipeline`
- Composer env name default: `nyc-tlc-analytics-composer`
- Composer variable `COMPOSER_REPO_ROOT_GCS` is set by deploy script and used as Spark code URI root.
- Composer variable `PIPELINE_SERVICE_ACCOUNT` is set by deploy script for Dataproc batch execution.

## Dashboard Build Assets

- Blueprint: docs/dashboard_blueprint.md
- BigQuery semantic views SQL: scripts/dashboard_views.sql
- Live Dashboard: [NYC TLC Analytics Dashboard](https://datastudio.google.com/reporting/b836db6d-8fbd-4f56-87ef-887983634be8)

## dbt Layers

- Staging: normalized trips from raw BigQuery table
- Dimensions: zones, vendors, payment types, date
- Fact: trip-level fact table
- Aggregations: daily, hourly, zone, and payment-type metrics
