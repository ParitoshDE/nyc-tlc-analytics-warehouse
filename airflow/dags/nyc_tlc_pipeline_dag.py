"""
nyc_tlc_pipeline_dag.py — Airflow DAG for the nyc-tlc-analytics-warehouse pipeline.

Workflow:
    1. download_from_tlc      — NYC TLC parquet download to local
    2. upload_raw_to_gcs      — push raw parquet files to GCS data lake
  3. spark_transform        — PySpark job: clean, enrich, write Parquet
  4. load_to_bigquery       — load Parquet from GCS into BigQuery raw dataset
  5. dbt_run                — run dbt models (staging → dims → facts → aggs)
  6. dbt_test               — run dbt data quality tests
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_DIR = "/opt/airflow/dags/nyc-tlc-analytics-warehouse"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_tlc_analytics_pipeline",
    default_args=default_args,
    description="Batch pipeline: NYC TLC → GCS → Spark → BigQuery → dbt",
    schedule_interval="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nyc-tlc", "batch", "warehouse"],
) as dag:

    download_from_tlc = BashOperator(
        task_id="download_from_tlc",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python scripts/download_data.py"
        ),
    )

    upload_raw_to_gcs = BashOperator(
        task_id="upload_raw_to_gcs",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python scripts/upload_to_gcs.py"
        ),
    )

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python spark/transform_events.py"
        ),
    )

    load_to_bigquery = BashOperator(
        task_id="load_to_bigquery",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python scripts/load_to_bigquery.py"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "docker compose -f /opt/airflow/dags/nyc-tlc-analytics-warehouse/docker-compose.yml "
            "run --rm dbt run"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "docker compose -f /opt/airflow/dags/nyc-tlc-analytics-warehouse/docker-compose.yml "
            "run --rm dbt test"
        ),
    )

    (
        download_from_tlc
        >> upload_raw_to_gcs
        >> spark_transform
        >> load_to_bigquery
        >> dbt_run
        >> dbt_test
    )
