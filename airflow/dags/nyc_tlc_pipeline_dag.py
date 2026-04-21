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
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

PROJECT_DIR = "/home/airflow/gcs/dags/nyc-tlc-analytics-warehouse"

ENV_EXPORT = (
    "export GCP_PROJECT_ID='{{ var.value.get(\"GCP_PROJECT_ID\", \"\") }}' && "
    "export GCP_REGION='{{ var.value.get(\"GCP_REGION\", \"us-central1\") }}' && "
    "export GCS_BUCKET='{{ var.value.get(\"GCS_BUCKET\", \"\") }}' && "
    "export BQ_RAW_DATASET='{{ var.value.get(\"BQ_RAW_DATASET\", \"nyc_tlc_raw\") }}' && "
    "export BQ_PROD_DATASET='{{ var.value.get(\"BQ_PROD_DATASET\", \"nyc_tlc_prod\") }}' && "
    "export TLC_TAXI_TYPE='{{ var.value.get(\"TLC_TAXI_TYPE\", \"yellow\") }}' && "
    "export TLC_START_MONTH='{{ var.value.get(\"TLC_START_MONTH\", \"2023-01\") }}' && "
    "export TLC_END_MONTH='{{ var.value.get(\"TLC_END_MONTH\", \"2023-12\") }}' && "
    "export GOOGLE_APPLICATION_CREDENTIALS='{{ var.value.get(\"GOOGLE_APPLICATION_CREDENTIALS\", \"\") }}' && "
    ""
)

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
            f"{ENV_EXPORT} cd {PROJECT_DIR} && "
            "python scripts/download_data.py"
        ),
    )

    upload_raw_to_gcs = BashOperator(
        task_id="upload_raw_to_gcs",
        bash_command=(
            f"{ENV_EXPORT} cd {PROJECT_DIR} && "
            "python scripts/upload_to_gcs.py"
        ),
    )

    spark_transform = DataprocCreateBatchOperator(
        task_id="spark_transform",
        project_id="{{ var.value.get('GCP_PROJECT_ID', '') }}",
        region="{{ var.value.get('GCP_REGION', 'us-central1') }}",
        batch_id="nyc-tlc-spark-{{ ts_nodash | lower }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "{{ var.value.get('COMPOSER_REPO_ROOT_GCS', '') }}/spark/transform_events.py",
                "args": [
                    "--gcs-bucket",
                    "{{ var.value.get('GCS_BUCKET', '') }}",
                ],
            },
            "runtime_config": {
                "version": "2.2",
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "{{ var.value.get('PIPELINE_SERVICE_ACCOUNT', '') }}",
                },
            },
        },
    )

    load_to_bigquery = BashOperator(
        task_id="load_to_bigquery",
        bash_command=(
            f"{ENV_EXPORT} cd {PROJECT_DIR} && "
            "python scripts/load_to_bigquery.py"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"{ENV_EXPORT} cd {PROJECT_DIR}/dbt && "
            "dbt deps --project-dir . --profiles-dir . && "
            "dbt run --project-dir . --profiles-dir ."
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"{ENV_EXPORT} cd {PROJECT_DIR}/dbt && "
            "dbt test --project-dir . --profiles-dir ."
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
