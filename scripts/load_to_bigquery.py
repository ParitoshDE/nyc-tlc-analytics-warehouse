"""
load_to_bigquery.py — Load processed Parquet files from GCS into BigQuery.

Loads Spark output into nyc_tlc_raw.trips table.
"""
from __future__ import annotations

import os

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()


def load_parquet_to_bigquery() -> None:
    project_id = os.environ["GCP_PROJECT_ID"]
    bucket_name = os.environ["GCS_BUCKET"]
    raw_dataset = os.getenv("BQ_RAW_DATASET", "nyc_tlc_raw")

    table_id = f"{project_id}.{raw_dataset}.trips"
    source_uri = f"gs://{bucket_name}/processed/*.parquet"

    client = bigquery.Client(project=project_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="pickup_date",
        ),
        clustering_fields=["pulocation_id", "dolocation_id", "payment_type"],
    )

    print(f"[bigquery] Loading {source_uri} → {table_id}")

    load_job = client.load_table_from_uri(
        source_uri,
        table_id,
        job_config=job_config,
    )

    load_job.result()  # Wait for completion

    table = client.get_table(table_id)
    print(f"[bigquery] Loaded {table.num_rows:,} rows into {table_id}")


if __name__ == "__main__":
    load_parquet_to_bigquery()
