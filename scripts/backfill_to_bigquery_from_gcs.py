"""
backfill_to_bigquery_from_gcs.py

Loads NYC TLC parquet files from GCS into BigQuery raw trips table month-by-month.
This avoids mixed-schema errors that can occur when querying many parquet files together.
"""
from __future__ import annotations

import os
from datetime import datetime

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()


def month_range(start_ym: str, end_ym: str) -> list[str]:
    start = datetime.strptime(start_ym, "%Y-%m")
    end = datetime.strptime(end_ym, "%Y-%m")

    out: list[str] = []
    current = start
    while current <= end:
        out.append(current.strftime("%Y-%m"))
        year = current.year + (1 if current.month == 12 else 0)
        month = 1 if current.month == 12 else current.month + 1
        current = current.replace(year=year, month=month)
    return out


def main() -> None:
    project_id = os.environ["GCP_PROJECT_ID"]
    bucket = os.environ["GCS_BUCKET"]
    dataset = os.getenv("BQ_RAW_DATASET", "nyc_tlc_raw")
    taxi_type = os.getenv("TLC_TAXI_TYPE", "yellow").strip().lower()
    start_ym = os.getenv("TLC_START_MONTH", "2022-01")
    end_ym = os.getenv("TLC_END_MONTH", "2023-12")

    if taxi_type != "yellow":
        raise ValueError("This loader currently supports TLC_TAXI_TYPE=yellow only")

    client = bigquery.Client(project=project_id)

    table_id = f"{project_id}.{dataset}.trips"

    ddl = f"""
    create or replace table `{table_id}` (
      vendor_id int64,
      pickup_datetime timestamp,
      dropoff_datetime timestamp,
      pickup_date date,
      pickup_hour int64,
      day_of_week int64,
      passenger_count float64,
      trip_distance float64,
      pulocation_id int64,
      dolocation_id int64,
      payment_type int64,
      fare_amount float64,
      tip_amount float64,
      total_amount float64,
      trip_duration_min float64
    )
    partition by pickup_date
    cluster by pulocation_id, dolocation_id, payment_type
    """
    client.query(ddl).result()
    print(f"[bq] Recreated target table: {table_id}")

    months = month_range(start_ym, end_ym)
    total_inserted = 0

    for ym in months:
        uri = f"gs://{bucket}/raw/{taxi_type}_tripdata_{ym}.parquet"
        ext_table = f"{project_id}.{dataset}._ext_{taxi_type}_{ym.replace('-', '_')}"

        create_ext = f"""
        create or replace external table `{ext_table}`
        options (
          format = 'PARQUET',
          uris = ['{uri}']
        )
        """
        client.query(create_ext).result()

        insert_sql = f"""
        insert into `{table_id}` (
          vendor_id,
          pickup_datetime,
          dropoff_datetime,
          pickup_date,
          pickup_hour,
          day_of_week,
          passenger_count,
          trip_distance,
          pulocation_id,
          dolocation_id,
          payment_type,
          fare_amount,
          tip_amount,
          total_amount,
          trip_duration_min
        )
        select
          cast(VendorID as int64) as vendor_id,
          timestamp(tpep_pickup_datetime) as pickup_datetime,
          timestamp(tpep_dropoff_datetime) as dropoff_datetime,
          date(timestamp(tpep_pickup_datetime)) as pickup_date,
          extract(hour from timestamp(tpep_pickup_datetime)) as pickup_hour,
          extract(dayofweek from timestamp(tpep_pickup_datetime)) as day_of_week,
          cast(coalesce(passenger_count, 0) as float64) as passenger_count,
          cast(coalesce(trip_distance, 0.0) as float64) as trip_distance,
          cast(PULocationID as int64) as pulocation_id,
          cast(DOLocationID as int64) as dolocation_id,
          cast(payment_type as int64) as payment_type,
          cast(coalesce(fare_amount, 0.0) as float64) as fare_amount,
          cast(coalesce(tip_amount, 0.0) as float64) as tip_amount,
          cast(coalesce(total_amount, 0.0) as float64) as total_amount,
          timestamp_diff(timestamp(tpep_dropoff_datetime), timestamp(tpep_pickup_datetime), second) / 60.0 as trip_duration_min
        from `{ext_table}`
        where tpep_pickup_datetime is not null
          and tpep_dropoff_datetime is not null
          and timestamp_diff(timestamp(tpep_dropoff_datetime), timestamp(tpep_pickup_datetime), second) > 0
        """

        insert_job = client.query(insert_sql)
        insert_job.result()
        inserted = insert_job.num_dml_affected_rows or 0
        total_inserted += int(inserted)
        print(f"[bq] {ym}: inserted {inserted:,} rows")

        client.query(f"drop external table `{ext_table}`").result()

    final_rows = next(iter(client.query(f"select count(*) c from `{table_id}`").result())).c
    print(f"[bq] Done. Total inserted (sum by month): {total_inserted:,}")
    print(f"[bq] Final row count in {table_id}: {final_rows:,}")


if __name__ == "__main__":
    main()
