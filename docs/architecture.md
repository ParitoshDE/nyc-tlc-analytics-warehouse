# Architecture

Batch data pipeline ingesting NYC TLC trip data through a GCP-native stack.
Data flows from NYC TLC monthly parquet files -> GCS -> Spark -> BigQuery -> dbt -> Looker Studio.

## End-to-End Flow

```
 NYC TLC host --> GCS (raw parquet) --> Spark (transform/enrich) --> BigQuery (raw) --> dbt (prod marts) --> Looker Studio
```

## Airflow DAG Tasks

1. download_from_tlc - scripts/download_data.py: download monthly parquet files
2. upload_raw_to_gcs - scripts/upload_to_gcs.py: upload raw parquet to GCS
3. spark_transform - spark/transform_events.py: normalize fields and derive metrics
4. load_to_bigquery - scripts/load_to_bigquery.py: load processed parquet into nyc_tlc_raw.trips
5. dbt_run - build dimensional and aggregate models
6. dbt_test - run dbt tests

## Storage and Modeling

### Raw Layer (BigQuery)
- Table: nyc_tlc_raw.trips
- Partition: pickup_date
- Clustering: pulocation_id, dolocation_id, payment_type

### Curated Layer (dbt)
- Dataset: nyc_tlc_prod
- Staging: stg_events (trip staging view)
- Dimensions: dim_date, dim_pickup_zone, dim_dropoff_zone, dim_vendor, dim_payment_type
- Fact: fct_event (trip-grain fact)
- Aggregations: agg_daily_kpis, agg_hourly_traffic, agg_zone_performance, agg_payment_type_mix

## Spark Transform Logic

Core transformations in spark/transform_events.py:

1. Read raw parquet files from data/raw
2. Normalize pickup/dropoff timestamps
3. Derive pickup_date and pickup_hour
4. Compute trip_duration_min and keep canonical trip fields
5. Write partitioned parquet output to data/processed by pickup_date

## Configuration

| Variable | Purpose |
|---|---|
| GCP_PROJECT_ID | Target GCP project |
| GCS_BUCKET | Data lake bucket |
| BQ_RAW_DATASET | Raw dataset (default nyc_tlc_raw) |
| BQ_PROD_DATASET | Curated dataset (default nyc_tlc_prod) |
| TLC_TAXI_TYPE | Taxi type to download |
| TLC_START_MONTH | Download start month (YYYY-MM) |
| TLC_END_MONTH | Download end month (YYYY-MM) |
| GOOGLE_APPLICATION_CREDENTIALS | Service account key path |

## Why This Design

- Monthly parquet source is stable and reproducible.
- Partitioning by pickup_date improves scan efficiency.
- Clustering supports common analytics by zones and payment type.
- dbt enforces modular, testable SQL transformations.
- Airflow keeps orchestration explicit and schedulable.
