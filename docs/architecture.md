# Architecture — nyc-tlc-analytics-warehouse

## Overview

Batch data pipeline ingesting 285M e-commerce clickstream events through a GCP-native stack. Data flows from Kaggle → GCS → Spark → BigQuery → dbt → Looker Studio.

## Pipeline Flow

```
┌────────────┐     ┌──────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│   Kaggle   │────▶│   GCS    │────▶│   PySpark    │────▶│  BigQuery   │────▶│ Looker Studio│
│  (source)  │     │ (raw CSV)│     │ (transform)  │     │   (DWH)     │     │ (dashboard)  │
└────────────┘     └──────────┘     └──────────────┘     └──────┬──────┘     └──────────────┘
                                                                │
                                                           ┌────▼────┐
                                                           │   dbt   │
                                                           │ (models)│
                                                           └─────────┘
```

Orchestrated by **Cloud Composer (Airflow 2.x)**. Infrastructure provisioned by **Terraform**.

## Airflow DAG

Six sequential tasks:

1. **download_from_kaggle** — `scripts/download_data.py`: Kaggle API download + optional REES46 extra months
2. **upload_raw_to_gcs** — `scripts/upload_to_gcs.py`: push CSVs to `gs://<bucket>/raw/`
3. **spark_transform** — `spark/transform_events.py`: parse timestamps, split categories, compute session metrics, write partitioned Parquet
4. **load_to_bigquery** — `scripts/load_to_bigquery.py`: load Parquet into `ecommerce_raw.events`
5. **dbt_run** — Docker: `dbt run` builds staging → dimensions → facts → aggregations
6. **dbt_test** — Docker: `dbt test` validates data quality

## Data Layers

### Raw (GCS)
- Monthly CSVs: `gs://<bucket>/raw/*.csv`
- 9 columns, ~285M rows

### Processed (GCS)
- Spark output: `gs://<bucket>/processed/event_date=YYYY-MM-DD/*.parquet`
- Enriched with: `event_date`, `hour`, `day_of_week`, `category_level1`, `category_level2`, session metrics

### BigQuery Raw
- Table: `ecommerce_raw.events`
- Partitioned by `event_date` (DAY)
- Clustered by `event_type`, `category_level1`

### BigQuery Prod (dbt)
- Star schema in `ecommerce_prod`:
  - **Staging**: `stg_events` (view) — type casting, null handling
  - **Dimensions**: `dim_product`, `dim_user`, `dim_session`, `dim_date`
  - **Fact**: `fct_event` — event grain with surrogate key, partitioned + clustered
  - **Aggregations**: `agg_funnel_by_category`, `agg_brand_performance`, `agg_hourly_traffic`, `agg_cart_abandonment`

## Spark Transformations

1. Parse `event_time` → extract `event_date`, `hour`, `day_of_week`
2. Split `category_code` on `.` → `category_level1`, `category_level2`
3. Fill null `brand` with "unknown"
4. Window functions per `user_session`: `session_duration_sec`, `session_event_count`, `session_has_purchase`, `session_has_cart`, `session_has_view`
5. Output partitioned by `event_date` as Snappy-compressed Parquet

## Infrastructure (Terraform)

| Resource | Name |
|---|---|
| GCS Bucket | `<project>-ecom-data-lake` |
| BigQuery Dataset (raw) | `ecommerce_raw` |
| BigQuery Dataset (prod) | `ecommerce_prod` |
| Cloud Composer | `nyc-tlc-analytics-composer` (small env) |
| Service Account | `nyc-tlc-pipeline-sa` with BigQuery Admin, Storage Admin, Composer Worker, Dataproc Editor |

## Design Decisions

- **Spark over Pandas**: 14.68 GB minimum dataset — Pandas hits OOM. Spark handles 30 GB with room to scale.
- **Parquet over CSV**: Columnar format, 5-10x compression, predicate pushdown in BigQuery.
- **Partitioning by date**: Most queries filter by date range — eliminates full table scans.
- **Clustering by event_type + category**: Most common filter/group-by columns.
- **dbt in Docker**: Reproducible, no local dbt install needed, same image in CI and Composer.
- **Monthly schedule**: Dataset updates monthly — aligns with source cadence.
