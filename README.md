# nyc-tlc-analytics-warehouse

End-to-end batch data pipeline processing **285 million e-commerce user events** (14.68 GB) through GCP using Airflow, Spark, BigQuery, and dbt.

## Business Question

> *What are the conversion funnel drop-off rates by product category and brand, and how do session behavior and time-of-day patterns affect purchase probability?*

## Dataset

| Property | Value |
|---|---|
| Source | [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) |
| Size | 14.68 GB (Oct + Nov 2019) — up to ~30 GB with extra months |
| Records | ~285 million events |
| Event types | `view`, `cart`, `remove_from_cart`, `purchase` |
| Columns | `event_time`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session` |

## Architecture

```
Kaggle API / REES46 URLs
        │
        ▼
   ┌─────────┐     ┌──────────┐     ┌──────────────┐     ┌─────────┐     ┌──────────────┐
   │ Airflow  │────▶│   GCS    │────▶│   PySpark    │────▶│BigQuery │────▶│ Looker Studio│
   │(Composer)│     │(raw CSV) │     │(transform →  │     │  (DWH)  │     │ (dashboard)  │
   └─────────┘     └──────────┘     │  Parquet)    │     └────┬────┘     └──────────────┘
                                    └──────────────┘          │
                                                         ┌────▼────┐
                                                         │   dbt   │
                                                         │ (models)│
                                                         └─────────┘
```

**Airflow DAG** (6 tasks):
```
download_from_kaggle → upload_raw_to_gcs → spark_transform → load_to_bigquery → dbt_run → dbt_test
```

## Tech Stack

| Layer | Technology |
|---|---|
| IaC | Terraform |
| Cloud | Google Cloud Platform |
| Orchestration | Cloud Composer (Airflow 2.x) |
| Data Lake | Google Cloud Storage |
| Batch Processing | PySpark |
| Data Warehouse | BigQuery (partitioned by `event_date`, clustered by `event_type`, `category_level1`) |
| Transformations | dbt Core (Dockerized) |
| Dashboard | Looker Studio |
| Containerization | Docker + Docker Compose |

## Project Structure

```
nyc-tlc-analytics-warehouse/
├── airflow/dags/              # Airflow DAG definition
│   └── nyc_tlc_pipeline_dag.py
├── dbt/                       # dbt project
│   ├── models/
│   │   ├── staging/           # stg_events (view)
│   │   ├── dimensions/        # dim_product, dim_user, dim_session, dim_date
│   │   ├── facts/             # fct_event (partitioned + clustered)
│   │   └── aggregations/      # funnel, brand, hourly, cart abandonment
│   ├── Dockerfile
│   ├── dbt_project.yml
│   ├── packages.yml
│   └── profiles.yml
├── spark/                     # PySpark transformation script
│   └── transform_events.py
├── scripts/                   # Python utility scripts
│   ├── download_data.py
│   ├── upload_to_gcs.py
│   └── load_to_bigquery.py
├── terraform/                 # GCP infrastructure
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
├── docker-compose.yml
├── Makefile
├── requirements.txt
├── .env.example
└── README.md
```

## Quick Start

### Prerequisites
- Python 3.10+
- Docker & Docker Compose
- Terraform >= 1.5
- GCP account with billing enabled
- Kaggle API token (`~/.kaggle/kaggle.json`)

### 1. Clone and configure

```bash
git clone https://github.com/yourname/nyc-tlc-analytics-warehouse.git
cd nyc-tlc-analytics-warehouse
cp .env.example .env
# Edit .env with your GCP project ID, bucket name, and Kaggle credentials
```

### 2. Install dependencies

```bash
make setup
```

### 3. Provision infrastructure

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Edit terraform.tfvars with your GCP project ID
make infra-up
```

### 4. Run the full pipeline

```bash
make run
```

This executes: download → upload to GCS → Spark transform → load to BigQuery → dbt run → dbt test

### 5. Individual steps

```bash
make download    # Download dataset from Kaggle
make upload      # Upload raw CSVs to GCS
make spark       # Run PySpark transformation
make load-bq     # Load Parquet into BigQuery
make dbt-run     # Run dbt models
make dbt-test    # Run dbt data quality tests
```

### 6. Tear down

```bash
make infra-down
make clean
```

## dbt Models

| Layer | Model | Type | Description |
|---|---|---|---|
| Staging | `stg_events` | view | Clean, typed events from raw |
| Dimension | `dim_product` | table | One row per product — latest brand/category |
| Dimension | `dim_user` | table | User profile — first/last seen, sessions, purchases |
| Dimension | `dim_session` | table | Session metrics — duration, funnel flags, revenue |
| Dimension | `dim_date` | table | Calendar dimension |
| Fact | `fct_event` | table | Event grain — surrogate key, partitioned + clustered |
| Aggregation | `agg_funnel_by_category` | table | View→cart→purchase conversion by category |
| Aggregation | `agg_brand_performance` | table | Revenue, AOV, conversion by brand |
| Aggregation | `agg_hourly_traffic` | table | Heatmap — events/conversions by hour × day |
| Aggregation | `agg_cart_abandonment` | table | Abandoned cart analysis by date/category/brand |

## Dashboard (Looker Studio)

| Tile | Chart Type | Filter |
|---|---|---|
| Conversion funnel | Funnel chart | Category dropdown |
| Revenue by brand (Top 20) | Horizontal bar | Month selector |
| Hourly traffic heatmap | Heatmap (hour × day_of_week) | Event type toggle |
| Cart abandonment trend | Line chart over time | Category filter |

## License

Dataset provided by [REES46 / Open CDP](https://rees46.com/en/open-cdp). Free to use with attribution.
