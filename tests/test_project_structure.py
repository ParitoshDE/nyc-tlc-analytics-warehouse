from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_expected_directories_exist() -> None:
    assert (ROOT / "terraform").exists()
    assert (ROOT / "airflow" / "dags").exists()
    assert (ROOT / "spark").exists()
    assert (ROOT / "dbt" / "models").exists()
    assert (ROOT / "scripts").exists()


def test_key_files_exist() -> None:
    assert (ROOT / "terraform" / "main.tf").exists()
    assert (ROOT / "airflow" / "dags" / "nyc_tlc_pipeline_dag.py").exists()
    assert (ROOT / "spark" / "transform_events.py").exists()
    assert (ROOT / "dbt" / "dbt_project.yml").exists()
    assert (ROOT / "dbt" / "packages.yml").exists()
    assert (ROOT / "docker-compose.yml").exists()
    assert (ROOT / "Makefile").exists()
    assert (ROOT / ".env.example").exists()
    assert (ROOT / "scripts" / "download_data.py").exists()
    assert (ROOT / "scripts" / "upload_to_gcs.py").exists()
    assert (ROOT / "scripts" / "load_to_bigquery.py").exists()


def test_dbt_models_exist() -> None:
    models = ROOT / "dbt" / "models"
    assert (models / "staging" / "stg_events.sql").exists()
    assert (models / "dimensions" / "dim_product.sql").exists()
    assert (models / "dimensions" / "dim_user.sql").exists()
    assert (models / "dimensions" / "dim_session.sql").exists()
    assert (models / "dimensions" / "dim_date.sql").exists()
    assert (models / "facts" / "fct_event.sql").exists()
    assert (models / "aggregations" / "agg_funnel_by_category.sql").exists()
    assert (models / "aggregations" / "agg_brand_performance.sql").exists()
    assert (models / "aggregations" / "agg_hourly_traffic.sql").exists()
    assert (models / "aggregations" / "agg_cart_abandonment.sql").exists()
