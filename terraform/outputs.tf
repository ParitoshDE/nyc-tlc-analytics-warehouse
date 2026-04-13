output "data_lake_bucket" {
  value = google_storage_bucket.data_lake.name
}

output "raw_dataset" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "prod_dataset" {
  value = google_bigquery_dataset.prod.dataset_id
}

output "composer_dag_gcs_prefix" {
  value = google_composer_environment.pipeline.config[0].dag_gcs_prefix
}

output "service_account_email" {
  value = google_service_account.pipeline_sa.email
}
