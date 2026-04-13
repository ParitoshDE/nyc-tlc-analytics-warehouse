variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-central1"
}

variable "data_lake_bucket_name" {
  description = "GCS bucket name for raw and processed data"
  type        = string
}

variable "raw_dataset_id" {
  description = "BigQuery raw dataset id"
  type        = string
  default     = "nyc_tlc_raw"
}

variable "prod_dataset_id" {
  description = "BigQuery curated dataset id"
  type        = string
  default     = "nyc_tlc_prod"
}
