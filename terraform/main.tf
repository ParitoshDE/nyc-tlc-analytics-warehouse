terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "local" {}
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------------------------------------------------------------
# Service Account
# ---------------------------------------------------------------
resource "google_service_account" "pipeline_sa" {
  account_id   = "nyc-tlc-pipeline-sa"
  display_name = "nyc-tlc-analytics-warehouse pipeline service account"
}

resource "google_project_iam_member" "sa_bigquery" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_storage" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_dataproc" {
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# ---------------------------------------------------------------
# GCS Bucket — Data Lake
# ---------------------------------------------------------------
resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-ecom-data-lake"
  location      = var.region
  force_destroy = true
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 90
    }
  }
}

# ---------------------------------------------------------------
# BigQuery Datasets
# ---------------------------------------------------------------
resource "google_bigquery_dataset" "raw" {
  dataset_id = "ecommerce_raw"
  location   = var.region

  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "prod" {
  dataset_id = "ecommerce_prod"
  location   = var.region

  delete_contents_on_destroy = true
}

# ---------------------------------------------------------------
# Cloud Composer Environment
# ---------------------------------------------------------------
resource "google_composer_environment" "pipeline" {
  name   = "nyc-tlc-analytics-composer"
  region = var.region

  config {
    software_config {
      image_version = "composer-2-airflow-2"

      pypi_packages = {
        "kaggle" = ""
      }
    }

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
      }
      worker {
        cpu        = 1
        memory_gb  = 4
        storage_gb = 4
        min_count  = 1
        max_count  = 2
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      service_account = google_service_account.pipeline_sa.email
    }
  }
}
