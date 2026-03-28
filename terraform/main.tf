terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "local" {
    path = "terraform.tfstate"
  }
}

provider "google" {
  project           = var.gcp_project_id
  region            = var.gcp_region
  billing_project   = var.gcp_project_id
  user_project_override = true
}

# ===== BigQuery Staging Dataset =====
resource "google_bigquery_dataset" "staging" {
  dataset_id    = var.staging_dataset_id
  project       = var.gcp_project_id
  location      = var.gcp_region
  friendly_name = "Buenos Aires Half Marathon Staging"
  description   = "Staging dataset for half-marathon analytics pipeline"

  delete_contents_on_destroy = false

  labels = {
    environment = "production"
    project     = "half-marathon-analytics"
  }
}

# ===== GCS Bucket for Raw Data Lake =====
resource "google_storage_bucket" "data_lake" {
  name          = var.gcs_bucket_name
  project       = var.gcp_project_id
  location      = var.gcs_location
  force_destroy = false

  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
    condition {
      age = 30
    }
  }

  labels = {
    environment = "production"
    project     = "half-marathon-analytics"
  }
}

# ===== Service Account for Bruin Pipeline =====
resource "google_service_account" "bruin_pipeline" {
  account_id   = var.service_account_id
  project      = var.gcp_project_id
  display_name = "Bruin Half Marathon Pipeline"
  description  = "Service account for Bruin half-marathon data pipeline"
}

# ===== IAM: BigQuery Dataset Access =====
resource "google_bigquery_dataset_iam_member" "bruin_editor" {
  dataset_id = google_bigquery_dataset.staging.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.bruin_pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "bruin_job_user" {
  dataset_id = google_bigquery_dataset.staging.dataset_id
  role       = "roles/bigquery.jobUser"
  member     = "serviceAccount:${google_service_account.bruin_pipeline.email}"
}

# ===== IAM: GCS Bucket Access =====
resource "google_storage_bucket_iam_member" "bruin_bucket_admin" {
  bucket = google_storage_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.bruin_pipeline.email}"
}

# ===== Service Account Key for Local Development =====
resource "google_service_account_key" "bruin_key" {
  service_account_id = google_service_account.bruin_pipeline.name
  private_key_type   = "TYPE_GOOGLE_CREDENTIALS_FILE"
}

# ===== Output credentials to JSON file (for local use) =====
resource "local_file" "credentials_json" {
  content    = base64decode(google_service_account_key.bruin_key.private_key)
  filename   = "${path.module}/../credentials.json"
  depends_on = [google_service_account_key.bruin_key]

  lifecycle {
    ignore_changes = [content]
  }
}

# ===== BigQuery Dataset for Raw Ingestion =====
resource "google_bigquery_dataset" "raw" {
  dataset_id    = var.raw_dataset_id
  project       = var.gcp_project_id
  location      = var.gcp_region
  friendly_name = "Half Marathon Raw Data"
  description   = "Raw ingestion dataset for half-marathon race results"

  delete_contents_on_destroy = false

  labels = {
    environment = "production"
    project     = "half-marathon-analytics"
  }
}

resource "google_bigquery_dataset_iam_member" "bruin_raw_editor" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.bruin_pipeline.email}"
}
