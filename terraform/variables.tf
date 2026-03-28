variable "gcp_project_id" {
  type        = string
  description = "GCP Project ID"
  default     = "moonlit-state-486723-r7"
}

variable "gcp_region" {
  type        = string
  description = "GCP region for resources"
  default     = "us-central1"
}

variable "gcs_location" {
  type        = string
  description = "Location for GCS bucket"
  default     = "us-central1"
}

variable "staging_dataset_id" {
  type        = string
  description = "BigQuery staging dataset ID"
  default     = "staging"
}

variable "raw_dataset_id" {
  type        = string
  description = "BigQuery raw dataset ID"
  default     = "raw"
}

variable "gcs_bucket_name" {
  type        = string
  description = "GCS bucket name for data lake"
  default     = "buenos-aires-half-marathon"
}

variable "service_account_id" {
  type        = string
  description = "Service account ID for Bruin pipeline"
  default     = "sr-data-engineering"
}
