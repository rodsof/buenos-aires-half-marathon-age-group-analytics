output "staging_dataset_id" {
  value       = google_bigquery_dataset.staging.dataset_id
  description = "BigQuery staging dataset ID"
}

output "raw_dataset_id" {
  value       = google_bigquery_dataset.raw.dataset_id
  description = "BigQuery raw dataset ID"
}

output "gcs_bucket_name" {
  value       = google_storage_bucket.data_lake.name
  description = "GCS bucket name"
}

output "service_account_email" {
  value       = google_service_account.bruin_pipeline.email
  description = "Service account email for Bruin pipeline"
}

output "credentials_file_path" {
  value       = local_file.credentials_json.filename
  description = "Path to credentials JSON file"
  sensitive   = true
}

output "terraform_setup_summary" {
  value = {
    staging_dataset           = google_bigquery_dataset.staging.dataset_id
    raw_dataset               = google_bigquery_dataset.raw.dataset_id
    data_lake_bucket          = google_storage_bucket.data_lake.name
    service_account           = google_service_account.bruin_pipeline.email
    credentials_location      = local_file.credentials_json.filename
    next_steps                = "Run 'terraform apply' then update README.md with outputs"
  }
  description = "Summary of provisioned resources"
}
