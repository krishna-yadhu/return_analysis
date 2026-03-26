output "bucket_name" {
  description = "Raw export bucket name."
  value       = google_storage_bucket.raw_data.name
}

output "dataset_ids" {
  description = "Provisioned BigQuery dataset IDs."
  value = {
    raw     = google_bigquery_dataset.raw.dataset_id
    staging = google_bigquery_dataset.staging.dataset_id
    mart    = google_bigquery_dataset.mart.dataset_id
  }
}
