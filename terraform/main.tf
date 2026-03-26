resource "google_storage_bucket" "raw_data" {
  name                        = var.bucket_name
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true

  labels = var.labels
}

resource "google_bigquery_dataset" "raw" {
  dataset_id                 = "raw"
  location                   = var.location
  delete_contents_on_destroy = false

  labels = var.labels
}

resource "google_bigquery_dataset" "staging" {
  dataset_id                 = "staging"
  location                   = var.location
  delete_contents_on_destroy = false

  labels = var.labels
}

resource "google_bigquery_dataset" "mart" {
  dataset_id                 = "mart"
  location                   = var.location
  delete_contents_on_destroy = false

  labels = var.labels
}
