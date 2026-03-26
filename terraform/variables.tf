variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "GCP region for regional resources."
  type        = string
}

variable "location" {
  description = "BigQuery dataset location."
  type        = string
}

variable "bucket_name" {
  description = "GCS bucket name for raw exports."
  type        = string
}

variable "credentials_file" {
  description = "Path to the service account JSON file."
  type        = string
}

variable "labels" {
  description = "Labels to apply to resources."
  type        = map(string)
  default = {
    project = "return_analysis"
    managed = "terraform"
  }
}
