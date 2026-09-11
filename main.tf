terraform {
  required_version = ">= 1.0.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = "your-gcp-project-id"
  region  = "europe-west1"
}

# Raw Storage Bucket for Lakehouse Bronze Layer
resource "google_storage_bucket" "crypto_raw_bucket" {
  name          = "raw-crypto-data-bucket-123"
  location      = "EU"
  force_destroy = true
}

# BigQuery Dataset for Analytics Target
resource "google_bigquery_dataset" "crypto_dataset" {
  dataset_id    = "crypto_analytics"
  friendly_name = "Crypto Analytics Dataset"
  location      = "EU"
}
