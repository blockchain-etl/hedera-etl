terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.13.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "6.13.0"
    }
  }
}
provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}
provider "google-beta" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}
terraform {
  backend "gcs" {
    bucket      = "h2gcp-tf-state"
    prefix      = "dev/infra"
    credentials = "./../../credentials/mystical-being.json"
  }
}
