provider "google" {
  project     = var.project_id
  credentials = file(var.credentials_path)
  region      = var.region
}

resource "google_storage_bucket" "terraform_state_bucket" {
  name          = "${var.project_name}-tf-state"
  location      = var.region
  force_destroy = false
  storage_class = "STANDARD"

  versioning {
    enabled = true
  }

  uniform_bucket_level_access = true
}

variable "project_name" {
  default = "h2gcp"
}

variable "project_id" {
  default = "mystical-being-444413-n9"
}

variable "credentials_path" {
  default = "./../credentials/mystical-being.json"
}

variable "region" {
  default = "europe-central2" # warsaw
}
