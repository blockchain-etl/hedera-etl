resource "google_storage_bucket" "dataflow_bucket" {
  name          = "${var.project_name}-dataflow"
  location      = var.region
  force_destroy = false
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
}
