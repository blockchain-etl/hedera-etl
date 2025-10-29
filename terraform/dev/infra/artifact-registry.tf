resource "google_artifact_registry_repository" "registry" {
  location      = var.region
  repository_id = var.project_name
  description   = "Registry for Flex docker images"
  format        = "DOCKER"

  depends_on = [
    module.enabled_google_apis
  ]
}

resource "google_storage_bucket" "flex_templates" {
  name          = "${var.project_name}-flex-templates"
  location      = var.region
  force_destroy = false
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
}
