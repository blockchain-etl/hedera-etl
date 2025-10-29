resource "google_bigquery_dataset" "hedera_public" {
  dataset_id    = "hedera_public"
  friendly_name = "Dataset containing hedera public data"
  location      = var.region

  labels = {
    project = var.project_name
  }

  depends_on = [
    module.enabled_google_apis
  ]
}

resource "google_bigquery_table" "public_tables" {
  for_each            = fileset("templates/bq-schemas/public", "*.json")
  dataset_id          = google_bigquery_dataset.hedera_public.dataset_id
  table_id            = replace(basename(each.key), ".json", "")
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "timestamp"
  }

  schema = file("templates/bq-schemas/public/${each.key}")
}

resource "google_bigquery_dataset" "hedera_restricted" {
  dataset_id    = "hedera_restricted"
  friendly_name = "Dataset containing hedera restricted data"
  location      = var.region

  labels = {
    project = var.project_name
  }

  depends_on = [
    module.enabled_google_apis
  ]
}

resource "google_bigquery_table" "restricted_tables" {
  for_each                 = fileset("templates/bq-schemas/restricted", "*.json")
  dataset_id               = google_bigquery_dataset.hedera_restricted.dataset_id
  table_id                 = replace(basename(each.key), ".json", "")
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = strcontains(file("templates/bq-schemas/restricted/${each.key}"), "modified") ? "modified" : "created"
  }

  schema = file("templates/bq-schemas/restricted/${each.key}")
}

resource "google_bigquery_dataset" "hedera_technical" {
  dataset_id    = "hedera_technical"
  friendly_name = "Dataset containing hedera technical data"
  location      = var.region

  labels = {
    project = var.project_name
  }

  depends_on = [
    module.enabled_google_apis
  ]
}

resource "google_bigquery_table" "technical_tables" {
  for_each                 = fileset("templates/bq-schemas/technical", "*.json")
  dataset_id               = google_bigquery_dataset.hedera_technical.dataset_id
  table_id                 = replace(basename(each.key), ".json", "")
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type  = "HOUR"
    field = strcontains(file("templates/bq-schemas/technical/${each.key}"), "modified") ? "modified" : "created"
  }

  schema = file("templates/bq-schemas/technical/${each.key}")
}
