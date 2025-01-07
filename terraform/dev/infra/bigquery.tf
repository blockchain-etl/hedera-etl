resource "google_bigquery_dataset" "hedera_mainnet_dataset" {
  dataset_id                  = "blockchain_hedera_mainnet_eu"
  friendly_name               = "Dataset containing hedera data"
  location                    = "EU"

  labels = {
    project = var.project_name
  }
}
