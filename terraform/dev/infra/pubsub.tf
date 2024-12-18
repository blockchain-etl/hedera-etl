module "pubsub" {
  source  = "terraform-google-modules/pubsub/google"
  version = "7.0.0"

  project_id = var.project_id
  topic      = "${var.project_name}-topic"
  topic_labels = {
    project = "${var.project_name}"
  }

  pull_subscriptions = [
    {
      name                 = "${var.project_name}-pull"
      ack_deadline_seconds = 10
    },
  ]

  depends_on = [
    module.enabled_google_apis
  ]
}

resource "google_project_iam_binding" "pubsub" {
  project = var.project_id
  role    = "roles/pubsub.editor"

  members = [
    "user:maciej.malik@arianelabs.com",
    "serviceAccount:${google_service_account.pubsub.email}"
  ]
}

resource "google_service_account" "pubsub" {
  account_id   = "pubsub"
  display_name = "pubsub"
}

resource "google_service_account_key" "pubsub" {
  service_account_id = google_service_account.pubsub.name
}

resource "local_file" "pubsub" {
  content  = base64decode(google_service_account_key.pubsub.private_key)
  filename = "./.terraform/pubsub-key.json"
}
