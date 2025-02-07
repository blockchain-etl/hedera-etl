variable "dashboard_json_files" {
  description = "The JSON file of the dashboard."
  type        = list(string)
  default     = ["templates/bigquery.json", "templates/cloud-storage-monitoring.json", "templates/dataflow-job.json", "templates/pub-sub-topic.json"]
}

resource "google_monitoring_dashboard" "dashboard" {
  for_each       = toset(var.dashboard_json_files)
  dashboard_json = file(each.key)
  project        = var.project_id
}

output "console_links" {
  value = {
    for k, f in google_monitoring_dashboard.dashboard : k => join("", ["https://console.cloud.google.com/monitoring/dashboards/custom/",
      element(split("/", f.id), 3),
      "?project=",
    var.project_id])
  }
}
