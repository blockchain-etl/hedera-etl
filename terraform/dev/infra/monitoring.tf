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

resource "google_monitoring_notification_channel" "email" {
  for_each     = var.email_notifications
  display_name = "Email Notification Channel ${each.key}"
  type         = "email"
  force_delete = true
  labels = {
    email_address = "${each.value}"
  }
}

## Alert Policies

resource "google_monitoring_alert_policy" "system_lag" {
  display_name = "Hedera Application System Lag"
  combiner     = "OR"
  conditions {
    display_name = "System Lag"
    condition_threshold {
      filter     = "resource.type = \"dataflow_job\" AND resource.labels.job_name = monitoring.regex.full_match(\"${var.dataflow_job_regex}\") AND metric.type = \"dataflow.googleapis.com/job/system_lag\""
      duration   = "0s"
      comparison = "COMPARISON_GT"
      aggregations {
        alignment_period   = "900s"
        per_series_aligner = "ALIGN_MEAN"
      }
      trigger {
        count = 1
      }
    }
  }

  user_labels = {
    hederaapplication = "system_lag"
  }
  notification_channels = [
    for name, _ in var.email_notifications : google_monitoring_notification_channel.email[name].id
  ]
}

resource "google_monitoring_alert_policy" "log_entry_count" {
  display_name = "Hedera Application Log Entry Count"
  combiner     = "OR"
  conditions {
    display_name = "Log Entry Count"
    condition_threshold {
      threshold_value = 50
      filter          = "resource.type = \"dataflow_job\" AND resource.labels.job_name = monitoring.regex.full_match(\"${var.dataflow_job_regex}\") AND metric.type = \"logging.googleapis.com/log_entry_count\" AND metric.labels.severity = \"ERROR\""
      duration        = "0s"
      comparison      = "COMPARISON_GT"
      aggregations {
        alignment_period     = "900s"
        per_series_aligner   = "ALIGN_SUM"
        cross_series_reducer = "REDUCE_SUM"
      }
      trigger {
        count = 1
      }
    }
  }

  user_labels = {
    hederaapplication = "log_entry_count"
  }
  notification_channels = [
    for name, _ in var.email_notifications : google_monitoring_notification_channel.email[name].id
  ]
}

resource "google_monitoring_alert_policy" "elements_produced_count" {
  display_name = "Hedera Application Elements Produced Count"
  combiner     = "OR"
  conditions {
    display_name = "Elements Produced Count"
    condition_threshold {
      threshold_value = 1
      filter          = "resource.type = \"dataflow_job\" AND resource.labels.job_name = monitoring.regex.full_match(\"${var.dataflow_job_regex}\") AND metric.type = \"dataflow.googleapis.com/job/elements_produced_count\" AND metric.labels.ptransform = monitoring.regex.full_match(\"Save.*to BigQuery.*\")"
      duration        = "0s"
      comparison      = "COMPARISON_LT"
      aggregations {
        alignment_period     = "900s"
        per_series_aligner   = "ALIGN_MEAN"
        cross_series_reducer = "REDUCE_MIN"
      }
      trigger {
        count = 1
      }
    }
  }

  user_labels = {
    hederaapplication = "elements_produced_count"
  }
  notification_channels = [
    for name, _ in var.email_notifications : google_monitoring_notification_channel.email[name].id
  ]
}


resource "google_monitoring_alert_policy" "cpu_utilization" {
  display_name = "Hedera Application CPU Utilization"
  combiner     = "OR"
  conditions {
    display_name = "CPU Utilization"
    condition_threshold {
      threshold_value = 0.95
      filter          = "resource.type = \"gce_instance\" AND metric.type = \"compute.googleapis.com/instance/cpu/utilization\" AND metric.labels.instance_name = monitoring.regex.full_match(\"${var.dataflow_job_regex}\")"
      duration        = "0s"
      comparison      = "COMPARISON_GT"
      aggregations {
        alignment_period     = "1800s"
        per_series_aligner   = "ALIGN_MAX"
        cross_series_reducer = "REDUCE_MAX"
      }
      trigger {
        count = 1
      }
    }
  }

  user_labels = {
    hederaapplication = "cpu_utilization"
  }
  notification_channels = [
    for name, _ in var.email_notifications : google_monitoring_notification_channel.email[name].id
  ]
}


resource "google_monitoring_alert_policy" "bytes_used" {
  display_name = "Hedera Application Bytes Used"
  combiner     = "OR"
  conditions {
    display_name = "Bytes Used"
    condition_monitoring_query_language {
      query    = "fetch gce_instance\n| metric 'compute.googleapis.com/guest/memory/bytes_used'\n| filter metric.instance_name =~ '${var.dataflow_job_regex}'\n| group_by [metric.instance_name], \n    [used: sum(if(metric.state == 'used', value.bytes_used, 0)),\n     total: sum(value.bytes_used),\n     ratio: sum(if(metric.state == 'used', value.bytes_used, 0)) / sum(value.bytes_used)]\n| group_by 30m, [value_bytes_used_ratio_max: max(val(2))]\n| every 30m\n| group_by [], [value_bytes_used_ratio_max_max: max(value_bytes_used_ratio_max)]\n| condition val() > 95 '10^2.%'"
      duration = "0s"
      trigger {
        count = 1
      }
    }
  }

  user_labels = {
    hederaapplication = "bytes_used"
  }
  notification_channels = [
    for name, _ in var.email_notifications : google_monitoring_notification_channel.email[name].id
  ]
}
