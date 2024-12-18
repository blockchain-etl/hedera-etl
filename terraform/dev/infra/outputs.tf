output "gke_endpoint" {
  value     = module.gke.endpoint
  sensitive = true
}

output "gke_node_pools_names" {
  value = module.gke.node_pools_names
}

output "gke_service_account" {
  value = module.gke.service_account
}

output "gke_ca_cert" {
  value = module.gke.ca_certificate
}
