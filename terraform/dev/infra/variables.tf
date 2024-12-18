variable "credentials_file" {
  default = "./../../credentials/mystical-being.json"
}

variable "project_id" {
  default = "mystical-being-444413-n9"
}

variable "region" {
  default = "europe-central2" # warsaw
}

variable "project_name" {
  default = "h2gcp"
}

variable "env_name" {
  default = "dev"
}

variable "gke_master_ipv4_cidr_block" {
  type    = string
  default = "172.23.0.0/28"
}

# authorized networks with access to GKE management
variable "authorized_source_ranges" {
  default = [{
    cidr_block   = "185.129.32.56/29"
    display_name = "office"
    }, {
    cidr_block   = "31.0.232.43/32"
    display_name = "office backup"
    }, {
    cidr_block   = "75.119.152.171/32"
    display_name = "vpn"
  }]
}

variable "pool_machine_type" {
  default = "e2-standard-4" # 4vCPU + 16GiB
}
variable "pool_initial_node_count" {
  default = 1
}
variable "pool_min_count" {
  default = 1
}
variable "pool_max_count" {
  default = 2
}
variable "pool_disk_size" {
  default = "50"
}

variable "gke_service_account_name" {
  default = "h2gcp-service-account"
}
