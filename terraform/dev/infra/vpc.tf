module "vpc" {
  source  = "terraform-google-modules/network/google"
  version = "10.0.0"

  project_id   = var.project_id
  network_name = "${var.project_name}-${var.env_name}-network"
  routing_mode = "GLOBAL"

  subnets = [
    {
      subnet_name               = "${var.project_name}-${var.env_name}-subnet-01"
      subnet_ip                 = "10.10.10.0/24"
      subnet_region             = var.region
      subnet_private_access     = "true"
      subnet_flow_logs          = "true"
      description               = "Production workloads"
      subnet_flow_logs          = "true"
      subnet_flow_logs_interval = "INTERVAL_10_MIN"
      subnet_flow_logs_sampling = 0.7
      subnet_flow_logs_metadata = "INCLUDE_ALL_METADATA"
    },
    {
      subnet_name               = "${var.project_name}-${var.env_name}-subnet-02"
      subnet_ip                 = "10.20.10.0/24"
      subnet_region             = var.region
      subnet_private_access     = "true"
      subnet_flow_logs          = "true"
      description               = "Production data"
      subnet_flow_logs          = "true"
      subnet_flow_logs_interval = "INTERVAL_10_MIN"
      subnet_flow_logs_sampling = 0.7
      subnet_flow_logs_metadata = "INCLUDE_ALL_METADATA"
    }
  ]
  secondary_ranges = {
    #! czy potrzebne?
    "${var.project_name}-${var.env_name}-subnet-01" = [
      {
        range_name    = "pods"
        ip_cidr_range = "10.1.0.0/20"
      },
      {
        range_name    = "services"
        ip_cidr_range = "10.10.11.0/24"
      },
    ]

    "${var.project_name}-${var.env_name}-subnet-02" = []
  }
}
