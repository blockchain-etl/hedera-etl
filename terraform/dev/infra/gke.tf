module "gke" {
  source  = "terraform-google-modules/kubernetes-engine/google//modules/private-cluster"
  version = "34.0.0"

  # req
  project_id        = var.project_id
  name              = "${var.project_name}-${var.env_name}-gke"
  region            = var.region
  zones             = ["${var.region}-a", "${var.region}-b", "${var.region}-c"]
  network           = module.vpc.network_name
  subnetwork        = module.vpc.subnets_names[index(module.vpc.subnets_names, "${var.project_name}-${var.env_name}-subnet-01")]
  ip_range_pods     = module.vpc.subnets_secondary_ranges[0].0.range_name
  ip_range_services = module.vpc.subnets_secondary_ranges[0].1.range_name
  # opt
  http_load_balancing             = true
  network_policy                  = true
  horizontal_pod_autoscaling      = true
  enable_vertical_pod_autoscaling = true
  filestore_csi_driver            = true
  enable_private_endpoint         = false
  enable_private_nodes            = true
  master_ipv4_cidr_block          = var.gke_master_ipv4_cidr_block
  grant_registry_access           = true
  remove_default_node_pool        = true
  master_authorized_networks      = var.authorized_source_ranges
  create_service_account          = true
  service_account_name            = var.gke_service_account_name

  node_pools = [
    {
      name            = "${var.project_name}-pool"
      machine_type    = var.pool_machine_type
      node_locations  = "${var.region}-a" # ,${var.region}-b,${var.region}-c"
      min_count       = var.pool_min_count
      max_count       = var.pool_max_count
      local_ssd_count = 0
      spot            = false
      disk_size_gb    = var.pool_disk_size
      disk_type       = "pd-standard"
      image_type      = "COS_CONTAINERD"
      enable_gcfs     = false
      enable_gvnic    = false
      auto_repair     = true
      auto_upgrade    = true
      # service_account    = var.service_account #! utworzyć
      preemptible        = false
      initial_node_count = var.pool_initial_node_count
    },
  ]

  #! obczaić i dodać/usunąć (nie)potrzebne
  node_pools_oauth_scopes = {
    all = [
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring",
      # "https://www.googleapis.com/auth/devstorage.read_only", # cloud storage
      "https://www.googleapis.com/auth/trace.append",
    ]
  }

  node_pools_labels = {
    all = {}

    workloads-pool = {
      workloads-pool = true
      project        = "${var.project_name}"
      environment    = "${var.env_name}"
    }
  }

  node_pools_metadata = {
    all = {}

    workloads-pool = {
      node-pool-metadata-custom-value = "${var.project_name}-pool"
    }
  }

  node_pools_taints = {
    all = []

    workloads-pool = [
      {
        key    = "${var.project_name}-pool"
        value  = true
        effect = "PREFER_NO_SCHEDULE"
      },
    ]
  }

  node_pools_tags = {
    all = []

    workloads-pool = [
      "${var.project_name}-pool",
    ]
  }

  cluster_resource_labels = {
    project     = "${var.project_name}"
    environment = "${var.env_name}"
  }

  depends_on = [
    module.enabled_google_apis
  ]
}
