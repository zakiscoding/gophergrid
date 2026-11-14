# GopherGrid on GKE: one zonal Standard cluster with a small spot node pool, plus an Artifact
# Registry repo for the scheduler/worker images. Everything here is destroyed after the run
# (terraform destroy, then delete the project); see README.md.

resource "google_project_service" "apis" {
  for_each           = toset(["container.googleapis.com", "artifactregistry.googleapis.com"])
  service            = each.value
  disable_on_destroy = false
}

resource "google_artifact_registry_repository" "images" {
  location      = var.region
  repository_id = "gophergrid"
  format        = "DOCKER"
  depends_on    = [google_project_service.apis]
}

resource "google_container_cluster" "gg" {
  name     = "gophergrid"
  location = var.zone

  # manage nodes in a separate pool so it can be sized/replaced without touching the cluster
  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = false

  release_channel { channel = "REGULAR" }
  networking_mode = "VPC_NATIVE"
  ip_allocation_policy {}
  # no GKE-managed Prometheus/logging extras: keep the bill to the nodes
  monitoring_config {
    enable_components = ["SYSTEM_COMPONENTS"]
    managed_prometheus { enabled = false }
  }
  logging_config { enable_components = ["SYSTEM_COMPONENTS"] }

  depends_on = [google_project_service.apis]
}

resource "google_container_node_pool" "workers" {
  name               = "pool-a"
  cluster            = google_container_cluster.gg.id
  initial_node_count = var.node_count

  # GKE cluster autoscaler: KEDA adds worker pods on queue depth; when they don't fit, the pool
  # grows (run 1 without this: 4 of 6 workers sat Pending on 2 nodes for the whole benchmark)
  autoscaling {
    min_node_count = var.node_count
    max_node_count = var.max_nodes
  }

  node_config {
    machine_type = var.machine_type
    spot         = var.spot
    disk_size_gb = 30
    disk_type    = "pd-standard"
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"] # pull from Artifact Registry
    labels       = { app = "gophergrid" }
  }
}
