output "registry" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}

output "get_credentials" {
  value = "gcloud container clusters get-credentials ${google_container_cluster.gg.name} --zone ${var.zone} --project ${var.project_id}"
}
