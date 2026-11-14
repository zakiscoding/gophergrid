variable "project_id" {
  description = "GCP project to create everything in (a throwaway project; delete it afterwards)"
  type        = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "zone" {
  description = "Zonal cluster: GKE's free tier covers the management fee of one zonal cluster"
  type        = string
  default     = "us-central1-a"
}

variable "node_count" {
  type    = number
  default = 2
}

variable "machine_type" {
  type    = string
  default = "e2-standard-2" # 2 vCPU, 8 GB
}

variable "spot" {
  description = "Spot VMs: ~60-90% cheaper, can be preempted (fine for a benchmark)"
  type        = bool
  default     = true
}

variable "max_nodes" {
  description = "Upper bound for the cluster autoscaler"
  type        = number
  default     = 4
}
