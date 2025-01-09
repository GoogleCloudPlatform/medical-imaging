# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Note: all configs in this directory have been created by using Cloud
# config-connector to export configs from existing Cloudflyer projects and
# tweaking / trimming them to get to a deployable state. So it is possible there
# are unnecessary settings here that might be hardcoding things that might as
# well be left as defaults, so it should be OK to edit these configs liberally
# and not treating them as set in stone.

locals {
  oauth_scopes = [
    "https://www.googleapis.com/auth/devstorage.read_only",
    "https://www.googleapis.com/auth/logging.write",
    "https://www.googleapis.com/auth/monitoring",
    "https://www.googleapis.com/auth/service.management.readonly",
    "https://www.googleapis.com/auth/servicecontrol",
    "https://www.googleapis.com/auth/trace.append"
  ]
}

resource "google_service_account" "cluster_service_account" {
  count        = var.service_account_email == "" ? 1 : 0
  project      = var.project_id
  account_id   = "${var.cluster_name}-cluster-sa"
  display_name = "Service account of GKE cluster."
}

# https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/container_cluster
resource "google_container_cluster" "cluster" {
  addons_config {
    network_policy_config {
      disabled = true
    }
  }
  cluster_autoscaling {
    auto_provisioning_defaults {
      oauth_scopes    = local.oauth_scopes
      service_account = var.service_account_email == "" ? google_service_account.cluster_service_account[0].email : var.service_account_email
    }
    enabled = true
    resource_limits {
      maximum       = 6400
      minimum       = 1
      resource_type = "cpu"
    }
    resource_limits {
      maximum       = 51200
      minimum       = 1
      resource_type = "memory"
    }
  }

  # This is set to avoid destruction/creation in every update, without this
  # terraform forces destruction of the resource when updating. (with a
  # `# forces replacement` comment in `terraform apply` output)
  # Blank value means use the default.
  # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/container_cluster#cluster_ipv4_cidr
  ip_allocation_policy {
    cluster_ipv4_cidr_block  = ""
    services_ipv4_cidr_block = ""
  }

  # Imitating instructions in
  # https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity
  # in terraform.
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  default_max_pods_per_node   = 110
  enable_intranode_visibility = true
  enable_shielded_nodes       = true
  location                    = var.zone == "" ? var.region : var.zone
  master_auth {
    client_certificate_config {
      issue_client_certificate = false
    }
  }
  name    = "${var.cluster_name}-cluster"
  network = var.network == "" ? "projects/${var.project_id}/global/networks/default" : var.network
  network_policy {
    enabled  = false
    provider = "PROVIDER_UNSPECIFIED"
  }

  project = var.project_id
  release_channel {
    channel = "REGULAR"
  }
  subnetwork = var.subnetwork == "" ? "projects/${var.project_id}/regions/${var.region}/subnetworks/default" : var.subnetwork
  vertical_pod_autoscaling {
    enabled = true
  }
  # Cannot create cluster without a node pool, so creating the smallest possible
  # initial pool and immediately deleting it. Using separately managed pools to
  # allow modifying them without having to recreate cluster.
  # See https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/container_node_pool
  initial_node_count       = 1
  remove_default_node_pool = true

  resource_labels = {
    goog-packaged-solution : "medical-imaging-suite"
    goog-packaged-solution-type : "digital-pathology"
  }
}

resource "google_container_node_pool" "e2_pool" {
  name     = "e2-pool"
  project  = var.project_id
  location = var.zone == "" ? var.region : var.zone
  cluster  = google_container_cluster.cluster.name
  autoscaling {
    min_node_count = 0
    max_node_count = 1000
  }
  initial_node_count = 1
  management {
    auto_repair  = true
    auto_upgrade = true
  }
  max_pods_per_node = 110
  node_config {
    disk_size_gb = 100
    disk_type    = "pd-standard"
    machine_type = "e2-medium"
    metadata     = { disable-legacy-endpoints = "true" }
    # Imitating instructions in
    # https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity
    # in terraform.
    workload_metadata_config {
      mode = "GKE_METADATA"
    }
    oauth_scopes    = local.oauth_scopes
    service_account = var.service_account_email == "" ? google_service_account.cluster_service_account[0].email : var.service_account_email
    shielded_instance_config {
      enable_integrity_monitoring = true
      enable_secure_boot          = true
    }
  }
  upgrade_settings {
    max_surge       = 1
    max_unavailable = 0
  }
  depends_on = [
    google_container_cluster.cluster,
  ]
}

resource "google_container_node_pool" "transformation_pool" {
  name     = "transformation-pool"
  project  = var.project_id
  location = var.zone == "" ? var.region : var.zone
  cluster  = google_container_cluster.cluster.name
  autoscaling {
    min_node_count = 0
    max_node_count = 5
  }
  initial_node_count = 1
  management {
    auto_repair  = true
    auto_upgrade = true
  }
  max_pods_per_node = 100
  node_config {
    disk_size_gb = 100
    disk_type    = "pd-ssd"
    machine_type = var.transformation_machine_type
    metadata     = { disable-legacy-endpoints = "true" }
    # Imitating instructions in
    # https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity
    # in terraform.
    workload_metadata_config {
      mode = "GKE_METADATA"
    }
    oauth_scopes    = local.oauth_scopes
    service_account = var.service_account_email == "" ? google_service_account.cluster_service_account[0].email : var.service_account_email
    shielded_instance_config {
      enable_integrity_monitoring = true
      enable_secure_boot          = true
    }
  }
  upgrade_settings {
    max_surge       = 1
    max_unavailable = 0
  }
  depends_on = [
    google_container_cluster.cluster,
  ]
}

resource "google_container_node_pool" "dicom_proxy_pool" {
  name     = "dicom-proxy-pool"
  project  = var.project_id
  location = var.zone == "" ? var.region : var.zone
  cluster  = google_container_cluster.cluster.name
  autoscaling {
    min_node_count = 0
    max_node_count = 5
  }
  initial_node_count = 1
  management {
    auto_repair  = true
    auto_upgrade = true
  }
  max_pods_per_node = 100
  node_config {
    disk_size_gb = 100
    disk_type    = "pd-ssd"
    machine_type = var.dicom_proxy_machine_type
    metadata     = { disable-legacy-endpoints = "true" }
    # Imitating instructions in
    # https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity
    # in terraform.
    workload_metadata_config {
      mode = "GKE_METADATA"
    }
    oauth_scopes    = local.oauth_scopes
    service_account = var.service_account_email == "" ? google_service_account.cluster_service_account[0].email : var.service_account_email
    shielded_instance_config {
      enable_integrity_monitoring = true
      enable_secure_boot          = true
    }
  }
  upgrade_settings {
    max_surge       = 1
    max_unavailable = 0
  }
  depends_on = [
    google_container_cluster.cluster,
  ]
}