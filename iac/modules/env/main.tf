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

locals {
  data_vault_label = { module_type : "data-vault" }

  required_apis_by_module = {
    shared = [
      "cloudresourcemanager.googleapis.com",
      "compute.googleapis.com",
      "container.googleapis.com",
      "iam.googleapis.com",
      "pubsub.googleapis.com",
    ],
    data_vault = [
      "bigquery.googleapis.com",
      "healthcare.googleapis.com",
      "monitoring.googleapis.com",
    ],
    dicom_proxy = var.deployment_spec.deploy_dicom_proxy ? [
      "redis.googleapis.com",
    ] : [],
    ilm = var.deployment_spec.deploy_ilm ? [
      "cloudscheduler.googleapis.com",
      "dataflow.googleapis.com",
    ] : [],
    transformation = var.deployment_spec.deploy_transformation ? [
      "redis.googleapis.com",
      "vision.googleapis.com",
    ] : [],
  }
}

module "compute_address" {
  source     = "../compute_address"
  region     = var.region
  project_id = var.project_id
}

resource "google_project_service" "shared_required_apis" {
  for_each = setunion(
    local.required_apis_by_module.shared
  )

  service                    = each.key
  project                    = var.project_id
  disable_dependent_services = true
}

module "cloud_pathology_cluster" {
  source                              = "../gke_cluster"
  cluster_name                        = "cloud-pathology"
  region                              = var.region
  project_id                          = var.project_id
  zone                                = var.gke_config.zone
  transformation_machine_type         = var.gke_config.transformation_machine_type
  dicom_proxy_machine_type            = var.gke_config.dicom_proxy_machine_type
  service_account_email               = var.gke_config.service_account_email
  network                             = var.gke_config.network
  subnetwork                          = var.gke_config.subnetwork
  configure_project_level_permissions = var.configure_project_level_permissions
  depends_on = [
    google_project_service.shared_required_apis,
  ]
}

resource "google_project_service" "medical_data_vaults_required_apis" {
  for_each = setunion(
    local.required_apis_by_module.data_vault
  )

  service                    = each.key
  project                    = var.project_id
  disable_dependent_services = true
}

module "medical_data_vaults" {
  depends_on = [
    google_project_service.medical_data_vaults_required_apis,
    module.transformation,
  ]
  source     = "../medical-data-vault"
  project_id = var.project_id
  name       = "dicom-pathology"
  location   = var.region
  medical_image_sets = [
    {
      name                              = "slide-dicom-store"
      structured_metadata               = true
      dicom_store_notification_topic_id = length(module.transformation) > 0 ? module.transformation[0].dicom_store_pubsub_topic_id : null
    },
    {
      name                              = "slide-dicom-store-deid"
      structured_metadata               = false
      dicom_store_notification_topic_id = null
    },
    {
      name                              = "annotations-dicom-store"
      structured_metadata               = false
      dicom_store_notification_topic_id = null
    },
  ]
  researchers = var.data_editors
  trusted_bots = concat(
    length(module.ilm) > 0 ? ["serviceAccount:${module.ilm[0].service_account_email}"] : [],
    length(module.transformation) > 0 ? ["serviceAccount:${module.transformation[0].service_account_email}"] : [],
  )
  data_provider_bots = []
  labels = merge(
    local.data_vault_label, { module_name : "dicom-pathology" }
  )
}
