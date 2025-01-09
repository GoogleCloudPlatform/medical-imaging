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

# Allow researchers & trusted bots to run BigQuery jobs
resource "google_bigquery_dataset_iam_binding" "bq_dataset_users" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.bq_dataset.dataset_id
  role       = "roles/bigquery.user"
  members    = concat(var.researchers, var.trusted_bots)
}

# Find out the service agent for cloud healthcare (CHC) API. Read more at
# https://cloud.google.com/healthcare-api/docs/permissions-healthcare-api-gcp-products#the_cloud_healthcare_service_agent
resource "google_project_service_identity" "chc_identity" {
  # TODO drop beta provider, when resource type is mainstream
  provider = google-beta
  project  = var.project_id
  service  = "healthcare.googleapis.com"
}

locals {
  any_dicom_to_bq_stream = (contains(
  [for i in var.medical_image_sets : i.structured_metadata], true))
  chc_bot = ["serviceAccount:${google_project_service_identity.chc_identity.email}"]
}

# Allow researchers & trusted bots to view & edit data in the BQ dataset
# Allow Cloud Healthcare (chc) API Service Agent to stream DICOM attributes
resource "google_bigquery_dataset_iam_binding" "bq_dataset_data_editors" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.bq_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  members = concat(var.researchers, var.trusted_bots,
  (local.any_dicom_to_bq_stream ? local.chc_bot : []))
}

# Allow researchers & trusted bots to view dicom stores within the dataset
resource "google_healthcare_dataset_iam_binding" "chc_dataset_dicom_store_viewers" {
  dataset_id = google_healthcare_dataset.chc_dataset.id
  role       = "roles/healthcare.dicomStoreViewer"
  members    = concat(var.researchers, var.trusted_bots)
}

# Allow researchers & trusted bots to view and edit images in all DICOM stores
resource "google_healthcare_dicom_store_iam_binding" "dicom_store_dicom_editors" {
  for_each       = module.dicom_stores
  dicom_store_id = each.value.resource_map.dicom_store_id
  role           = "roles/healthcare.dicomEditor"
  members        = concat(var.researchers, var.trusted_bots)
}

