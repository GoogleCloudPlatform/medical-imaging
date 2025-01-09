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

# 1. Create a Clould healthcare (chc) dataset
resource "google_healthcare_dataset" "chc_dataset" {
  project  = var.project_id
  name     = var.name
  location = var.location
  # TODO add labels when suppported
  # labels   = var.labels
}

# 2. Create a BigQuery dataset to organize structured data
resource "google_bigquery_dataset" "bq_dataset" {
  project     = var.project_id
  dataset_id  = replace(var.name, "-", "_")
  description = <<-EOT
  Data warehouse organizing structured data for the
  medical data vault ${var.name}.
  EOT
  location    = var.location
  labels      = var.labels
}

# 3. Create requested DICOM store(s)
module "dicom_stores" {
  source         = "../base-dicom-store"
  for_each       = { for medical_image_set in var.medical_image_sets : medical_image_set.name => medical_image_set }
  store_name     = each.value.name
  project_id     = var.project_id
  chc_dataset_id = google_healthcare_dataset.chc_dataset.id
  structured_metadata = {
    enabled       = each.value.structured_metadata
    bq_dataset_id = google_bigquery_dataset.bq_dataset.dataset_id
  }
  dicom_store_notification_topic_id = each.value.dicom_store_notification_topic_id
  labels                            = var.labels
  depends_on = [
    google_healthcare_dataset.chc_dataset,
    google_bigquery_dataset.bq_dataset,
  ]
}
