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

output "name" {
  description = "Name of the data vault"
  value       = var.name
}

output "resource_map" {
  description = "Map of resources provisioned for storing training input"
  value = {
    bq_dataset_id  = google_bigquery_dataset.bq_dataset.dataset_id
    chc_dataset_id = google_healthcare_dataset.chc_dataset.id
    medical_image_storages = [
      for dicom_store in module.dicom_stores :
      tomap(
        {
          dicom_store_id          = dicom_store.resource_map.dicom_store_id,
          dicom_metadata_table_id = dicom_store.resource_map.dicom_metadata_table_id
        }
      )
    ]
  }
}