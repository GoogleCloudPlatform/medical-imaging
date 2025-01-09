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

output "resource_map" {
  description = "Map of resource Ids provisioned by the module"
  value = {
    dicom_store_id = (
      var.structured_metadata.enabled ?
      google_healthcare_dicom_store.dicom_images_with_structured_metadata[0].id :
      google_healthcare_dicom_store.dicom_images[0].id
    )
    dicom_metadata_table_id = (
      var.structured_metadata.enabled ?
      google_bigquery_table.dicom_metadata[0].table_id : null
    )
  }
}