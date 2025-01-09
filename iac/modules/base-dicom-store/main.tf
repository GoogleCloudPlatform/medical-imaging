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

# Create a BQ table, if structured_metadata is requested
resource "google_bigquery_table" "dicom_metadata" {
  count      = var.structured_metadata.enabled ? 1 : 0
  project    = var.project_id
  dataset_id = var.structured_metadata.bq_dataset_id
  table_id   = "${var.store_name}-metadata"
  labels = merge(
    var.labels,
    {
      goog-packaged-solution : "medical-imaging-suite"
      goog-packaged-solution-type : "digital-pathology"
    }
  )
  deletion_protection = true
}

# Create a DICOM store with BQ stream, if structured_metadata is requested
resource "google_healthcare_dicom_store" "dicom_images_with_structured_metadata" {
  count = var.structured_metadata.enabled ? 1 : 0
  # TODO drop beta provider, when resource type is mainstream
  provider = google-beta
  name     = var.store_name
  dataset  = var.chc_dataset_id
  labels = merge(
    var.labels,
    {
      goog-packaged-solution : "medical-imaging-suite"
      goog-packaged-solution-type : "digital-pathology"
    }
  )
  stream_configs {
    bigquery_destination {
      table_uri = "bq://${var.project_id}.${var.structured_metadata.bq_dataset_id}.${google_bigquery_table.dicom_metadata[0].table_id}"
    }
  }
  dynamic "notification_config" {
    for_each = var.dicom_store_notification_topic_id != null ? [1] : []
    content {
      pubsub_topic = var.dicom_store_notification_topic_id
    }
  }
}

# Create a DICOM store w/o BQ stream, if structured_metadata is not requested
resource "google_healthcare_dicom_store" "dicom_images" {
  count   = ! var.structured_metadata.enabled ? 1 : 0
  name    = var.store_name
  dataset = var.chc_dataset_id
  labels = merge(
    var.labels,
    {
      goog-packaged-solution : "medical-imaging-suite"
      goog-packaged-solution-type : "digital-pathology"
    }
  )
  dynamic "notification_config" {
    for_each = var.dicom_store_notification_topic_id != null ? [1] : []
    content {
      pubsub_topic = var.dicom_store_notification_topic_id
    }
  }
}