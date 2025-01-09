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

variable "project_id" {
  description = "(Required) Unique identifier for an existing project."
  type        = string
  validation {
    condition     = var.project_id != null
    error_message = "Please set project_id."
  }
}

variable "chc_dataset_id" {
  description = <<-EOT
  "(Required) Unique identifier for an existing Healthcare dataset."
  EOT
  type        = string
  validation {
    condition     = var.chc_dataset_id != null
    error_message = "Please set chc_dataset_id."
  }
}

variable "store_name" {
  description = "(Required) Name of the DICOM Store to be created."
  type        = string
  validation {
    condition     = var.store_name != null
    error_message = "Please set store_name."
  }
}

variable "structured_metadata" {
  description = <<-EOT
  (Optional) If enabled, DICOM attributes are streamed to a BigQuery table
  within the specified dataset for SQL analysis.
  EOT
  type = object({
    enabled       = bool
    bq_dataset_id = string
  })
  validation {
    condition = (
      ! var.structured_metadata.enabled ||
      var.structured_metadata.bq_dataset_id != null
    )
    error_message = "Please set bq_dataset_id."
  }
}

variable "dicom_store_notification_topic_id" {
  description = <<-EOT
  (Optional) Pub/Sub topic id to publish notifications from DICOM store.
  EOT
  type        = string
}

variable "labels" {
  description = "(Optional) Map of key-value pairs for labeling resources."
  type        = map
}