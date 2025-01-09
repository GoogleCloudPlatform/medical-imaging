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
  description = "(Required) Unique identifier for an existing project"
  type        = string
  validation {
    condition     = var.project_id != null
    error_message = "Please set the project_id."
  }
}
variable "name" {
  description = <<-EOT
  (Required) Name of the vault; used in naming low level resources
  within the vault
  EOT
  type        = string
  validation {
    condition     = var.name != null
    error_message = "Please set the name."
  }
}
variable "location" {
  description = <<-EOT
  (Required) Location of the provisioned resources.
  Consult the following guides and find a location where all Cloud services
  within the module are available:
  https://cloud.google.com/storage/docs/locations
  https://cloud.google.com/healthcare-api/docs/regions
  https://cloud.google.com/bigquery/docs/locations
  EOT
  type        = string
  validation {
    condition     = var.location != null
    error_message = "Please set the location."
  }
}
variable "medical_image_sets" {
  description = <<-EOT
  (Required) List of DICOM stores to be created within the vault.
  Set structured_metadata=true to stream DICOM metadata to BigQuery; see
  https://cloud.google.com/healthcare-api/docs/how-tos/dicom-bigquery-streaming.
  Set dicom_store_notification_topic_id=<pub/sub topic id> to enable
  notifications on it from DICOM store, otherwise set to null.
  EOT
  type = list(object({
    name                              = string
    structured_metadata               = bool
    dicom_store_notification_topic_id = string
  }))
  validation {
    condition = (var.medical_image_sets != null &&
    length(var.medical_image_sets) > 0)
    error_message = "Please define a list of one or more medical_image_sets."
  }
}
variable "researchers" {
  description = <<-EOT
  (Optional) List of principals with priviledges access to the data valut
  EOT
  type        = list(string)
}
variable "trusted_bots" {
  description = <<-EOT
  (Optional) Service accounts, typically created as part of the research
  lab module, which are given the same priviledges as the humans (researchers).
  They may belong to other projects owned by your organization.
  EOT
  type        = list(string)
}
variable "data_provider_bots" {
  description = <<-EOT
  (Optional) Service accounts, typically owned by research partner
  organizations, which are given blind write-only access to the vault entry
  bucket.
  EOT
  type        = list(string)
}
variable "labels" {
  description = "(Optional) Map of key-value pairs for labeling resources"
  type        = map(any)
}