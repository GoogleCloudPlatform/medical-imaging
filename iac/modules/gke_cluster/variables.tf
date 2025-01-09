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
  description = "The GCP project ID for resources in this module"
  type        = string
}
variable "cluster_name" {
  description = "Used to name this collection of GKE resources, e.g. foo-cluster"
  type        = string
}
variable "region" {
  description = "The GCP region for resources in this module"
  type        = string
}
variable "zone" {
  description = <<-EOT
  (Optional) The GCP zone for resources in this module. If unset, region is used
  as location instead, which increases availability.
  See https://cloud.google.com/kubernetes-engine/docs/concepts/types-of-clusters
  for more details.
  EOT
  type        = string
  default     = ""
}
variable "transformation_machine_type" {
  description = "The machine type to use in transformation node pool."
  type        = string
}
variable "dicom_proxy_machine_type" {
  description = "The machine type to use in DICOM proxy node pool."
  type        = string
}
variable "configure_project_level_permissions" {
  description = <<-EOT
  Whether project level permissions should be configured for this module.
  If false, module may not work properly and will require manually configuring
  permissions.
  EOT
  type        = bool
}
variable "service_account_email" {
  description = <<-EOT
  (Optional) Service account to use for GKE cluster. If unset, a new service
  account will be created.
  EOT
  type        = string
  default     = ""
}
variable "network" {
  description = <<-EOT
  (Optional) Name or self_link of the Google Compute Engine network to which
  the GKE cluster will be connected. For Shared VPC, set this to the self link
  of the shared network. If unset, default network is used.
  EOT
  type        = string
  default     = ""
}
variable "subnetwork" {
  description = <<-EOT
  (Optional) Name or self_link of the Google Compute Engine subnetwork in which
  the GKE cluster instances are launched. If unset, default subnetwork is used.
  EOT
  type        = string
  default     = ""
}
