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

variable "region" {
  description = "The GCP region for resources in this environment"
  type        = string
}

variable "project_id" {
  description = "The GCP project ID for this environment"
  type        = string
}

variable "gke_config" {
  description = <<-EOT
  Configuration for GKE resources.

  Parameters:
    dicom_proxy_machine_type: The machine type to use for GKE DICOM proxy
      node pool. n2-highmem-4, n2-highmem-8, or c3-highmem-8 are recommended
      (please note you may need to increased CPUs quota, see playbook for more
      details).
      See available machine types in
      https://cloud.google.com/compute/docs/general-purpose-machines
    transformation_machine_type: The machine type to use in GKE transformation
      node pool. c2-standard-30 is recommended for optimized performance
      (please note you may need to require increased C2 CPUs quota in this
      case, see playbook for more details).
      See available C2 machine types in
      https://cloud.google.com/compute/docs/compute-optimized-machines#c2_machine_types
    zone: (Optional) The GCP zone for GKE resources. If empty, region is used as
      location instead, which increases availability.
      See more details in
      https://cloud.google.com/kubernetes-engine/docs/concepts/types-of-clusters
    service_account_email: (Optional) Service account email to use for GKE
      cluster. If empty, a new service account will be created.
    network: (Optional) Name or self_link of the Google Compute Engine network
      to which GKE cluster will be connected. For Shared VPC, set this to the
      self link of the shared network. If empty, default network is used.
    subnetwork:   (Optional) Name or self_link of the Google Compute Engine
      subnetwork in which the GKE cluster instances are launched. If empty,
      default subnetwork is used.

  EOT
  type = object({
    dicom_proxy_machine_type    = string
    transformation_machine_type = string
    zone                        = string
    service_account_email       = string
    network                     = string
    subnetwork                  = string
  })
  default = {
    dicom_proxy_machine_type    = ""
    transformation_machine_type = ""
    zone                        = ""
    service_account_email       = ""
    network                     = ""
    subnetwork                  = ""
  }
}

variable "configure_project_level_permissions" {
  description = <<-EOT
  Whether project level permissions should be configured for modules.
  If false, modules may not work properly and will require manually configuring
  permissions.
  EOT
  type        = bool
}

variable "configure_audit_logs" {
  description = <<-EOT
  Whether audit logs should be configured for modules.
  If false, modules may not work properly and will require manually configuring
  logs.
  EOT
  type        = bool
}

variable "data_editors" {
  description = <<-EOT
  (Optional) List of principals with priviledged access to the data vault.
  Should be in Terraform IAM bindings format.
  Example: ["group:my_data_editor_group@example.com", "user:me@example.com"]
  EOT
  type        = list(string)
}

variable "iap_users" {
  description = <<-EOT
  (Optional) List of principals to receive IAP access (iap.httpsResourceAccessor
  IAM role) for the project. Should be in Terraform IAM bindings format.
  Example: ["group:my_data_editor_group@example.com", "user:me@example.com"]
  Only used if configure_project_level_permissions is set to true.
  EOT
  type        = list(string)
  default     = []
}

variable "deployment_spec" {
  description = "Specification of the deployment configuration"
  type = object({
    deploy_dicom_proxy    = bool
    deploy_ilm            = bool
    deploy_transformation = bool
  })
  default = {
    deploy_dicom_proxy    = true
    deploy_ilm            = true
    deploy_transformation = true
  }
}

variable "dicom_proxy_redis_config" {
  description = "Redis instance config for DICOM proxy"
  type = object({
    enable_redis              = bool
    memory_gb                 = number
    enable_auth               = bool
    enable_transit_encryption = bool
    network                   = string
  })
  default = {
    enable_redis              = true
    memory_gb                 = 16
    enable_auth               = false
    enable_transit_encryption = false
    network                   = ""
  }
}

variable "transformation_redis_config" {
  description = "Redis instance config for Transformation pipeline"
  type = object({
    enable_redis              = bool
    memory_gb                 = number
    enable_auth               = bool
    enable_transit_encryption = bool
    network                   = string
  })
  default = {
    enable_redis              = true
    memory_gb                 = 1
    enable_auth               = false
    enable_transit_encryption = false
    network                   = ""
  }
}