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
variable "region" {
  description = "The GCP region for resources in this module"
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
variable "data_editors" {
  description = <<-EOT
  (Optional) List of users with editor access to transformation GCS buckets.
  Should be in Terraform IAM bindings format.
  Example: ["group:my_data_editor_group@example.com", "user:me@example.com"]
  EOT
  type        = list(string)
  default     = []
}
variable "redis_config" {
  description = "Redis instance config for transformation"
  type = object({
    enable_redis              = bool
    memory_gb                 = number
    enable_auth               = bool
    enable_transit_encryption = bool
    network                   = string
  })
}
