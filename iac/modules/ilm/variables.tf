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
variable "configure_audit_logs" {
  description = <<-EOT
  Whether audit logs should be configured for this module. If false, module may
  not work properly and will require manually configuring logs.
  EOT
  type        = bool
}
