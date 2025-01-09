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

variable "spec" {
  description = "(Required) Google Cloud Project spec"
  type = object({
    existing_project = bool
    id               = string
    name             = string
    org_id           = string
    folder_id        = string
    billing_account  = string
    project_roles = object({
      owners                = list(string)
      data_privacy_auditors = list(string)
    })
    core_project_id   = string
    retain_audit_logs = bool
  })
  validation {
    condition = (var.spec.existing_project == false ||
    var.spec.project_roles.owners == null)
    error_message = "Do not specify project owners for an existing project."
  }
  validation {
    condition     = var.spec.id != null
    error_message = "Please set project_spec.id."
  }
  validation {
    condition     = length(var.spec.id) <= 30
    error_message = "Please ensure project_spec.id is no longer than 30 characters."
  }
  validation {
    condition     = (var.spec.org_id == null || var.spec.folder_id == null)
    error_message = "Please set only one of org_id or folder_id."
  }
  validation {
    condition = (var.spec.existing_project == true ||
    var.spec.project_roles.owners != null)
    error_message = "Please set spec.project_roles.owners for non-existing projects."
  }
  validation {
    condition     = (! var.spec.retain_audit_logs || var.spec.core_project_id != null)
    error_message = "Please set spec.core_project_id."
  }
}