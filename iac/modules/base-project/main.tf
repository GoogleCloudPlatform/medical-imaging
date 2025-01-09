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

# 1. Create the project
resource "google_project" "the_project" {
  count               = var.spec.existing_project ? 0 : 1
  project_id          = var.spec.id
  name                = var.spec.name != null ? var.spec.name : var.spec.id
  org_id              = var.spec.org_id
  folder_id           = var.spec.folder_id
  billing_account     = var.spec.billing_account
  auto_create_network = "false"
  labels = {
    goog-packaged-solution : "medical-imaging-suite"
    goog-packaged-solution-type : "digital-pathology"
  }
}

# 3. If requested, route admin activities and data access logs to the
# audit logs bucket in the specified core project
locals {
  activity_log     = "\"projects/${var.spec.id}/logs/cloudaudit.googleapis.com%2Factivity\""
  data_access_log  = "\"projects/${var.spec.id}/logs/cloudaudit.googleapis.com%2Fdata_access\""
  system_event_log = "\"projects/${var.spec.id}/logs/cloudaudit.googleapis.com%2Fsystem_event\""
  policy_log       = "\"projects/${var.spec.id}/logs/cloudaudit.googleapis.com%2Fpolicy\""
  resoure_type     = "(resource.type = audited_resource OR resource.type = project)"

  core_project_id = (var.spec.core_project_id == "self" ?
    var.spec.id :
  var.spec.core_project_id)
  # WARNING: hard coded values; assuming logs bucket and view names
  # and location remain fixed. They are set in
  # templates/core-project/main.tf; look them up by the matching WARNING.
  audit_logs_bucket  = "projects/${local.core_project_id}/locations/global/buckets/audit-logs-bucket"
  audit_logs_view_id = "${local.audit_logs_bucket}/views/audit-logs-view"
}

resource "google_logging_project_sink" "audit_logs_sink" {
  count                  = var.spec.retain_audit_logs ? 1 : 0
  project                = var.spec.id
  name                   = "audit-logs-sink"
  destination            = "logging.googleapis.com/${local.audit_logs_bucket}"
  filter                 = <<-EOT
  ${local.resoure_type} AND logName=(${local.activity_log} OR
  ${local.data_access_log} OR ${local.system_event_log} OR ${local.policy_log})
  EOT
  unique_writer_identity = true
  depends_on             = [google_project.the_project]
  labels = {
    goog-packaged-solution : "medical-imaging-suite"
    goog-packaged-solution-type : "digital-pathology"
  }
}