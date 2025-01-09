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

locals {
  project_iam_member_roles = toset([
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/monitoring.viewer",
    "roles/stackdriver.resourceMetadata.writer",
  ])
}

# Permissions required by cluster service account to operate GKE.
# See documentation here:
# https://cloud.google.com/kubernetes-engine/docs/how-to/hardening-your-cluster#use_least_privilege_sa
resource "google_project_iam_member" "project_role_iam_member" {
  for_each = var.configure_project_level_permissions ? local.project_iam_member_roles : []
  role     = each.key
  project  = var.project_id
  member   = "serviceAccount:${var.service_account_email == "" ? google_service_account.cluster_service_account[0].email : var.service_account_email}"
  depends_on = [
    google_service_account.cluster_service_account,
  ]
}
