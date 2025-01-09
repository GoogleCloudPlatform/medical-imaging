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

resource "google_service_account_iam_member" "app_workload_identity" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${google_service_account.default.account_id}@${var.project_id}.iam.gserviceaccount.com"
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[default/${var.service_name}-k8s-sa]"

  depends_on = [
    google_service_account.default,
  ]
}

resource "google_project_iam_member" "logs_writing_iam_member" {
  count   = var.configure_project_level_permissions ? 1 : 0
  role    = "roles/logging.logWriter"
  project = var.project_id
  member  = "serviceAccount:${google_service_account.default.email}"
}
