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
    "roles/artifactregistry.writer",
    "roles/cloudbuild.builds.editor",
    "roles/dataflow.worker",
    "roles/datapipelines.invoker",
    "roles/logging.logWriter",
    "roles/viewer",
  ])
}

# GCS IAM bindings
data "google_project" "ilm_project" {
  project_id = var.project_id
}
data "google_iam_policy" "gcs_iam_policy" {
  binding {
    role = "roles/storage.objectViewer"
    members = [
      # The Cloud Healthcare API service account must have the GCS viewer
      # role for filter files in storage class change requests.
      "serviceAccount:service-${data.google_project.ilm_project.number}@gcp-sa-healthcare.iam.gserviceaccount.com",
    ]
  }
  binding {
    role = "roles/storage.admin"
    members = [
      "serviceAccount:${google_service_account.ilm_service_account.email}",
    ]
  }
}
resource "google_storage_bucket_iam_policy" "gcs_iam_binding" {
  bucket      = google_storage_bucket.ilm_bucket.name
  policy_data = data.google_iam_policy.gcs_iam_policy.policy_data
  depends_on = [
    google_storage_bucket.ilm_bucket,
  ]
}

# Permissions required by service account to create and run Dataflow pipeline.
# See documentation here:
# https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates#permissions
resource "google_project_iam_member" "project_role_iam_member" {
  for_each = var.configure_project_level_permissions ? local.project_iam_member_roles : []
  role     = each.key
  project  = var.project_id
  member   = "serviceAccount:${google_service_account.ilm_service_account.email}"
  depends_on = [
    google_service_account.ilm_service_account,
  ]
}
