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
  gcs_buckets = toset([
    google_storage_bucket.gcs_input_images_bucket.name,
    google_storage_bucket.gcs_input_metadata_bucket.name,
    google_storage_bucket.gcs_output_bucket.name,
    google_storage_bucket.gcs_restore_bucket.name,
  ])
  pubsub_subscriptions = toset([
    google_pubsub_subscription.dicom_store_subscription.name,
    google_pubsub_subscription.gcs_subscription.name,
  ])
}

# GCS IAM bindings
data "google_iam_policy" "gcs_iam_policy" {
  binding {
    role = "roles/storage.admin"
    members = concat(
      ["serviceAccount:${module.gke.service_account_email}"],
      var.data_editors,
    )
  }
}
resource "google_storage_bucket_iam_policy" "gcs_iam_binding" {
  for_each    = local.gcs_buckets
  bucket      = each.key
  policy_data = data.google_iam_policy.gcs_iam_policy.policy_data
  depends_on = [
    module.gke,
  ]
}

# Pub/Sub IAM bindings
resource "google_pubsub_subscription_iam_binding" "pubsub_iam_binding" {
  project      = var.project_id
  for_each     = local.pubsub_subscriptions
  subscription = each.key
  role         = "roles/pubsub.editor"
  members      = ["serviceAccount:${module.gke.service_account_email}"]
}
