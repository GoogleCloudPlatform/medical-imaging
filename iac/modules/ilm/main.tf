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

resource "google_project_iam_audit_config" "healthcare_audit_config" {
  count   = var.configure_audit_logs ? 1 : 0
  project = var.project_id
  service = "healthcare.googleapis.com"
  audit_log_config {
    log_type = "ADMIN_READ"
  }
  audit_log_config {
    log_type = "DATA_READ"
  }
  audit_log_config {
    log_type = "DATA_WRITE"
  }
}

resource "google_service_account" "ilm_service_account" {
  project      = var.project_id
  account_id   = "ilm-dataflow-sa"
  display_name = "Default service account for ILM Dataflow pipeline."
}

resource "google_storage_bucket" "ilm_bucket" {
  project                     = var.project_id
  name                        = "${var.project_id}-ilm"
  location                    = var.region
  uniform_bucket_level_access = true
  labels = {
    goog-packaged-solution : "medical-imaging-suite"
    goog-packaged-solution-type : "digital-pathology"
  }
}
