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


# 3. If routing logs to a different project, allow the router service account
# to write to the log bucket. Note:
#  1) This in a non-authoratative binding and keeps other members.
#  2) writer_identity remains blank, NOT null, unless
#     unique_writer_identity=true AND the sync is routing to an external project
resource "google_project_iam_member" "audit_logs_bucket_writer" {
  count   = var.spec.core_project_id != "self" ? 1 : 0
  project = local.core_project_id
  role    = "roles/logging.bucketWriter"
  member  = google_logging_project_sink.audit_logs_sink[0].writer_identity
  depends_on = [
    google_logging_project_sink.audit_logs_sink
  ]
}