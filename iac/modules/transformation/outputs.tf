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

output "dicom_store_pubsub_topic_id" {
  description = "Pub/Sub topic id for DICOM Store notifications."
  value       = google_pubsub_topic.dicom_store_topic.id
}
output "service_account_email" {
  description = "Email of service account created for transformation."
  value       = module.gke.service_account_email
}
