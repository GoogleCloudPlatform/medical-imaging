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
  example_schemas = toset([
    "example_schema_vl_microscopic_image.json",
    "example_schema_vl_slide_coordinates_microscopic_image.json",
    "example_schema_vl_whole_slide_microscopy_image.json",
  ])
}

# GCS buckets
resource "google_storage_bucket" "gcs_input_images_bucket" {
  project                     = var.project_id
  name                        = "${var.project_id}-transformation-input-images"
  location                    = var.region
  uniform_bucket_level_access = true
  labels = {
    goog-packaged-solution : "medical-imaging-suite"
    goog-packaged-solution-type : "digital-pathology"
  }
}
resource "google_storage_bucket" "gcs_input_metadata_bucket" {
  project                     = var.project_id
  name                        = "${var.project_id}-transformation-input-metadata"
  location                    = var.region
  uniform_bucket_level_access = true
  labels = {
    goog-packaged-solution : "medical-imaging-suite"
    goog-packaged-solution-type : "digital-pathology"
  }
}
resource "google_storage_bucket" "gcs_output_bucket" {
  project                     = var.project_id
  name                        = "${var.project_id}-transformation-output"
  location                    = var.region
  uniform_bucket_level_access = true
  labels = {
    goog-packaged-solution : "medical-imaging-suite"
    goog-packaged-solution-type : "digital-pathology"
  }
}
resource "google_storage_bucket" "gcs_restore_bucket" {
  project                     = var.project_id
  name                        = "${var.project_id}-transformation-restore"
  location                    = var.region
  uniform_bucket_level_access = true
  labels = {
    goog-packaged-solution : "medical-imaging-suite"
    goog-packaged-solution-type : "digital-pathology"
  }
}

# GCS objects
resource "google_storage_bucket_object" "example_schema_objects" {
  for_each     = local.example_schemas
  name         = each.key
  content      = file("${path.module}/${each.key}")
  content_type = "text/plain"
  bucket       = google_storage_bucket.gcs_input_metadata_bucket.id
  depends_on = [
    google_storage_bucket.gcs_input_metadata_bucket,
  ]
}

# Pub/Sub topics
resource "google_pubsub_topic" "gcs_topic" {
  name    = "transformation-gcs-topic"
  project = var.project_id
  labels = {
    goog-packaged-solution : "medical-imaging-suite"
    goog-packaged-solution-type : "digital-pathology"
  }
}
resource "google_pubsub_topic" "dicom_store_topic" {
  name    = "transformation-dicom-store-topic"
  project = var.project_id
  labels = {
    goog-packaged-solution : "medical-imaging-suite"
    goog-packaged-solution-type : "digital-pathology"
  }
}

# Pub/Sub subscriptions
resource "google_pubsub_subscription" "gcs_subscription" {
  ack_deadline_seconds       = 600
  message_retention_duration = "604800s"
  name                       = "transformation-gcs-subscription"
  project                    = var.project_id
  retry_policy {
    maximum_backoff = "600s"
    minimum_backoff = "10s"
  }
  topic = google_pubsub_topic.gcs_topic.id
  labels = {
    goog-packaged-solution : "medical-imaging-suite"
    goog-packaged-solution-type : "digital-pathology"
  }
  depends_on = [
    google_pubsub_topic.gcs_topic,
  ]
}
resource "google_pubsub_subscription" "dicom_store_subscription" {
  ack_deadline_seconds       = 600
  message_retention_duration = "604800s"
  name                       = "transformation-dicom-store-subscription"
  project                    = var.project_id
  retry_policy {
    maximum_backoff = "600s"
    minimum_backoff = "10s"
  }
  topic = google_pubsub_topic.dicom_store_topic.id
  labels = {
    goog-packaged-solution : "medical-imaging-suite"
    goog-packaged-solution-type : "digital-pathology"
  }
  depends_on = [
    google_pubsub_topic.dicom_store_topic,
  ]
}

# Configure Pub/Sub for GCS bucket
data "google_storage_project_service_account" "transformation_gcs_account" {
  project = var.project_id
}
resource "google_pubsub_topic_iam_binding" "gcs_topic_iam_binding" {
  topic   = google_pubsub_topic.gcs_topic.id
  role    = "roles/pubsub.publisher"
  members = ["serviceAccount:${data.google_storage_project_service_account.transformation_gcs_account.email_address}"]
}
resource "google_storage_notification" "gcs_pubsub_notification" {
  bucket         = google_storage_bucket.gcs_input_images_bucket.name
  payload_format = "JSON_API_V1"
  topic          = google_pubsub_topic.gcs_topic.id
  event_types    = ["OBJECT_FINALIZE"]
  depends_on = [
    google_storage_bucket.gcs_input_images_bucket,
    google_pubsub_topic.gcs_topic,
    google_pubsub_topic_iam_binding.gcs_topic_iam_binding,
  ]
}

# Redis
resource "google_redis_instance" "transformation_redis_instance" {
  count                   = var.redis_config.enable_redis ? 1 : 0
  provider                = google-beta
  name                    = "transformation-redis-instance"
  project                 = var.project_id
  tier                    = "BASIC"
  memory_size_gb          = var.redis_config.memory_gb
  region                  = var.region
  redis_version           = "REDIS_7_0"
  auth_enabled            = var.redis_config.enable_auth
  transit_encryption_mode = var.redis_config.enable_transit_encryption ? "SERVER_AUTHENTICATION" : "DISABLED"
  authorized_network      = var.redis_config.network
  redis_configs = {
    activedefrag     = "yes"
    maxmemory-gb     = format("%.2f", var.redis_config.memory_gb * 0.8)
    maxmemory-policy = "allkeys-lru"
  }
}

# Configure GKE
module "gke" {
  source                              = "../gke_service"
  service_name                        = "transformation"
  project_id                          = var.project_id
  configure_project_level_permissions = var.configure_project_level_permissions
}

