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

resource "google_redis_instance" "dicom_proxy_redis_instance" {
  count                   = var.redis_config.enable_redis ? 1 : 0
  provider                = google-beta
  name                    = "dicom-proxy-redis-instance"
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

module "gke" {
  source                              = "../gke_service"
  service_name                        = "dicom-proxy"
  project_id                          = var.project_id
  configure_project_level_permissions = var.configure_project_level_permissions
}
