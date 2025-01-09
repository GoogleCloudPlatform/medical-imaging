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

resource "google_project_service" "dicom_proxy_required_apis" {
  for_each = setunion(
    local.required_apis_by_module.dicom_proxy
  )

  service                    = each.key
  project                    = var.project_id
  disable_dependent_services = true
}

module "dicom_proxy" {
  count                               = var.deployment_spec.deploy_dicom_proxy ? 1 : 0
  source                              = "../dicom_proxy"
  project_id                          = var.project_id
  region                              = var.region
  redis_config                        = var.dicom_proxy_redis_config
  configure_project_level_permissions = var.configure_project_level_permissions
  depends_on = [
    google_project_service.dicom_proxy_required_apis,
    google_project_service.shared_required_apis,
    module.cloud_pathology_cluster,
  ]
}
