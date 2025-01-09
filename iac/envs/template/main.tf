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

# Instantiation of the _PROJECT_ID_ environment.
module "env" {
  source = "../../modules/env"

  project_id                          = "_PROJECT_ID_"
  region                              = "_REGION_"
  configure_project_level_permissions = true
  configure_audit_logs                = true
  deployment_spec                     = _DEPLOYMENT_SPEC_
  data_editors                        = _DATA_EDITORS_
  iap_users                           = _IAP_USERS_
  gke_config = {
    dicom_proxy_machine_type    = "_DICOM_PROXY_MACHINE_TYPE_"
    transformation_machine_type = "_TRANSFORMATION_MACHINE_TYPE_"
    zone                        = "_ZONE_"
    service_account_email       = "_GKE_SERVICE_ACCOUNT_EMAIL_"
    network                     = "_GKE_NETWORK_"
    subnetwork                  = "_GKE_SUBNETWORK_"
  }
}
