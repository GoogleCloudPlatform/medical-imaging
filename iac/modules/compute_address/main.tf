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

# Reserve a new static external IP address
# https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address#reserve_new_static

# This is used in Kustomize configs to set up an Ingress for the GKE services.
resource "google_compute_global_address" "default" {
  address_type = "EXTERNAL"
  description  = "Static IP reservation for DPAS external front-end load balancer"
  ip_version   = "IPV4"
  name         = "cloud-pathology-global-static-ip-address"
  project      = var.project_id

  lifecycle {
    # Prevent destruction of IP address by Terraform. Destroying the IP address
    # is usually not recommended and should be done with care because the IP
    # address DNS mapping is used by the managed certificates used by the
    # ingress, and re-creation of IP address requires update of the DNS record.
    prevent_destroy = true
  }
}
