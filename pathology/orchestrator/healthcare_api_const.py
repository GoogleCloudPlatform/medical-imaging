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

"""Shared constants used to build the Healthcare API client."""
from absl import flags
from pathology.shared_libs.flags import secret_flag_utils

HEALTHCARE_API_BASE_URL_FLG = flags.DEFINE_string(
    'healthcare_api_base_url',
    secret_flag_utils.get_secret_or_env(
        'HEALTHCARE_API_BASE_URL', 'https://healthcare.googleapis.com/v1'
    ),
    'Base URL for Cloud Healthcare API endpoint v1.',
)
HEALTHCARE_API_VERSION = 'v1beta1'
HEALTHCARE_SERVICE_NAME = 'healthcare'


def get_healthcare_api_discovery_url(base_url: str) -> str:
  return f'{base_url.rstrip("v1")}/$discovery/rest?version=v1beta1'
