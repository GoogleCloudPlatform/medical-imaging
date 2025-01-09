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
# ==============================================================================
"""Initializes the build version in Cloud Client Lib."""
import os

from pathology.shared_libs.logging_lib import cloud_logging_client


def init_cloud_logging_build_version(build_version_not_found: str = '') -> None:
  """Initializes the build version in Cloud Client Lib."""
  try:
    build_version_dir = os.path.dirname(__file__)
    with open(
        os.path.join(build_version_dir, 'build_version.txt'), 'rt'
    ) as infile:
      version = infile.read()
  except (FileNotFoundError, NotADirectoryError) as _:
    version = build_version_not_found
  cloud_logging_client.set_build_version(version)
