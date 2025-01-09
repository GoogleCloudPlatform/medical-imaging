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

from absl.testing import absltest

from pathology.shared_libs.build_version import build_version
from pathology.shared_libs.logging_lib import cloud_logging_client


class BuildVersionTest(absltest.TestCase):

  def test_init_cloud_logging_build_version(self):
    self.assertEmpty(cloud_logging_client.get_build_version())
    self.assertNotIn('BUILD_VERSION', cloud_logging_client.get_log_signature())
    build_version.init_cloud_logging_build_version()
    self.assertNotEmpty(cloud_logging_client.get_build_version())
    self.assertIn('BUILD_VERSION', cloud_logging_client.get_log_signature())


if __name__ == '__main__':
  absltest.main()
