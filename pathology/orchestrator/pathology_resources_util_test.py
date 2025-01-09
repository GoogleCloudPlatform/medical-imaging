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

"""Tests for pathology_resources_util."""

from absl.testing import absltest
from pathology.orchestrator import pathology_resources_util


class PathologyResourceUtilTest(absltest.TestCase):
  """Tests for pathology_resource_util."""

  def test_convert_cohort_id_to_name(self):
    resource_name = pathology_resources_util.convert_cohort_id_to_name(
        123, 1234)
    self.assertEqual(resource_name, 'pathologyUsers/123/pathologyCohorts/1234')

  def test_convert_scan_uid_to_name(self):
    resource_name = pathology_resources_util.convert_scan_uid_to_name(1234)
    self.assertEqual(resource_name, 'pathologySlides/1234')

  def test_convert_user_id_to_name(self):
    resource_name = pathology_resources_util.convert_user_id_to_name(1234)
    self.assertEqual(resource_name, 'pathologyUsers/1234')

  def test_convert_operation_id_to_name(self):
    resource_name = pathology_resources_util.convert_operation_id_to_name(1234)
    self.assertEqual(resource_name, 'operations/1234')

  def test_get_id_from_name_succeeds(self):
    id_val = pathology_resources_util.get_id_from_name('pathologyUsers/1234')
    self.assertEqual(id_val, 1234)

  def test_get_id_from_name_fails(self):
    with self.assertRaises(ValueError):
      pathology_resources_util.get_id_from_name('pathologyUsers/str')


if __name__ == '__main__':
  absltest.main()
