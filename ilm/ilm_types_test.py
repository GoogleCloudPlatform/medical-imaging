# Copyright 2023 Google LLC
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
#
# ==============================================================================
"""Tests for ilm_types."""

from absl.testing import absltest

import ilm_config
import ilm_types


class IlmTypesTest(absltest.TestCase):

  def test_access_metadata_unordered_counts_fails(self):
    with self.assertRaises(ValueError):
      ilm_types.AccessMetadata(
          cumulative_access_counts=[
              ilm_config.AccessCount(count=5, num_days=4),
              ilm_config.AccessCount(count=3, num_days=2),
          ]
      )

  def test_access_metadata_ordered_counts_succeeds(self):
    _ = ilm_types.AccessMetadata(
        cumulative_access_counts=[
            ilm_config.AccessCount(count=3, num_days=2),
            ilm_config.AccessCount(count=5, num_days=4),
        ]
    )

  def test_get_access_count_smaller_num_days(self):
    access_metadata = ilm_types.AccessMetadata(
        cumulative_access_counts=[
            ilm_config.AccessCount(count=3, num_days=2),
            ilm_config.AccessCount(count=5, num_days=4),
        ]
    )
    self.assertEqual(access_metadata.get_access_count(1), 0)

  def test_get_access_count_equal_num_days(self):
    access_metadata = ilm_types.AccessMetadata(
        cumulative_access_counts=[
            ilm_config.AccessCount(count=3, num_days=2),
            ilm_config.AccessCount(count=5, num_days=4),
            ilm_config.AccessCount(count=7, num_days=6),
        ]
    )
    self.assertEqual(access_metadata.get_access_count(2), 3)
    self.assertEqual(access_metadata.get_access_count(4), 5)
    self.assertEqual(access_metadata.get_access_count(6), 7)

  def test_get_access_count_intermediate_num_days(self):
    access_metadata = ilm_types.AccessMetadata(
        cumulative_access_counts=[
            ilm_config.AccessCount(count=3, num_days=2),
            ilm_config.AccessCount(count=5, num_days=4),
        ]
    )
    self.assertEqual(access_metadata.get_access_count(3), 3)

  def test_get_access_count_larger_num_days(self):
    access_metadata = ilm_types.AccessMetadata(
        cumulative_access_counts=[
            ilm_config.AccessCount(count=3, num_days=2),
            ilm_config.AccessCount(count=5, num_days=4),
        ]
    )
    self.assertEqual(access_metadata.get_access_count(10), 5)

  def test_get_access_count_single_access_count(self):
    access_metadata = ilm_types.AccessMetadata(
        cumulative_access_counts=[
            ilm_config.AccessCount(count=3, num_days=2),
        ]
    )
    self.assertEqual(access_metadata.get_access_count(1), 0)
    self.assertEqual(access_metadata.get_access_count(2), 3)
    self.assertEqual(access_metadata.get_access_count(10), 3)


if __name__ == "__main__":
  absltest.main()
