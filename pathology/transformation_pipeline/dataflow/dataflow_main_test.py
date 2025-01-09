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
# ==============================================================================
"""Tests for dataflow main."""

from unittest import mock

from absl import app
from absl.testing import absltest
from apache_beam.testing import test_pipeline

from pathology.transformation_pipeline import gke_main
from pathology.transformation_pipeline.dataflow import dataflow_main


class DataflowMainTest(absltest.TestCase):

  @mock.patch.object(gke_main, 'main')
  def test_run_transformation_pipeline(self, mock_gke_main):
    test_gcs_file_to_ingest_list = ['/path/to/file1.svs', '/path/to/file2.svs']
    with test_pipeline.TestPipeline() as pipeline:
      dataflow_main.run_transformation_pipeline(
          pipeline, test_gcs_file_to_ingest_list, []
      )
    self.assertEqual(mock_gke_main.call_count, 2)

  def test_gcs_file_to_ingest_list_is_empty_raises_exception(self):
    with self.assertRaises(app.UsageError):
      dataflow_main.main(argv=[])


if __name__ == '__main__':
  absltest.main()
