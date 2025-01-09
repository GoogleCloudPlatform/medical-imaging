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

"""Tests for spanner_resource_converters."""

from absl.testing import absltest
from pathology.orchestrator.spanner import spanner_resource_converters

from pathology.orchestrator.spanner import spanner_resources_pb2
from pathology.orchestrator.v1alpha import digital_pathology_pb2

_TRANSFERRED_RESOURCE_NAME = 'test-resource'
_EXPORT_GCS_PATH = 'gs://export-test/output'
_FILTER_FILE_PATH = 'gs://filter_files/filter'


class SpannerResourceConvertersTest(absltest.TestCase):
  """Tests for spanner_resource_converters."""

  def test_to_stored_operation_metadata_export(self):
    export_op_metadata = digital_pathology_pb2.PathologyOperationMetadata(
        export_metadata=digital_pathology_pb2.ExportOperationMetadata(
            gcs_dest_path=_EXPORT_GCS_PATH),
        filter_file_path=_FILTER_FILE_PATH)

    stored_metadata = spanner_resource_converters.to_stored_operation_metadata(
        export_op_metadata)

    self.assertEqual(stored_metadata.export_metadata.gcs_dest_path,
                     export_op_metadata.export_metadata.gcs_dest_path)
    self.assertEqual(stored_metadata.filter_file_path,
                     export_op_metadata.filter_file_path)

  def test_to_stored_operation_metadata_deid(self):
    deid_op_metadata = digital_pathology_pb2.PathologyOperationMetadata(
        transfer_deid_metadata=digital_pathology_pb2
        .TransferDeidOperationMetadata(
            resource_name_transferred=_TRANSFERRED_RESOURCE_NAME),
        filter_file_path=_FILTER_FILE_PATH)

    stored_metadata = spanner_resource_converters.to_stored_operation_metadata(
        deid_op_metadata)

    self.assertEqual(
        stored_metadata.transfer_deid_metadata.resource_name_transferred,
        deid_op_metadata.transfer_deid_metadata.resource_name_transferred)
    self.assertEqual(stored_metadata.filter_file_path,
                     deid_op_metadata.filter_file_path)

  def test_from_stored_operation_metadata_export(self):
    stored_metadata = spanner_resources_pb2.StoredPathologyOperationMetadata(
        export_metadata=spanner_resources_pb2.ExportOperationMetadata(
            gcs_dest_path=_EXPORT_GCS_PATH),
        filter_file_path=_FILTER_FILE_PATH)

    op_metadata = spanner_resource_converters.from_stored_operation_metadata(
        stored_metadata)

    self.assertEqual(op_metadata.export_metadata.gcs_dest_path,
                     stored_metadata.export_metadata.gcs_dest_path)
    self.assertEqual(op_metadata.filter_file_path,
                     stored_metadata.filter_file_path)

  def test_from_stored_operation_metadata_deid(self):
    stored_metadata = spanner_resources_pb2.StoredPathologyOperationMetadata(
        transfer_deid_metadata=spanner_resources_pb2
        .TransferDeidOperationMetadata(
            resource_name_transferred=_TRANSFERRED_RESOURCE_NAME),
        filter_file_path=_FILTER_FILE_PATH)

    op_metadata = spanner_resource_converters.from_stored_operation_metadata(
        stored_metadata)

    self.assertEqual(
        op_metadata.transfer_deid_metadata.resource_name_transferred,
        stored_metadata.transfer_deid_metadata.resource_name_transferred)
    self.assertEqual(op_metadata.filter_file_path,
                     stored_metadata.filter_file_path)


if __name__ == '__main__':
  absltest.main()
