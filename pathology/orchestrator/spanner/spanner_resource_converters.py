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

"""Converters between Spanner messages and API proto messages."""

from pathology.orchestrator.spanner import spanner_resources_pb2
from pathology.orchestrator.v1alpha import digital_pathology_pb2


def to_stored_operation_metadata(
    operation_metadata: digital_pathology_pb2.PathologyOperationMetadata
) -> spanner_resources_pb2.StoredPathologyOperationMetadata:
  """Converts PathologyOperationMetadata to StoredPathologyOperationMetadata.

  Args:
    operation_metadata: API proto instance of OperationMetadata.

  Returns:
    StoredPathologyOperationMetadata
  """
  stored_metadata = spanner_resources_pb2.StoredPathologyOperationMetadata()
  stored_metadata.filter_file_path = operation_metadata.filter_file_path

  if operation_metadata.export_metadata.gcs_dest_path:
    stored_metadata.export_metadata.CopyFrom(
        spanner_resources_pb2.ExportOperationMetadata(
            gcs_dest_path=operation_metadata.export_metadata.gcs_dest_path))
  else:
    stored_metadata.transfer_deid_metadata.CopyFrom(
        spanner_resources_pb2.TransferDeidOperationMetadata(
            resource_name_transferred=operation_metadata.transfer_deid_metadata
            .resource_name_transferred))

  return stored_metadata


def from_stored_operation_metadata(
    stored_metadata: spanner_resources_pb2.StoredPathologyOperationMetadata
) -> digital_pathology_pb2.PathologyOperationMetadata:
  """Converts StoredPathologyOperationMetadata to PathologyOperationMetadata.

  Args:
    stored_metadata: Spanner resource instance of OperationMetadata.

  Returns:
    PathologyOperationMetadata
  """
  operation_metadata = digital_pathology_pb2.PathologyOperationMetadata()
  operation_metadata.filter_file_path = stored_metadata.filter_file_path

  if stored_metadata.export_metadata.gcs_dest_path:
    operation_metadata.export_metadata.CopyFrom(
        digital_pathology_pb2.ExportOperationMetadata(
            gcs_dest_path=stored_metadata.export_metadata.gcs_dest_path))
  else:
    operation_metadata.transfer_deid_metadata.CopyFrom(
        digital_pathology_pb2.TransferDeidOperationMetadata(
            resource_name_transferred=stored_metadata.transfer_deid_metadata
            .resource_name_transferred))

  return operation_metadata
