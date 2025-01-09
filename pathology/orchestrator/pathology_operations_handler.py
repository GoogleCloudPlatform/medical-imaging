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
"""Handler for OperationsService rpc methods of DigitalPathology service.

Implementation of OperationsService rpc methods for DPAS API.
"""

import base64

from google.api_core import exceptions
from google.cloud import spanner
from googleapiclient import discovery
import grpc

from google.longrunning import operations_pb2
from google.protobuf import any_pb2
from google.rpc import code_pb2
from google.rpc import status_pb2
from pathology.orchestrator import healthcare_api_const
from pathology.orchestrator import logging_util
from pathology.orchestrator import pathology_resources_util
from pathology.orchestrator import rpc_status
from pathology.orchestrator.refresher import refresher_thread
from pathology.orchestrator.spanner import schema_resources
from pathology.orchestrator.spanner import spanner_resource_converters
from pathology.orchestrator.spanner import spanner_resources_pb2
from pathology.orchestrator.v1alpha import cohorts_pb2
from pathology.shared_libs.logging_lib import cloud_logging_client
from pathology.shared_libs.spanner_lib import cloud_spanner_client

_OPERATIONS_TABLE = schema_resources.ORCHESTRATOR_TABLE_NAMES[
    schema_resources.OrchestratorTables.OPERATIONS
]


class PathologyOperationsHandler:
  """Handler for PathologyOperations rpc methods of DigitalPathology service."""

  def __init__(self):
    super().__init__()
    self._healthcare_api_client = discovery.build(
        healthcare_api_const.HEALTHCARE_SERVICE_NAME,
        healthcare_api_const.HEALTHCARE_API_VERSION,
        discoveryServiceUrl=healthcare_api_const.get_healthcare_api_discovery_url(
            healthcare_api_const.HEALTHCARE_API_BASE_URL_FLG.value
        ),
    )
    self._spanner_client = cloud_spanner_client.CloudSpannerClient()

  def _check_status_and_update_operation(
      self, transaction, dpas_op_id: int
  ) -> None:
    """Checks current status and updates the operation row in Spanner.

    Args:
      transaction: Transaction to run Spanner read/writes on.
      dpas_op_id: Unique id of operation to check status for.

    Returns:
      None

    Raises:
      RpcFailureError if operation is not found.
    """
    row = transaction.read(
        _OPERATIONS_TABLE,
        schema_resources.OPERATIONS_COLS,
        spanner.KeySet([[dpas_op_id]]),
    )
    try:
      row_dict = dict(zip(schema_resources.OPERATIONS_COLS, row.one()))
    except exceptions.NotFound as exc:
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.NOT_FOUND,
              'Could not retrieve Operation.'
              f' Operation with id {dpas_op_id} not found.',
              exp=exc,
          )
      ) from exc

    # Updates table with latest data by calling datasets.GetOperation.
    refresher_thread.update_operation(
        transaction, self._healthcare_api_client, row_dict
    )

  def _get_operation_by_id(self, dpas_op_id: int) -> operations_pb2.Operation:
    """Gets an operation by looking up the DPAS Operation Id.

    Args:
      dpas_op_id: Unique id of operation to retrieve.

    Returns:
      Operation proto instance

    Raises:
      RpcFailureError if operation is not found.
    """
    row = self._spanner_client.read_data(
        _OPERATIONS_TABLE,
        schema_resources.OPERATIONS_COLS,
        spanner.KeySet([[dpas_op_id]]),
    )
    try:
      row_dict = dict(zip(schema_resources.OPERATIONS_COLS, row.one()))
    except exceptions.NotFound as exc:
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.NOT_FOUND,
              'Could not retrieve Operation.'
              f' Operation with id {dpas_op_id} not found.',
              exp=exc,
          )
      ) from exc

    is_done = (
        row_dict[schema_resources.OPERATION_STATUS]
        == schema_resources.OperationStatus.COMPLETE.value
    )

    # Convert stored metadata to api metadata.
    stored_metadata = spanner_resources_pb2.StoredPathologyOperationMetadata()
    stored_metadata.ParseFromString(
        base64.b64decode(
            row_dict[schema_resources.STORED_PATHOLOGY_OPERATION_METADATA]
        )
    )
    op_metadata = any_pb2.Any()
    op_metadata.Pack(
        spanner_resource_converters.from_stored_operation_metadata(
            stored_metadata
        )
    )

    operation = operations_pb2.Operation(
        name=pathology_resources_util.convert_operation_id_to_name(dpas_op_id),
        metadata=op_metadata,
        done=is_done,
    )

    # If operation is complete, build response.
    if is_done:
      if row_dict[schema_resources.RPC_ERROR_CODE] != code_pb2.OK:
        operation.error.CopyFrom(
            status_pb2.Status(code=row_dict[schema_resources.RPC_ERROR_CODE])
        )
      else:
        response = any_pb2.Any()
        if stored_metadata.export_metadata.gcs_dest_path:
          response.Pack(
              cohorts_pb2.ExportPathologyCohortResponse(
                  status=status_pb2.Status(code=code_pb2.Code.OK)
              )
          )
        else:
          response.Pack(
              cohorts_pb2.TransferDeIdPathologyCohortResponse(
                  status=status_pb2.Status(code=code_pb2.Code.OK)
              )
          )
        operation.response.CopyFrom(response)

    return operation

  def get_operation(
      self, request: operations_pb2.GetOperationRequest
  ) -> operations_pb2.Operation:
    """Returns the long running operation instance of an initiated operation.

    Args:
      request: A GetOperationRequest with the resource name of the operation.

    Raises:
      RpcFailureError if operation is not found.
    """
    op_name = request.name
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.GET_OPERATION, op_name
        )
    )
    dpas_op_id = pathology_resources_util.get_id_from_name(op_name)
    cloud_logging_client.info(f'Retrieving Operation {op_name}.')

    self._spanner_client.run_in_transaction(
        self._check_status_and_update_operation, dpas_op_id
    )
    return self._get_operation_by_id(dpas_op_id)
