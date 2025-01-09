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
"""Thread that checks on status of DPAS operations and updates Spanner."""

import base64
import datetime
import time
from typing import Any, Dict

from absl import flags
import google.api_core
from google.cloud import spanner
import googleapiclient
from googleapiclient import discovery

from google.rpc import code_pb2
from pathology.orchestrator import gcs_util
from pathology.orchestrator import healthcare_api_const
from pathology.orchestrator import pathology_resources_util
from pathology.orchestrator.spanner import schema_resources
from pathology.orchestrator.spanner import spanner_resources_pb2
from pathology.orchestrator.v1alpha import cohorts_pb2
from pathology.shared_libs.flags import secret_flag_utils
from pathology.shared_libs.logging_lib import cloud_logging_client
from pathology.shared_libs.spanner_lib import cloud_spanner_client

SLEEP_SECONDS_FLG = flags.DEFINE_integer(
    'sleep_seconds',
    int(secret_flag_utils.get_secret_or_env('REFRESHER_SLEEP_SECONDS', '300')),
    'Contriols the amount of time RefresherThread sleeps for.Default is 300s.',
)


def _check_operation_status(
    healthcare_api_client: discovery.Resource, cloud_op_name: str
) -> Dict[str, Any]:
  """Checks the status of an operation by calling datasets.GetOperation.

  Args:
    healthcare_api_client: Healthcare API client to use to check status of
      operation.
    cloud_op_name: Cloud Operation resource name to check status of.

  Returns:
    JSON Dict of Operation response
  """
  return (
      healthcare_api_client.projects()
      .locations()
      .datasets()
      .operations()
      .get(name=cloud_op_name)
      .execute()
  )


def _check_and_update_deid_cohort(
    transaction,
    stored_metadata: spanner_resources_pb2.StoredPathologyOperationMetadata,
    operation_success: bool,
) -> None:
  """Checks if operation is De-Id and activates or suspends the new cohort.

  Activates the cohort if operation succeeded.
  Marks cohort as suspended if operation failed.


  Args:
    transaction: Transaction to run Spanner read/writes on.
    stored_metadata: PathologyOperationMetadata stored in spanner for operation.
    operation_success: True if operation completed successfully.

  Returns:
    None
  """
  if stored_metadata.transfer_deid_metadata.resource_name_transferred:
    cohort_id = pathology_resources_util.get_id_from_name(
        stored_metadata.transfer_deid_metadata.resource_name_transferred
    )
    if operation_success:
      # Operation completed successfully. Activate cohort.
      cloud_logging_client.info(
          'Operation succeeded. Activating pending Deid cohort.',
          {'CohortId': cohort_id},
      )
      transaction.update(
          'Cohorts',
          schema_resources.COHORTS_ACTIVATE_COLS,
          [[
              cohort_id,
              cohorts_pb2.PATHOLOGY_COHORT_LIFECYCLE_STAGE_ACTIVE,
              spanner.COMMIT_TIMESTAMP,
          ]],
      )
    else:
      # Operation failed. Suspend cohort.
      cloud_logging_client.info(
          'Operation failed. Marking pending Deid cohort for deletion.',
          {'CohortId': cohort_id},
      )
      transaction.update(
          'Cohorts',
          schema_resources.COHORTS_ACTIVATE_COLS,
          [[
              cohort_id,
              cohorts_pb2.PATHOLOGY_COHORT_LIFECYCLE_STAGE_SUSPENDED,
              spanner.COMMIT_TIMESTAMP,
          ]],
      )


def _process_json_operation_response(
    transaction, op_row_dict: Dict[str, Any], json_response: Dict[str, Any]
) -> None:
  """Processes response from datasets.GetOperation and updates data in Spanner.

  Args:
    transaction: Transaction to run Spanner read/writes on.
    op_row_dict: Dict representation of row for Operation in Spanner.
    json_response: Json Dict of GetOperation response.

  Returns:
    None
  """
  dpas_op_id = op_row_dict['DpasOperationId']
  if 'done' in json_response and json_response['done']:
    stored_metadata = spanner_resources_pb2.StoredPathologyOperationMetadata()
    stored_metadata.ParseFromString(
        base64.b64decode(op_row_dict['StoredPathologyOperationMetadata'])
    )
    if 'error' in json_response.keys():
      # Operation failed. Update spanner with error.
      error_code = json_response['error']['code']
      transaction.update(
          'Operations',
          schema_resources.OPERATIONS_UPDATE_STATUS_COLS,
          [[
              dpas_op_id,
              schema_resources.OperationStatus.COMPLETE.value,
              error_code,
              spanner.COMMIT_TIMESTAMP,
          ]],
      )
      cloud_logging_client.error(
          'Operation failed.',
          {
              'OperationId': dpas_op_id,
              'error_code': json_response['error']['code'],
              'error_message': json_response['error']['message'],
          },
      )
      _check_and_update_deid_cohort(transaction, stored_metadata, False)
    else:
      # Operation completed successfully. Update spanner as complete.
      transaction.update(
          'Operations',
          schema_resources.OPERATIONS_UPDATE_STATUS_COLS,
          [[
              dpas_op_id,
              schema_resources.OperationStatus.COMPLETE.value,
              code_pb2.OK,
              spanner.COMMIT_TIMESTAMP,
          ]],
      )
      cloud_logging_client.info(
          'Operation completed successfully.', {'OperationId': dpas_op_id}
      )
      _check_and_update_deid_cohort(transaction, stored_metadata, True)

    # Operation is complete, delete filter file.
    if (
        stored_metadata.filter_file_path is not None
        and stored_metadata.filter_file_path
    ):
      try:
        gcs_util.GcsUtil().delete_file(stored_metadata.filter_file_path)
      except google.api_core.exceptions.Forbidden as exp:
        cloud_logging_client.error(
            'Failed to delete filter file.',
            {'filter_file_path': stored_metadata.filter_file_path},
            exp,
        )
  else:
    # Operation still in progress. Only update timestamp.
    transaction.update(
        'Operations',
        schema_resources.OPERATIONS_UPDATE_TIME_COLS,
        [[dpas_op_id, spanner.COMMIT_TIMESTAMP]],
    )


def update_operation(
    transaction: ...,
    healthcare_api_client: discovery.Resource,
    op_row_dict: Dict[str, Any],
) -> None:
  """Calls datasets.GetOperation and processes the JSON response.

  Used by pathology_operations_handler.

  Args:
    transaction: Transaction to run spanner read/writes on.
    healthcare_api_client: Healthcare API client to use to check status of
      operation.
    op_row_dict: Dict representation of row for Operation in Spanner.

  Returns:
    None
  """
  cloud_logging_client.info(
      'Checking status of operation.',
      {'OperationId': op_row_dict['DpasOperationId']},
  )
  try:
    json_response = _check_operation_status(
        healthcare_api_client, op_row_dict['CloudOperationName']
    )
  except googleapiclient.errors.HttpError as exp:
    cloud_logging_client.error('Failed to check status of operation.', exp)
    json_response = {
        'done': 'done',
        'error': {
            'code': exp.status_code,
            'message': str(exp),
        },
    }
  _process_json_operation_response(transaction, op_row_dict, json_response)


def update_running_operations(
    transaction: ..., healthcare_api_client: discovery.Resource
) -> None:
  """Checks status of running operations.

  Args:
    transaction: Transaction to run spanner read/writes on.
    healthcare_api_client: Healthcare API client to use to check status of
      operation.

  Returns:
    None
  """
  cloud_logging_client.info('Scanning table for running operations.')
  current_timestamp = (
      datetime.datetime.now(datetime.UTC).isoformat().split('+')[0] + 'Z'
  )
  op_rows = transaction.execute_sql(
      'SELECT * FROM Operations WHERE OperationStatus=@in_progress AND'
      ' UpdateTime < @current_time;',
      params={
          'in_progress': schema_resources.OperationStatus.IN_PROGRESS.value,
          'current_time': current_timestamp,
      },
      param_types={
          'in_progress': spanner.param_types.INT64,
          'current_time': spanner.param_types.TIMESTAMP,
      },
  )

  for op in op_rows:
    op_row_dict = dict(zip(schema_resources.OPERATIONS_COLS, op))
    update_operation(transaction, healthcare_api_client, op_row_dict)


class RefresherThread:
  """Thread that checks on the status of DPAS operations and updates Spanner."""

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

  def sleep(self) -> None:
    self.is_awake = False
    time.sleep(SLEEP_SECONDS_FLG.value)
    self.is_awake = True

  def run(self):
    """Thread wakes up and updates operations."""
    self.is_awake = True
    cloud_logging_client.info('Refresher is awake and running.')
    self._spanner_client.run_in_transaction(
        update_running_operations, self._healthcare_api_client
    )
