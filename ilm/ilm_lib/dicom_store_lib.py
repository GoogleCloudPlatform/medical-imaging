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
"""DICOM store utilities for ILM."""

import collections
import dataclasses
import datetime
import json
import logging
import os
import time
from typing import Any, Iterable, List, Mapping, Optional, Tuple
import urllib.parse
import uuid

import apache_beam as beam
import google.auth
import google.auth.transport.requests
from google.cloud import bigquery
from google.cloud import storage
import requests

import ilm_config
import ilm_types
from ilm_lib import pipeline_util


# Required DICOM Store BigQuery table columns.
_BLOB_STORAGE_SIZE = 'BlobStorageSize'
_LAST_UPDATED = 'LastUpdated'
_LAST_UPDATED_STORAGE_CLASS = 'LastUpdatedStorageClass'
_SERIES_INSTANCE_UID = 'SeriesInstanceUID'
_SOP_INSTANCE_UID = 'SOPInstanceUID'
_STORAGE_CLASS = 'StorageClass'
_STUDY_INSTANCE_UID = 'StudyInstanceUID'
_TYPE = 'Type'

# Optional DICOM Store BigQuery table columns.
_ACQUISITION_DATE = 'AcquisitionDate'
_CONTENT_DATE = 'ContentDate'
_IMAGE_TYPE = 'ImageType'
_MODALITY = 'Modality'
_NUMBER_OF_FRAMES = 'NumberOfFrames'
_PIXEL_SPACING = 'PixelSpacing'
_SERIES_DATE = 'SeriesDate'
_SOP_CLASS_UID = 'SOPClassUID'
_STUDY_DATE = 'StudyDate'

_OPTIONAL_DICOM_STORE_METADATA_COLUMNS = frozenset([
    _ACQUISITION_DATE,
    _CONTENT_DATE,
    _IMAGE_TYPE,
    _MODALITY,
    _NUMBER_OF_FRAMES,
    _PIXEL_SPACING,
    _SERIES_DATE,
    _SOP_CLASS_UID,
    _STUDY_DATE,
])

# BigQuery query keywords
_INSTANCE_DICOM_WEB_PATH = 'InstanceDicomWebPath'


class _DicomTags:
  SERIES_INSTANCE_UID = '0020000E'
  SOP_INSTANCE_UID = '00080018'


@dataclasses.dataclass(frozen=True)
class _StorageClassResult:
  succeeded_count: int
  failed_count: int
  unfinished_count: int

  succeeded_instances: List[str]
  failed_instances: List[str]
  unfinished_instances: List[str]


class DicomStoreError(Exception):
  pass


def _get_optional_dicom_store_metadata_table_columns(
    ilm_cfg: ilm_config.ImageLifecycleManagementConfig,
) -> List[str]:
  """Fetches metadata columns present in DICOM Store BigQuery table."""
  project_id, dataset_id, table_id = (
      ilm_cfg.dicom_store_config.dicom_store_bigquery_table.split('.')
  )
  client = bigquery.Client(project=project_id)
  table_ref = client.dataset(dataset_id).table(table_id)
  table = client.get_table(table_ref)
  metadata_columns = []
  for field in table.schema:
    if field.name in _OPTIONAL_DICOM_STORE_METADATA_COLUMNS:
      metadata_columns.append(field.name)
  return metadata_columns


def _metadata_columns_select(metadata_columns: List[str]) -> str:
  return (
      ', '.join(metadata_columns)
      .replace(
          _PIXEL_SPACING,
          f'{_PIXEL_SPACING}[SAFE_OFFSET(0)] AS {_PIXEL_SPACING}',
      )
      .replace(
          _IMAGE_TYPE, f'ARRAY_TO_STRING({_IMAGE_TYPE}, "/") AS {_IMAGE_TYPE}'
      )
  )


def _metadata_columns_group_by(metadata_columns: List[str]) -> str:
  return (
      ', '.join(metadata_columns)
      .replace(_PIXEL_SPACING, f'{_PIXEL_SPACING}[SAFE_OFFSET(0)]')
      .replace(_IMAGE_TYPE, f'ARRAY_TO_STRING({_IMAGE_TYPE}, "/")')
  )


def get_dicom_store_query(
    ilm_cfg: ilm_config.ImageLifecycleManagementConfig,
):
  """Generates query to fetch DICOM metadata from BigQuery table."""
  metadata_columns = _get_optional_dicom_store_metadata_table_columns(ilm_cfg)
  metadata_columns.extend([_BLOB_STORAGE_SIZE, _STORAGE_CLASS, _TYPE])
  metadata_columns_select = _metadata_columns_select(metadata_columns)
  metadata_columns_group_by = _metadata_columns_group_by(metadata_columns)
  partition_by_uid_triple = (
      f'PARTITION BY {_STUDY_INSTANCE_UID}, {_SERIES_INSTANCE_UID}, '
      f'{_SOP_INSTANCE_UID}'
  )
  return (
      'SELECT '
      '  CONCAT( '
      f'   "studies/", {_STUDY_INSTANCE_UID}, '
      f'   "/series/", {_SERIES_INSTANCE_UID}, '
      f'   "/instances/", {_SOP_INSTANCE_UID}) AS {_INSTANCE_DICOM_WEB_PATH}, '
      f'  {metadata_columns_select}, '
      f'  MAX({_LAST_UPDATED}) AS {_LAST_UPDATED}, '
      f'  MAX({_LAST_UPDATED_STORAGE_CLASS}) AS {_LAST_UPDATED_STORAGE_CLASS} '
      'FROM ( '
      '  SELECT '
      '    *, '
      f'   MAX({_LAST_UPDATED}) '
      f'     OVER ({partition_by_uid_triple}) AS MaxLastUpdated, '
      '    CASE '
      f'     WHEN {_TYPE}="CREATE" OR {_STORAGE_CLASS}!=LAG({_STORAGE_CLASS}) '
      f'       OVER ({partition_by_uid_triple} ORDER BY {_LAST_UPDATED} ASC) '
      f'     THEN {_LAST_UPDATED} '
      '      ELSE NULL '
      f'   END AS {_LAST_UPDATED_STORAGE_CLASS} '
      f'  FROM `{ilm_cfg.dicom_store_config.dicom_store_bigquery_table}` '
      ') '
      'WHERE '
      f'  {_LAST_UPDATED} = MaxLastUpdated '
      f'  AND {_STORAGE_CLASS} IS NOT NULL '
      f'  AND {_TYPE}!="DELETE" '
      f'GROUP BY {_STUDY_INSTANCE_UID}, {_SERIES_INSTANCE_UID}, '
      f'  {_SOP_INSTANCE_UID}, {metadata_columns_group_by}'
  )


def parse_dicom_metadata(
    raw_metadata: Mapping[str, Any],
    today: datetime.datetime = datetime.datetime.now(tz=datetime.timezone.utc),
) -> Tuple[str, ilm_types.InstanceMetadata]:
  """Parses metadata from DICOM store table query result."""
  try:
    instance = raw_metadata[_INSTANCE_DICOM_WEB_PATH]
    pixel_spacing = (
        float(raw_metadata[_PIXEL_SPACING])
        if raw_metadata.get(_PIXEL_SPACING)
        else None
    )
    last_updated_storage_class = raw_metadata[_LAST_UPDATED_STORAGE_CLASS]
    if last_updated_storage_class is None:
      logging.info(
          'Missing last updated for storage class in DICOM Store BigQuery '
          'table for instance %s. Using last updated timestamp instead.',
          instance,
      )
      last_updated_storage_class = raw_metadata[_LAST_UPDATED]
    days_in_current_storage_class = (today - last_updated_storage_class).days

    metadata = ilm_types.InstanceMetadata(
        instance=instance,
        modality=raw_metadata.get(_MODALITY, None),
        num_frames=int(raw_metadata.get(_NUMBER_OF_FRAMES) or 0),
        pixel_spacing=pixel_spacing,
        sop_class_uid=raw_metadata.get(_SOP_CLASS_UID, ''),
        acquisition_date=raw_metadata.get(_ACQUISITION_DATE, None),
        content_date=raw_metadata.get(_CONTENT_DATE, None),
        series_date=raw_metadata.get(_SERIES_DATE, None),
        study_date=raw_metadata.get(_STUDY_DATE, None),
        image_type=raw_metadata.get(_IMAGE_TYPE, None),
        size_bytes=raw_metadata[_BLOB_STORAGE_SIZE],
        storage_class=ilm_config.StorageClass(raw_metadata[_STORAGE_CLASS]),
        num_days_in_current_storage_class=days_in_current_storage_class,
    )
  except (KeyError, ValueError, TypeError) as e:
    raise RuntimeError(
        f'Invalid metadata in DICOM Store BigQuery table: {raw_metadata}'
    ) from e
  return (instance, metadata)


class GenerateFilterFilesDoFn(beam.DoFn):
  """DoFn to generate filter files for DICOM instances storage class changes."""

  def __init__(self, ilm_cfg: ilm_config.ImageLifecycleManagementConfig):
    self._ilm_cfg = ilm_cfg
    self._dry_run = ilm_cfg.dry_run
    self._storage_client = None

  def setup(self):
    self._storage_client = storage.Client()

  def process(
      self,
      storage_class_to_changes: Tuple[
          ilm_config.StorageClass, Iterable[ilm_types.StorageClassChange]
      ],
  ) -> Iterable[ilm_types.SetStorageClassRequestMetadata]:
    new_storage_class, changes = storage_class_to_changes
    filter_file_gcs_uri = os.path.join(
        self._ilm_cfg.tmp_gcs_uri, f'instances-filter-{uuid.uuid4()}.txt'
    )
    changes = list(changes)
    if not self._dry_run:
      instances_str = '\n'.join([change.instance for change in changes])
      pipeline_util.write_gcs_file(
          file_content=instances_str,
          gcs_uri=filter_file_gcs_uri,
          storage_client=self._storage_client,
      )
    if self._ilm_cfg.report_config.detailed_results_report_gcs_uri:
      instances = [change.instance for change in changes]
    else:
      instances = []
    yield ilm_types.SetStorageClassRequestMetadata(
        [change.move_rule_id for change in changes],
        instances,
        filter_file_gcs_uri,
        new_storage_class,
    )


class DicomStoreClient:
  """DICOM store client for storage class operations."""

  _HEALTHCARE_API = 'https://healthcare.googleapis.com/v1beta1/'
  _OPERATION_NAME_KEY = 'name'
  _OPERATION_STATUS_KEY = 'done'

  def __init__(self, ilm_cfg: ilm_config.ImageLifecycleManagementConfig):
    self._dicom_store_resource = ilm_cfg.dicom_store_config.dicom_store_path
    self._dicom_store_path = urllib.parse.urljoin(
        DicomStoreClient._HEALTHCARE_API, self._dicom_store_resource.lstrip('/')
    ).rstrip('/')
    self._auth_credentials = None

  def _add_auth_to_header(
      self, msg_headers: Mapping[str, str]
  ) -> Mapping[str, str]:
    """Updates credentials returns header with authentication added."""
    if self._auth_credentials is None:
      self._auth_credentials = google.auth.default(
          scopes=['https://www.googleapis.com/auth/cloud-platform']
      )[0]
    if not self._auth_credentials.valid:
      auth_req = google.auth.transport.requests.Request()
      self._auth_credentials.refresh(auth_req)
    self._auth_credentials.apply(msg_headers)
    return msg_headers

  def _get_tag_value(
      self, instance_metadata: Mapping[str, Any], tag: str
  ) -> Optional[Any]:
    try:
      return instance_metadata[tag]['Value'][0]
    except (KeyError, IndexError):
      return None

  def fetch_instances(self, dicomweb_path: str) -> List[str]:
    """Fetches instances for given DICOMweb path.

    Args:
      dicomweb_path: path to either a study (i.e. studies/<STUDY>) or a series
        (i.e. studies/<STUDY>/series/<SERIES>) in DICOM store.

    Returns:
      List of full DICOMweb paths of instances.

    Raises:
      DicomStoreError: in case of unexpected or error response from DICOM Store.
    """
    uri = os.path.join(
        self._dicom_store_path, 'dicomWeb', dicomweb_path, 'instances'
    )
    headers = self._add_auth_to_header({})
    try:
      response = requests.get(uri, headers=headers)
      response.raise_for_status()
      response_json = response.json()
    except (requests.HTTPError, requests.exceptions.JSONDecodeError) as e:
      raise DicomStoreError(
          f'Failed to get instances metadata for {dicomweb_path}.'
      ) from e

    has_series = 'series/' in dicomweb_path
    instances = []
    for instance_metadata in response_json:
      instance_dicomweb_path = [dicomweb_path]
      if not has_series:
        series_instance_uid = self._get_tag_value(
            instance_metadata, _DicomTags.SERIES_INSTANCE_UID
        )
        instance_dicomweb_path.append(f'series/{series_instance_uid}')
      sop_instance_uid = self._get_tag_value(
          instance_metadata, _DicomTags.SOP_INSTANCE_UID
      )
      instance_dicomweb_path.append(f'instances/{sop_instance_uid}')
      instance_dicomweb_path = '/'.join(instance_dicomweb_path)
      instances.append(instance_dicomweb_path)
    return instances

  def set_blob_storage_settings(
      self,
      request: ilm_types.SetStorageClassRequestMetadata,
  ) -> str:
    """Sends setBlobStorageSettings request with filter file.

    Args:
      request: metadata for sending setBlobStorageSettings request.

    Returns:
      setBlobStorageSettings response operation name.

    Raises:
      DicomStoreError if request fails.
    """
    uri = f'{self._dicom_store_path}:setBlobStorageSettings'
    headers = self._add_auth_to_header(
        {'Content-Type': 'application/json; charset=utf-8'}
    )
    data = (
        '{{ '
        '  resource: "{dicom_store_resource}",'
        '  filter_config: {{ resourcePathsGcsUri: "{filter_file_gcs_uri}" }},'
        '  blobStorageSettings: {{ blob_storage_class: "{new_storage_class}" }}'
        '}}'.format(
            dicom_store_resource=self._dicom_store_resource,
            filter_file_gcs_uri=request.filter_file_gcs_uri,
            new_storage_class=request.new_storage_class.value,
        )
    )
    try:
      response = requests.post(uri, data=data, headers=headers)
      response.raise_for_status()
      operation = json.loads(response.content)
      return operation[DicomStoreClient._OPERATION_NAME_KEY]
    except (requests.HTTPError, json.JSONDecodeError) as exp:
      raise DicomStoreError(
          f'Failed to change storage class for {len(request.move_rule_ids)} '
          f'instance(s) with error: {exp}'
      ) from exp

  def is_operation_done(self, operation_name: str) -> bool:
    """Get operation status.

    Args:
      operation_name: Operation resource name.

    Returns:
      True if operation is done.

    Raises:
      DicomStoreError if unable to get operation status.
    """
    uri = os.path.join(DicomStoreClient._HEALTHCARE_API, operation_name)
    headers = self._add_auth_to_header({})
    try:
      response = requests.get(uri, headers=headers)
      response.raise_for_status()
      operation = json.loads(response.content)
      return operation[DicomStoreClient._OPERATION_NAME_KEY]
    except (requests.HTTPError, json.JSONDecodeError) as exp:
      raise DicomStoreError(
          f'Failed to get operation status for {operation_name}'
      ) from exp
    except KeyError:
      return False


class UpdateStorageClassesDoFn(beam.DoFn):
  """DoFn to update storage classes of instances in DICOM store."""

  def __init__(self, ilm_cfg: ilm_config.ImageLifecycleManagementConfig):
    self._dry_run = ilm_cfg.dry_run
    self._cfg = ilm_cfg
    self._dicom_store_client = DicomStoreClient(self._cfg)
    self._throttler = pipeline_util.Throttler(
        self._cfg.dicom_store_config.max_dicom_store_qps
    )

  def setup(self):
    self._dicom_store_client = DicomStoreClient(self._cfg)
    self._throttler = pipeline_util.Throttler(
        self._cfg.dicom_store_config.max_dicom_store_qps
    )

  def process(
      self, request: ilm_types.SetStorageClassRequestMetadata
  ) -> Iterable[ilm_types.SetStorageClassOperationMetadata]:
    if self._dry_run:
      logging.info('Dry-run mode. Skipping DICOM store changes.')
      return [
          ilm_types.SetStorageClassOperationMetadata(
              operation_name='',
              move_rule_ids=request.move_rule_ids,
              instances=request.instances,
              succeeded=True,
          )
      ]
    try:
      self._throttler.wait()
      operation_name = self._dicom_store_client.set_blob_storage_settings(
          request
      )
      return [
          ilm_types.SetStorageClassOperationMetadata(
              operation_name=operation_name,
              move_rule_ids=request.move_rule_ids,
              instances=request.instances,
              start_time=time.time(),
              filter_file_gcs_uri=request.filter_file_gcs_uri,
          )
      ]
    except DicomStoreError as e:
      logging.error(e)
      return [
          ilm_types.SetStorageClassOperationMetadata(
              operation_name='',
              move_rule_ids=request.move_rule_ids,
              instances=request.instances,
              succeeded=False,
              filter_file_gcs_uri=request.filter_file_gcs_uri,
          )
      ]


class GenerateReportDoFn(beam.DoFn):
  """DoFn to generate report of storage class updates after LROs finish."""

  def __init__(self, ilm_cfg: ilm_config.ImageLifecycleManagementConfig):
    self._dry_run = ilm_cfg.dry_run
    self._move_rules = ilm_cfg.storage_class_config.move_rules
    self._operation_timeout_seconds = (
        ilm_cfg.dicom_store_config.set_storage_class_timeout_min * 60
    )
    self._cfg = ilm_cfg

  def setup(self):
    self._operations_in_progress = collections.deque()
    self._operations_done = []
    self._storage_class_results = {}
    timestamp = str(datetime.datetime.now())
    self._summarized_report = (
        self._cfg.report_config.summarized_results_report_gcs_uri.format(
            timestamp
        )
    )
    if self._cfg.report_config.detailed_results_report_gcs_uri:
      self._detailed_report = (
          self._cfg.report_config.detailed_results_report_gcs_uri.format(
              timestamp
          )
      )
    else:
      self._detailed_report = ''
    self._dicom_store_client = DicomStoreClient(self._cfg)
    self._throttler = pipeline_util.Throttler(
        target_qps=self._cfg.dicom_store_config.max_dicom_store_qps
    )

  def _wait_for_operations_to_finish(self):
    """Waits for DICOM store operations to finish.

    Operations that time out have succeeded status = None.
    """
    logging.info('Waiting for SetBlobStorageSettings operations to finish.')
    if not self._operations_in_progress:
      return
    num_operations_to_verify = len(self._operations_in_progress)
    success = 0
    failure = len(self._operations_done)  # Operations that had already failed.
    timeout = 0
    while self._operations_in_progress:
      if num_operations_to_verify <= 0:
        # Wait to verify operations again after a full round.
        time.sleep(60)
        num_operations_to_verify = len(self._operations_in_progress)
      operation = self._operations_in_progress.popleft()
      try:
        self._throttler.wait()
        if self._dicom_store_client.is_operation_done(operation.operation_name):
          operation = dataclasses.replace(operation, succeeded=True)
          self._operations_done.append(operation)
          success += 1
          num_operations_to_verify -= 1
          continue
      except DicomStoreError as e:
        logging.error(e)
        operation = dataclasses.replace(operation, succeeded=False)
        self._operations_done.append(operation)
        failure += 1
        num_operations_to_verify -= 1
        continue
      if time.time() > operation.start_time + self._operation_timeout_seconds:
        self._operations_done.append(operation)
        timeout += 1
        num_operations_to_verify -= 1
        continue
      self._operations_in_progress.append(operation)
      num_operations_to_verify -= 1

    logging.info(
        'Finished waiting for SetBlobStorageSettings operations: '
        '%s succeeded, %s failed, %s timed out.',
        success,
        failure,
        timeout,
    )

  def _cleanup_filter_files(self):
    if not self._cfg.dicom_store_config.set_storage_class_delete_filter_files:
      return
    for operation in self._operations_in_progress:
      if operation.filter_file_gcs_uri:
        pipeline_util.delete_gcs_file(operation.filter_file_gcs_uri)
    for operation in self._operations_done:
      if operation.filter_file_gcs_uri:
        pipeline_util.delete_gcs_file(operation.filter_file_gcs_uri)

  def _compute_results(self):
    """Computes instances results based on status of DICOM store operations."""
    # Rule id to counts
    succeeded = collections.defaultdict(int)
    failed = collections.defaultdict(int)
    unfinished = collections.defaultdict(int)

    succeeded_instances = collections.defaultdict(list)
    failed_instances = collections.defaultdict(list)
    unfinished_instances = collections.defaultdict(list)
    rule_ids = set()
    for operation in self._operations_done:
      if operation.succeeded is None:
        counter = unfinished
        instances = unfinished_instances
      elif operation.succeeded:
        counter = succeeded
        instances = succeeded_instances
      else:
        counter = failed
        instances = failed_instances
      for rule_id in operation.move_rule_ids:
        counter[rule_id] += 1
        rule_ids.add(rule_id)
      if operation.instances and len(operation.instances) == len(
          operation.move_rule_ids
      ):
        for rule_id, instance in zip(
            operation.move_rule_ids, operation.instances
        ):
          instances[rule_id].append(instance)

    for rule_id in rule_ids:
      self._storage_class_results[rule_id] = _StorageClassResult(
          succeeded_count=succeeded[rule_id],
          failed_count=failed[rule_id],
          unfinished_count=unfinished[rule_id],
          succeeded_instances=succeeded_instances[rule_id],
          failed_instances=failed_instances[rule_id],
          unfinished_instances=unfinished_instances[rule_id],
      )

  def _generate_report(self):
    """Generates report of storage class updates.

    Reports instances' updates and status (success, failure, still running) for
    each move rule and condition in ILM config.
    """
    self._compute_results()
    csv_separator = ','
    header = csv_separator.join([
        'from_storage_class',
        'to_storage_class',
        'condition',
        'rule_index',
        'condition_index',
        'succeeded',
        'failed',
        'unfinished',
    ])
    summarized_report_results = [header]
    detailed_report_results = [header]
    logging.info('Generating storage class updates report(s).')
    # TODO: Also add operation name.
    for rule_index, rule in enumerate(self._move_rules):
      from_storage_class = rule.from_storage_class
      to_storage_class = rule.to_storage_class
      for condition_index, condition in enumerate(rule.conditions):
        rule_id = ilm_types.MoveRuleId(
            rule_index=rule_index, condition_index=condition_index
        )
        if rule_id not in self._storage_class_results:
          continue
        condition_results = self._storage_class_results[rule_id]
        logging.info(
            'Instances moved from %s to %s due to %s (move rule %s, condition '
            '%s): %s succeeded, %s failed, %s unfinished',
            from_storage_class.value,
            to_storage_class.value,
            condition,
            rule_index,
            condition_index,
            condition_results.succeeded_count,
            condition_results.failed_count,
            condition_results.unfinished_count,
        )
        summarized_report_results.append(
            csv_separator.join([
                str(i)
                for i in [
                    from_storage_class.value,
                    to_storage_class.value,
                    condition,
                    rule_index,
                    condition_index,
                    condition_results.succeeded_count,
                    condition_results.failed_count,
                    condition_results.unfinished_count,
                ]
            ]),
        )
        if self._detailed_report:
          detailed_report_results.append(
              csv_separator.join([
                  str(i)
                  for i in [
                      from_storage_class.value,
                      to_storage_class.value,
                      condition,
                      rule_index,
                      condition_index,
                      f'"{condition_results.succeeded_instances}"',
                      f'"{condition_results.failed_instances}"',
                      f'"{condition_results.unfinished_instances}"',
                  ]
              ])
          )
    summarized_report_results_str = '\n'.join(summarized_report_results)
    pipeline_util.write_gcs_file(
        file_content=summarized_report_results_str,
        gcs_uri=self._summarized_report,
    )
    logging.info(
        'Wrote summarized report of updates to %s.', self._summarized_report
    )
    if self._detailed_report:
      detailed_report_results_str = '\n'.join(detailed_report_results)
      pipeline_util.write_gcs_file(
          file_content=detailed_report_results_str,
          gcs_uri=self._detailed_report,
      )
      logging.info(
          'Wrote detailed report of updates to %s.', self._detailed_report
      )

  def process(
      self,
      key_to_operations: Tuple[
          int, Iterable[ilm_types.SetStorageClassOperationMetadata]
      ],
  ) -> None:
    _, operations = key_to_operations
    if self._dry_run:
      self._operations_done = collections.deque(operations)
    else:
      for op in operations:
        if op.succeeded is None:
          self._operations_in_progress.append(op)
        else:
          self._operations_done.append(op)
      self._wait_for_operations_to_finish()
      self._cleanup_filter_files()
    self._generate_report()
