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
"""Data access logs utilities for ILM."""

import collections
import datetime
import enum
import json
import logging
from typing import Any, Iterable, Iterator, List, Mapping, Tuple

import apache_beam as beam

import ilm_config
import ilm_types
from ilm_lib import dicom_store_lib
from ilm_lib import pipeline_util


# Audit log entry columns
_DICOMWEB_SERVICE_METHOD = 'protopayload_auditlog.methodName'
_DICOM_STORE_PATH = 'protopayload_auditlog.resourceName'
_REQUEST_TIMESTAMP = (
    'protopayload_auditlog.requestMetadata.requestAttributes.time'
)
_REQUEST_JSON = 'protopayload_auditlog.requestJson'
_STATUS_CODE = 'protopayload_auditlog.status.code'


# Audit log query result keywords
_DICOMWEB_SERVICE_METHOD_KEY = 'methodName'
_REQUEST_TIMESTAMP_KEY = 'time'
_REQUEST_JSON_KEY = 'requestJson'
_REQUEST_JSON_DICOMWEB_PATH_KEY = 'dicomWebPath'


class AuditLogEntryParsingError(Exception):
  pass


class DicomWebServiceMethods(enum.Enum):
  """DICOM Web Service API methods for accessing instances in DICOM Store."""

  RETRIEVE_INSTANCE = 'RetrieveInstance'
  RETRIEVE_RENDERED_INSTANCE = 'RetrieveRenderedInstance'
  RETRIEVE_FRAMES = 'RetrieveFrames'
  RETRIEVE_RENDERED_FRAMES = 'RetrieveRenderedFrames'
  RETRIEVE_STUDY = 'RetrieveStudy'
  RETRIEVE_SERIES = 'RetrieveSeries'


# TODO: Adapt query to fetch #days in current storage class.
def get_data_access_logs_query(
    ilm_cfg: ilm_config.ImageLifecycleManagementConfig,
):
  """Generates query to fetch DICOM instances access requests in data logs."""
  methods = [
      'REGEXP_CONTAINS('
      '    protoPayload_auditlog.methodName, '
      f'   "^(google.cloud.healthcare.(v1|v1beta1).dicomweb.DicomWebService.{method.value})$"'
      ')'
      for method in DicomWebServiceMethods
  ]
  method_conditions = ' OR '.join(methods)
  if ilm_cfg.logs_config.log_entries_date_equal_or_after:
    date_condition = (
        f'  AND {_REQUEST_TIMESTAMP} > PARSE_TIMESTAMP("%Y%m%d",'
        f' "{ilm_cfg.logs_config.log_entries_date_equal_or_after}")'
    )
  else:
    date_condition = ''
  return (
      'SELECT '
      f'  {_DICOMWEB_SERVICE_METHOD}, {_DICOM_STORE_PATH}, '
      f'  {_REQUEST_TIMESTAMP}, {_REQUEST_JSON} '
      f'FROM `{ilm_cfg.logs_config.logs_bigquery_table}` '
      'WHERE '
      f'  ({method_conditions}) '
      f'  AND {_STATUS_CODE} IS NULL'  # Ignore requests with error status.
      '   AND'
      f'    {_DICOM_STORE_PATH}="{ilm_cfg.dicom_store_config.dicom_store_path}"'
      f'  {date_condition}'
  )


class ParseDataAccessLogsDoFn(beam.DoFn):
  """DoFn to parse relevant metadata in data access logs."""

  def __init__(self, ilm_cfg: ilm_config.ImageLifecycleManagementConfig):
    self._cfg = ilm_cfg
    self._today = datetime.datetime.now(tz=datetime.timezone.utc)
    self._dicom_store_client = dicom_store_lib.DicomStoreClient(self._cfg)
    self._throttler = pipeline_util.Throttler(
        self._cfg.logs_config.max_dicom_store_qps
    )

  def setup(self):
    self._dicom_store_client = dicom_store_lib.DicomStoreClient(self._cfg)
    self._throttler = pipeline_util.Throttler(
        self._cfg.logs_config.max_dicom_store_qps
    )

  def _cleanup_dicomweb_path(self, dicomweb_path: str) -> Tuple[str, int]:
    """Cleans up DICOMweb path and counts # of requested frames, if present.

    Args:
      dicomweb_path: request DICOMweb path, which may or may not contain
        {series, instances, frames, rendered} parts:
        studies/<>/series/<>/instances/<>/frames/<>/rendered

    Returns:
      DICOMweb path without /frames* or /rendered suffix, and number of frames
      if present.
    """
    dicomweb_path = dicomweb_path.removesuffix('/rendered')
    if 'frames' not in dicomweb_path:
      return dicomweb_path, 0
    dicomweb_path, frames = dicomweb_path.split('/frames/')
    return dicomweb_path, len(frames.split(','))

  def _instance_access_count(
      self,
      dicomweb_service_method: str,
      dicomweb_path: str,
      access_num_days: int,
  ) -> List[Tuple[str, ilm_types.LogAccessCount]]:
    dicomweb_path, request_num_frames = self._cleanup_dicomweb_path(
        dicomweb_path
    )
    if dicomweb_service_method.endswith(
        DicomWebServiceMethods.RETRIEVE_INSTANCE.value
    ) or dicomweb_service_method.endswith(
        DicomWebServiceMethods.RETRIEVE_RENDERED_INSTANCE.value
    ):
      log_access_count = ilm_types.LogAccessCount(
          frames_access_count=None,
          instance_access_count=ilm_config.AccessCount(
              num_days=access_num_days, count=1.0
          ),
      )
      return [(dicomweb_path, log_access_count)]
    if dicomweb_service_method.endswith(
        DicomWebServiceMethods.RETRIEVE_STUDY.value
    ) or dicomweb_service_method.endswith(
        DicomWebServiceMethods.RETRIEVE_SERIES.value
    ):
      try:
        self._throttler.wait()
        instances = self._dicom_store_client.fetch_instances(dicomweb_path)
      except dicom_store_lib.DicomStoreError as e:
        logging.warning(
            'Unable to fetch instances for %s: %s. Skipping.', dicomweb_path, e
        )
        return []
      log_access_count = ilm_types.LogAccessCount(
          frames_access_count=None,
          instance_access_count=ilm_config.AccessCount(
              num_days=access_num_days, count=1.0
          ),
      )
      return [(instance, log_access_count) for instance in instances]
    if dicomweb_service_method.endswith(
        DicomWebServiceMethods.RETRIEVE_FRAMES.value
    ) or dicomweb_service_method.endswith(
        DicomWebServiceMethods.RETRIEVE_RENDERED_FRAMES.value
    ):
      log_access_count = ilm_types.LogAccessCount(
          frames_access_count=ilm_config.AccessCount(
              num_days=access_num_days, count=request_num_frames
          ),
          instance_access_count=None,
      )
      return [(dicomweb_path, log_access_count)]
    return []

  def process(
      self, raw_log_entry: Mapping[str, Any]
  ) -> Iterator[Tuple[str, ilm_types.LogAccessCount]]:
    try:
      log_date = raw_log_entry[_REQUEST_TIMESTAMP_KEY]
      dicomweb_service_method = raw_log_entry[_DICOMWEB_SERVICE_METHOD_KEY]
      request_json = json.loads(raw_log_entry[_REQUEST_JSON_KEY])
      dicomweb_path = request_json[_REQUEST_JSON_DICOMWEB_PATH_KEY]
    except (KeyError, json.decoder.JSONDecodeError, TypeError) as exp:
      logging.warning(
          'Parsing of log entry %s failed with error: %s. Skipping.',
          raw_log_entry,
          exp,
      )
      return

    log_date = log_date.astimezone(datetime.timezone.utc)
    access_num_days = max((self._today - log_date).days, 0)
    instance_to_access_count = self._instance_access_count(
        dicomweb_service_method, dicomweb_path, access_num_days
    )
    for instance, log_access_count in instance_to_access_count:
      if instance in self._cfg.instances_disallow_list:
        continue
      yield instance, log_access_count


def _merge_same_day_access_counts(
    access_counts: List[ilm_config.AccessCount],
) -> List[ilm_config.AccessCount]:
  num_days_to_count = collections.defaultdict(float)
  for access in access_counts:
    num_days_to_count[access.num_days] += access.count
  return [
      ilm_config.AccessCount(num_days=num_days, count=count)
      for num_days, count in num_days_to_count.items()
  ]


def compute_log_access_metadata(
    instance_to_access_counts: Tuple[str, Iterable[ilm_types.LogAccessCount]],
) -> Tuple[str, ilm_types.LogAccessMetadata]:
  """Computes log access metadata, merging same day access counts."""
  instance, log_access_counts = instance_to_access_counts
  log_access_counts = list(log_access_counts)
  frames_access_counts = [
      a.frames_access_count
      for a in log_access_counts
      if a.frames_access_count is not None
  ]
  instance_access_counts = [
      a.instance_access_count
      for a in log_access_counts
      if a.instance_access_count is not None
  ]
  return (
      instance,
      ilm_types.LogAccessMetadata(
          frames_access_counts=_merge_same_day_access_counts(
              frames_access_counts
          ),
          instance_access_counts=_merge_same_day_access_counts(
              instance_access_counts
          ),
      ),
  )
