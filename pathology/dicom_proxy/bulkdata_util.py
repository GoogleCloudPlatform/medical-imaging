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
"""Utility for detecting testing if store supports returning uri bulk metadata."""
import functools
import os
import re
import sys
import threading
from typing import Any, Mapping, Union

import flask

from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import flask_util

# match stores bulkdata uri.
PROXY_BULK_DATA_URI = 'bulkdata'
_GET_TILE_SERVER_PROJECT_REGEX = re.compile(
    '(http|https)://.*?/(projects/.*?/locations/.*?/datasets/.*?/dicomStores/.*?/dicomWeb)/.*'
)
_HEALTHCARE_API_URL_VERSION_REGEX = re.compile(
    'https://healthcare.googleapis.com/(.*?)(/.*)?', re.IGNORECASE
)
_HEALTHCARE_API_BULKDATA_RESPONSE = re.compile(
    r'"https://healthcare\.googleapis\.com/.+?/projects/.+?/locations/.+?/'
    r'datasets/.+?/dicomStores/.+?/dicomWeb/studies/(.+?)/series/(.+?)/'
    r'instances/(.+?)/bulkdata/(.+?)"'.encode('utf-8'),
    re.IGNORECASE,
)
_SQ = 'SQ'
_VALUE = 'Value'
_VR = 'vr'
BULK_DATA_URI_KEY = 'BulkDataURI'
LOCALHOST = 'localhost'

_UrlType = Union[str, dicom_url_util.DicomWebBaseURL]


class _UnexpectedDicomJsonMetadataError(Exception):
  pass


class InvalidDicomStoreUrlError(Exception):

  def __init__(self, url: str):
    super().__init__(f'Invalid DICOM store URL: {url}')


_dicom_store_versions_supporting_bulkdata = set()
_dicom_store_versions_supporting_bulkdata_lock = threading.Lock()
# internal flag to disable dicom store versions supporting bulk data cache
# if running in unit tests.
_is_debugging = 'UNITTEST_ON_FORGE' in os.environ or 'unittest' in sys.modules


def _init_fork_module_state() -> None:
  global _dicom_store_versions_supporting_bulkdata
  global _dicom_store_versions_supporting_bulkdata_lock
  _dicom_store_versions_supporting_bulkdata = set()
  _dicom_store_versions_supporting_bulkdata_lock = threading.Lock()


def _does_json_have_bulkdata_uri_key(
    dicom_dataset_metadata: Mapping[str, Any]
) -> bool:
  """Returns true if DICOM JSON metadata defines key anywhere in structure.

  Args:
    dicom_dataset_metadata: DICOM formated JSON metadata.

  Returns:
    True if a tag is found with BULK_DATA_URI_KEY in DICOM dataset.

  Raises:
    _UnexpectedDicomJsonMetadataError: Metadata is formatted unexpectedly.
  """
  try:
    for tag_metadata in dicom_dataset_metadata.values():
      if BULK_DATA_URI_KEY in tag_metadata and tag_metadata.get(
          BULK_DATA_URI_KEY
      ):
        return True
      vr_code = tag_metadata.get(_VR, '').strip().upper()
      if vr_code == _SQ:
        seq_datasets = tag_metadata.get(_VALUE, [])
        for value in seq_datasets:
          if _does_json_have_bulkdata_uri_key(value):
            return True
    return False
  except (AttributeError, TypeError) as exp:
    raise _UnexpectedDicomJsonMetadataError() from exp


def _parse_store_url_version(url: str) -> str:
  match = _HEALTHCARE_API_URL_VERSION_REGEX.fullmatch(url)
  if match is None:
    raise InvalidDicomStoreUrlError(url)
  version = match.groups()[0].lower().strip()
  if not version:
    raise InvalidDicomStoreUrlError(url)
  return version


def _does_store_version_support_bulkdata(store_version: str) -> bool:
  return store_version == 'v1beta1'


def _get_url(store_url: _UrlType) -> str:
  if isinstance(store_url, str):
    return store_url
  return store_url.full_url


def _func_match(baseurl: bytes, match: re.Match[bytes]) -> bytes:
  studies, series, instance, path = match.groups()
  return b''.join((
      b'"',
      baseurl,
      b'/studies/',
      studies,
      b'/series/',
      series,
      b'/instances/',
      instance,
      f'/{PROXY_BULK_DATA_URI}/'.encode('utf-8'),
      path,
      b'"',
  ))


def _set_bulk_data_uri_protocal(bulk_data_uri: str) -> str:
  # Replace url_root protocal with flag protocal.
  try:
    idx = bulk_data_uri.index('://')
    bulkdata_protocal = (
        dicom_proxy_flags.BULK_DATA_URI_PROTOCOL_FLG.value.strip().lower()
    )
    return f'{bulkdata_protocal}{bulk_data_uri[idx:]}'
  except ValueError:
    return ''


def get_bulk_data_base_url(
    dicom_store_base_url: dicom_url_util.DicomWebBaseURL,
) -> str:
  """Returns base url for bulk data response."""
  base_url = dicom_proxy_flags.BULK_DATA_PROXY_URL_FLG.value
  if base_url:
    return base_url.rstrip('/')
  url_root = _set_bulk_data_uri_protocal(flask_util.get_url_root())
  if not url_root:
    return ''
  match = _GET_TILE_SERVER_PROJECT_REGEX.fullmatch(flask_util.get_base_url())
  if match is None:
    return ''
  tile_location_url = match.groups()[1]
  return (
      f'{url_root}{dicom_proxy_flags.PROXY_SERVER_URL_PATH_PREFIX}/'
      f'{dicom_store_base_url.dicom_store_api_version}/{tile_location_url}'
  )


def proxy_dicom_store_bulkdata_response(
    dicom_store_base_url: dicom_url_util.DicomWebBaseURL,
    response: flask.Response,
) -> None:
  base_url = get_bulk_data_base_url(dicom_store_base_url)
  if not base_url:
    # Could not determine base url for bulkdata response. Leave flask response
    # unchanged.
    return
  response.data = _HEALTHCARE_API_BULKDATA_RESPONSE.sub(
      functools.partial(_func_match, base_url.encode('utf-8')), response.data
  )


def test_dicom_store_metadata_for_bulkdata_uri_support(
    store_url: _UrlType,
    metadata: Union[Mapping[str, Any], bytes],
) -> None:
  """If DICOM store bulk data support is unknown tests metadata and sets state."""
  store_url = _get_url(store_url)
  if not store_url or store_url == LOCALHOST:
    return
  store_url_version = _parse_store_url_version(store_url)
  if _does_store_version_support_bulkdata(store_url_version):
    return
  with _dicom_store_versions_supporting_bulkdata_lock:
    try:
      if isinstance(metadata, Mapping):
        if _does_json_have_bulkdata_uri_key(metadata):
          _dicom_store_versions_supporting_bulkdata.add(store_url_version)
      elif isinstance(metadata, bytes):
        if _HEALTHCARE_API_BULKDATA_RESPONSE.search(metadata) is not None:
          _dicom_store_versions_supporting_bulkdata.add(store_url_version)
    except _UnexpectedDicomJsonMetadataError:
      pass


def set_dicom_store_supports_bulkdata(store_url: _UrlType) -> None:
  """Flags that DICOM store support for bulkdata URI was detected."""
  store_url = _get_url(store_url)
  if not store_url or store_url == LOCALHOST:
    return
  version = _parse_store_url_version(store_url)
  with _dicom_store_versions_supporting_bulkdata_lock:
    if not _does_store_version_support_bulkdata(version):
      _dicom_store_versions_supporting_bulkdata.add(version)


def does_dicom_store_support_bulkdata(store_url: _UrlType) -> bool:
  """Returns true if DICOM store supports bulkdata."""
  store_url = _get_url(store_url)
  if store_url == LOCALHOST:
    return False
  version = _parse_store_url_version(_get_url(store_url))
  with _dicom_store_versions_supporting_bulkdata_lock:
    return (
        _does_store_version_support_bulkdata(version)
        or version in _dicom_store_versions_supporting_bulkdata
    ) and not _is_debugging


# The digitial_pathology_dicom proxy runs using gunicorn, which forks worker
# processes. Forked processes do not re-init global state and assume their
# values at the time of the fork. This can result in forked modules being
# started with invalid global state, e.g., acquired locks that will not release
# or references state. os.register at fork, defines a function run in child
# forked processes following the fork to re-initalize the forked global module
# state.
os.register_at_fork(after_in_child=_init_fork_module_state)
