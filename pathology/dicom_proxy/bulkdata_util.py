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
import re

import flask

from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import flask_util

# match stores bulkdata uri.
PROXY_BULK_DATA_URI = 'bulkdata'
_GET_TILE_SERVER_PROJECT_REGEX = re.compile(
    '(http|https)://.*?/(projects/.*?/locations/.*?/datasets/.*?/dicomStores/.*?/dicomWeb)/.*'
)
_HEALTHCARE_API_BULKDATA_RESPONSE = re.compile(
    r'"https://healthcare\.googleapis\.com/.+?/projects/.+?/locations/.+?/'
    r'datasets/.+?/dicomStores/.+?/dicomWeb/studies/(.+?)/series/(.+?)/'
    r'instances/(.+?)/bulkdata/(.+?)"'.encode('utf-8'),
    re.IGNORECASE,
)
BULK_DATA_URI_KEY = 'BulkDataURI'
LOCALHOST = 'localhost'


class _UnexpectedDicomJsonMetadataError(Exception):
  pass


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
