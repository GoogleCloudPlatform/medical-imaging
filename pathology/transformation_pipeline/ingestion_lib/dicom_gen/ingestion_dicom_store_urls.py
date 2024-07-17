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
"""Flags defining DICOM Store base urls."""
import functools
import re
from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import ingest_const


_DICOM_STORE_PATH_REGEX = re.compile(
    f'({ingest_const.HEALTHCARE_API.rstrip("/")}'
    r'/projects/.+?/locations/.+?/datasets/.+?/dicomStores/.+?)'
    r'(/(dicomWeb(/.*)?)?.*)?'
)

_APPEND_DICOM_WEB = True


@functools.cache
def normalize_dicom_store_url(url: str, dicom_web: bool) -> str:
  """Normalize DICOM store url.

  If url differs from expectation return stripped version of url.
  Function memoized to avoid re-normalization.

  Args:
    url: DICOM store url.
    dicom_web: Append /dicomWeb onto end of returned URL.

  Returns:
    Normalized URL or origional.strip()
  """
  url = url.strip()
  if not url:
    return url
  match = _DICOM_STORE_PATH_REGEX.fullmatch(url)
  if match is None:
    return url
  base = match.groups()[0]
  base = f'{base}/dicomWeb' if dicom_web else base
  if base is None:
    return url
  return base


def get_main_dicom_web_url() -> str:
  """Returns main DICOMweb DICOM store url.

  Raises:
    ValueError: URL not defined.
  """
  dicomweb_path = normalize_dicom_store_url(
      ingest_flags.DICOMWEB_URL_FLG.value, _APPEND_DICOM_WEB
  )
  if not dicomweb_path:
    msg = 'Main DICOM store DICOMweb path is undefined.'
    cloud_logging_client.critical(msg)
    raise ValueError(msg)
  return dicomweb_path


def get_oof_dicom_web_url() -> str:
  """Returns oof DICOMweb DICOM store url."""
  return normalize_dicom_store_url(
      ingest_flags.OOF_DICOMWEB_BASE_URL_FLG.value, _APPEND_DICOM_WEB
  )
