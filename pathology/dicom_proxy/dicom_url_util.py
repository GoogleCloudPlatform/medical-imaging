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
"""Utility functions for creating DICOM Proxy urls."""
import dataclasses
from typing import List, Mapping, NewType, Optional, Union

import dataclasses_json

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import render_frame_params
from pathology.dicom_proxy import user_auth_util

_HEALTHCARE_API_URL = 'https://healthcare.googleapis.com'
_ACCEPT = 'Accept'

_AuthSession = user_auth_util.AuthSession

_UNTRANSCODED_FRAME_ACCEPT_VALUE = (
    'multipart/related; type="application/octet-stream"; transfer-syntax=*'
)


DICOM_WEB_BASE_URL_DEFAULT_VERSION = (
    '/projects/<string:projectid>/locations/<string:location>/datasets/'
    '<string:datasetid>/dicomStores/<string:dicomstore>/dicomWeb'
)
DICOM_WEB_STUDY_URL_DEFAULT_VERSION = (
    f'{DICOM_WEB_BASE_URL_DEFAULT_VERSION}/studies/<string:study_instance_uid>'
)

DICOM_WEB_SERIES_URL_DEFAULT_VERSION = (
    f'{DICOM_WEB_STUDY_URL_DEFAULT_VERSION}/series/<string:series_instance_uid>'
)
DICOM_WEB_INSTANCE_URL_DEFAULT_VERSION = (
    f'{DICOM_WEB_SERIES_URL_DEFAULT_VERSION}'
    '/instances/<string:sop_instance_uid>'
)
DICOM_WEB_BASE_URL = (
    '/<string:store_api_version>/projects/<string:projectid>/locations/'
    '<string:location>/datasets/<string:datasetid>/dicomStores/'
    '<string:dicomstore>/dicomWeb'
)
DICOM_WEB_STUDY_URL = (
    f'{DICOM_WEB_BASE_URL}/studies/<string:study_instance_uid>'
)

DICOM_WEB_SERIES_URL = (
    f'{DICOM_WEB_STUDY_URL}/series/<string:series_instance_uid>'
)
DICOM_WEB_INSTANCE_URL = (
    f'{DICOM_WEB_SERIES_URL}/instances/<string:sop_instance_uid>'
)


@dataclasses.dataclass(frozen=True)
class DicomWebBaseURL:
  """Defines target DICOM Store."""

  dicom_store_api_version: str
  gcp_project_id: str
  location: str
  dataset_id: str
  dicom_store: str

  def __str__(self) -> str:
    """Returns URL to target defined DICOM Store."""
    return (
        f'{self.dicom_store_api_version}/projects/'
        f'{self.gcp_project_id}/locations/{self.location}/datasets/'
        f'{self.dataset_id}/dicomStores/{self.dicom_store}/dicomWeb'
    )

  @property
  def full_url(self) -> str:
    return f'{_HEALTHCARE_API_URL}/{self.__str__()}'

  @property
  def root_url(self) -> str:
    return _HEALTHCARE_API_URL


@dataclasses.dataclass(frozen=True)
class StudyInstanceUID:
  """Defines DICOM StudyInstanceUID."""

  study_instance_uid: str

  def __str__(self) -> str:
    """Returns DICOMweb URL component identifying UID."""
    return f'studies/{self.study_instance_uid}'


@dataclasses.dataclass(frozen=True)
class SeriesInstanceUID:
  """Defines DICOM SeriesInstanceUID."""

  series_instance_uid: str

  def __str__(self) -> str:
    """Returns DICOMweb URL component identifying UID."""
    return f'series/{self.series_instance_uid}'


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class SOPInstanceUID:
  """Defines DICOM SOPInstanceUID."""

  sop_instance_uid: str

  def __str__(self) -> str:
    """Returns DICOMweb URL component identifying UID."""
    return f'instances/{self.sop_instance_uid}'


DicomStudyUrl = NewType('DicomStudyUrl', str)
DicomSeriesUrl = NewType('DicomSeriesUrl', str)
DicomSopInstanceUrl = NewType('DicomSopInstanceUrl', str)


def base_dicom_study_url(
    baseurl: DicomWebBaseURL, study: StudyInstanceUID
) -> DicomStudyUrl:
  """Returns URL identifying DICOM study from components."""
  return DicomStudyUrl(f'{baseurl.full_url}/{study}')


def base_dicom_series_url(
    baseurl: DicomWebBaseURL, study: StudyInstanceUID, series: SeriesInstanceUID
) -> DicomSeriesUrl:
  """Returns URL identifying DICOM series from components."""
  return DicomSeriesUrl(f'{baseurl.full_url}/{study}/{series}')


def base_dicom_instance_url(
    baseurl: DicomWebBaseURL,
    study: StudyInstanceUID,
    series: SeriesInstanceUID,
    instance: SOPInstanceUID,
) -> DicomSopInstanceUrl:
  """Returns URL identifying DICOM instance from components."""
  return DicomSopInstanceUrl(f'{baseurl.full_url}/{study}/{series}/{instance}')


def series_dicom_instance_url(
    series_url: DicomSeriesUrl, instance: SOPInstanceUID
) -> DicomSopInstanceUrl:
  """Returns URL identifying DICOM instance from components."""
  return DicomSopInstanceUrl(f'{series_url}/{instance}')


def get_dicom_store_url(url: str) -> str:
  url = url.lstrip('/')
  if url.lower().startswith('projects'):
    url = f'{dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION}/{url}'
  return f'{_HEALTHCARE_API_URL}/{url}'


@dataclasses.dataclass(kw_only=True, frozen=True)
class DicomStoreTransaction:
  """Wraps URL and Headers to use and pass to execute DICOMweb transaction."""

  url: str
  headers: Mapping[str, str]


@dataclasses.dataclass(kw_only=True, frozen=True)
class DicomStoreFrameTransaction(DicomStoreTransaction):
  """Wraps URL and Headers to use and pass to execute DICOMweb transaction."""

  frame_numbers: List[int]
  enable_caching: cache_enabled_type.CachingEnabled


def dicom_instance_tag_query(
    user_auth: _AuthSession,
    study_or_series_url: Union[DicomSeriesUrl, DicomStudyUrl],
    instance: Optional[SOPInstanceUID] = None,
    additional_tags: Optional[List[str]] = None,
) -> DicomStoreTransaction:
  """Request queries DICOM series instances for JSON formatted metadata.

  Args:
    user_auth: User header authentication.
    study_or_series_url: DICOM study or series to query.
    instance: Optional SOPInstanceUID to query in series.
    additional_tags: Optional list of additional tags(keywords) to return
      metadata for.

  Returns:
    DicomStoreTransaction
  """
  query = f'{study_or_series_url}/instances'
  if instance is None or not instance:
    parameters = []
  else:
    parameters = [f'SOPInstanceUID={instance.sop_instance_uid}']
  if additional_tags is not None:
    parameters.extend([f'includefield={tag}' for tag in additional_tags])
  if parameters:
    parameters = '&'.join(parameters)
    query = f'{query}?{parameters}'
  return DicomStoreTransaction(
      url=query,
      headers=user_auth.add_to_header({_ACCEPT: 'application/dicom+json'}),
  )


def dicom_instance_metadata_query(
    user_auth: _AuthSession,
    series_url: DicomSeriesUrl,
    instance: Optional[SOPInstanceUID] = None,
) -> DicomStoreTransaction:
  """Request queries DICOM series instances for JSON formatted metadata.

  Args:
    user_auth: User header authentication.
    series_url: DICOM series to query.
    instance: Optional SOPInstanceUID to query in series.

  Returns:
    DicomStoreTransaction
  """
  if instance is None or not instance:
    query = f'{series_url}/metadata'
  else:
    query = f'{series_url}/instances/{instance.sop_instance_uid}/metadata'
  return DicomStoreTransaction(
      url=query,
      headers=user_auth.add_to_header({_ACCEPT: 'application/dicom+json'}),
  )


def download_dicom_instance_not_transcoded(
    user_auth: _AuthSession, instance_url: DicomSopInstanceUrl
) -> DicomStoreTransaction:
  """Request to downloads untranscoded DICOM instance from store.

  Args:
    user_auth: User header authentication.
    instance_url: DICOM URL identifying SOP Instance UID.

  Returns:
    DicomStoreTransaction
  """
  return DicomStoreTransaction(
      url=str(instance_url),
      headers=user_auth.add_to_header(
          {_ACCEPT: 'application/dicom; transfer-syntax=*'}
      ),
  )


def download_bulkdata(
    user_auth: _AuthSession, bulkdata_url: str
) -> DicomStoreTransaction:
  """Request to downloads untranscoded DICOM instance from store.

  Args:
    user_auth: User header authentication.
    bulkdata_url: DICOM URL identifying SOP Instance UID.

  Returns:
    DicomStoreTransaction
  """
  return DicomStoreTransaction(
      url=bulkdata_url,
      headers=user_auth.add_to_header(
          {_ACCEPT: 'application/octet-stream; transfer-syntax=*'}
      ),
  )


def download_dicom_raw_frame(
    user_auth: _AuthSession,
    instance_url: DicomSopInstanceUrl,
    frame_numbers: List[int],
    params: render_frame_params.RenderFrameParams,
) -> DicomStoreFrameTransaction:
  """Downloads raw DICOM frame from store.

  Args:
    user_auth: User header authentication.
    instance_url: DICOM URL identifying SOP Instance UID.
    frame_numbers: List of frame numbers to return.
    params: render_frame_params.RenderFrameParams

  Returns:
    DicomStoreFrameTransaction
  """
  frame_str = ','.join([str(num) for num in frame_numbers])
  return DicomStoreFrameTransaction(
      url=f'{instance_url}/frames/{frame_str}',
      # Accept header defines the format of what the tile-server is requesting
      # from the server. “application/octet-stream transfer-syntax=*” indicates
      # that the tile-server wants the server to return imaging from the DICOM
      # store as stored, unaltered (no transcoding).
      headers=user_auth.add_to_header(
          {_ACCEPT: _UNTRANSCODED_FRAME_ACCEPT_VALUE}
      ),
      frame_numbers=frame_numbers,
      enable_caching=params.enable_caching,
  )


def get_rendered_frame_compression(
    compression: enum_types.Compression,
) -> enum_types.Compression:
  """Converts client requested render frame compression into server request.

     Not all exposed formats are supported by the DICOM server.

  Args:
    compression: Client requested frame pixel compression.

  Returns:
    Compression format which will be requested from the server. Some formats
    will require transcoding by the DICOM Proxy.
  """
  if compression == enum_types.Compression.JPEG:
    return enum_types.Compression.JPEG
  if compression == enum_types.Compression.PNG:
    return enum_types.Compression.PNG
  if compression in (
      enum_types.Compression.WEBP,
      enum_types.Compression.GIF,
      enum_types.Compression.RAW,
  ):
    return enum_types.Compression.PNG  # PNG is a lossless compression return
    # PNG to maximize quality.
  raise ValueError('Unhandled rendered frame compression')


def _rendered_frame_accept(
    compression: enum_types.Compression,
) -> Mapping[str, str]:
  """Returns MIME Accept type for DICOM Store rendered frame request.

  Args:
    compression: Compression encoding requested by the client may not be a
      format supported by the server.
  """
  compression = get_rendered_frame_compression(compression)
  if compression == enum_types.Compression.JPEG:
    return {_ACCEPT: 'image/jpeg'}
  if compression == enum_types.Compression.PNG:
    return {_ACCEPT: 'image/png'}
  raise ValueError('Unhandled rendered frame compression')


def download_rendered_dicom_frame(
    user_auth: _AuthSession,
    instance_url: DicomSopInstanceUrl,
    frame_number: int,
    params: render_frame_params.RenderFrameParams,
) -> DicomStoreFrameTransaction:
  """Downloads rendered DICOM frame from store.

  Args:
    user_auth: User header authentication.
    instance_url: DICOM URL identifying SOP Instance UID.
    frame_number: Frame number to request.
    params: RenderedFrameParams

  Returns:
    DicomStoreFrameTransaction
  """
  return DicomStoreFrameTransaction(
      url=f'{instance_url}/frames/{frame_number}/rendered',
      headers=user_auth.add_to_header(
          _rendered_frame_accept(params.compression)
      ),
      frame_numbers=[frame_number],
      enable_caching=params.enable_caching,
  )
