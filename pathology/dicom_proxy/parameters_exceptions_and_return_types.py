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
"""Parameters and return types for downsample_util."""

from __future__ import annotations

import copy
import dataclasses
import shutil
from typing import Any, List, Mapping, Optional, Union

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import color_conversion_util
from pathology.dicom_proxy import dicom_instance_request
from pathology.dicom_proxy import dicom_store_util
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import frame_retrieval_util
from pathology.dicom_proxy import icc_color_transform
from pathology.dicom_proxy import metadata_util
from pathology.dicom_proxy import pydicom_single_instance_read_cache
from pathology.dicom_proxy import render_frame_params
from pathology.dicom_proxy import user_auth_util


# Types
_AuthSession = user_auth_util.AuthSession
_Compression = enum_types.Compression
_DicomInstanceMetadata = metadata_util.DicomInstanceMetadata
_FrameImages = frame_retrieval_util.FrameImages
_IccColorTransform = icc_color_transform.IccColorTransform
_PyDicomFilePath = pydicom_single_instance_read_cache.PyDicomFilePath
_PyDicomSingleInstanceCache = (
    pydicom_single_instance_read_cache.PyDicomSingleInstanceCache
)
_RenderFrameParams = render_frame_params.RenderFrameParams


class DownsamplingFrameRequestError(Exception):
  pass


def _return_frame_bytes(
    frame_data_list: List[frame_retrieval_util.FrameData],
) -> List[bytes]:
  results = []
  for data in frame_data_list:
    if data.image is None:
      # should never happen
      raise frame_retrieval_util.BaseFrameRetrievalError(
          'Frame image bytes is none'
      )
    results.append(data.image)
  return results


class DicomInstanceWebRequest(dicom_instance_request.DicomInstanceRequest):
  """Wraps a downsampling request for DICOM instance in DICOM Store."""

  def __init__(
      self,
      user_auth: _AuthSession,
      baseurl: dicom_url_util.DicomWebBaseURL,
      study: dicom_url_util.StudyInstanceUID,
      series: dicom_url_util.SeriesInstanceUID,
      instance: dicom_url_util.SOPInstanceUID,
      enable_caching: cache_enabled_type.CachingEnabled,
      url_args: Mapping[str, str],
      url_header: Mapping[str, str],
      instance_metadata: Optional[_DicomInstanceMetadata] = None,
  ):
    """Constructor.

    Args:
      user_auth: User authentication to use for DICOM store communication.
      baseurl: Base URL for DICOMweb requests.
      study: Study Instance UID requested.
      series: Series Instance UID requested.
      instance: SOP Instance UID requested.
      enable_caching: Enable metadata caching.
      url_args: Arguments to url instance request.
      url_header: Header passed to http request.
      instance_metadata: Optional Metadata for DICOM instance if supplied
        doesn't query store.
    """
    self._url_args = copy.copy(url_args)
    self._url_header = copy.copy(url_header)
    self._enable_caching = enable_caching
    self._source_instance_metadata = instance_metadata
    self._user_auth = user_auth
    self._instance = instance
    self._dicom_series_url = dicom_url_util.base_dicom_series_url(
        baseurl, study, series
    )
    self._request_session_handler = frame_retrieval_util.RequestSessionHandler()

  @property
  def url_args(self) -> Mapping[str, str]:
    return self._url_args

  @property
  def url_header(self) -> Mapping[str, str]:
    return self._url_header

  @property
  def user_auth(self) -> _AuthSession:
    return self._user_auth

  @property
  def dicom_sop_instance_url(self) -> dicom_url_util.DicomSopInstanceUrl:
    return dicom_url_util.series_dicom_instance_url(
        self._dicom_series_url, self._instance
    )

  def as_dict(self) -> Mapping[str, Any]:
    return dict(
        source_instance_metadata=self._source_instance_metadata,
        user_auth=self._user_auth.add_to_header({}),
        dicom_series_url=str(self._dicom_series_url),
        dicom_instance=str(self._instance.sop_instance_uid),
        enable_caching=bool(self._enable_caching),
        url_args=self._url_args,
        url_header=self._url_header,
    )

  def download_instance(self, path: str) -> None:
    """Downloads DICOM instance from DICOM Store to path."""
    dicom_store_util.download_dicom_instance(
        self._user_auth, self._dicom_series_url, self._instance, path
    )

  def icc_profile(self) -> Optional[bytes]:
    return color_conversion_util.get_instance_icc_profile_bytes(
        self._user_auth,
        self._dicom_series_url,
        self._instance,
        self._enable_caching,
    )

  def icc_profile_color_space(self) -> str:
    return self.metadata.icc_profile_colorspace

  def icc_profile_transform(
      self, transform: enum_types.ICCProfile
  ) -> Optional[_IccColorTransform]:
    """Returns ICC Profile transformation used to convert instance to SRGB.

    Internally transform request is cached at DICOMStore/Study/Series to
    speed repeated requests for the same transformation.

    Args:
      transform: ICC Color transform to perfrom.

    Returns:
      ICC ColorProfile Transformation.
    """
    return color_conversion_util.get_icc_profile_transform_for_dicom_url(
        self._user_auth,
        self._dicom_series_url,
        self._instance,
        self._enable_caching,
        transform,
    )

  @property
  def metadata(self) -> _DicomInstanceMetadata:
    """Returns select downsampling metadata DICOM instance."""
    if self._source_instance_metadata is None:
      self._source_instance_metadata = metadata_util.get_instance_metadata(
          self._user_auth,
          self._dicom_series_url,
          self._instance,
          self._enable_caching,
      )
    return self._source_instance_metadata

  def get_dicom_frames(
      self, params: _RenderFrameParams, frames: List[int]
  ) -> _FrameImages:
    """Returns frame images in requested compression format.

       Frame images request is retrieved using thread pool to speed request and
       is cached internally to speed repeated requests for the same frame.

    Args:
      params: Rendered Parameters.
      frames: List/Set of frames numbers to return from instance.

    Returns:
      FrameImages
    """
    return frame_retrieval_util.get_dicom_frame_map(
        self._user_auth,
        dicom_url_util.series_dicom_instance_url(
            self._dicom_series_url, self._instance
        ),
        params,
        self.metadata,
        frames,
        self._request_session_handler,
    )

  def get_raw_dicom_frames(
      self, params: _RenderFrameParams, frame_numbers: List[int]
  ) -> List[bytes]:
    """Performs single HTTP request to retrieve block of frames from DICOM.

    DICOM store method is uncached and performs frame retrieval in a single
    HTTP request. Returned results stored in cache.

    Args:
      params: RenderFrameParameters for the request.
      frame_numbers: List of DICOM frame numbers to load; first frame index = 1.

    Returns:
      List of bytes stored in requested frames.

    Raises:
      DicomFrameRequestError: Error occurred retrieving frames.
      _FrameIndexError: Invalid frame index.
      _InvalidNumberOfReturnedFramesError: # of frames returned != requested.
    """
    return _return_frame_bytes(
        frame_retrieval_util.get_raw_frame_data(
            self._user_auth,
            dicom_url_util.series_dicom_instance_url(
                self._dicom_series_url, self._instance
            ),
            frame_numbers,
            params,
            self._request_session_handler,
        )
    )


class LocalDicomInstance(dicom_instance_request.DicomInstanceRequest):
  """Wraps a downsampling request for DICOM instance stored on the file system."""

  def __init__(
      self,
      cache: Union[_PyDicomSingleInstanceCache, _PyDicomFilePath, str],
      url_args: Optional[Mapping[str, str]] = None,
      url_header: Optional[Mapping[str, str]] = None,
  ):
    """Constructor.

    Args:
      cache: Path to local DICOM instance on FS or cached instance.
      url_args: Optional parameters to control how instance is processed.
      url_header: Optional parameters to provided pseudo http headers.
    """
    self._user_auth = _AuthSession(None)
    if isinstance(cache, str):
      cache = _PyDicomSingleInstanceCache(_PyDicomFilePath(cache))
    self._cache = cache
    self._dicom_sop_instance_url = None
    self._url_args = {} if url_args is None else copy.copy(url_args)
    self._url_header = {} if url_header is None else copy.copy(url_header)

  @property
  def url_header(self) -> Mapping[str, str]:
    return self._url_header

  @property
  def url_args(self) -> Mapping[str, str]:
    return self._url_args

  @property
  def cache(self) -> _PyDicomSingleInstanceCache:
    return self._cache

  @property
  def user_auth(self) -> _AuthSession:
    return self._user_auth

  @property
  def dicom_sop_instance_url(self) -> dicom_url_util.DicomSopInstanceUrl:
    """Used for testing."""
    if self._dicom_sop_instance_url is not None:
      return self._dicom_sop_instance_url
    return dicom_url_util.DicomSopInstanceUrl(str(self._cache.path))

  @dicom_sop_instance_url.setter
  def dicom_sop_instance_url(
      self, value: Union[str, dicom_url_util.DicomSopInstanceUrl]
  ) -> None:
    """Used for testing."""
    if isinstance(value, str):
      self._dicom_sop_instance_url = dicom_url_util.DicomSopInstanceUrl(value)
    else:
      self._dicom_sop_instance_url = value

  def download_instance(self, path: str) -> None:
    """Copies local instance to path."""
    shutil.copyfile(self._cache.path, path)

  def icc_profile(self) -> Optional[bytes]:
    return self._cache.icc_profile

  def icc_profile_color_space(self) -> str:
    return self._cache.color_space

  def icc_profile_transform(
      self, transform: enum_types.ICCProfile
  ) -> Optional[_IccColorTransform]:
    """Returns ICC Profile transformation used to convert instance to SRGB.

    Args:
      transform: ICC Color transform to perfrom.

    Returns:
      ICC ColorProfile Transformation.
    """
    return color_conversion_util.get_icc_profile_transform_for_local_file(
        self._cache, transform
    )

  @property
  def metadata(self) -> _DicomInstanceMetadata:
    """Returns select downsampling metadata DICOM instance."""
    return self._cache.metadata  # type: ignore

  def get_dicom_frames(
      self, params: _RenderFrameParams, frames: List[int]
  ) -> _FrameImages:
    """Returns frame images in requested compression format.

       Frame images request is retrieved using thread pool to speed request and
       is cached internally to speed repeated requests for the same frame.

    Args:
      params: Rendered Parameters.
      frames: List of frames numbers to return from instance.

    Returns:
      FrameImages
    """
    return frame_retrieval_util.get_dicom_frame_map(
        self._user_auth,
        self._cache,
        params,
        self._cache.metadata,  # type: ignore
        frames,
    )

  def get_raw_dicom_frames(
      self, params: _RenderFrameParams, frame_numbers: List[int]
  ) -> List[bytes]:
    """Returns list of bytes stored in frames in DICOM instance.

    Args:
      params: RenderFrameParameters for the request.
      frame_numbers: List of DICOM frame numbers to load; first frame index = 1.

    Returns:
      List of bytes stored in requested frames.

    Raises:
      BaseFrameRetrievalError: Invalid frame index or empty frame bytes.
    """
    del params
    return _return_frame_bytes(
        frame_retrieval_util.get_local_frame_list(
            self._cache, frame_numbers, frame_numbers_start_at_index_0=False
        )
    )


@dataclasses.dataclass
class Metrics:
  """Metrics for get_rendered_dicom_frames."""

  mini_batch_requests: int = 0
  frame_requests: int = 0
  images_transcoded: bool = False
  number_of_frames_downloaded_from_store: int = 0

  def add_metrics(self, metric: Metrics):
    self.mini_batch_requests += metric.mini_batch_requests
    self.frame_requests += metric.frame_requests
    self.images_transcoded = self.images_transcoded or metric.images_transcoded
    self.number_of_frames_downloaded_from_store += (
        metric.number_of_frames_downloaded_from_store
    )

  def str_dict(self) -> Mapping[str, str]:
    return {key: str(value) for key, value in dataclasses.asdict(self).items()}


class RenderedDicomFrames:
  """Return type for get_rendered_dicom_frame."""

  def __init__(
      self, images: List[bytes], params: _RenderFrameParams, metrics: Metrics
  ):
    """Constructor.

    Args:
      images: List of compressed images (bytes).  Images in order requested.
      params: Parameters defining downsampled rendered frame request.
      metrics: Metrics describing request.
    """
    self._images = images
    self._compression = params.compression
    self._metrics = metrics

  @property
  def images(self) -> List[bytes]:
    """Returns list of image bytes."""
    return self._images

  @property
  def compression(self) -> _Compression:
    """Returns compression format used to encode images."""
    return self._compression

  @property
  def metrics(self) -> Metrics:
    """Returns metrics describing request."""
    return self._metrics

  @property
  def content_type(self) -> str:
    """Returns content mime type for image compression encoding.

    Raises:
      ValueError: Image compression is not recognized.
    """
    if self.compression == _Compression.JPEG:
      return 'image/jpeg'
    if self.compression == _Compression.PNG:
      return 'image/png'
    if self.compression == _Compression.WEBP:
      return 'image/webp'
    if self.compression == _Compression.RAW:
      return 'image/raw'
    if self.compression == _Compression.GIF:
      return 'image/gif'
    if self.compression == _Compression.JPEG_TRANSCODED_TO_JPEGXL:
      return 'image/jxl'
    if self.compression == _Compression.JPEGXL:
      return 'image/jxl'
    raise ValueError('Unknown image compression.')

  @property
  def multipart_content_type(self) -> str:
    """Returns multipart content mime type for image compression encoding.

    Raises:
      ValueError: Image compression is not recognized.
    """
    if self.compression == _Compression.JPEG:
      return 'image/jpeg; transfer-syntax=1.2.840.10008.1.2.4.50'
    if self.compression == _Compression.PNG:
      return 'image/png'
    if self.compression == _Compression.WEBP:
      return 'image/webp'
    if self.compression == _Compression.RAW:
      return 'application/octet-stream; transfer-syntax=1.2.840.10008.1.2.1'
    if self.compression == _Compression.GIF:
      return 'image/gif'
    if self.compression == _Compression.JPEG_TRANSCODED_TO_JPEGXL:
      return 'image/jxl; transfer-syntax=1.2.840.10008.1.2.4.111'
    if self.compression == _Compression.JPEGXL:
      return 'image/jxl; transfer-syntax=1.2.840.10008.1.2.4.112'
    raise ValueError('Unknown image compression.')
