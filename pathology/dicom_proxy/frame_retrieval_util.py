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
"""DICOM frame retrieval utility."""
from collections.abc import Mapping
from concurrent import futures
import copy
import dataclasses
import http.client
from typing import Any, List, MutableMapping, Optional, Union

import requests
import requests_toolbelt

from pathology.dicom_proxy import base_dicom_request_error
from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import metadata_util
from pathology.dicom_proxy import pydicom_single_instance_read_cache
from pathology.dicom_proxy import redis_cache
from pathology.dicom_proxy import render_frame_params
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.logging_lib import cloud_logging_client

# types
_PyDicomSingleInstanceCache = (
    pydicom_single_instance_read_cache.PyDicomSingleInstanceCache
)
_DicomSopInstanceUrl = dicom_url_util.DicomSopInstanceUrl
_AuthSession = user_auth_util.AuthSession
_Compression = enum_types.Compression
_DicomStoreFrameTransaction = dicom_url_util.DicomStoreFrameTransaction

# Constants
_ACCEPT = 'Accept'

# Expected RAW Metadata
_CONTENT_TYPE = 'Content-Type'

CACHE_LOADING_FRAME_BYTES = b'LoadingCache'

# supported bulk data requests
# https://cloud.google.com/healthcare-api/docs/dicom#json_metadata_and_bulk_data_requests
_BASELINE_JPEG_MIME_TYPE_AND_TRANSFER_SYNTAX = (
    b'image/jpeg; transfer-syntax=1.2.840.10008.1.2.4.50'
)
_JPEG2000_LOSSLESS_MIME_TYPE_AND_TRANSFER_SYNTAX = (
    b'image/jp2; transfer-syntax=1.2.840.10008.1.2.4.90'
)
_JPEG2000_LOSSY_MIME_TYPE_AND_TRANSFER_SYNTAX = (
    b'image/jp2; transfer-syntax=1.2.840.10008.1.2.4.91'
)
_JPEGXL_LOSSLESS_TRANSFER_SYNTAX = (
    b'image/jxl; transfer-syntax=1.2.840.10008.1.2.4.110'
)
_JPEGXL_JPEG_RECOMPRESSION_TRANSFER_SYNTAX = (
    b'image/jxl; transfer-syntax=1.2.840.10008.1.2.4.111'
)
_JPEGXL_TRANSFER_SYNTAX = b'image/jxl; transfer-syntax=1.2.840.10008.1.2.4.112'
_JPEGXL_LOSSLESS_APPLICATION_OCTET_STREAM = (
    b'application/octet-stream; transfer-syntax=1.2.840.10008.1.2.4.110'
)
_JPEGXL_JPEG_RECOMPRESSION_APPLICATION_OCTET_STREAM = (
    b'application/octet-stream; transfer-syntax=1.2.840.10008.1.2.4.111'
)
_JPEGXL_APPLICATION_OCTET_STREAM = (
    b'application/octet-stream; transfer-syntax=1.2.840.10008.1.2.4.112'
)
_SUPPORTED_BULK_DATA_REQUESTS = (
    _BASELINE_JPEG_MIME_TYPE_AND_TRANSFER_SYNTAX,
    _JPEG2000_LOSSLESS_MIME_TYPE_AND_TRANSFER_SYNTAX,
    _JPEG2000_LOSSY_MIME_TYPE_AND_TRANSFER_SYNTAX,
    _JPEGXL_LOSSLESS_TRANSFER_SYNTAX,
    _JPEGXL_JPEG_RECOMPRESSION_TRANSFER_SYNTAX,
    _JPEGXL_TRANSFER_SYNTAX,
    _JPEGXL_LOSSLESS_APPLICATION_OCTET_STREAM,
    _JPEGXL_JPEG_RECOMPRESSION_APPLICATION_OCTET_STREAM,
    _JPEGXL_APPLICATION_OCTET_STREAM,
)


class DicomFrameRequestError(base_dicom_request_error.BaseDicomRequestError):
  """Exception which wraps error responses from DICOM store."""

  def __init__(self, response: requests.Response, msg: Optional[str] = None):
    super().__init__('DicomFrameRequestError', response, msg)


class BaseFrameRetrievalError(Exception):
  """Baseclass for exceptions associated with frame retrieval after request."""


class _FrameIndexError(BaseFrameRetrievalError):
  """Exception occurred requesting images."""


class _EmptyFrameURLError(BaseFrameRetrievalError):
  """Exception occurred retrieving images."""


class _InvalidNumberOfReturnedFramesError(BaseFrameRetrievalError):

  def __init__(self, msg: Optional[str] = None):
    if msg is None:
      msg = 'Number of frames returned != number of frames requested.'
    super().__init__(msg)


@dataclasses.dataclass(frozen=True)
class FrameData:
  image: Optional[bytes]
  downloaded_from_dicom_store: bool


class RequestSessionHandler:
  """Holds current transaction http request session.

  Enables session reuse within transaction do not use session in parallel.
  """

  def __init__(self):
    self._session: Optional[requests.Session] = None

  def session_get(
      self, url: str, headers: Optional[Mapping[str, str]]
  ) -> requests.Response:
    """Returns request session."""
    if self._session is None:
      self._session = requests.Session()
    return self._session.get(url, headers=headers)

  def __getstate__(self) -> MutableMapping[str, Any]:
    """Do not pickle session."""
    dct = copy.copy(self.__dict__)
    del dct['_session']
    return dct

  def __setstate__(self, dct: MutableMapping[str, Any]) -> None:
    """Init session to empty on de-serializing."""
    self.__dict__ = dct
    self._session = None

  def __del__(self):
    """Close session when handler destoryed."""
    if self._session is None:
      return
    self._session.close()


def frame_lru_cache_key(transaction: _DicomStoreFrameTransaction) -> str:
  """Returns cachetools lru cache key based on transaction request.

  Args:
    transaction: DICOM Store query that returns requested frame data.

  Returns:
    LRU cache key.
  """
  rendered_image_type = transaction.headers[_ACCEPT]
  return f'{transaction.url}:{rendered_image_type}'


def frame_cache_ttl() -> Optional[int]:
  """Returns TTL in seconds frames should be held in REDIS frame cache."""
  ttl = dicom_proxy_flags.FRAME_CACHE_TTL_FLG.value
  if ttl < 0:
    return None
  return ttl


def get_raw_frame_data(
    user_auth: _AuthSession,
    instance_url: _DicomSopInstanceUrl,
    frame_numbers: List[int],
    render_params: render_frame_params.RenderFrameParams,
    session_handler: Optional[RequestSessionHandler] = None,
) -> List[FrameData]:
  """Requests contiguous list of untranscoded frame data from DICOM store.

  Caches returned frames in redis for use by higher level functions.

  Args:
    user_auth: User authentication session, session can be none if reading local
      instance (PyDicomSingleInstanceCache).
    instance_url: DICOM Instance to read from.
    frame_numbers: Frame numbers to return.
    render_params: Rendered Frame Params. (Requested compression imaging format.
      Imaging may be returned in a different format and require transcoding and
      disable caching).
    session_handler: Request session object to enable connection pooling on
      session. Set to None if session can be access simultationusly from
      multiple threads.

  Returns:
    List of data encoded in frames with flag set to indicate frames loaded from
    DICOM Server.

  Raises:
    _EmptyFrameURLError: Transaction URL empty.
    DicomFrameRequestError: HTTP error in DICOM frame request.
  """
  transaction = dicom_url_util.download_dicom_raw_frame(
      user_auth,
      instance_url,
      frame_numbers,
      render_params,
  )
  if session_handler is None:
    response = requests.get(transaction.url, headers=transaction.headers)
  else:
    response = session_handler.session_get(transaction.url, transaction.headers)
  if response.status_code != http.client.OK:
    raise DicomFrameRequestError(response)
  try:
    multipart_data = requests_toolbelt.MultipartDecoder.from_response(response)
  except (
      AttributeError,
      requests_toolbelt.NonMultipartContentTypeException,
      requests_toolbelt.ImproperBodyPartContentException,
  ) as exp:
    raise DicomFrameRequestError(
        response, msg='NonMultipartContentTypeException'
    ) from exp
  try:
    part_count = len(multipart_data.parts)
    expected_frame_count = len(transaction.frame_numbers)
    if part_count != expected_frame_count:
      raise DicomFrameRequestError(
          response,
          msg=(
              f'Expected {expected_frame_count} multipart response actually'
              f' received {part_count}'
          ),
      )
    frame_data = []
    for index in range(part_count):
      part = multipart_data.parts[index]
      content_type = part.headers.get(_CONTENT_TYPE.encode('utf-8'))
      if content_type not in _SUPPORTED_BULK_DATA_REQUESTS:
        raise DicomFrameRequestError(
            response,
            msg=(
                'Frame request returned invalid content type. Received: '
                f'{content_type}'
            ),
        )
      result = part.content
      if result is None or not result:
        raise DicomFrameRequestError(
            response, msg='Frame request returned no data.'
        )
      frame_data.append(result)
  except (IndexError, KeyError, AttributeError) as exp:
    raise DicomFrameRequestError(
        response, msg='Incorrectly formatted multipart response.'
    ) from exp

  redis = redis_cache.RedisCache(transaction.enable_caching)
  frame_data_list = []
  for index, result in zip(frame_numbers, frame_data):
    transaction = dicom_url_util.download_dicom_raw_frame(
        user_auth, instance_url, [index], render_params
    )
    cache_key = frame_lru_cache_key(transaction)
    redis.set(
        cache_key, result, allow_overwrite=True, ttl_sec=frame_cache_ttl()
    )
    frame_data_list.append(FrameData(result, True))
  return frame_data_list


def _get_rendered_frame(transaction: _DicomStoreFrameTransaction) -> FrameData:
  """Returns result of rendered frame request.

  Caches frames in local redis.  Cache mannaged by LRU.
  Cache works across users and process.

  Args:
    transaction: DICOM Store query that returns requested frame data.

  Returns:
    Requested frame bytes encoding frame in jpeg or png format.

  Raises:
    _EmptyFrameURLError: Transaction URL empty.
    DicomFrameRequestError: HTTP error in DICOM frame request.
  """
  if not transaction.url:
    raise _EmptyFrameURLError()
  redis = redis_cache.RedisCache(transaction.enable_caching)
  cache_key = frame_lru_cache_key(transaction)
  result = redis.get(cache_key)
  if (
      result is not None
      and result.value is not None
      and result.value != CACHE_LOADING_FRAME_BYTES
  ):
    return FrameData(result.value, False)
  response = requests.get(transaction.url, headers=transaction.headers)
  if response.status_code != http.client.OK:
    raise DicomFrameRequestError(response)
  received_content_type = response.headers.get(_CONTENT_TYPE)
  expected_content_type = transaction.headers.get(_ACCEPT)
  if (
      received_content_type is None
      or received_content_type != expected_content_type
  ):
    raise DicomFrameRequestError(
        response,
        msg=(
            f'Invalid content type. Expected: {expected_content_type}; '
            f'Received: {received_content_type}'
        ),
    )
  response_bytes = response.content
  if response_bytes is None or not response_bytes:
    raise DicomFrameRequestError(
        response, msg='Frame request returned no data.'
    )
  redis.set(cache_key, response_bytes, ttl_sec=frame_cache_ttl())
  return FrameData(response_bytes, True)


def _get_raw_frame_list(
    user_auth: _AuthSession,
    instance_url: _DicomSopInstanceUrl,
    frame_numbers: List[int],
    render_params: render_frame_params.RenderFrameParams,
    session_handler: Optional[RequestSessionHandler],
) -> List[FrameData]:
  """Returns list of untranscoded frame data.

  Caches frames in local redis.  Cache mannaged by LRU and works across users
  and processes. Contiguous blocks of frames retrieved in single HTTP
  transaction discontinous frames retrieved in parallel.

  Args:
    user_auth: User authentication session, session can be none if reading local
      instance (PyDicomSingleInstanceCache).
    instance_url: DICOM Instance to read from.
    frame_numbers: List of frame numbers to return imaging for.
    render_params: Rendered Frame Params. (Requested compression imaging format.
      Imaging may be returned in a different format and require transcoding and
      disable caching).
    session_handler: Request session object to enable connection pooling on
      session. Set to None if session can be access simultationusly from
      multiple threads.

  Returns:
    List of bytes encoded in frame with flag indicating if data loaded from
    cache or server.

  Raises:
    _EmptyFrameURLError: Transaction URL empty.
    DicomFrameRequestError: HTTP error in DICOM frame request.
  """
  if not instance_url:
    raise _EmptyFrameURLError()
  if not frame_numbers:
    return []
  results = []
  redis = redis_cache.RedisCache(render_params.enable_caching)
  load_frame_numbers = []
  load_frame_index = []
  for index, frame_number in enumerate(frame_numbers):
    result = redis.get(
        frame_lru_cache_key(
            dicom_url_util.download_dicom_raw_frame(
                user_auth, instance_url, [frame_number], render_params
            )
        )
    )
    if (
        result is not None
        and result.value is not None
        and result.value != CACHE_LOADING_FRAME_BYTES
    ):
      results.append(FrameData(result.value, False))
    else:
      results.append(FrameData(None, True))
      load_frame_numbers.append(frame_number)
      load_frame_index.append(index)
  if not load_frame_numbers:
    return results
  frame_results = get_raw_frame_data(
      user_auth,
      instance_url,
      load_frame_numbers,
      render_params,
      session_handler,
  )
  for index, result_index in enumerate(load_frame_index):
    results[result_index] = frame_results[index]
  return results


def _get_rendered_frame_list(
    dicom_frames: List[_DicomStoreFrameTransaction],
) -> List[FrameData]:
  """Returns frame imaging bytes retrieved using thread pool.

  Args:
    dicom_frames: List of frame numbers to return imaging for.

  Returns:
    List of frame imaging bytes.
  """
  if len(dicom_frames) == 1:
    return [_get_rendered_frame(dicom_frames[0])]
  if not dicom_frames:
    return []
  max_workers = min(
      len(dicom_frames),
      dicom_proxy_flags.MAX_PARALLEL_FRAME_DOWNLOADS_FLG.value,
  )
  with futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
    return list(pool.map(_get_rendered_frame, dicom_frames))


def get_local_frame_list(
    dicom_instance_source: _PyDicomSingleInstanceCache,
    dicom_frames: List[int],
    frame_numbers_start_at_index_0: bool = True,
) -> List[FrameData]:
  """Returns frame imaging bytes retrieved using thread pool.

  Args:
    dicom_instance_source: Local DICOM Instance.
    dicom_frames: List of frame numbers to return imaging for.
    frame_numbers_start_at_index_0: Frame number index starts at 0.

  Returns:
    List of frame imaging bytes.

  Raises:
    _FrameIndexError: Invalid frame index requested.
  """
  try:
    if frame_numbers_start_at_index_0:
      return [
          FrameData(dicom_instance_source.get_encapsulated_frame(index), False)
          for index in dicom_frames
      ]
    return [
        FrameData(
            dicom_instance_source.get_encapsulated_frame(index - 1), False
        )
        for index in dicom_frames
    ]
  except IndexError as exp:
    raise _FrameIndexError(
        f'Invalid frame index requested; requested frames: {dicom_frames}'
    ) from exp


@dataclasses.dataclass
class FrameImages:
  """Data class holding frame bytes and compression."""

  images: MutableMapping[int, bytes]  # Mapping of frame number: bytes.
  compression: _Compression  # Compression format of bytes
  number_of_frames_downloaded_from_store: int


def _create_frame_images(
    frame_numbers: List[int],
    frame_images: List[FrameData],
    compression: Optional[_Compression],
) -> FrameImages:
  """Creates FrameImages dataclass.

  Args:
    frame_numbers: List of frame numbers.
    frame_images: Corresponding list of frame images.
    compression: Image compression encoding.

  Raises:
    _InvalidNumberOfReturnedFramesError: len of frame numbers != len of frame
    images.

  Returns:
    FrameImages Dataclass
  """
  if len(frame_numbers) != len(frame_images):
    raise _InvalidNumberOfReturnedFramesError()
  if compression is None:
    # Should only be raised if additional compression format is added
    # programmatically and caller to _create_frame_images is not
    # updated to handle the new format.
    raise ValueError('Compression format for frames not initialized.')

  number_of_frames_downloaded_from_store = sum([
      1 for frame_data in frame_images if frame_data.downloaded_from_dicom_store
  ])
  return FrameImages(
      {
          frame_index: image_mem.image
          for frame_index, image_mem in zip(frame_numbers, frame_images)
      },
      compression,
      number_of_frames_downloaded_from_store,
  )


def get_dicom_frame_map(
    user_auth: _AuthSession,
    dicom_instance_source: Union[
        _PyDicomSingleInstanceCache, _DicomSopInstanceUrl
    ],
    render_params: render_frame_params.RenderFrameParams,
    metadata: metadata_util.DicomInstanceMetadata,
    frames: List[int],
    session_handler: Optional[RequestSessionHandler] = None,
) -> FrameImages:
  """Gets frame images from DICOM instance in store or locally.

  Args:
    user_auth: User authentication session, session can be none if reading local
      instance (PyDicomSingleInstanceCache)
    dicom_instance_source: DICOM Instance to read from, either URL to instance
      on server (DicomSopInstanceUrl) or Reference to local file
      (PyDicomSingleInstanceCache)
    render_params: Rendered Frame Params. (Requested compression imaging format.
      Imaging may be returned in a different format and require transcoding and
      disable caching).
    metadata: Metadata for DICOM Instance.
    frames: List/set of DICOM frames to return.
    session_handler: Request session object to enable connection pooling on
      session. Set to None if session can be access simultationusly from
      multiple threads.

  Returns:
    FrameImages

  Raises:
    DicomFrameRequestError: Error occurred retrieving frames.
    _FrameIndexError: Invalid frame index.
    _InvalidNumberOfReturnedFramesError: # of frames returned != requested.
  """
  if min(frames) < 0:
    raise _FrameIndexError(
        f'Requesting frame # < 0; Frame numbers requested: {frames}'
    )
  if max(frames) >= metadata.number_of_frames:
    raise _FrameIndexError(
        'Requesting frame # >= metadata number of frames; '
        f'Number of frames: {metadata.number_of_frames} '
        f'Frame numbers requested: {frames}'
    )

  is_local_file = isinstance(dicom_instance_source, _PyDicomSingleInstanceCache)
  if is_local_file:
    frame_numbers = []
  else:
    frame_numbers = [frame_number + 1 for frame_number in frames]
    if not render_params.enable_caching:
      cloud_logging_client.warning('Frame caching disabled')
  try:
    # Raw image retrieval requires imaging be stored in DICOM transfer syntax
    # DICOM Proxy can decode. This option can result in higher performance and
    # quality because the image are returned from the store without transcoding.
    # Returning a rendered instance of JPEG encoded instance from the DICOM
    # store will result in decoding and re-encoding the encoded instance to
    # jpeg. Raw retrieval just returns stored bytes.
    #
    # The DICOM Proxy only supports direct decoding of Baseline JPEG, JPEG2000,
    # or RAW encoded pixel data.
    if metadata.is_baseline_jpeg or metadata.is_jpeg2000 or metadata.is_jpegxl:
      if is_local_file:
        frame_images = get_local_frame_list(dicom_instance_source, frames)
      else:
        try:
          frame_images = _get_raw_frame_list(
              user_auth,
              dicom_instance_source,
              frame_numbers,
              render_params,
              session_handler,
          )
        except DicomFrameRequestError as exp:
          cloud_logging_client.warning(
              (
                  'Dicom frame request failed. Trying to retrieve frame using '
                  'rendered api. Fail over will reduce performance.'
              ),
              exp,
          )
          frame_images = []

      # frame retrieval may return empty if instances are returned from
      # an instance which has transfer syntrax other than Jpeg Baseline,
      # jpeg 2000, or JPGX
      if frame_images:
        if metadata.is_baseline_jpeg:
          returned_image_compression = _Compression.JPEG
        elif metadata.is_jpeg2000:
          returned_image_compression = _Compression.JPEG2000
        elif metadata.is_jpg_transcoded_to_jpegxl:
          returned_image_compression = _Compression.JPEG_TRANSCODED_TO_JPEGXL
        elif metadata.is_jpegxl:
          returned_image_compression = _Compression.JPEGXL
        else:
          returned_image_compression = None
        return _create_frame_images(
            frames, frame_images, returned_image_compression
        )

    if is_local_file:
      raise ValueError(
          'Generation of rendered frames from local files is not supported.'
      )
    params = []
    for frame_number in frame_numbers:
      params.append(
          dicom_url_util.download_rendered_dicom_frame(
              user_auth, dicom_instance_source, frame_number, render_params
          )
      )
    return _create_frame_images(
        frames,
        _get_rendered_frame_list(params),
        dicom_url_util.get_rendered_frame_compression(
            render_params.compression
        ),
    )
  except _InvalidNumberOfReturnedFramesError as exp:
    if isinstance(dicom_instance_source, _PyDicomSingleInstanceCache):
      source = dicom_instance_source.path
    else:
      source = dicom_instance_source
    raise _InvalidNumberOfReturnedFramesError(
        'Number of images retrieved != number of images requested; '
        f'Source: {source}'
    ) from exp
