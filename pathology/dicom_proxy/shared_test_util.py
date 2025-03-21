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
"""Shared test utilities."""

from __future__ import annotations

import contextlib
import copy
import http
import io
import json
import os
from typing import Any, Mapping, MutableMapping, Optional
from unittest import mock

import numpy as np
import PIL
import pydicom

from pathology.dicom_proxy import flask_util
from pathology.dicom_proxy import metadata_util
from pathology.dicom_proxy import parameters_exceptions_and_return_types
from pathology.dicom_proxy import pydicom_single_instance_read_cache
from pathology.dicom_proxy import render_frame_params


# Types
_DicomInstanceMetadata = metadata_util.DicomInstanceMetadata
_PyDicomSingleInstanceCache = (
    pydicom_single_instance_read_cache.PyDicomSingleInstanceCache
)
_LocalDicomInstance = parameters_exceptions_and_return_types.LocalDicomInstance
_RenderFrameParams = render_frame_params.RenderFrameParams


def _http_status_response(status: http.HTTPStatus) -> str:
  return (f'{status.value} {status.phrase}').upper()


def http_not_found_status() -> str:
  return _http_status_response(http.HTTPStatus.NOT_FOUND)


def http_ok_status() -> str:
  return _http_status_response(http.HTTPStatus.OK)


def http_bad_request_status() -> str:
  return _http_status_response(http.HTTPStatus.BAD_REQUEST)


class MockFlaskRequest(contextlib.ExitStack):
  """Mock wrapper for flask request globals."""

  def __init__(
      self,
      args: Optional[Mapping[str, str]] = None,
      data: bytes = b'',
      headers: Optional[Mapping[str, str]] = None,
      method: str = 'GET',
      path: str = '',
  ):
    super().__init__()
    self._args = copy.copy(args) if args is not None else {}
    self._data = data
    self._headers = copy.copy(headers) if headers is not None else {}
    self._method = method
    self._path = path

  def __enter__(self) -> MockFlaskRequest:
    result = super().__enter__()
    result.enter_context(
        mock.patch.object(
            flask_util,
            'get_headers',
            autospec=True,
            return_value=self._headers,
        )
    )
    result.enter_context(
        mock.patch.object(
            flask_util, 'get_method', autospec=True, return_value=self._method
        )
    )
    result.enter_context(
        mock.patch.object(
            flask_util,
            'get_key_args_list',
            autospec=True,
            return_value=self._args,
        )
    )
    result.enter_context(
        mock.patch.object(
            flask_util, 'get_data', autospec=True, return_value=self._data
        )
    )
    result.enter_context(
        mock.patch.object(
            flask_util, 'get_path', autospec=True, return_value=self._path
        )
    )
    return result


class RedisMock:
  """Redis cache mock."""

  def __init__(self):
    self._redis = {}

  def clear(self):
    self._redis.clear()

  def get(self, key: str) -> Any:
    return self._redis.get(key)

  def set(
      self, key: str, value: bytes, nx: bool, ex: Optional[int]
  ) -> Optional[str]:
    del ex
    if not nx:
      self._redis[key] = value
      return 'ok'
    if key not in self._redis:
      self._redis[key] = value
      return 'ok'
    return None

  def delete(self, key: str) -> int:
    if key in self._redis:
      del self._redis[key]
      return 1
    return 0

  def __len__(self) -> int:
    return len(self._redis)


def _get_decoded_image_bytes(compressed_image_bytes: bytes) -> np.ndarray:
  with io.BytesIO(compressed_image_bytes) as byte_buff:
    return np.frombuffer(
        PIL.Image.open(byte_buff).tobytes(), dtype=np.uint8
    ).flatten()


def rgb_image_almost_equal(
    image_1: bytes, image_2: bytes, threshold: int = 3
) -> bool:
  """Test image RGB bytes values are close."""
  return np.all(
      np.abs(
          _get_decoded_image_bytes(image_1) - _get_decoded_image_bytes(image_2)
      )
      < threshold
  )


def get_dir_path(*args: str) -> str:
  return os.path.join(os.path.dirname(__file__), *args)


def get_testdir_path(*args: str) -> str:
  return get_dir_path('testdata', *args)


def jpeg_encoded_dicom_instance_test_path() -> str:
  """Returns path to small WSI DICOM instance."""
  return get_testdir_path('multi_frame_jpeg_camelyon_challenge_image.dcm')


def jpeg_encoded_dicom_instance() -> pydicom.FileDataset:
  """Returns pydicom dataset to jpeg encoded WSI DICOM instance."""
  return pydicom.dcmread(jpeg_encoded_dicom_instance_test_path())


def jpeg_encoded_dicom_instance_json() -> MutableMapping[str, Any]:
  """Returns dicom json for jpeg encoded  WSI DICOM instance."""
  with jpeg_encoded_dicom_instance() as ds:
    file_meta = json.loads(ds.file_meta.to_json())
    dicom_meta = json.loads(ds.to_json())
    dicom_meta.update(file_meta)
    return dicom_meta


def jpeg_encoded_dicom_instance_metadata(
    changes: Optional[Mapping[str, Any]] = None
) -> _DicomInstanceMetadata:
  """Returns DICOM Proxy metadata for small WSI DICOM instance."""
  md = metadata_util.get_instance_metadata_from_local_instance(
      jpeg_encoded_dicom_instance_test_path()
  )
  if changes is None:
    return md
  return md.replace(changes)


def jpeg_encoded_pydicom_instance_cache(
    metadata: Optional[Mapping[str, Any]] = None
) -> _PyDicomSingleInstanceCache:
  """Returns pydicom instance cache for small WSI DICOM instance."""
  pydicom_instance = _PyDicomSingleInstanceCache(
      pydicom_single_instance_read_cache.PyDicomFilePath(
          jpeg_encoded_dicom_instance_test_path()
      )
  )
  if metadata is not None:
    pydicom_instance._metadata = pydicom_instance._metadata.replace(metadata)  # pylint: disable=protected-access
  return pydicom_instance


def jpeg_encoded_dicom_local_instance(
    metadata: Optional[Mapping[str, Any]] = None
) -> _LocalDicomInstance:
  return _LocalDicomInstance(
      jpeg_encoded_pydicom_instance_cache(
          metadata if metadata is not None else {}
      )
  )


def mock_multi_frame_test_metadata() -> _DicomInstanceMetadata:
  """Returns metadata which mocks multi-frame using very small instance.

  Imaging data does not match mock.
  """
  # Metadata describes a instance with single frame.
  # Modifying metadat to make metadata reflect a tiled image (65 x 55 pixels)
  # composed of frames which are 5 x 5 pixels.  This metadata enables test
  # cases to compute meaningfull DICOM frame cooordinate x pixel calculations.
  return jpeg_encoded_dicom_instance_metadata(
      dict(
          rows=5,
          columns=5,
          total_pixel_matrix_columns=65,
          total_pixel_matrix_rows=55,
      )
  )


def jpeg2000_dicom_cache() -> _PyDicomSingleInstanceCache:
  path = get_testdir_path('multi_frame_jpeg2000_camelyon_challenge_image.dcm')
  return _PyDicomSingleInstanceCache(
      pydicom_single_instance_read_cache.PyDicomFilePath(path)
  )


def jpeg2000_encoded_dicom_local_instance() -> _LocalDicomInstance:
  return _LocalDicomInstance(jpeg2000_dicom_cache())


def wsi_dicom_annotation_path() -> str:
  return get_testdir_path('wsi_annotation.dcm')


def wsi_dicom_annotation_instance() -> pydicom.FileDataset:
  return pydicom.dcmread(wsi_dicom_annotation_path())


def mock_annotation_dicom_json_path() -> str:
  return get_testdir_path('mock_annotation_dicom.json')


def mock_jpeg_dicom_json_path() -> str:
  return get_testdir_path('mock_jpeg_dicom.json')
