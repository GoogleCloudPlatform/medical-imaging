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
"""Tests for frame retrieval util."""
import dataclasses
import http.client
import io
import re
from typing import Dict, List, Optional
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
import cv2
import numpy as np
import PIL.Image
import redis
import requests_mock
import requests_toolbelt
from requests_toolbelt.multipart import decoder

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import frame_retrieval_util
from pathology.dicom_proxy import image_util
from pathology.dicom_proxy import metadata_util
from pathology.dicom_proxy import pydicom_single_instance_read_cache
from pathology.dicom_proxy import render_frame_params
from pathology.dicom_proxy import shared_test_util
from pathology.dicom_proxy import user_auth_util

# Types
_AuthSession = user_auth_util.AuthSession
_Compression = enum_types.Compression
_DicomStoreFrameTransaction = dicom_url_util.DicomStoreFrameTransaction
_PyDicomSingleInstanceCache = (
    pydicom_single_instance_read_cache.PyDicomSingleInstanceCache
)
_FrameImages = frame_retrieval_util.FrameImages

_TEST_URL = 'http://foo.com'
_TEST_FRAME_NUMBER = 5
_TEST_FRAME_REQUEST_URL = f'{_TEST_URL}/frames/{_TEST_FRAME_NUMBER}'
_TEST_RENDERED_FRAME_REQUEST_URL = f'{_TEST_FRAME_REQUEST_URL}/rendered'


def _mock_get_frame_data(
    data: bytes, downloaded_from_dicom_store: bool
) -> frame_retrieval_util.FrameData:
  return frame_retrieval_util.FrameData(data, downloaded_from_dicom_store)


def _get_render_frame_params(
    compression: _Compression,
    enable_caching: cache_enabled_type.CachingEnabled = cache_enabled_type.CachingEnabled(
        True
    ),
) -> render_frame_params.RenderFrameParams:
  return render_frame_params.RenderFrameParams(
      compression=compression, enable_caching=enable_caching
  )


def _redis_frame_cache_key(url: str, accept: str) -> str:
  return f'{url}:{accept}'


def _mock_transaction(
    rendered_image_type: str,
    enable_caching: cache_enabled_type.CachingEnabled = cache_enabled_type.CachingEnabled(
        True
    ),
    url: Optional[str] = None,
) -> _DicomStoreFrameTransaction:
  url = _TEST_URL if url is None else url
  if not url:
    frame_numbers = []
  else:
    match = re.fullmatch('.*/frames/([0-9, ]+).*', url)
    if match is None:
      raise ValueError(f'URL({url}) is missing frame numbers.')
    frame_numbers = [
        int(frame_number_str)
        for frame_number_str in match.groups()[0].replace(' ', '').split(',')
    ]
  return _DicomStoreFrameTransaction(
      url=_TEST_URL if url is None else url,
      headers={frame_retrieval_util._ACCEPT: rendered_image_type},
      frame_numbers=frame_numbers,
      enable_caching=enable_caching,
  )


def mock_multipart_data(content_type: List[bytes], content: List[bytes]) -> str:
  fields = []
  for index, _ in enumerate(content_type):
    fields.append((
        str(index),
        (
            None,
            content[index].decode('ascii'),
            content_type[index].decode('ascii'),
        ),
    ))
  return (
      requests_toolbelt.MultipartEncoder(
          fields=fields, boundary='message_boundary_0001'
      )
      .to_string()
      .decode('ascii')
  )


@dataclasses.dataclass
class _MockPartResponse:
  headers: Dict[bytes, bytes]
  content: bytes


@dataclasses.dataclass
class _EmptyMockMultiPartDataResponse:
  parts: List[_MockPartResponse]


def _mock_create_empty_multipart_response() -> _EmptyMockMultiPartDataResponse:
  return _EmptyMockMultiPartDataResponse([
      _MockPartResponse(
          {
              b'Content-Type': (
                  b'image/jpeg; transfer-syntax=1.2.840.10008.1.2.4.50'
              )
          },
          b'',
      )
  ])


class FrameRetrievalUtilTest(parameterized.TestCase):

  @parameterized.parameters([None, 'test'])
  def test_invalid_number_of_returned_frames_error(self, msg):
    exception_msg = str(
        frame_retrieval_util._InvalidNumberOfReturnedFramesError(msg)
    )
    if msg is None:
      self.assertEqual(
          exception_msg,
          'Number of frames returned != number of frames requested.',
      )
    else:
      self.assertEqual(exception_msg, msg)

  def test_frameimages_constructor(self):
    images = {}
    images[1] = b'1234'
    images[2] = b'5678'
    frameimages = _FrameImages(images, _Compression.JPEG, len(images))

    self.assertEqual(frameimages.images, images)
    self.assertEqual(frameimages.compression, _Compression.JPEG)
    self.assertLen(images, frameimages.number_of_frames_downloaded_from_store)
    self.assertEqual(
        set(dataclasses.asdict(frameimages)),
        {'images', 'compression', 'number_of_frames_downloaded_from_store'},
    )

  def test_create_frame_images_raises_frame_retrieval_error(self):
    compression = _Compression.JPEG
    frame_numbers = [1, 2, 3]
    frame_data = (len(frame_numbers) - 1) * [
        frame_retrieval_util.FrameData(bytearray(b'1234'), True)
    ]
    with self.assertRaises(
        frame_retrieval_util._InvalidNumberOfReturnedFramesError
    ):
      frame_retrieval_util._create_frame_images(
          frame_numbers, frame_data, compression
      )

  def test_create_frame_images_raises_undefined_compression(self):
    compression = None
    frame_numbers = [1, 2, 3]
    frame_data = (len(frame_numbers)) * [
        frame_retrieval_util.FrameData(bytearray(b'1234'), True)
    ]
    with self.assertRaises(ValueError):
      frame_retrieval_util._create_frame_images(
          frame_numbers, frame_data, compression
      )

  @parameterized.parameters([(True, 3), (False, 0)])
  def test_create_frame_images_success(
      self, downloaded_from_store: bool, expected: int
  ):
    compression = _Compression.JPEG
    frame_numbers = [1, 2, 3]
    test_bytes = b'1234'
    frame_data = len(frame_numbers) * [
        frame_retrieval_util.FrameData(
            bytearray(test_bytes), downloaded_from_store
        )
    ]
    expected_dict = {fnum: test_bytes for fnum in frame_numbers}

    result = frame_retrieval_util._create_frame_images(
        frame_numbers, frame_data, compression
    )
    self.assertEqual(result.images, expected_dict)
    self.assertEqual(result.compression, compression)
    self.assertEqual(result.number_of_frames_downloaded_from_store, expected)

  def test_frame_lru_cache_key(self):
    rendered_image_type = 'image/jpeg'
    expected_key = _redis_frame_cache_key(
        _TEST_FRAME_REQUEST_URL, rendered_image_type
    )
    transaction = _mock_transaction(
        rendered_image_type, url=_TEST_FRAME_REQUEST_URL
    )
    result = frame_retrieval_util.frame_lru_cache_key(transaction)
    self.assertEqual(result, expected_key)

  def test_get_local_frame_list(self):
    cache = shared_test_util.jpeg_encoded_pydicom_instance_cache()
    self.assertEqual(
        frame_retrieval_util.get_local_frame_list(cache, [0])[0],
        _mock_get_frame_data(cache.get_frame(0), False),
    )

  def test_get_local_frame_list_raises_if_index_out_of_bounds(self):
    cache = shared_test_util.jpeg_encoded_pydicom_instance_cache()
    with self.assertRaises(frame_retrieval_util._FrameIndexError):
      frame_retrieval_util.get_local_frame_list(cache, [99999])

  def test_get_dicom_frame_map_from_local_instance_frame_less_than_zero(self):
    cache = shared_test_util.jpeg_encoded_pydicom_instance_cache()
    invalid_frame_number = -1
    with self.assertRaises(frame_retrieval_util._FrameIndexError):
      frame_retrieval_util.get_dicom_frame_map(
          _AuthSession(None),
          cache,
          _get_render_frame_params(_Compression.JPEG),
          cache.metadata,
          [invalid_frame_number],
      )

  def test_get_dicom_frame_map_from_local_instance_frame_number_to_big(self):
    cache = shared_test_util.jpeg_encoded_pydicom_instance_cache()
    invalid_frame_number = cache.metadata.number_of_frames
    with self.assertRaises(frame_retrieval_util._FrameIndexError):
      frame_retrieval_util.get_dicom_frame_map(
          _AuthSession(None),
          cache,
          _get_render_frame_params(_Compression.JPEG),
          cache.metadata,
          [invalid_frame_number],
      )

  @parameterized.named_parameters([
      (
          'jpeg_encoded',
          _Compression.JPEG,
          'frame_',
      ),
      (
          'jpeg2000_encoded',
          _Compression.JPEG2000,
          'jpeg2000_frame_',
      ),
  ])
  def test_get_dicom_frame_map_from_local_instance_succeeds(
      self,
      expected_compression: _Compression,
      frame_prefix: str,
  ):
    cache = (
        shared_test_util.jpeg2000_dicom_cache()
        if expected_compression == _Compression.JPEG2000
        else shared_test_util.jpeg_encoded_pydicom_instance_cache()
    )
    frame_list = list(range(cache.metadata.number_of_frames))
    requested_compression_format = _Compression.JPEG
    frame_data = frame_retrieval_util.get_dicom_frame_map(
        _AuthSession(None),
        cache,
        _get_render_frame_params(requested_compression_format),
        cache.metadata,
        frame_list,
    )

    self.assertEqual(frame_data.compression, expected_compression)
    self.assertLen(frame_data.images, len(frame_list))
    for idx, img in frame_data.images.items():
      if frame_data.compression != _Compression.JPEG:
        # If image not returned as JPEG convert to JPEG
        # this conversion is done to make images visible in cider and critque.
        with PIL.Image.open(io.BytesIO(img)) as img:
          # Read image using PIL and convert to OpenCV BGR Colorspace.
          img = cv2.cvtColor(np.asarray(img), cv2.COLOR_RGB2BGR)
        img = image_util.encode_image(img, _Compression.JPEG, 95, None)
      with open(
          shared_test_util.get_testdir_path(
              'frame_retrieval_util', f'{frame_prefix}{idx}.jpeg'
          ),
          'rb',
      ) as infile:
        self.assertTrue(
            shared_test_util.rgb_image_almost_equal(img, infile.read())
        )

  @mock.patch.object(redis.Redis, 'set', autospec=True)
  @mock.patch.object(redis.Redis, 'get', autospec=True, return_value=None)
  def test_get_dicom_frame_map_jpeg_from_server_single_instance_succeeds(
      self, unused_mock_get, unused_mock_set
  ):
    cache = shared_test_util.jpeg_encoded_pydicom_instance_cache()
    requested_compression_format = _Compression.JPEG
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          _TEST_FRAME_REQUEST_URL,
          status_code=http.client.OK,
          text=mock_multipart_data(
              [b'image/jpeg; transfer-syntax=1.2.840.10008.1.2.4.50'],
              [b'test_data'],
          ),
          headers={
              'content-type': 'multipart/related;boundary=message_boundary_0001'
          },
      )
      frame_images = frame_retrieval_util.get_dicom_frame_map(
          _AuthSession(None),
          dicom_url_util.DicomSopInstanceUrl(_TEST_URL),
          _get_render_frame_params(requested_compression_format),
          cache.metadata,
          [_TEST_FRAME_NUMBER - 1],
      )
    self.assertEqual(
        frame_images,
        frame_retrieval_util.FrameImages(
            {(_TEST_FRAME_NUMBER - 1): b'test_data'}, _Compression.JPEG, 1
        ),
    )

  @mock.patch.object(redis.Redis, 'set', autospec=True)
  @mock.patch.object(redis.Redis, 'get', autospec=True, return_value=None)
  def test_get_dicom_frame_map_jpeg_from_server_multiple_frames_succeeds(
      self, unused_mock_get, unused_mock_set
  ):
    cache = shared_test_util.jpeg_encoded_pydicom_instance_cache()
    requested_compression_format = _Compression.JPEG
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          f'{_TEST_URL}/frames/1,2',
          status_code=http.client.OK,
          text=mock_multipart_data(
              [
                  b'image/jpeg; transfer-syntax=1.2.840.10008.1.2.4.50',
                  b'image/jpeg; transfer-syntax=1.2.840.10008.1.2.4.50',
              ],
              [b'test_frame_1', b'test_frame_2'],
          ),
          headers={
              'content-type': 'multipart/related;boundary=message_boundary_0001'
          },
      )
      frame_images = frame_retrieval_util.get_dicom_frame_map(
          _AuthSession(None),
          dicom_url_util.DicomSopInstanceUrl(_TEST_URL),
          _get_render_frame_params(requested_compression_format),
          cache.metadata,
          [0, 1],
      )
    self.assertEqual(
        frame_images,
        frame_retrieval_util.FrameImages(
            {0: b'test_frame_1', 1: b'test_frame_2'}, _Compression.JPEG, 2
        ),
    )

  @parameterized.parameters(
      (_Compression.JPEG, 'image/jpeg'), (_Compression.PNG, 'image/png')
  )
  @mock.patch.object(redis.Redis, 'set', autospec=True)
  @mock.patch.object(redis.Redis, 'get', autospec=True, return_value=None)
  def test_get_dicom_frame_map_rendered_instance_succeeds(
      self,
      requested_compression_format,
      content_type,
      unused_mock_get,
      unused_mock_set,
  ):
    cache = shared_test_util.jpeg_encoded_pydicom_instance_cache()
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          _TEST_FRAME_REQUEST_URL,
          status_code=http.client.BAD_REQUEST,
          text='fail',
          headers={
              'content-type': 'multipart/related;boundary=message_boundary_0001'
          },
      )
      mock_request.register_uri(
          'GET',
          _TEST_RENDERED_FRAME_REQUEST_URL,
          status_code=http.client.OK,
          text='test_frame_1',
          headers={'content-type': content_type},
      )
      frame_images = frame_retrieval_util.get_dicom_frame_map(
          _AuthSession(None),
          dicom_url_util.DicomSopInstanceUrl(_TEST_URL),
          _get_render_frame_params(requested_compression_format),
          cache.metadata,
          [_TEST_FRAME_NUMBER - 1],
      )
    self.assertEqual(
        frame_images,
        frame_retrieval_util.FrameImages(
            {
                (_TEST_FRAME_NUMBER - 1): b'test_frame_1',
            },
            requested_compression_format,
            1,
        ),
    )

  @mock.patch('redis.Redis', autospec=True)
  def test_get_dicom_frame_map_rendered_instance_succeeds_multi_response(
      self, mock_redis
  ):
    cache = shared_test_util.jpeg_encoded_pydicom_instance_cache()
    redis_mock = shared_test_util.RedisMock()
    mock_redis.return_value = redis_mock
    data_1_frame_number = 2
    data_2_frame_number = 3
    data_1 = b'123'
    data_2 = b'456'
    redis_mock.set(
        _redis_frame_cache_key(
            f'{_TEST_URL}/frames/{data_1_frame_number}',
            'multipart/related; type="application/octet-stream";'
            ' transfer-syntax=*',
        ),
        data_1,
        False,
        None,
    )
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          f'{_TEST_URL}/frames/{data_2_frame_number}',
          status_code=http.client.OK,
          text=mock_multipart_data(
              [
                  frame_retrieval_util._BASELINE_JPEG_MIME_TYPE_AND_TRANSFER_SYNTAX
              ],
              [data_2],
          ),
          headers={
              'content-type': 'multipart/related;boundary=message_boundary_0001'
          },
      )
      frame_images = frame_retrieval_util.get_dicom_frame_map(
          _AuthSession(None),
          dicom_url_util.DicomSopInstanceUrl(_TEST_URL),
          _get_render_frame_params(_Compression.PNG),
          cache.metadata,
          [data_1_frame_number - 1, data_2_frame_number - 1],
      )
    self.assertEqual(
        frame_images,
        frame_retrieval_util.FrameImages(
            {
                (data_1_frame_number - 1): data_1,
                (data_2_frame_number - 1): data_2,
            },
            _Compression.JPEG,
            1,
        ),
    )
    for frame_number, expected_data in zip(
        [data_1_frame_number, data_2_frame_number], [data_1, data_2]
    ):
      self.assertEqual(
          redis_mock.get(
              _redis_frame_cache_key(
                  f'{_TEST_URL}/frames/{frame_number}',
                  'multipart/related; type="application/octet-stream";'
                  ' transfer-syntax=*',
              )
          ),
          expected_data,
      )

  @mock.patch.object(redis.Redis, 'set', autospec=True)
  @mock.patch.object(redis.Redis, 'get', autospec=True, return_value=None)
  def test_get_dicom_frame_map_png_from_server_single_instance_succeeds(
      self, unused_mock_get, unused_mock_set
  ):
    cache = shared_test_util.jpeg_encoded_pydicom_instance_cache()
    requested_compression_format = _Compression.PNG
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          _TEST_FRAME_REQUEST_URL,
          status_code=http.client.OK,
          text=mock_multipart_data(
              [
                  frame_retrieval_util._BASELINE_JPEG_MIME_TYPE_AND_TRANSFER_SYNTAX
              ],
              [b'test_data'],
          ),
          headers={
              'content-type': 'multipart/related;boundary=message_boundary_0001'
          },
      )
      frame_images = frame_retrieval_util.get_dicom_frame_map(
          _AuthSession(None),
          dicom_url_util.DicomSopInstanceUrl(_TEST_URL),
          _get_render_frame_params(requested_compression_format),
          cache.metadata,
          [_TEST_FRAME_NUMBER - 1],
      )
    self.assertEqual(
        frame_images,
        frame_retrieval_util.FrameImages(
            {(_TEST_FRAME_NUMBER - 1): b'test_data'}, _Compression.JPEG, 1
        ),
    )

  def test_get_rendered_frame_list_no_frames_returns_empty(self):
    self.assertEmpty(frame_retrieval_util._get_rendered_frame_list([]))

  @mock.patch.object(
      frame_retrieval_util,
      '_get_rendered_frame_list',
      autospec=True,
      return_value=[1, 2, 3, 4],
  )
  def test_get_dicom_frame_map_invalid_number_of_frames_returned_from_server(
      self, unused__get_rendered_frame_list_mock
  ):
    cache = shared_test_util.jpeg_encoded_pydicom_instance_cache(
        {'dicom_transfer_syntax': metadata_util._IMPLICIT_VR_ENDIAN}
    )
    # Set metadata to raw format to force frame rendered request.
    requested_compression_format = _Compression.PNG
    with self.assertRaisesRegex(
        frame_retrieval_util._InvalidNumberOfReturnedFramesError,
        (
            r'^Number of images retrieved != number of images requested;'
            f' Source: {_TEST_URL}'
        ),
    ):
      frame_retrieval_util.get_dicom_frame_map(
          _AuthSession(None),
          dicom_url_util.DicomSopInstanceUrl(_TEST_URL),
          _get_render_frame_params(requested_compression_format),
          cache.metadata,
          [0],
      )

  @mock.patch.object(
      frame_retrieval_util,
      'get_local_frame_list',
      autospec=True,
      return_value=[1, 2, 3, 4],
  )
  def test_get_dicom_frame_map_invalid_number_of_frames_returned_from_local(
      self, unused_get_frame_list_mock
  ):
    cache = shared_test_util.jpeg_encoded_pydicom_instance_cache()
    requested_compression_format = _Compression.PNG
    with self.assertRaisesRegex(
        frame_retrieval_util._InvalidNumberOfReturnedFramesError,
        (
            r'^Number of images retrieved != number of images requested;'
            r' Source: .+/dicom_proxy/testdata/'
            r'multi_frame_jpeg_camelyon_challenge_image\.dcm'
        ),
    ):
      frame_retrieval_util.get_dicom_frame_map(
          _AuthSession(None),
          cache,
          _get_render_frame_params(requested_compression_format),
          cache.metadata,
          [0],
      )

  @parameterized.parameters([
      cache_enabled_type.CachingEnabled(True),
      cache_enabled_type.CachingEnabled(False),
  ])
  def test_get_raw_frame_raises_if_url_empty(self, enable_caching):
    with self.assertRaises(frame_retrieval_util._EmptyFrameURLError):
      frame_retrieval_util._get_raw_frame_list(
          _AuthSession(None),
          instance_url=dicom_url_util.DicomSopInstanceUrl(''),
          frame_numbers=[],
          render_params=_get_render_frame_params(
              _Compression.JPEG, enable_caching=enable_caching
          ),
          session_handler=None,
      )

  def test_get_raw_frame_no_frames_returns_empty(self):
    self.assertEmpty(
        frame_retrieval_util._get_raw_frame_list(
            _AuthSession(None),
            instance_url=dicom_url_util.DicomSopInstanceUrl(_TEST_URL),
            frame_numbers=[],
            render_params=_get_render_frame_params(_Compression.JPEG),
            session_handler=None,
        )
    )

  @mock.patch('redis.Redis', autospec=True)
  def test_get_raw_frame_cached_result(self, redis_mock):
    test_value = b'bar'
    expected_value = b'bar'
    test_url = f'{_TEST_URL}'
    redis_mock.return_value = {
        _redis_frame_cache_key(
            _TEST_FRAME_REQUEST_URL,
            dicom_url_util._UNTRANSCODED_FRAME_ACCEPT_VALUE,
        ): test_value
    }
    self.assertEqual(
        frame_retrieval_util._get_raw_frame_list(
            _AuthSession(None),
            instance_url=dicom_url_util.DicomSopInstanceUrl(test_url),
            frame_numbers=[_TEST_FRAME_NUMBER],
            render_params=_get_render_frame_params(_Compression.JPEG),
            session_handler=None,
        )[0],
        _mock_get_frame_data(expected_value, False),
    )

  @parameterized.parameters([
      frame_retrieval_util._BASELINE_JPEG_MIME_TYPE_AND_TRANSFER_SYNTAX,
      frame_retrieval_util._JPEG2000_LOSSLESS_MIME_TYPE_AND_TRANSFER_SYNTAX,
      frame_retrieval_util._JPEG2000_LOSSY_MIME_TYPE_AND_TRANSFER_SYNTAX,
  ])
  @mock.patch.object(redis.Redis, 'set')
  @mock.patch.object(redis.Redis, 'get', autospec=True, return_value=None)
  def test_get_raw_frame_list_from_dicom_store_succeeds(
      self, content_type, unused_mock_redis_get, mock_redis_set
  ):
    data = b'123423232323'
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          _TEST_FRAME_REQUEST_URL,
          text=mock_multipart_data([content_type], [data]),
          headers={
              'content-type': 'multipart/related;boundary=message_boundary_0001'
          },
      )
      self.assertEqual(
          frame_retrieval_util._get_raw_frame_list(
              _AuthSession(None),
              instance_url=dicom_url_util.DicomSopInstanceUrl(_TEST_URL),
              frame_numbers=[_TEST_FRAME_NUMBER],
              render_params=_get_render_frame_params(_Compression.JPEG),
              session_handler=None,
          )[0],
          _mock_get_frame_data(data, True),
      )
    mock_redis_set.assert_called_with(
        _redis_frame_cache_key(
            _TEST_FRAME_REQUEST_URL,
            dicom_url_util._UNTRANSCODED_FRAME_ACCEPT_VALUE,
        ),
        data,
        nx=False,
        ex=3600,
    )

  @parameterized.parameters([
      frame_retrieval_util._BASELINE_JPEG_MIME_TYPE_AND_TRANSFER_SYNTAX,
      frame_retrieval_util._JPEG2000_LOSSLESS_MIME_TYPE_AND_TRANSFER_SYNTAX,
      frame_retrieval_util._JPEG2000_LOSSY_MIME_TYPE_AND_TRANSFER_SYNTAX,
  ])
  @mock.patch.object(redis.Redis, 'set')
  @mock.patch.object(redis.Redis, 'get', autospec=True, return_value=None)
  def test_get_raw_frame_data_from_dicom_store_succeeds(
      self, content_type, unused_mock_redis_get, mock_redis_set
  ):
    data = b'123423232323'
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          _TEST_FRAME_REQUEST_URL,
          text=mock_multipart_data([content_type], [data]),
          headers={
              'content-type': 'multipart/related;boundary=message_boundary_0001'
          },
      )
      self.assertEqual(
          frame_retrieval_util.get_raw_frame_data(
              _AuthSession(None),
              instance_url=dicom_url_util.DicomSopInstanceUrl(_TEST_URL),
              frame_numbers=[_TEST_FRAME_NUMBER],
              render_params=_get_render_frame_params(_Compression.JPEG),
          )[0],
          _mock_get_frame_data(data, True),
      )
    mock_redis_set.assert_called_with(
        _redis_frame_cache_key(
            _TEST_FRAME_REQUEST_URL,
            dicom_url_util._UNTRANSCODED_FRAME_ACCEPT_VALUE,
        ),
        data,
        nx=False,
        ex=3600,
    )

  @mock.patch('redis.Redis', autospec=True)
  def test_get_raw_frame_from_dicom_store_multi_part_response_succeeds(
      self, mock_redis
  ):
    data_1 = b'1234'
    data_2 = b'5678'
    data_3 = b'9ABC'
    data_4 = b'DEFG'
    redis_mock = shared_test_util.RedisMock()
    mock_redis.return_value = redis_mock
    redis_mock.set(
        _redis_frame_cache_key(
            f'{_TEST_URL}/frames/2',
            dicom_url_util._UNTRANSCODED_FRAME_ACCEPT_VALUE,
        ),
        data_2,
        nx=True,
        ex=3600,
    )
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          f'{_TEST_URL}/frames/1,5,6',
          text=mock_multipart_data(
              [
                  frame_retrieval_util._BASELINE_JPEG_MIME_TYPE_AND_TRANSFER_SYNTAX,
                  frame_retrieval_util._BASELINE_JPEG_MIME_TYPE_AND_TRANSFER_SYNTAX,
                  frame_retrieval_util._BASELINE_JPEG_MIME_TYPE_AND_TRANSFER_SYNTAX,
              ],
              [data_1, data_3, data_4],
          ),
          headers={
              'content-type': 'multipart/related;boundary=message_boundary_0001'
          },
      )
      self.assertEqual(
          frame_retrieval_util._get_raw_frame_list(
              _AuthSession(None),
              instance_url=dicom_url_util.DicomSopInstanceUrl(_TEST_URL),
              frame_numbers=[1, 2, 5, 6],
              render_params=_get_render_frame_params(_Compression.JPEG),
              session_handler=None,
          ),
          [
              _mock_get_frame_data(data_1, True),
              _mock_get_frame_data(data_2, False),
              _mock_get_frame_data(data_3, True),
              _mock_get_frame_data(data_4, True),
          ],
      )
    for frame_number, expected_data in zip(
        [1, 2, 5, 6], [data_1, data_2, data_3, data_4]
    ):
      self.assertEqual(
          redis_mock.get(
              _redis_frame_cache_key(
                  f'{_TEST_URL}/frames/{frame_number}',
                  dicom_url_util._UNTRANSCODED_FRAME_ACCEPT_VALUE,
              )
          ),
          expected_data,
      )

  @mock.patch.object(redis.Redis, 'set')
  @mock.patch.object(redis.Redis, 'get', autospec=True, return_value=None)
  def test_get_raw_frame_from_dicom_store_fails_cannot_parse_multipart_response(
      self, unused_mock_redis_get, mock_redis_set
  ):
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          _TEST_FRAME_REQUEST_URL,
          text=mock_multipart_data([b'1234.2323'], [b'1234']),
          headers={
              'content-type': 'multipart/related;boundary=message_boundary_0001'
          },
      )
      with self.assertRaisesRegex(
          frame_retrieval_util.DicomFrameRequestError,
          (
              'DicomFrameRequestError Frame request returned invalid content'
              " type. Received: b'1234.2323'; status:200;"
              " headers:{'content-type':"
              " 'multipart/related;boundary=message_boundary_0001'}"
          ),
      ):
        frame_retrieval_util._get_raw_frame_list(
            _AuthSession(None),
            instance_url=dicom_url_util.DicomSopInstanceUrl(_TEST_URL),
            frame_numbers=[_TEST_FRAME_NUMBER],
            render_params=_get_render_frame_params(_Compression.JPEG),
            session_handler=None,
        )
    mock_redis_set.assert_not_called()

  @mock.patch.object(redis.Redis, 'set')
  @mock.patch.object(redis.Redis, 'get', autospec=True, return_value=None)
  def test_get_raw_frame_fails_server_error(
      self, unused_mock_redis_get, mock_redis_set
  ):
    data = b'123423232323'
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          _TEST_FRAME_REQUEST_URL,
          text=mock_multipart_data(
              [
                  frame_retrieval_util._BASELINE_JPEG_MIME_TYPE_AND_TRANSFER_SYNTAX
              ],
              [data],
          ),
          headers={
              'content-type': 'multipart/related;boundary=message_boundary_0001'
          },
          status_code=http.client.NOT_FOUND,
      )
      with self.assertRaisesRegex(
          frame_retrieval_util.DicomFrameRequestError,
          'DicomFrameRequestError; status:404',
      ):
        frame_retrieval_util._get_raw_frame_list(
            _AuthSession(None),
            instance_url=dicom_url_util.DicomSopInstanceUrl(_TEST_URL),
            frame_numbers=[_TEST_FRAME_NUMBER],
            render_params=_get_render_frame_params(_Compression.JPEG),
            session_handler=None,
        )
    mock_redis_set.assert_not_called()

  @mock.patch.object(redis.Redis, 'set')
  @mock.patch.object(redis.Redis, 'get', autospec=True, return_value=None)
  def test_get_raw_frame_fails_no_returned_data(
      self, unused_mock_redis_get, mock_redis_set
  ):
    data = b'123423232323'
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          _TEST_FRAME_REQUEST_URL,
          text=mock_multipart_data(
              [
                  frame_retrieval_util._BASELINE_JPEG_MIME_TYPE_AND_TRANSFER_SYNTAX
              ],
              [data],
          ),
          headers={
              'content-type': 'multipart/related;boundary=message_boundary_0001'
          },
      )
      with mock.patch.object(
          decoder.MultipartDecoder,
          'from_response',
          autospec=True,
          return_value=_mock_create_empty_multipart_response(),
      ):
        with self.assertRaisesRegex(
            frame_retrieval_util.DicomFrameRequestError,
            'DicomFrameRequestError Frame request returned no data.',
        ):
          frame_retrieval_util._get_raw_frame_list(
              _AuthSession(None),
              instance_url=dicom_url_util.DicomSopInstanceUrl(_TEST_URL),
              frame_numbers=[_TEST_FRAME_NUMBER],
              render_params=_get_render_frame_params(_Compression.JPEG),
              session_handler=None,
          )
    mock_redis_set.assert_not_called()

  @mock.patch.object(redis.Redis, 'set')
  @mock.patch.object(redis.Redis, 'get', autospec=True, return_value=None)
  def test_get_raw_frame_fails_invalid_parsed_response(
      self, unused_mock_redis_get, mock_redis_set
  ):
    data = b'123423232323'
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          _TEST_FRAME_REQUEST_URL,
          text=mock_multipart_data(
              [
                  frame_retrieval_util._BASELINE_JPEG_MIME_TYPE_AND_TRANSFER_SYNTAX
              ],
              [data],
          ),
          headers={
              'content-type': 'multipart/related;boundary=message_boundary_0001'
          },
      )
      with mock.patch.object(
          decoder.MultipartDecoder,
          'from_response',
          autospec=True,
          return_value=None,
      ):
        with self.assertRaisesRegex(
            frame_retrieval_util.DicomFrameRequestError,
            'DicomFrameRequestError Incorrectly formatted multipart response.',
        ):
          frame_retrieval_util._get_raw_frame_list(
              _AuthSession(None),
              instance_url=dicom_url_util.DicomSopInstanceUrl(_TEST_URL),
              frame_numbers=[_TEST_FRAME_NUMBER],
              render_params=_get_render_frame_params(_Compression.JPEG),
              session_handler=None,
          )
    mock_redis_set.assert_not_called()

  @mock.patch.object(redis.Redis, 'set')
  @mock.patch.object(redis.Redis, 'get', autospec=True, return_value=None)
  def test_get_raw_frame_from_dicom_store_fails_to_many_multipart_response(
      self, unused_mock_redis_get, mock_redis_set
  ):
    parts_returned = 2
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          _TEST_FRAME_REQUEST_URL,
          text=mock_multipart_data(
              [
                  frame_retrieval_util._BASELINE_JPEG_MIME_TYPE_AND_TRANSFER_SYNTAX,
              ]
              * parts_returned,
              [b'1234'] * parts_returned,
          ),
          headers={
              'content-type': 'multipart/related;boundary=message_boundary_0001'
          },
      )
      with self.assertRaisesRegex(
          frame_retrieval_util.DicomFrameRequestError,
          (
              'DicomFrameRequestError Expected 1 multipart response actually'
              ' received 2;'
          ),
      ):
        frame_retrieval_util._get_raw_frame_list(
            _AuthSession(None),
            instance_url=dicom_url_util.DicomSopInstanceUrl(_TEST_URL),
            frame_numbers=[_TEST_FRAME_NUMBER],
            render_params=_get_render_frame_params(_Compression.JPEG),
            session_handler=None,
        )
    mock_redis_set.assert_not_called()

  def test_multipart_frame_request_invalid_response_throws(self):
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          _TEST_FRAME_REQUEST_URL,
          text='test',
          headers={'content-type': 'plain/text'},
      )
      with self.assertRaisesRegex(
          frame_retrieval_util.DicomFrameRequestError,
          'DicomFrameRequestError NonMultipartContentTypeException',
      ):
        frame_retrieval_util._get_raw_frame_list(
            _AuthSession(None),
            instance_url=dicom_url_util.DicomSopInstanceUrl(_TEST_URL),
            frame_numbers=[_TEST_FRAME_NUMBER],
            render_params=_get_render_frame_params(_Compression.JPEG),
            session_handler=None,
        )

  @mock.patch.object(redis.Redis, 'set')
  @mock.patch.object(redis.Redis, 'get')
  def test_get_rendered_frame_callable_empty_url(
      self, redis_mock_get, redis_mock_set
  ):
    transaction = _mock_transaction(
        'test', enable_caching=cache_enabled_type.CachingEnabled(True), url=''
    )
    with self.assertRaises(frame_retrieval_util._EmptyFrameURLError):
      frame_retrieval_util._get_rendered_frame(transaction)
    redis_mock_get.assert_not_called()
    redis_mock_set.assert_not_called()

  @mock.patch.object(redis.Redis, 'set')
  @mock.patch.object(redis.Redis, 'get')
  def test_get_rendered_frame_cached_result(
      self, redis_mock_get, redis_mock_set
  ):
    accept_str = 'test'
    test_value = b'bar'
    expected_value = b'bar'
    transaction = _mock_transaction(
        accept_str,
        enable_caching=cache_enabled_type.CachingEnabled(True),
        url=_TEST_FRAME_REQUEST_URL,
    )
    redis_mock_get.return_value = test_value
    result = frame_retrieval_util._get_rendered_frame(transaction)
    self.assertEqual(
        result, frame_retrieval_util.FrameData(expected_value, False)
    )
    redis_mock_get.assert_called_once_with(
        _redis_frame_cache_key(_TEST_FRAME_REQUEST_URL, accept_str)
    )
    redis_mock_set.assert_not_called()

  @mock.patch.object(redis.Redis, 'set')
  @mock.patch.object(redis.Redis, 'get', return_value=None)
  def test_get_rendered_frame_bad_response(
      self, redis_mock_get, redis_mock_set
  ):
    accept_str = 'test'
    transaction = _mock_transaction(
        accept_str,
        enable_caching=cache_enabled_type.CachingEnabled(True),
        url=_TEST_FRAME_REQUEST_URL,
    )
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          _TEST_FRAME_REQUEST_URL,
          status_code=http.client.NOT_FOUND,
          text='foo',
          headers={},
      )
      with self.assertRaisesRegex(
          frame_retrieval_util.DicomFrameRequestError,
          'DicomFrameRequestError; status:404;',
      ):
        frame_retrieval_util._get_rendered_frame(transaction)
    redis_mock_get.assert_called_once_with(
        _redis_frame_cache_key(_TEST_FRAME_REQUEST_URL, accept_str)
    )
    redis_mock_set.assert_not_called()

  @mock.patch.object(redis.Redis, 'set')
  @mock.patch.object(redis.Redis, 'get', return_value=None)
  def test_get_rendered_frame_bad_content_type(
      self, redis_mock_get, redis_mock_set
  ):
    accept_str = 'image/png'
    transaction = _mock_transaction(
        accept_str,
        enable_caching=cache_enabled_type.CachingEnabled(True),
        url=_TEST_FRAME_REQUEST_URL,
    )
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          _TEST_FRAME_REQUEST_URL,
          status_code=http.client.OK,
          text='foo',
          headers={'content-type': 'image/jpeg'},
      )
      with self.assertRaisesRegex(
          frame_retrieval_util.DicomFrameRequestError,
          (
              'DicomFrameRequestError Invalid content type. Expected:'
              ' image/png; Received: image/jpeg;'
          ),
      ):
        frame_retrieval_util._get_rendered_frame(transaction)

    redis_mock_get.assert_called_once_with(
        _redis_frame_cache_key(_TEST_FRAME_REQUEST_URL, accept_str)
    )
    redis_mock_set.assert_not_called()

  @mock.patch.object(redis.Redis, 'set')
  @mock.patch.object(redis.Redis, 'get', return_value=None)
  def test_get_rendered_frame_empty(self, redis_mock_get, redis_mock_set):
    accept_str = 'image/png'
    transaction = _mock_transaction(
        accept_str,
        enable_caching=cache_enabled_type.CachingEnabled(True),
        url=_TEST_FRAME_REQUEST_URL,
    )
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          _TEST_FRAME_REQUEST_URL,
          status_code=http.client.OK,
          text='',
          headers={'content-type': 'image/png'},
      )
      with self.assertRaisesRegex(
          frame_retrieval_util.DicomFrameRequestError,
          'DicomFrameRequestError Frame request returned no data',
      ):
        frame_retrieval_util._get_rendered_frame(transaction)
    redis_mock_get.assert_called_once_with(
        _redis_frame_cache_key(_TEST_FRAME_REQUEST_URL, accept_str)
    )
    redis_mock_set.assert_not_called()

  @mock.patch.object(redis.Redis, 'set')
  @mock.patch.object(redis.Redis, 'get', return_value=None)
  def test_get_rendered_frame_happy_path(self, redis_mock_get, redis_mock_set):
    accept_str = 'image/png'
    transaction = _mock_transaction(
        accept_str,
        enable_caching=cache_enabled_type.CachingEnabled(True),
        url=_TEST_FRAME_REQUEST_URL,
    )
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          _TEST_FRAME_REQUEST_URL,
          status_code=http.client.OK,
          text='test_data',
          headers={'content-type': 'image/png'},
      )
      result = frame_retrieval_util._get_rendered_frame(transaction)
    self.assertEqual(result, frame_retrieval_util.FrameData(b'test_data', True))
    cache_key = _redis_frame_cache_key(_TEST_FRAME_REQUEST_URL, accept_str)
    redis_mock_get.assert_called_once_with(cache_key)
    redis_mock_set.assert_called_once_with(
        cache_key, b'test_data', nx=False, ex=3600
    )


if __name__ == '__main__':
  absltest.main()
