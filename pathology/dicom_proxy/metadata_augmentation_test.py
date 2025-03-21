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
"""Tests for dicom metadata augmentation."""

from concurrent import futures
import http
import json
from typing import Iterator, Mapping
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import flask

from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import flask_util
from pathology.dicom_proxy import metadata_augmentation
from pathology.dicom_proxy import metadata_util
from pathology.dicom_proxy import shared_test_util
from pathology.shared_libs.iap_auth_lib import auth

# Types
_EMPTY_STUDY = dicom_url_util.StudyInstanceUID('')
_EMPTY_SERIES = dicom_url_util.SeriesInstanceUID('')
_EMPTY_SOPINSTANCE = dicom_url_util.SOPInstanceUID('')
_MOCK_DICOMWEBBASE_URL = dicom_url_util.DicomWebBaseURL(
    'v1', 'proj', 'loc', 'dset', 'dstore'
)


class _MockFlaskRequest:

  def __init__(self, url: str):
    self._url = url

  @property
  def url(self) -> str:
    return self._url

  @property
  def url_root(self) -> str:
    path = self.url
    prefix = ''
    for strip_prefix in ('http://', 'https://'):
      if path.startswith(strip_prefix):
        path = path[len(strip_prefix) :]
        prefix = strip_prefix
        break
    if '/' in path:
      path = path[: path.index('/')]
    return f'{prefix}{path}'

  @property
  def base_url(self) -> str:
    if '?' in self.url:
      return self.url[: self.url.index('?')]
    return self.url

  @property
  def path(self) -> str:
    return self.base_url[len(self.url_root) :]

  @property
  def args(self) -> Mapping[str, str]:
    if '?' not in self.url:
      return {}
    params = self.url[self.url.index('?') + 1 :]
    if not params:
      return {}
    result = {}
    for param in params.split('&'):
      parts = param.split('=')
      if len(parts) == 2:
        key, value = parts
        result[key] = value
    return result


def _mock_flask_stream_context(msg: Iterator[str]) -> str:
  return ''.join(msg)


class MetadataAugmenationTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.saved_flag_values = flagsaver.save_flag_values()
    # Mock IAP
    self._mock_iap_headers = self.enter_context(
        mock.patch.object(
            auth, '_get_flask_headers', autospec=True, return_value={}
        )
    )
    self._mock_valid_jwt = self.enter_context(
        mock.patch.object(auth, '_is_valid', autospec=True, return_value=True)
    )
    self.enter_context(
        mock.patch.object(
            flask, 'stream_with_context', side_effect=_mock_flask_stream_context
        )
    )

  def tearDown(self):
    flagsaver.restore_flag_values(self.saved_flag_values)
    super().tearDown()

  def test_stream_metadata_response_empty_list(self):
    self.assertEqual(
        list(metadata_augmentation._stream_metadata_response([])), ['[]']
    )

  def test_stream_metadata_response_skips_empty_results(self):
    self.assertEqual(
        list(
            metadata_augmentation._stream_metadata_response(['abc', '', '123'])
        ),
        ['[abc,123]'],
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='chunksize_1',
          chunksize=1,
          expected=list('[abc,123]'),
      ),
      dict(
          testcase_name='chunksize_2',
          chunksize=2,
          expected=['[a', 'bc', ',1', '23', ']'],
      ),
      dict(
          testcase_name='chunksize_3',
          chunksize=3,
          expected=['[ab', 'c,1', '23]'],
      ),
      dict(
          testcase_name='chunksize_7',
          chunksize=7,
          expected=['[abc,12', '3]'],
      ),
      dict(
          testcase_name='chunksize_8',
          chunksize=8,
          expected=['[abc,123', ']'],
      ),
      dict(
          testcase_name='chunksize_9',
          chunksize=9,
          expected=['[abc,123]'],
      ),
      dict(
          testcase_name='chunksize_100',
          chunksize=100,
          expected=['[abc,123]'],
      ),
  ])
  def test_stream_metadata_response(self, chunksize, expected):
    with flagsaver.flagsaver(streaming_chunksize=chunksize):
      self.assertEqual(
          list(metadata_augmentation._stream_metadata_response(['abc', '123'])),
          expected,
      )

  @flagsaver.flagsaver(streaming_chunksize=1)
  def test_stream_metadata_response_from_futures(self):
    with futures.ThreadPoolExecutor(max_workers=10) as pool:
      future_list = [pool.submit(lambda x: x, txt) for txt in ('abc', '123')]
      self.assertEqual(
          list(metadata_augmentation._stream_metadata_response(future_list)),
          list('[abc,123]'),
      )

  @mock.patch.object(
      flask_util,
      'get_first_key_args',
      autosepc=True,
      return_value={},
  )
  def test_augment_instance_metadata_bad_response(self, unused_mocks):
    resp = flask.Response('bad', status=400)
    result = metadata_augmentation.augment_instance_metadata(
        _MOCK_DICOMWEBBASE_URL,
        _EMPTY_STUDY,
        _EMPTY_SERIES,
        _EMPTY_SOPINSTANCE,
        resp,
    )
    self.assertEqual(result.status_code, http.HTTPStatus.BAD_REQUEST)
    self.assertEqual(result.data, b'bad')

  @mock.patch.object(
      flask_util, 'get_includefields', autospec=True, return_value=set()
  )
  @mock.patch.object(
      flask_util,
      'get_first_key_args',
      autosepc=True,
      return_value={'accept': 'abc', 'downsample': '2.0'},
  )
  @mock.patch.object(
      flask_util,
      'get_request',
      autosepc=True,
      return_value=_MockFlaskRequest('instances'),
  )
  def test_augment_instance_metadata_bad_parameters(self, *unused_mocks):
    content_type = 'text/plain'
    expected_msg = 'test'
    resp = flask.Response(
        expected_msg, status=http.HTTPStatus.OK, content_type=content_type
    )
    result = metadata_augmentation.augment_instance_metadata(
        _MOCK_DICOMWEBBASE_URL,
        _EMPTY_STUDY,
        _EMPTY_SERIES,
        _EMPTY_SOPINSTANCE,
        resp,
    )

    self.assertEqual(result.status, shared_test_util.http_ok_status())
    self.assertEqual(result.data, expected_msg.encode('utf-8'))

  @parameterized.parameters(['text/plain', 'application/dicom+json'])
  @mock.patch.object(
      flask_util,
      'get_first_key_args',
      autosepc=True,
      return_value={'downsample': '1.0'},
  )
  @mock.patch.object(
      flask_util, 'get_includefields', autospec=True, return_value=set()
  )
  @mock.patch.object(
      flask_util,
      'get_request',
      autosepc=True,
      return_value=_MockFlaskRequest('instances'),
  )
  def test_augment_instance_metadata_passthrough_downsample_one(
      self, content_type, *unused_mocks
  ):
    test_msg = 'test'
    resp = flask.Response(
        test_msg, status=http.HTTPStatus.OK, content_type=content_type
    )
    result = metadata_augmentation.augment_instance_metadata(
        _MOCK_DICOMWEBBASE_URL,
        _EMPTY_STUDY,
        _EMPTY_SERIES,
        _EMPTY_SOPINSTANCE,
        resp,
    )

    self.assertEqual(result.status, shared_test_util.http_ok_status())
    self.assertEqual(result.data, test_msg.encode('utf-8'))

  @mock.patch.object(
      flask_util, 'get_includefields', autospec=True, return_value=set()
  )
  @mock.patch.object(
      flask_util,
      'get_first_key_args',
      autosepc=True,
      return_value={'downsample': '1.0'},
  )
  @mock.patch.object(
      flask_util,
      'get_request',
      autosepc=True,
      return_value=_MockFlaskRequest('instances'),
  )
  def test_augment_instance_metadata_passthrough_non_json_content(
      self, *unused_mocks
  ):
    test_msg = 'test'
    resp = flask.Response(
        test_msg, status=http.HTTPStatus.OK, content_type='text/plain'
    )
    result = metadata_augmentation.augment_instance_metadata(
        _MOCK_DICOMWEBBASE_URL,
        _EMPTY_STUDY,
        _EMPTY_SERIES,
        _EMPTY_SOPINSTANCE,
        resp,
    )

    self.assertEqual(result.status, shared_test_util.http_ok_status())
    self.assertEqual(result.data, test_msg.encode('utf-8'))

  @mock.patch.object(
      flask_util, 'get_includefields', autospec=True, return_value=set()
  )
  @mock.patch.object(
      flask_util,
      'get_first_key_args',
      autosepc=True,
      return_value={'downsample': '0.5'},
  )
  def test_augment_instance_metadata_bad_downsample(self, *unused_mocks):
    test_msg = 'test'
    resp = flask.Response(test_msg, status=http.HTTPStatus.OK)
    result = metadata_augmentation.augment_instance_metadata(
        _MOCK_DICOMWEBBASE_URL,
        _EMPTY_STUDY,
        _EMPTY_SERIES,
        _EMPTY_SOPINSTANCE,
        resp,
    )
    self.assertEqual(result.status, shared_test_util.http_bad_request_status())
    self.assertEqual(result.data, b'Invalid downsample')

  @mock.patch.object(
      flask_util, 'get_includefields', autospec=True, return_value=set()
  )
  @mock.patch.object(
      flask_util,
      'get_first_key_args',
      autosepc=True,
      return_value={'downsample': '2.0'},
  )
  @mock.patch.object(
      flask_util,
      'get_request',
      autosepc=True,
      return_value=_MockFlaskRequest('instances'),
  )
  def test_augment_instance_metadata_bad_content_type(self, *unused_mocks):
    expected_msg = 'test'
    resp = flask.Response(
        expected_msg, status=http.HTTPStatus.OK, content_type='text/plain'
    )
    result = metadata_augmentation.augment_instance_metadata(
        _MOCK_DICOMWEBBASE_URL,
        _EMPTY_STUDY,
        _EMPTY_SERIES,
        _EMPTY_SOPINSTANCE,
        resp,
    )
    self.assertEqual(result.status, shared_test_util.http_ok_status())
    self.assertEqual(result.data, expected_msg.encode('utf-8'))

  @parameterized.parameters(['', '[abc', '1', '"abc"', '[1, 2, 3]'])
  @mock.patch.object(
      flask_util, 'get_includefields', autospec=True, return_value=set()
  )
  @mock.patch.object(
      flask_util,
      'get_first_key_args',
      autosepc=True,
      return_value={'downsample': '2.0'},
  )
  @mock.patch.object(
      flask_util,
      'get_request',
      autosepc=True,
      return_value=_MockFlaskRequest('instances'),
  )
  def test_augment_instance_metadata_bad_json(self, test_msg, *unused_mocks):
    resp = flask.Response(
        test_msg,
        status=http.HTTPStatus.OK,
        content_type='application/dicom+json',
    )
    result = metadata_augmentation.augment_instance_metadata(
        _MOCK_DICOMWEBBASE_URL,
        _EMPTY_STUDY,
        _EMPTY_SERIES,
        _EMPTY_SOPINSTANCE,
        resp,
    )
    self.assertEqual(result.status, shared_test_util.http_ok_status())
    self.assertEqual(result.data, test_msg.encode('utf-8'))

  @mock.patch.object(
      flask_util, 'get_includefields', autospec=True, return_value=set()
  )
  @mock.patch.object(
      flask_util,
      'get_first_key_args',
      autosepc=True,
      return_value={'downsample': '2.0'},
  )
  @mock.patch.object(
      flask_util,
      'get_request',
      autosepc=True,
      return_value=_MockFlaskRequest('instances'),
  )
  def test_augment_instance_metadata_downsample_error(self, *unused_mocks):
    test_msg = shared_test_util.jpeg_encoded_dicom_instance_json()
    del test_msg[metadata_util._COLUMNS_DICOM_ADDRESS_TAG]
    test_msg = json.dumps(test_msg)

    resp = flask.Response(
        test_msg,
        status=http.HTTPStatus.OK,
        content_type='application/dicom+json',
    )
    result = metadata_augmentation.augment_instance_metadata(
        _MOCK_DICOMWEBBASE_URL,
        _EMPTY_STUDY,
        _EMPTY_SERIES,
        _EMPTY_SOPINSTANCE,
        resp,
    )

    self.assertEqual(result.status, shared_test_util.http_bad_request_status())
    expected_msg = b'DICOM tag 00280011 is undefined cannot'
    self.assertEqual(result.data[: len(expected_msg)], expected_msg)

  @parameterized.named_parameters([
      dict(
          testcase_name='single_instance',
          metadata=shared_test_util.jpeg_encoded_dicom_instance_json(),
      ),
      dict(
          testcase_name='multiple_instance',
          metadata=[
              shared_test_util.jpeg_encoded_dicom_instance_json(),
              shared_test_util.jpeg_encoded_dicom_instance_json(),
          ],
      ),
  ])
  @mock.patch.object(
      flask_util, 'get_includefields', autospec=True, return_value=set()
  )
  @mock.patch.object(
      flask_util,
      'get_request',
      autosepc=True,
      return_value=_MockFlaskRequest('instances'),
  )
  def test_augment_instance_metadata_downsample_success(
      self, *unused_mocks, metadata
  ):
    total_cols_tag = metadata_util._TOTAL_PIXEL_MATRIX_COLUMNS_DICOM_ADDRESS_TAG
    total_rows_tag = metadata_util._TOTAL_PIXEL_MATRIX_ROWS_DICOM_ADDRESS_TAG
    value = metadata_util._VALUE
    downsample = 2.0
    resp = flask.Response(
        json.dumps(metadata),
        status=http.HTTPStatus.OK,
        content_type='application/dicom+json',
    )
    fl_request_args = {'downsample': str(downsample)}
    with mock.patch.object(
        flask_util,
        'get_first_key_args',
        autosepc=True,
        return_value=fl_request_args,
    ):
      result = metadata_augmentation.augment_instance_metadata(
          _MOCK_DICOMWEBBASE_URL,
          _EMPTY_STUDY,
          _EMPTY_SERIES,
          _EMPTY_SOPINSTANCE,
          resp,
      )

    if isinstance(metadata, dict):
      metadata = [metadata]
    self.assertEqual(result.status, shared_test_util.http_ok_status())
    for index, downsampled_metadata in enumerate(
        json.loads(result.data.decode('utf-8'))
    ):
      for test_tag in (total_cols_tag, total_rows_tag):
        self.assertEqual(
            downsampled_metadata[test_tag][value][0],
            max(1, int(metadata[index][test_tag][value][0] / downsample)),
        )


if __name__ == '__main__':
  absltest.main()
