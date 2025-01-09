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
"""Tests for dicom store util."""
import collections
import copy
import http
import time
from typing import List, Mapping, Union
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import flask
import requests

from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import dicom_store_util
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import flask_util
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import shared_test_util
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.test_utils.dicom_store_mock import dicom_store_mock

_MOCK_BASE_URL = dicom_url_util.DicomWebBaseURL(
    'v1', 'project', 'location', 'dataset', 'dicomstore'
)


class _Args:

  def __init__(self, args):
    self._args = args

  def _make_dict(self) -> Mapping[str, str]:
    return {key: value for key, value in self._args}

  def __len__(self) -> int:
    return len(self._args)

  def __getitem__(self, key: str) -> str:
    return self._make_dict()[key]

  def to_dict(self, flat: bool) -> Mapping[str, Union[str, List[str]]]:
    if not flat:
      result = collections.defaultdict(list)
      for key, value in self._args:
        result[key].append(value)
      return result
    return self._make_dict()


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
  def args(self) -> _Args:
    if '?' not in self.url:
      return _Args([])
    params = self.url[self.url.index('?') + 1 :]
    if not params:
      _Args([])
    result = []
    for param in params.split('&'):
      parts = param.split('=')
      if len(parts) == 2:
        key, value = parts
        result.append((key, value))
    return _Args(result)


def _test_get_metadata(val: float):
  time.sleep(val)


class DicomStoreUtilTest(parameterized.TestCase):

  @parameterized.parameters([('', ''), ('abc=123', '?abc=123')])
  @mock.patch.object(
      flask_util,
      'get_request',
      autospec=True,
  )
  def test_get_flask_full_path_removing_unsupported_proxy_params(
      self, query_param, expected_query, mk_flask_request
  ):
    strip_args = set([
        'disable_caching',
        'downsample',
        'embed_iccprofile',
        'iccprofile',
        'interpolation',
        'quality',
    ]).union(set(dicom_store_util._UNSUPPORTED_PROXY_PARAMS))
    mock_args = [f'{key}=value' for key in strip_args]
    mock_args.append(query_param)
    mock_args = '&'.join(mock_args)
    mk_flask_request.return_value = _MockFlaskRequest(
        f'https://test.foo.com/dicomWeb/study/134?{mock_args}'
    )
    path = (
        dicom_store_util._get_flask_full_path_removing_unsupported_proxy_params()
    )
    self.assertEqual(path, f'/dicomWeb/study/134{expected_query}')

  @mock.patch.object(
      flask_util,
      'get_request',
      autospec=True,
      return_value=_MockFlaskRequest('https://test.foo.com/dicomWeb/study/134'),
  )
  def test_get_flask_full_path_removing_unsupported_proxy_params_no_params(
      self, _
  ):
    path = (
        dicom_store_util._get_flask_full_path_removing_unsupported_proxy_params()
    )
    self.assertEqual(path, '/dicomWeb/study/134')

  @parameterized.parameters([
      (
          f'{dicom_proxy_flags.PROXY_SERVER_URL_PATH_PREFIX}/abc/123',
          '/abc/123',
      ),
      ('/no_prefix/abc/123', '/no_prefix/abc/123'),
  ])
  def test_remove_dicom_proxy_url_path_prefix(self, test_url, expected):
    self.assertEqual(
        dicom_store_util._remove_dicom_proxy_url_path_prefix(test_url),
        expected,
    )

  @mock.patch.object(
      flask_util,
      'get_first_key_args',
      autospec=True,
      return_value={'downsample': '2.0'},
  )
  def test_dicom_store_proxy_invalid_method(self, _):
    request = flask.Request(
        {
            'REQUEST_METHOD': 'PATCH',
            'PATH_INFO': 'TEST_PATH',
            'HTTP_ACCEPT': 'image/jpeg',
        },
        populate_request=False,
    )

    with mock.patch.object(
        flask_util, 'get_request', autospec=True, return_value=request
    ):
      response = dicom_store_util.dicom_store_proxy()

    self.assertEqual(response.status_code, http.HTTPStatus.BAD_REQUEST)
    self.assertEqual(response.content_type, 'text/plain')

  @parameterized.named_parameters([
      dict(
          testcase_name='remove_pixeldata',
          exclude_tags=['PixelData'],
      ),
      dict(testcase_name='all', exclude_tags=[]),
      dict(
          testcase_name='exclude_bad_tag',
          exclude_tags=['BadTag'],
      ),
  ])
  @mock.patch.object(
      user_auth_util,
      '_get_email_from_bearer_token',
      autospec=True,
      return_value='foo@bar.com',
  )
  @mock.patch.object(flask_util, 'get_headers', autospec=True)
  @flagsaver.flagsaver(validate_iap=False)
  def test_download_instance_return_metadata(
      self, mk_get_headers, unused_mock, exclude_tags
  ):
    mk_get_headers.return_value = {
        proxy_const.HeaderKeywords.AUTH_HEADER_KEY: '123'
    }
    dcm = shared_test_util.jpeg_encoded_dicom_instance()
    with dicom_store_mock.MockDicomStores(_MOCK_BASE_URL.full_url) as mk_stores:
      mk_stores[_MOCK_BASE_URL.full_url].add_instance(dcm)
      instance_metadata = dicom_store_util.download_instance_return_metadata(
          user_auth_util.AuthSession(
              {proxy_const.HeaderKeywords.AUTH_HEADER_KEY: 'mock_token'}
          ),
          _MOCK_BASE_URL,
          dcm.StudyInstanceUID,
          dcm.SeriesInstanceUID,
          dcm.SOPInstanceUID,
          exclude_tags=exclude_tags,
      )
      for tag in exclude_tags:
        try:
          del dcm[tag]
        except (ValueError, KeyError, TypeError) as _:
          pass
      expected = dcm.to_json_dict()
      expected.update(dcm.file_meta.to_json_dict())
      self.assertEqual(instance_metadata, expected)

  @parameterized.named_parameters([
      dict(
          testcase_name='bad_request',
          http_response=http.HTTPStatus.BAD_REQUEST,
          http_bytes=b'',
      ),
      dict(
          testcase_name='bad_dicom',
          http_response=http.HTTPStatus.OK,
          http_bytes=b'1234',
      ),
  ])
  @mock.patch.object(
      user_auth_util,
      '_get_email_from_bearer_token',
      autospec=True,
      return_value='foo@bar.com',
  )
  @mock.patch.object(flask_util, 'get_headers', autospec=True)
  @flagsaver.flagsaver(validate_iap=False)
  def test_download_instance_error_raises(
      self, mk_get_headers, unused_mock, http_response, http_bytes
  ):
    mk_get_headers.return_value = {
        proxy_const.HeaderKeywords.AUTH_HEADER_KEY: '123'
    }
    dcm = shared_test_util.jpeg_encoded_dicom_instance()
    with dicom_store_mock.MockDicomStores(_MOCK_BASE_URL.full_url) as mk_stores:
      mk_stores[_MOCK_BASE_URL.full_url].add_instance(dcm)
      mk_stores[_MOCK_BASE_URL.full_url].set_mock_response(
          dicom_store_mock.MockHttpResponse(
              f'studies/{dcm.StudyInstanceUID}/series/{dcm.SeriesInstanceUID}/instances/{dcm.SOPInstanceUID}',
              dicom_store_mock.RequestMethod.GET,
              http_response,
              http_bytes,
          )
      )
      with self.assertRaises(
          dicom_store_util.DicomInstanceMetadataRetrievalError
      ):
        dicom_store_util.download_instance_return_metadata(
            user_auth_util.AuthSession(
                {proxy_const.HeaderKeywords.AUTH_HEADER_KEY: 'mock_token'}
            ),
            _MOCK_BASE_URL,
            dcm.StudyInstanceUID,
            dcm.SeriesInstanceUID,
            dcm.SOPInstanceUID,
            exclude_tags=['PixelData'],
        )

  def test_metadata_thread_pool_raises_if_download_exceeds_size(self):
    with self.assertRaises(
        dicom_store_util.MetadataDownloadExceedsMaxSizeLimitError
    ):
      with dicom_store_util.MetadataThreadPoolDownloadManager(1, 10) as pool:
        pool.inc_data_downloaded(11)

  def test_metadata_thread_pool_inc_succeeds(self):
    with dicom_store_util.MetadataThreadPoolDownloadManager(1, 10) as pool:
      for i in range(10):
        self.assertEqual(pool.inc_data_downloaded(1), i + 1)
      self.assertEmpty(pool._future_list)

  def test_metadata_thread_pool_raises_if_inc_called_outside_context_block(
      self,
  ):
    with self.assertRaises(
        dicom_store_util.MetadataThreadPoolDownloadManagerError
    ):
      pool = dicom_store_util.MetadataThreadPoolDownloadManager(1, 10)
      pool.inc_data_downloaded(11)

  def test_metadata_thread_pool_raises_if_submit_called_outside_context_block(
      self,
  ):
    with self.assertRaises(
        dicom_store_util.MetadataThreadPoolDownloadManagerError
    ):
      pool = dicom_store_util.MetadataThreadPoolDownloadManager(1, 10)
      pool.submit(_test_get_metadata, 0.01)

  def test_metadata_thread_pool_submit_single_thread(self):
    with dicom_store_util.MetadataThreadPoolDownloadManager(1, 10) as pool:
      for _ in range(10):
        pool.submit(_test_get_metadata, 0.01)
      self.assertEmpty(pool._future_list)

  def test_metadata_thread_pool_submit_multi_thread(self):
    with dicom_store_util.MetadataThreadPoolDownloadManager(2, 10) as pool:
      for _ in range(10):
        pool.submit(_test_get_metadata, 2)
      future_list = copy.copy(pool._future_list)
      self.assertLen(pool._future_list, 10)
    self.assertTrue(all([future.done() for future in future_list]))
    # test pool calback was called and closed.
    self.assertEmpty(pool._future_list)
    self.assertFalse(pool._context_entered)

  @parameterized.named_parameters([
      dict(testcase_name='single_thread', setting=1, expected=1),
      dict(testcase_name='multi_thread', setting=2, expected=2),
      dict(testcase_name='max_thread', setting=20, expected=4),
  ])
  def test_metadata_thread_pool_thread_count(self, setting, expected):
    with dicom_store_util.MetadataThreadPoolDownloadManager(
        setting, 10
    ) as pool:
      self.assertEqual(pool._max_threads, expected)

  @mock.patch.object(
      user_auth_util,
      '_get_email_from_bearer_token',
      autospec=True,
      return_value='foo@bar.com',
  )
  @flagsaver.flagsaver(validate_iap=False)
  def test_get_series_instance_uid_for_study_and_instance_uid_success(self, _):
    dcm = shared_test_util.jpeg_encoded_dicom_instance()
    with dicom_store_mock.MockDicomStores(
        _MOCK_BASE_URL.full_url
    ) as mocked_dicom_stores:
      mocked_dicom_stores[_MOCK_BASE_URL.full_url].add_instance(dcm)
      result = dicom_store_util.get_dicom_study_instance_metadata(
          user_auth_util.AuthSession(
              {proxy_const.HeaderKeywords.AUTH_HEADER_KEY: 'mock_token'}
          ),
          dicom_url_util.base_dicom_study_url(
              _MOCK_BASE_URL,
              dicom_url_util.StudyInstanceUID(dcm.StudyInstanceUID),
          ),
          dicom_url_util.SOPInstanceUID(dcm.SOPInstanceUID),
      )

    self.assertEqual(
        [dcm['0020000E']['Value'][0] for dcm in result], [dcm.SeriesInstanceUID]
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='base_url',
          url='https://foo.bar.com',
          expected='https://foo.bar.com?test1&test2',
      ),
      dict(
          testcase_name='url_with_slash',
          url='https://foo.bar.com/',
          expected='https://foo.bar.com?test1&test2',
      ),
      dict(
          testcase_name='url_with_empty_params',
          url='https://foo.bar.com?',
          expected='https://foo.bar.com?test1&test2',
      ),
      dict(
          testcase_name='url_with_slash_and_empty_params',
          url='https://foo.bar.com/?',
          expected='https://foo.bar.com?test1&test2',
      ),
      dict(
          testcase_name='url_with_slash_and_params',
          url='https://foo.bar.com/?abc=123',
          expected='https://foo.bar.com?abc=123&test1&test2',
      ),
      dict(
          testcase_name='url_with_params',
          url='https://foo.bar.com?abc=123',
          expected='https://foo.bar.com?abc=123&test1&test2',
      ),
  ])
  def test_add_parameters_to_url_params(self, url, expected):
    self.assertEqual(
        dicom_store_util._add_parameters_to_url(url, ['test1', 'test2']),
        expected,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='parameters_none',
          param=None,
          expected_param='',
      ),
      dict(
          testcase_name='parameters_empty',
          param=[],
          expected_param='',
      ),
      dict(
          testcase_name='url_with_params',
          param=['abc'],
          expected_param='?abc',
      ),
  ])
  @mock.patch.object(
      flask_util, 'get_key_args_list', autospec=True, return_value={}
  )
  @mock.patch.object(flask_util, 'get_headers', autospec=True, return_value={})
  @mock.patch.object(
      flask_util, 'get_method', autospec=True, return_value='GET'
  )
  @mock.patch.object(flask_util, 'get_path', autospec=True)
  @mock.patch.object(requests, 'get', autospec=True)
  def test_add_parameters_dicom_store_proxy_requst(
      self, mock_request, mock_path, *unused_mocks, param, expected_param
  ):
    request_path = 'v2/projects/prj/locations/l/datasets/d/dicomStores/ds/dicomWeb/instances'
    mock_path.return_value = request_path
    mock_response = mock.create_autospec(requests.Response, instance=True)
    mock_response.headers = {}
    mock_response.status_code = http.HTTPStatus.OK
    mock_request.return_value = mock_response
    dicom_store_util.dicom_store_proxy(param)
    expected_base = f'https://healthcare.googleapis.com/{request_path}'
    self.assertEqual(
        mock_request.call_args[0][0], f'{expected_base}{expected_param}'
    )


if __name__ == '__main__':
  absltest.main()
