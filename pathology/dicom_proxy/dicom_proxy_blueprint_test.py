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
"""Tests for dicom proxy blueprint."""

from concurrent import futures
import contextlib
import copy
import http
import json
import os
import re
from typing import Iterator, Mapping, Optional
from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import flask
import pydicom
import redis
import requests_mock
import requests_toolbelt
import werkzeug

# Mock flask_compressed decorator prior to usage in dicom_proxy_blueprint to
# disable decorator in unit tests.
mock.patch('flask_compress.Compress.compressed', lambda x: lambda x: x).start()

from pathology.dicom_proxy import bulkdata_util  # pylint: disable=g-import-not-at-top
from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import color_conversion_util
from pathology.dicom_proxy import dicom_proxy_blueprint
from pathology.dicom_proxy import dicom_store_util
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import flask_util
from pathology.dicom_proxy import metadata_util
from pathology.dicom_proxy import parameters_exceptions_and_return_types
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import pydicom_single_instance_read_cache
from pathology.dicom_proxy import redis_cache
from pathology.dicom_proxy import render_frame_params
from pathology.dicom_proxy import server
from pathology.dicom_proxy import shared_test_util
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.iap_auth_lib import auth
from pathology.shared_libs.test_utils.dicom_store_mock import dicom_store_mock


# Mark flags as parsed to avoid UnparsedFlagAccessError for tests using
# --test_srcdir flag in parameters.
flags.FLAGS.mark_as_parsed()

# Types
_Compression = enum_types.Compression
_Interpolation = enum_types.Interpolation
_DicomInstanceWebRequest = (
    parameters_exceptions_and_return_types.DicomInstanceWebRequest
)
_LocalDicomInstance = parameters_exceptions_and_return_types.LocalDicomInstance
_EMPTY_STUDY = dicom_url_util.StudyInstanceUID('')
_EMPTY_SERIES = dicom_url_util.SeriesInstanceUID('')
_EMPTY_SOPINSTANCE = dicom_url_util.SOPInstanceUID('')
_DICOM_PIXELDATA_TAG_ADDRESS = '7FE00010'
_DICOM_FILE_META_INFORMATION_VERSION_TAG_ADDRESS = '00020001'
_MOCK_DICOM_STORE_API_VERSION = 'v1'
_MOCK_DICOMWEBBASE_URL = dicom_url_util.DicomWebBaseURL(
    'v1', 'proj', 'loc', 'dset', 'dstore'
)


def _mock_sparse_dicom() -> pydicom.FileDataset:
  dcm = shared_test_util.jpeg_encoded_dicom_instance()
  dcm.DimensionOrganizationType = 'TILED_SPARSE'
  ds1 = pydicom.Dataset()
  ds1.StudyID = '1'
  ds2 = pydicom.Dataset()
  ds2.StudyID = '2'
  dcm.PerFrameFunctionalGroupsSequence = [ds1, ds2]
  return dcm


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


def _http_status_response(status: http.HTTPStatus) -> str:
  return (f'{status.value} {status.phrase}').upper()


def _http_ok_status() -> str:
  return _http_status_response(http.HTTPStatus.OK)


def _http_bad_request_status() -> str:
  return _http_status_response(http.HTTPStatus.BAD_REQUEST)


def _http_not_found_status() -> str:
  return _http_status_response(http.HTTPStatus.NOT_FOUND)


def _mock_flask_stream_context(msg: Iterator[str]) -> str:
  return ''.join(msg)


@flagsaver.flagsaver(validate_iap=False)
@mock.patch.object(
    user_auth_util,
    '_get_email_from_bearer_token',
    autospec=True,
    return_value='mock@email.com',
)
@mock.patch.object(
    flask_util,
    'get_headers',
    autospec=True,
    return_value={
        proxy_const.HeaderKeywords.AUTH_HEADER_KEY: 'bearer mock_token',
    },
)
@mock.patch.object(
    flask_util,
    'get_first_key_args',
    autospec=True,
    return_value={},
)
@mock.patch.object(
    flask_util,
    'get_key_args_list',
    autospec=True,
    return_value={},
)
@mock.patch.object(
    flask_util,
    'get_method',
    autospec=True,
    return_value='GET',
)
@mock.patch.object(flask_util, 'get_base_url', autospec=True)
@mock.patch.object(flask_util, 'get_path', autospec=True)
@mock.patch.object(flask_util, 'get_full_request_url', autospec=True)
@mock.patch.object(flask_util, 'get_url_root', autospec=True)
def _dicom_metadata_search(
    dcm: pydicom.FileDataset,
    bulkdata_uri_enabled: bool,
    mk_get_url_root,
    mk_get_full_request_url,
    mk_get_path,
    mk_get_base_url,
    *unused_mocks,
    add_instance_to_store: bool = True,
    proxy_root: str = '',
    proxy_path: str = '',
    mock_dicom_store_response: Optional[
        dicom_store_mock.MockHttpResponse
    ] = None,
) -> flask.Response:
  base_url = _MOCK_DICOMWEBBASE_URL
  dicom_web_path = f'/studies/{dcm.StudyInstanceUID}/series/{dcm.SeriesInstanceUID}/instances/{dcm.SOPInstanceUID}/metadata'
  external_url_root = base_url.root_url if not proxy_root else proxy_root
  full_path = f'{external_url_root}/{proxy_path}/{base_url}{dicom_web_path}'
  mk_get_url_root.return_value = external_url_root
  mk_get_full_request_url.return_value = full_path
  mk_get_path.return_value = f'/{proxy_path}/{base_url}{dicom_web_path}'
  mk_get_base_url.return_value = full_path
  with dicom_store_mock.MockDicomStores(
      base_url.full_url, bulkdata_uri_enabled=bulkdata_uri_enabled
  ) as mocked_dicom_stores:
    if add_instance_to_store:
      mocked_dicom_stores[base_url.full_url].add_instance(dcm)
      if mock_dicom_store_response is not None:
        mocked_dicom_stores[base_url.full_url].set_mock_response(
            mock_dicom_store_response
        )
    return dicom_proxy_blueprint._metadata_search(
        base_url.dicom_store_api_version,
        base_url.gcp_project_id,
        base_url.location,
        base_url.dataset_id,
        base_url.dicom_store,
        dcm.StudyInstanceUID,
        dcm.SeriesInstanceUID,
        dcm.SOPInstanceUID,
    )


@flagsaver.flagsaver(validate_iap=False)
@mock.patch.object(
    user_auth_util,
    '_get_email_from_bearer_token',
    autospec=True,
    return_value='mock@email.com',
)
@mock.patch.object(
    flask_util,
    'get_headers',
    autospec=True,
    return_value={
        proxy_const.HeaderKeywords.AUTH_HEADER_KEY: 'bearer mock_token',
    },
)
@mock.patch.object(
    flask_util,
    'get_method',
    autospec=True,
    return_value='GET',
)
@mock.patch.object(
    flask_util,
    'get_first_key_args',
    autospec=True,
)
@mock.patch.object(
    flask_util,
    'get_key_args_list',
    autospec=True,
)
@mock.patch.object(
    dicom_proxy_blueprint, '_get_sop_instance_uid_param_value', autospec=True
)
@mock.patch.object(flask_util, 'get_includefields', autspec=True)
@mock.patch.object(flask_util, 'get_base_url', autospec=True)
@mock.patch.object(flask_util, 'get_path', autospec=True)
@mock.patch.object(flask_util, 'get_full_request_url', autospec=True)
@mock.patch.object(flask_util, 'get_url_root', autospec=True)
def _dicom_instance_search(
    dcm: pydicom.FileDataset,
    bulkdata_uri_enabled: bool,
    mk_get_url_root,
    mk_get_full_request_url,
    mk_get_path,
    mk_get_base_url,
    mk_get_includefields,
    mk_get_sop_instance_uid_param_value,
    mk_get_key_args_list,
    mk_get_first_key_args,
    *unused_mocks,
    add_instance_to_store: bool = True,
    proxy_root: str = '',
    proxy_path: str = '',
    include_field: str = 'all',
    mock_dicom_store_response: Optional[
        dicom_store_mock.MockHttpResponse
    ] = None,
    other_parameters: Optional[Mapping[str, str]] = None,
) -> flask.Response:
  key_args_list = (
      {}
      if other_parameters is None
      else {key: [value] for key, value in other_parameters.items()}
  )
  key_args_list['includefield'] = include_field.split(',')
  mk_get_key_args_list.return_value = key_args_list
  mk_get_first_key_args.return_value = {
      key: value[0] for key, value in key_args_list.items()
  }
  base_url = _MOCK_DICOMWEBBASE_URL
  params = f'?SOPInstanceUID={dcm.SOPInstanceUID}&includefield={include_field}'
  dicom_web_path = f'/studies/{dcm.StudyInstanceUID}/series/{dcm.SeriesInstanceUID}/instances'
  external_url_root = base_url.root_url if not proxy_root else proxy_root
  base_path = f'{external_url_root}/{proxy_path}/{base_url}{dicom_web_path}'
  mk_get_includefields.return_value = set(include_field.split(','))
  mk_get_sop_instance_uid_param_value.return_value = dcm.SOPInstanceUID
  mk_get_url_root.return_value = external_url_root
  mk_get_full_request_url.return_value = f'{base_path}{params}'
  mk_get_path.return_value = f'/{proxy_path}/{base_url}{dicom_web_path}'
  mk_get_base_url.return_value = base_path
  with dicom_store_mock.MockDicomStores(
      base_url.full_url, bulkdata_uri_enabled=bulkdata_uri_enabled
  ) as mocked_dicom_stores:
    if add_instance_to_store:
      mocked_dicom_stores[base_url.full_url].add_instance(dcm)
      if mock_dicom_store_response is not None:
        mocked_dicom_stores[base_url.full_url].set_mock_response(
            mock_dicom_store_response
        )
    return dicom_proxy_blueprint._instances_search(
        base_url.dicom_store_api_version,
        base_url.gcp_project_id,
        base_url.location,
        base_url.dataset_id,
        base_url.dicom_store,
        dcm.StudyInstanceUID,
        dcm.SeriesInstanceUID,
    )


@mock.patch.object(
    flask_util,
    'get_headers',
    autospec=True,
    return_value={proxy_const.HeaderKeywords.AUTH_HEADER_KEY: '123'},
)
@mock.patch.object(
    user_auth_util,
    '_get_email_from_bearer_token',
    autospec=True,
    return_value='foo@bar.com',
)
@mock.patch.object(flask_util, 'get_method', autospec=True, return_value='GET')
@mock.patch.object(
    flask_util, 'get_first_key_args', autospec=True, return_value={}
)
@mock.patch.object(flask_util, 'get_full_request_url', autospec=True)
@flagsaver.flagsaver(validate_iap=False)
def _get_frame_instance(
    add_dicom: bool,
    is_rendered_request: bool,
    mk_get_url,
    *unused_mocks,
):
  dcm = shared_test_util.jpeg_encoded_dicom_instance()
  base_url = _MOCK_DICOMWEBBASE_URL
  mk_get_url.return_value = base_url.full_url
  with dicom_store_mock.MockDicomStores(
      base_url.full_url
  ) as mocked_dicom_store:
    if add_dicom:
      mocked_dicom_store[base_url.full_url].add_instance(dcm)
    return dicom_proxy_blueprint._get_frame_instance(
        base_url.dicom_store_api_version,
        base_url.gcp_project_id,
        base_url.location,
        base_url.dataset_id,
        base_url.dicom_store,
        dcm.StudyInstanceUID,
        dcm.SeriesInstanceUID,
        dcm.SOPInstanceUID,
        '1',
        is_rendered_request,
    )


@mock.patch.object(
    flask_util,
    'get_headers',
    autospec=True,
    return_value={proxy_const.HeaderKeywords.AUTH_HEADER_KEY: '123'},
)
@mock.patch.object(
    user_auth_util,
    '_get_email_from_bearer_token',
    autospec=True,
    return_value='foo@bar.com',
)
@mock.patch.object(flask_util, 'get_method', autospec=True, return_value='GET')
@mock.patch.object(
    flask_util, 'get_first_key_args', autospec=True, return_value={}
)
@mock.patch.object(flask_util, 'get_full_request_url', autospec=True)
@flagsaver.flagsaver(validate_iap=False)
def _get_instance_rendered(add_dicom: bool, mk_get_url, *unused_mocks):
  dcm = shared_test_util.jpeg_encoded_dicom_instance()
  base_url = _MOCK_DICOMWEBBASE_URL
  mk_get_url.return_value = base_url.full_url
  with dicom_store_mock.MockDicomStores(
      base_url.full_url
  ) as mocked_dicom_store:
    if add_dicom:
      mocked_dicom_store[base_url.full_url].add_instance(dcm)
    return dicom_proxy_blueprint._rendered_instance(
        base_url.dicom_store_api_version,
        base_url.gcp_project_id,
        base_url.location,
        base_url.dataset_id,
        base_url.dicom_store,
        dcm.StudyInstanceUID,
        dcm.SeriesInstanceUID,
        dcm.SOPInstanceUID,
    )


class _MissingSeriesRedirectTestContext(contextlib.ExitStack):
  """Context manager for testing find missing series uid redirect."""

  def __init__(
      self,
      root: str,
      base_url: dicom_url_util.DicomWebBaseURL,
      dcm: pydicom.FileDataset,
      param: str,
      add_dicom: bool,
  ):
    super().__init__()
    self._root = root
    self._dcm = dcm
    self._add_dicom = add_dicom
    self._base_url = base_url
    self._param = param

  def __enter__(self) -> contextlib.ExitStack:
    super().__enter__()
    self.enter_context(
        mock.patch.object(
            flask_util,
            'get_headers',
            autospec=True,
            return_value={proxy_const.HeaderKeywords.AUTH_HEADER_KEY: '123'},
        )
    )
    self.enter_context(
        mock.patch.object(
            user_auth_util,
            '_get_email_from_bearer_token',
            autospec=True,
            return_value='foo@bar.com',
        )
    )
    self.enter_context(
        mock.patch.object(
            flask_util, 'get_method', autospec=True, return_value='GET'
        )
    )
    self.enter_context(
        mock.patch.object(
            flask_util, 'get_first_key_args', autospec=True, return_value={}
        )
    )
    self.enter_context(
        mock.patch.object(
            flask_util, 'get_url_root', autospec=True, return_value=self._root
        )
    )
    base_url_str = f'{self._root}/{self._base_url}/studies/{self._dcm.StudyInstanceUID}/instances/{self._dcm.SOPInstanceUID}/rendered'
    self.enter_context(
        mock.patch.object(
            flask_util, 'get_base_url', autospec=True, return_value=base_url_str
        )
    )
    self.enter_context(
        mock.patch.object(
            flask_util,
            'get_full_request_url',
            autospec=True,
            return_value=f'{base_url_str}{self._param}',
        )
    )
    self.enter_context(flagsaver.flagsaver(validate_iap=False))
    ds_store = self.enter_context(
        dicom_store_mock.MockDicomStores(self._base_url.full_url)
    )
    if self._add_dicom:
      ds_store[self._base_url.full_url].add_instance(self._dcm)
    return self


class DicomProxyBlueprintTest(parameterized.TestCase):

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

  @mock.patch.object(flask_util, 'get_first_key_args', autospec=True)
  def test_get_dicom_proxy_request_args(self, mock_get_flask_args):
    mock_get_flask_args.return_value = {
        ' IcCpRoFiLe ': 'abcd',
        ' disable_caching ': 'TRUE',
        ' DownSample ': '2.0',
        ' interpolation ': 'AREA',
        'quality': '100',
        'Not_in_list': 'bogus',
        '': '',
    }
    self.assertEqual(
        dicom_proxy_blueprint._get_dicom_proxy_request_args(),
        {
            'disable_caching': 'TRUE',
            'downsample': '2.0',
            'iccprofile': 'abcd',
            'interpolation': 'AREA',
            'quality': '100',
        },
    )

  @parameterized.parameters([
      (True, server.HEALTH_CHECK_HTML),
      (False, server.LOCAL_REDIS_SERVER_DISABLED),
  ])
  @mock.patch.object(redis_cache.RedisCache, 'ping', autospec=True)
  def test_healthcheck_redis_running(
      self, redis_ping, expected_response, mock_ping
  ):
    server._last_health_check_log_time = 0
    mock_ping.return_value = redis_ping
    with server.flask_app.test_client() as client:
      result = client.post('/')
    self.assertEqual(result.data.decode('utf-8'), expected_response)
    self.assertEqual(result.mimetype, 'text/html')

  @parameterized.parameters([
      (True, server.HEALTH_CHECK_HTML, '123'),
      (False, server.HEALTH_CHECK_HTML, '123'),
      (True, server.HEALTH_CHECK_HTML, ' '),
      (False, server.HEALTH_CHECK_HTML, ' '),
  ])
  @mock.patch.object(redis_cache.RedisCache, 'ping', autospec=True)
  def test_healthcheck_redis_server_disabled(
      self, redis_ping, expected_response, redis_cache_host_ip, mock_ping
  ):
    server._last_health_check_log_time = 0
    server._redis_server_is_alive_healthcheck = True
    mock_ping.return_value = redis_ping
    with flagsaver.flagsaver(redis_cache_host_ip=redis_cache_host_ip):
      with server.flask_app.test_client() as client:
        result = client.post('/')
    self.assertEqual(result.data.decode('utf-8'), expected_response)
    self.assertEqual(result.mimetype, 'text/html')

  @parameterized.parameters([
      (' AREA ', _Interpolation.AREA),
      (' Cubic ', _Interpolation.CUBIC),
      (' lanczos4 ', _Interpolation.LANCZOS4),
      (' linear ', _Interpolation.LINEAR),
      ('NEARest', _Interpolation.NEAREST),
      (None, _Interpolation.AREA),
  ])
  def test_parse_interpolation(self, interpolation, expected):
    if interpolation is None:
      args = {}
    else:
      args = {'interpolation': interpolation}

    interp = dicom_proxy_blueprint._parse_interpolation(args)

    self.assertEqual(interp, expected)

  def test_parse_interpolation_unknown_raises(self):
    with self.assertRaises(ValueError):
      dicom_proxy_blueprint._parse_interpolation({'interpolation': 'unknown'})

  @parameterized.parameters(['0, 1, 2, 3'])
  def test_parse_frame_list_index_err(self, frame_lst):
    with self.assertRaises(IndexError):
      dicom_proxy_blueprint._parse_frame_list(frame_lst)

  @parameterized.parameters([' A, B, C', ' 1,  , 2', '-1, 1', '', ' '])
  def test_parse_frame_list_invalid_chars(self, frame_lst):
    with self.assertRaises(ValueError):
      dicom_proxy_blueprint._parse_frame_list(frame_lst)

  def test_parse_frame_list(self):
    self.assertEqual(
        dicom_proxy_blueprint._parse_frame_list('1, 2, 3'), [1, 2, 3]
    )

  @parameterized.parameters(['0', '101', 'A', ''])
  def test_parse_compression_quality_invalid_value(self, quality):
    with self.assertRaises(ValueError):
      dicom_proxy_blueprint._parse_compression_quality({'quality': quality})

  @parameterized.parameters([' 1 ', ' 100 '])
  def test_parse_compression_quality(self, quality):
    self.assertEqual(
        dicom_proxy_blueprint._parse_compression_quality({'quality': quality}),
        int(quality),
    )

  def test_parse_compression_quality_default(self):
    self.assertEqual(dicom_proxy_blueprint._parse_compression_quality({}), 95)

  def test_parse_downsample(self):
    value = 5.0
    self.assertEqual(
        dicom_proxy_blueprint._parse_downsample({'downsample': str(value)}),
        value,
    )

  def test_parse_downsample_no_value(self):
    self.assertEqual(dicom_proxy_blueprint._parse_downsample({}), 1.0)

  def test_parse_downsample_throws(self):
    with self.assertRaises(ValueError):
      dicom_proxy_blueprint._parse_downsample({'downsample': '0.5'})

  @parameterized.parameters(['Yes', 'True', 'On', '1'])
  def test_parse_cache_enabled_false(self, val):
    self.assertFalse(
        dicom_proxy_blueprint._parse_cache_enabled({'disable_caching': val})
    )

  @parameterized.parameters(['No', 'False', 'Off', '0'])
  def test_parse_cache_disabled_true(self, val):
    self.assertTrue(
        dicom_proxy_blueprint._parse_cache_enabled({'disable_caching': val})
    )

  def test_parse_cache_disabled_raises(self):
    with self.assertRaises(ValueError):
      dicom_proxy_blueprint._parse_cache_enabled({'disable_caching': 'ABC'})

  @parameterized.parameters([
      (' NO ', proxy_const.ICCProfile.NO),
      (' no ', proxy_const.ICCProfile.NO),
      ('srgb', proxy_const.ICCProfile.SRGB),
      ('SRGB', proxy_const.ICCProfile.SRGB),
      ('yes', proxy_const.ICCProfile.YES),
      ('adobergb', proxy_const.ICCProfile.ADOBERGB),
      ('rommrgb', proxy_const.ICCProfile.ROMMRGB),
  ])
  def test_parse_iccprofile(self, param, expected):
    self.assertEqual(
        dicom_proxy_blueprint._parse_iccprofile({'iccprofile': param}),
        expected,
    )

  @parameterized.parameters([
      (' yes ', True),
      (' true ', True),
      ('false', False),
      ('no', False),
  ])
  def test_parse_embed_icc_profile(self, param: str, expected: bool):
    self.assertEqual(
        dicom_proxy_blueprint._parse_embed_icc_profile(
            {'embed_iccprofile': param}
        ),
        expected,
    )

  def test_parse_embed_icc_profile_default(self):
    self.assertTrue(dicom_proxy_blueprint._parse_iccprofile({}))

  @flagsaver.flagsaver(disable_icc_profile_color_correction=True)
  def test_icc_profile_correction_disabled(self):
    self.assertEqual(
        dicom_proxy_blueprint._parse_iccprofile({'iccprofile': 'srgb'}),
        proxy_const.ICCProfile.NO,
    )

  @parameterized.parameters([
      (' Image/Jpeg ', _Compression.JPEG),
      ('image/png', _Compression.PNG),
      (' image/WEBP', _Compression.WEBP),
      ('image/gif', _Compression.GIF),
      ('', _Compression.JPEG),
      (None, _Compression.JPEG),
      ('*/*', _Compression.JPEG),
      ('image/jxl', _Compression.JPEG_TRANSCODED_TO_JPEGXL),
  ])
  def test_get_request_compression(self, accept_header, expected):
    self.assertEqual(
        dicom_proxy_blueprint._get_request_compression(
            accept_header, _Compression.JPEG
        ),
        expected,
    )

  def test_get_request_jpgxl_compression_source_not_jpeg(self):
    self.assertEqual(
        dicom_proxy_blueprint._get_request_compression(
            'image/jxl', _Compression.JPEGXL
        ),
        _Compression.JPEGXL,
    )

  def test_get_request_compression_throws(self):
    with self.assertRaises(ValueError):
      dicom_proxy_blueprint._get_request_compression(
          'unexpected', _Compression.JPEG
      )

  def test_parse_request_params(self):
    params = {
        'iccprofile': ' srgb ',
        'interpolation ': ' AREA ',
        'quality': ' 85 ',
        'downsample': '2.5',
        'embed_iccprofile': 'False',
    }
    header = {'accept': ' Image/Jpeg '}
    result = dicom_proxy_blueprint._parse_request_params(
        params, header, _Compression.JPEG
    )
    self.assertEqual(result.downsample, 2.5)
    self.assertEqual(result.interpolation, _Interpolation.AREA)
    self.assertEqual(result.compression, _Compression.JPEG)
    self.assertEqual(result.quality, 85)
    self.assertEqual(result.icc_profile, proxy_const.ICCProfile.SRGB)
    self.assertEqual(result.embed_iccprofile, False)

  @parameterized.parameters([Exception('bad request'), 'bad request'])
  def test_exception_flask_response(self, exp):
    response = dicom_proxy_blueprint._exception_flask_response(exp)

    self.assertEqual(response.data, b'bad request')
    self.assertEqual(response.status_code, http.HTTPStatus.BAD_REQUEST)
    self.assertEqual(response.content_type, 'text/plain')

  def test_get_rendered_frames_invalid_frames(self):
    local_instance = _LocalDicomInstance(
        shared_test_util.jpeg_encoded_pydicom_instance_cache()
    )
    frames = 'abc'
    rendered_request = True
    response = dicom_proxy_blueprint._get_rendered_frames(
        local_instance, frames, rendered_request
    )
    self.assertEqual(response.status_code, http.HTTPStatus.BAD_REQUEST)
    self.assertEqual(
        response.data, b'Frame string contains invalid chars Frames:abc'
    )

  def test_get_rendered_frames_invalid_params(self):
    local_instance = _LocalDicomInstance(
        shared_test_util.jpeg_encoded_pydicom_instance_cache(),
        {},
        {'accept': 'abc'},
    )
    frames = '1,2,3'
    rendered_request = True
    response = dicom_proxy_blueprint._get_rendered_frames(
        local_instance, frames, rendered_request
    )
    self.assertEqual(response.status_code, http.HTTPStatus.BAD_REQUEST)
    self.assertEqual(response.data, b'Unsupported compression format.')

  def test_get_rendered_frames_invalid_dimension_organization(self):
    local_instance = _LocalDicomInstance(
        shared_test_util.jpeg_encoded_pydicom_instance_cache({
            'number_of_frames': 3,
            'dimension_organization_type': 'TILED_SPARSE',
        }),
        {'downsample': '2.0'},
    )
    frames = '1,2,3'
    rendered_request = True
    response = dicom_proxy_blueprint._get_rendered_frames(
        local_instance, frames, rendered_request
    )
    self.assertEqual(response.status_code, http.HTTPStatus.BAD_REQUEST)
    self.assertEqual(
        response.data,
        dicom_proxy_blueprint._DIMENSION_ORG_ERR.encode('utf-8'),
    )

  def test_get_rendered_frames_invalid_rendered_frame_request(self):
    local_instance = _LocalDicomInstance(
        shared_test_util.jpeg_encoded_pydicom_instance_cache()
    )
    frames = '9999'
    rendered_request = True
    response = dicom_proxy_blueprint._get_rendered_frames(
        local_instance, frames, rendered_request
    )
    self.assertEqual(response.status_code, http.HTTPStatus.BAD_REQUEST)
    expected_msg = b'Requesting frame # > metadata number of frames; '
    self.assertEqual(response.data[: len(expected_msg)], expected_msg)

  def test_get_rendered_frames_rendered_request(self):
    local_instance = _LocalDicomInstance(
        shared_test_util.jpeg_encoded_pydicom_instance_cache()
    )
    frames = '1'
    rendered_request = True
    response = dicom_proxy_blueprint._get_rendered_frames(
        local_instance, frames, rendered_request
    )
    self.assertEqual(response.status_code, http.HTTPStatus.OK)
    self.assertEqual(response.content_type, 'image/jpeg')
    self.assertLen(response.data, 8418)

  def test_get_rendered_frames_request(self):
    local_instance = _LocalDicomInstance(
        shared_test_util.jpeg_encoded_pydicom_instance_cache()
    )
    frames = '1,2'
    rendered_request = False
    response = dicom_proxy_blueprint._get_rendered_frames(
        local_instance, frames, rendered_request
    )
    self.assertEqual(response.status_code, http.HTTPStatus.OK)
    self.assertStartsWith(response.content_type, 'multipart/related; boundary=')
    self.assertLen(response.data, 17846)
    multipart_data = requests_toolbelt.MultipartDecoder(
        response.data, response.content_type
    )
    self.assertLen(multipart_data.parts, 2)

  @mock.patch.object(redis.Redis, 'get', autospec=True, return_value=None)
  @mock.patch.object(
      user_auth_util,
      '_get_email_from_bearer_token',
      autospec=True,
      return_value='mock@email.com',
  )
  @flagsaver.flagsaver(validate_iap=False)
  def test_failed_get_rendered_frames_request(self, *unused_mocks):
    local_instance = _LocalDicomInstance(
        shared_test_util.jpeg_encoded_pydicom_instance_cache(
            {'dicom_transfer_syntax': metadata_util._IMPLICIT_VR_ENDIAN}
        )
    )
    params = dict(
        api_version=_MOCK_DICOM_STORE_API_VERSION,
        project='test-project',
        location='us-west1',
        dataset='bigdata',
        dicom_store='bigdicomstore',
        study='1.2.3',
        series='1.2.3.4',
        instance='1.2.3.4.5',
        authorization='test_auth',
        authority='test_authority',
        enable_caching=True,
    )
    user_auth = user_auth_util.AuthSession({
        'authorization': params['authorization'],
        'authority': params['authority'],
    })
    web_request = _DicomInstanceWebRequest(
        user_auth,
        dicom_url_util.DicomWebBaseURL(
            params['api_version'],
            params['project'],
            params['location'],
            params['dataset'],
            params['dicom_store'],
        ),
        dicom_url_util.StudyInstanceUID(params['study']),
        dicom_url_util.SeriesInstanceUID(params['series']),
        dicom_url_util.SOPInstanceUID(params['instance']),
        cache_enabled_type.CachingEnabled(params['enable_caching']),
        {},
        {'accept': 'image/png'},
        local_instance.metadata,
    )
    frames = '1'
    rendered_request = True

    transaction = dicom_url_util.download_rendered_dicom_frame(
        user_auth,
        web_request.dicom_sop_instance_url,
        1,
        render_frame_params.RenderFrameParams(),
    )
    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          transaction.url,
          status_code=http.HTTPStatus.NOT_FOUND,
          text='test_data',
          headers={b'content-type': 'image/png'},
      )
      # Mock DICOM store metadata request.
      mock_request.register_uri(
          'GET',
          'https://healthcare.googleapis.com/v1/projects/test-project/locations/us-west1/datasets/bigdata/dicomStores/bigdicomstore/dicomWeb/studies/1.2.3/series/1.2.3.4/instances?includefield=ImageType',
          status_code=http.HTTPStatus.OK,
          text=f'[{pydicom.dcmread(local_instance.cache.path).to_json()}]',
      )
      # Mock dicom store return dicom instance.
      with open(local_instance.cache.path, 'rb') as infile:
        data = infile.read()
      mock_request.register_uri(
          'GET',
          'https://healthcare.googleapis.com/v1/projects/test-project/locations/us-west1/datasets/bigdata/dicomStores/bigdicomstore/dicomWeb/studies/1.2.3/series/1.2.3.4/instances/1.2.276.0.7230010.3.1.4.296485376.89.1688794081.412405',
          status_code=http.HTTPStatus.OK,
          content=data,
      )
      response = dicom_proxy_blueprint._get_rendered_frames(
          web_request, frames, rendered_request
      )
      self.assertEqual(response.status, _http_not_found_status())
      self.assertEqual(response.content_type, 'image/png')
      self.assertEqual(response.data, b'test_data')

  def test_rendered_wsi_instance_returns_error_parameters(self):
    local_instance = _LocalDicomInstance(
        shared_test_util.jpeg_encoded_pydicom_instance_cache(),
        {'downsample': '0.5'},
    )

    result = dicom_proxy_blueprint._rendered_wsi_instance(local_instance)

    self.assertEqual(result.status, _http_bad_request_status())
    self.assertEqual(result.data, b'Invalid downsample')

  @flagsaver.flagsaver(max_number_of_frame_per_request=0)
  def test_rendered_wsi_instance_returns_downsample_error(self):
    local_instance = _LocalDicomInstance(
        shared_test_util.jpeg_encoded_pydicom_instance_cache(),
        {'downsample': '2.0'},
    )

    result = dicom_proxy_blueprint._rendered_wsi_instance(local_instance)

    self.assertEqual(result.status, _http_bad_request_status())
    self.assertEqual(result.data, b'No frames requested.')

  def test_rendered_wsi_instance_returns_success(self):
    local_instance = _LocalDicomInstance(
        shared_test_util.jpeg_encoded_pydicom_instance_cache(),
        {'downsample': '2.0'},
    )

    result = dicom_proxy_blueprint._rendered_wsi_instance(local_instance)

    self.assertEqual(result.status, _http_ok_status())
    self.assertEqual(result.content_type, 'image/jpeg')
    self.assertLen(result.data, 64547)

  @mock.patch.object(
      user_auth_util.AuthSession,
      'email',
      new_callable=mock.PropertyMock,
      return_value='mock@email.com',
  )
  @mock.patch.object(redis.Redis, 'get', autospec=True, return_value=None)
  def test_failed_get_rendered_wsi_request(self, *unused_mocks):
    local_instance = _LocalDicomInstance(
        shared_test_util.jpeg_encoded_pydicom_instance_cache()
    )
    params = dict(
        api_version=_MOCK_DICOM_STORE_API_VERSION,
        project='test-project',
        location='us-west1',
        dataset='bigdata',
        dicom_store='bigdicomstore',
        study='1.2.3',
        series='1.2.3.4',
        instance='1.2.3.4.5',
        authorization='test_auth',
        authority='test_authority',
        enable_caching=True,
    )
    user_auth = user_auth_util.AuthSession({
        'authorization': params['authorization'],
        'authority': params['authority'],
    })
    web_request = _DicomInstanceWebRequest(
        user_auth,
        dicom_url_util.DicomWebBaseURL(
            params['api_version'],
            params['project'],
            params['location'],
            params['dataset'],
            params['dicom_store'],
        ),
        dicom_url_util.StudyInstanceUID(params['study']),
        dicom_url_util.SeriesInstanceUID(params['series']),
        dicom_url_util.SOPInstanceUID(params['instance']),
        cache_enabled_type.CachingEnabled(params['enable_caching']),
        {},
        {'accept': 'image/png'},
        local_instance.metadata,
    )

    transaction = dicom_url_util.download_dicom_instance_not_transcoded(
        user_auth, web_request.dicom_sop_instance_url
    )

    with requests_mock.Mocker() as mock_request:
      mock_request.register_uri(
          'GET',
          transaction.url,
          status_code=http.HTTPStatus.NOT_FOUND,
          text='test_data',
          headers={b'content-type': 'image/png'},
      )
      response = dicom_proxy_blueprint._rendered_wsi_instance(web_request)
      self.assertEqual(response.status_code, http.HTTPStatus.NOT_FOUND)
      self.assertEqual(response.content_type, 'image/png')
      self.assertEqual(response.data, b'test_data')

  @mock.patch.object(
      flask_util,
      'get_first_key_args',
      autosepc=True,
      return_value={},
  )
  def test_augment_instance_metadata_bad_response(self, unused_mocks):
    resp = flask.Response('bad', status=400)
    result = dicom_proxy_blueprint._augment_instance_metadata(
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
    result = dicom_proxy_blueprint._augment_instance_metadata(
        _MOCK_DICOMWEBBASE_URL,
        _EMPTY_STUDY,
        _EMPTY_SERIES,
        _EMPTY_SOPINSTANCE,
        resp,
    )

    self.assertEqual(result.status, _http_ok_status())
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
    result = dicom_proxy_blueprint._augment_instance_metadata(
        _MOCK_DICOMWEBBASE_URL,
        _EMPTY_STUDY,
        _EMPTY_SERIES,
        _EMPTY_SOPINSTANCE,
        resp,
    )

    self.assertEqual(result.status, _http_ok_status())
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
    result = dicom_proxy_blueprint._augment_instance_metadata(
        _MOCK_DICOMWEBBASE_URL,
        _EMPTY_STUDY,
        _EMPTY_SERIES,
        _EMPTY_SOPINSTANCE,
        resp,
    )

    self.assertEqual(result.status, _http_ok_status())
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
    result = dicom_proxy_blueprint._augment_instance_metadata(
        _MOCK_DICOMWEBBASE_URL,
        _EMPTY_STUDY,
        _EMPTY_SERIES,
        _EMPTY_SOPINSTANCE,
        resp,
    )
    self.assertEqual(result.status, _http_bad_request_status())
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
    result = dicom_proxy_blueprint._augment_instance_metadata(
        _MOCK_DICOMWEBBASE_URL,
        _EMPTY_STUDY,
        _EMPTY_SERIES,
        _EMPTY_SOPINSTANCE,
        resp,
    )
    self.assertEqual(result.status, _http_ok_status())
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
    result = dicom_proxy_blueprint._augment_instance_metadata(
        _MOCK_DICOMWEBBASE_URL,
        _EMPTY_STUDY,
        _EMPTY_SERIES,
        _EMPTY_SOPINSTANCE,
        resp,
    )
    self.assertEqual(result.status, _http_ok_status())
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
    result = dicom_proxy_blueprint._augment_instance_metadata(
        _MOCK_DICOMWEBBASE_URL,
        _EMPTY_STUDY,
        _EMPTY_SERIES,
        _EMPTY_SOPINSTANCE,
        resp,
    )

    self.assertEqual(result.status, _http_bad_request_status())
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
      result = dicom_proxy_blueprint._augment_instance_metadata(
          _MOCK_DICOMWEBBASE_URL,
          _EMPTY_STUDY,
          _EMPTY_SERIES,
          _EMPTY_SOPINSTANCE,
          resp,
      )

    if isinstance(metadata, dict):
      metadata = [metadata]
    self.assertEqual(result.status, _http_ok_status())
    for index, downsampled_metadata in enumerate(
        json.loads(result.data.decode('utf-8'))
    ):
      for test_tag in (total_cols_tag, total_rows_tag):
        self.assertEqual(
            downsampled_metadata[test_tag][value][0],
            max(1, int(metadata[index][test_tag][value][0] / downsample)),
        )

  def test_return_icc_profile_bulkdata(self):
    tmp_dir = self.create_tempdir()
    dicom_path = os.path.join(tmp_dir, 'test_dicom.dcm')
    icc_profile = color_conversion_util._read_internal_icc_profile(
        'srgb', 'sRGB_v4_ICC_preference.icc'
    )
    with pydicom.dcmread(
        shared_test_util.jpeg_encoded_dicom_instance_test_path()
    ) as dcm:
      dcm.ICCProfile = icc_profile
      dcm.save_as(dicom_path)

    response = dicom_proxy_blueprint._dicom_instance_icc_profile_bulkdata(
        _LocalDicomInstance(
            pydicom_single_instance_read_cache.PyDicomFilePath(dicom_path)
        )
    )
    multipart_data = requests_toolbelt.MultipartDecoder(
        response.data, response.content_type
    )
    self.assertLen(multipart_data.parts, 1)
    self.assertEqual(multipart_data.parts[0].content, icc_profile)

  def test_get_wsi_instance_metadata_dicom_store_bulkdata_not_enabled_success(
      self,
  ):
    dcm = shared_test_util.jpeg_encoded_dicom_instance()
    expected_metadata = dcm.to_json_dict()
    expected_metadata.update(dcm.file_meta.to_json_dict())
    try:
      del expected_metadata[_DICOM_PIXELDATA_TAG_ADDRESS]
      del expected_metadata[_DICOM_FILE_META_INFORMATION_VERSION_TAG_ADDRESS]
    except KeyError:
      pass
    expected_metadata = [expected_metadata]
    metadata = _dicom_metadata_search(dcm, False)
    md = json.loads(metadata.data.decode('utf-8'))
    self.assertEqual(metadata.status_code, http.HTTPStatus.OK)
    self.assertLen(md, len(expected_metadata))
    for index, metadata in enumerate(md):
      self.assertEqual(metadata, expected_metadata[index])

  def test_get_wsi_annotation_metadata_dicom_store_bulkdata_not_enabled_success(
      self,
  ):
    dcm = shared_test_util.wsi_dicom_annotation_instance()
    expected_metadata = dcm.to_json_dict()
    expected_metadata.update(dcm.file_meta.to_json_dict())
    expected_metadata = [expected_metadata]
    metadata = _dicom_metadata_search(dcm, False)
    md = json.loads(metadata.data.decode('utf-8'))
    self.assertEqual(metadata.status_code, http.HTTPStatus.OK)
    self.assertLen(md, len(expected_metadata))
    for index, metadata in enumerate(md):
      self.assertEqual(metadata, expected_metadata[index])

  @parameterized.named_parameters([
      dict(
          testcase_name='WSI_ANNOTATION',
          dcm=shared_test_util.wsi_dicom_annotation_instance(),
          expected_bulkdata_tags=[],
      ),
      dict(
          testcase_name='WSI_INSTANCE',
          dcm=shared_test_util.jpeg_encoded_dicom_instance(),
          expected_bulkdata_tags=['/00020001'],
      ),
  ])
  @flagsaver.flagsaver(proxy_dicom_store_bulk_data=True)
  def test_get_instance_dicom_store_bulkdata_enabled_success(
      self, dcm, expected_bulkdata_tags
  ):
    try:
      bulkdata_util._is_debugging = False
      metadata = _dicom_instance_search(
          dcm, True, proxy_root='https://proxy.com', proxy_path='tile'
      )
      md = json.dumps(json.loads(metadata.data.decode('utf-8')), sort_keys=True)
      self.assertEqual(metadata.status_code, http.HTTPStatus.OK)
      md = re.findall(
          r'"[A-F0-9]{8}": \{"BulkDataURI":'
          r' "https://proxy.com/tile/v1/projects/proj/locations/loc/datasets/dset/dicomStores/dstore/dicomWeb/studies.*?bulkdata(/.*?)",'
          r' "vr":'
          r' "[A-Z]{2}"\}',
          md,
      )
      self.assertEqual(md, expected_bulkdata_tags)
    finally:
      bulkdata_util._is_debugging = True

  def test_get_sparse_dicom_instance_tags(self):
    dcm = _mock_sparse_dicom()
    instance_search_metadata = dcm.to_json_dict()
    instance_search_metadata.update(dcm.file_meta.to_json_dict())
    expected_metadata = copy.deepcopy(instance_search_metadata)
    del expected_metadata['00020001']
    del expected_metadata['7FE00010']
    expected_metadata = [expected_metadata]
    del instance_search_metadata['52009230']
    result = _dicom_instance_search(
        dcm,
        False,
        mock_dicom_store_response=dicom_store_mock.MockHttpResponse(
            f'studies/{dcm.StudyInstanceUID}/series/{dcm.SeriesInstanceUID}/instances?SOPInstanceUID={dcm.SOPInstanceUID}&includefield=52009230',
            dicom_store_mock.RequestMethod.GET,
            http.HTTPStatus.OK,
            json.dumps(instance_search_metadata).encode('utf-8'),
        ),
        include_field='52009230',
    )
    self.assertEqual(result.status_code, http.HTTPStatus.OK)
    md = json.loads(result.data.decode('utf-8'))
    self.assertLen(md, len(expected_metadata))
    for index, metadata in enumerate(md):
      self.assertEqual(metadata, expected_metadata[index])

  def test_get_sparse_dicom_metadata_downsample_returns_bad_request(self):
    dcm = _mock_sparse_dicom()
    instance_search_metadata = dcm.to_json_dict()
    instance_search_metadata.update(dcm.file_meta.to_json_dict())
    result = _dicom_instance_search(
        dcm, False, other_parameters={'downsample': '2.0'}
    )
    self.assertEqual(result.status_code, http.HTTPStatus.BAD_REQUEST)

  def test_get_sparse_dicom_metadata_search(self):
    dcm = _mock_sparse_dicom()
    instance_search_metadata = dcm.to_json_dict()
    instance_search_metadata.update(dcm.file_meta.to_json_dict())
    expected_metadata = copy.deepcopy(instance_search_metadata)
    del expected_metadata['00020001']
    del expected_metadata['7FE00010']
    expected_metadata = [expected_metadata]
    del instance_search_metadata['52009230']
    result = _dicom_metadata_search(
        dcm,
        False,
        mock_dicom_store_response=dicom_store_mock.MockHttpResponse(
            f'studies/{dcm.StudyInstanceUID}/series/{dcm.SeriesInstanceUID}/dcm.SOPInstanceUID/metadata',
            dicom_store_mock.RequestMethod.GET,
            http.HTTPStatus.OK,
            json.dumps(instance_search_metadata).encode('utf-8'),
        ),
    )
    self.assertEqual(result.status_code, http.HTTPStatus.OK)
    md = json.loads(result.data.decode('utf-8'))
    self.assertLen(md, len(expected_metadata))
    for index, metadata in enumerate(md):
      self.assertEqual(metadata, expected_metadata[index])

  @parameterized.named_parameters([
      dict(
          testcase_name='WSI_ANNOTATION',
          dcm=shared_test_util.wsi_dicom_annotation_instance(),
          expected_bulkdata_tags=[
              '/00020001',
              '/006A0002/0/00660016',
              '/006A0002/0/00660040',
          ],
      ),
      dict(
          testcase_name='WSI_INSTANCE',
          dcm=shared_test_util.jpeg_encoded_dicom_instance(),
          expected_bulkdata_tags=['/00020001'],
      ),
  ])
  @flagsaver.flagsaver(proxy_dicom_store_bulk_data=True)
  def test_get_instance_metadata_dicom_store_bulkdata_proxy_response_success(
      self,
      dcm,
      expected_bulkdata_tags,
  ):
    try:
      bulkdata_util._is_debugging = False
      metadata = _dicom_metadata_search(
          dcm, True, proxy_root='https://proxy.com', proxy_path='tile'
      )
      md = json.dumps(json.loads(metadata.data.decode('utf-8')), sort_keys=True)
      self.assertEqual(metadata.status_code, http.HTTPStatus.OK)
      md = re.findall(
          r'"[A-F0-9]{8}": \{"BulkDataURI":'
          r' "https://proxy.com/tile/v1/projects/proj/locations/loc/datasets/dset/dicomStores/dstore/dicomWeb/studies.*?bulkdata(/.*?)",'
          r' "vr":'
          r' "[A-Z]{2}"\}',
          md,
      )
      self.assertEqual(md, expected_bulkdata_tags)
    finally:
      bulkdata_util._is_debugging = True

  @mock.patch.object(
      bulkdata_util,
      'does_dicom_store_support_bulkdata',
      autospec=True,
      return_value=True,
  )
  def test_get_wsi_annotation_instance_tags_success(self, _):
    dcm = shared_test_util.wsi_dicom_annotation_instance()
    expected_metadata = dcm.to_json_dict()
    expected_metadata.update(dcm.file_meta.to_json_dict())
    expected_metadata = [expected_metadata]
    metadata = _dicom_instance_search(dcm, True)
    md = json.loads(metadata.data.decode('utf-8'))
    self.assertEqual(metadata.status_code, http.HTTPStatus.OK)
    self.assertLen(md, len(expected_metadata))
    for index, metadata in enumerate(md):
      self.assertEqual(metadata, expected_metadata[index])

  @mock.patch.object(
      bulkdata_util,
      'does_dicom_store_support_bulkdata',
      autospec=True,
      return_value=True,
  )
  def test_get_wsi_instance_tags_success(self, _):
    dcm = shared_test_util.jpeg_encoded_dicom_instance()
    expected_metadata = dcm.to_json_dict()
    expected_metadata.update(dcm.file_meta.to_json_dict())
    # remove PixelData from expected metadata.
    del expected_metadata[_DICOM_PIXELDATA_TAG_ADDRESS]
    expected_metadata[_DICOM_FILE_META_INFORMATION_VERSION_TAG_ADDRESS] = {
        'vr': 'OB',
        'BulkDataURI': 'https://healthcare.googleapis.com/v1/projects/proj/locations/loc/datasets/dset/dicomStores/dstore/dicomWeb/studies/1.3.6.1.4.1.11129.5.7.999.18649109954048068.740.1688792381777315/series/1.3.6.1.4.1.11129.5.7.0.1.517182092386.24422120.1688792467737634/instances/1.2.276.0.7230010.3.1.4.296485376.89.1688794081.412405/bulkdata/00020001',
    }
    expected_metadata = [expected_metadata]
    metadata = _dicom_instance_search(dcm, True)
    md = json.loads(metadata.data.decode('utf-8'))
    self.assertEqual(metadata.status_code, http.HTTPStatus.OK)
    self.assertLen(md, len(expected_metadata))
    for index, metadata in enumerate(md):
      self.assertEqual(metadata, expected_metadata[index])

  @parameterized.named_parameters([
      dict(
          testcase_name='dicom_store_error',
          dicom_store_http_response=http.HTTPStatus.BAD_REQUEST,
          dicom_store_http_bytes=b'',
          expected_msg='Error occured retrieving DICOM metadata.',
      ),
      dict(
          testcase_name='dicom_read_error',
          dicom_store_http_response=http.HTTPStatus.OK,
          dicom_store_http_bytes=b'1234',
          expected_msg='Error occured retrieving DICOM metadata.',
      ),
  ])
  def test_get_annotation_instance_tags_dicom_store_error(
      self,
      *unused_mocks,
      dicom_store_http_response,
      dicom_store_http_bytes,
      expected_msg,
  ):
    dcm = shared_test_util.wsi_dicom_annotation_instance()
    metadata = _dicom_instance_search(
        dcm,
        True,
        mock_dicom_store_response=dicom_store_mock.MockHttpResponse(
            f'studies/{dcm.StudyInstanceUID}/series/{dcm.SeriesInstanceUID}/instances/{dcm.SOPInstanceUID}',
            dicom_store_mock.RequestMethod.GET,
            dicom_store_http_response,
            dicom_store_http_bytes,
        ),
    )
    self.assertEqual(
        metadata.status_code, http.HTTPStatus.INTERNAL_SERVER_ERROR
    )
    self.assertEqual(metadata.data.decode('utf-8'), expected_msg)

  @flagsaver.flagsaver(max_augmented_metadata_download_size=100)
  def test_get_annotation_instance_read_exceeds_size_limit(self):
    dcm = shared_test_util.wsi_dicom_annotation_instance()
    metadata = _dicom_instance_search(dcm, False)
    self.assertEqual(metadata.status_code, http.HTTPStatus.BAD_REQUEST)
    self.assertEqual(
        metadata.data.decode('utf-8'),
        'DICOM instance metadata retrieval exceeded 100 byte limit.',
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='WSI_ANNOTATION',
          dcm=shared_test_util.wsi_dicom_annotation_instance(),
      ),
      dict(
          testcase_name='WSI_INSTANCE',
          dcm=shared_test_util.jpeg_encoded_dicom_instance(),
      ),
  ])
  def test_get_annotation_metadata_not_found(
      self,
      dcm,
  ):
    metadata = _dicom_metadata_search(dcm, False, add_instance_to_store=False)
    self.assertEqual(metadata.status_code, http.HTTPStatus.NOT_FOUND)

  @flagsaver.flagsaver(validate_iap=True)
  def test_invalid_iap_delete_annotation_instance_raises(self):
    self._mock_valid_jwt.return_value = False
    with self.assertRaises(werkzeug.exceptions.Unauthorized):
      dicom_proxy_blueprint._delete_annotation_instance(
          'projectid',
          'location',
          'datasetid',
          'dicomstore',
          'study_instance_uid',
          'series_instance_uid',
          'instance_uid',
      )

  @flagsaver.flagsaver(validate_iap=True)
  def test_invalid_iap_store_annotation_instance_raises(self):
    self._mock_valid_jwt.return_value = False
    with self.assertRaises(werkzeug.exceptions.Unauthorized):
      dicom_proxy_blueprint._store_annotation_instance(
          'projectid',
          'location',
          'datasetid',
          'dicomstore',
      )

  def test_stream_metadata_response_empty_list(self):
    self.assertEqual(
        list(dicom_proxy_blueprint._stream_metadata_response([])), ['[]']
    )

  def test_stream_metadata_response_skips_empty_results(self):
    self.assertEqual(
        list(
            dicom_proxy_blueprint._stream_metadata_response(['abc', '', '123'])
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
          list(dicom_proxy_blueprint._stream_metadata_response(['abc', '123'])),
          expected,
      )

  @flagsaver.flagsaver(streaming_chunksize=1)
  def test_stream_metadata_response_from_futures(self):
    with futures.ThreadPoolExecutor(max_workers=10) as pool:
      future_list = [pool.submit(lambda x: x, txt) for txt in ('abc', '123')]
      self.assertEqual(
          list(dicom_proxy_blueprint._stream_metadata_response(future_list)),
          list('[abc,123]'),
      )

  def test_get_frame_instance(self):
    self.assertLen(_get_frame_instance(True, False).data, 8604)

  def test_get_frame_instance_rendered_request(self):
    self.assertLen(_get_frame_instance(True, True).data, 8418)

  def test_get_instance_rendered(self):
    self.assertLen(_get_instance_rendered(True).data, 180747)

  @parameterized.parameters(['', '?a=132'])
  def test_missing_series_uid_redirect(self, param):
    root = 'https://proxy.com'
    dcm = shared_test_util.jpeg_encoded_dicom_instance()
    base_url = _MOCK_DICOMWEBBASE_URL
    with _MissingSeriesRedirectTestContext(root, base_url, dcm, param, True):
      response = dicom_proxy_blueprint._missing_series_uid_redirect(
          base_url, dcm.StudyInstanceUID, dcm.SOPInstanceUID, '/rendered'
      )
    url = f'{root}/tile/{base_url}/studies/{dcm.StudyInstanceUID}/series/{dcm.SeriesInstanceUID}/instances/{dcm.SOPInstanceUID}/rendered{param}'
    self.assertIn(url.encode('utf-8'), response.data)

  @mock.patch.object(
      dicom_store_util,
      'dicom_store_proxy',
      autospec=True,
      return_value='PROXY_RESPONSE',
  )
  def test_missing_series_uid_redirect_can_not_find_series_proxy_response(
      self, _
  ):
    dcm = shared_test_util.jpeg_encoded_dicom_instance()
    base_url = _MOCK_DICOMWEBBASE_URL
    with _MissingSeriesRedirectTestContext(
        'https://proxy.com', base_url, dcm, '', False
    ):
      response = dicom_proxy_blueprint._missing_series_uid_redirect(
          base_url, dcm.StudyInstanceUID, dcm.SOPInstanceUID, '/rendered'
      )
    self.assertEqual(response, 'PROXY_RESPONSE')

  def test_redirect_render_instance_missing_series_query(self):
    root = 'https://proxy.com'
    dcm = shared_test_util.jpeg_encoded_dicom_instance()
    base_url = _MOCK_DICOMWEBBASE_URL
    with _MissingSeriesRedirectTestContext(root, base_url, dcm, '', True):
      response = (
          dicom_proxy_blueprint._redirect_render_instance_missing_series_query(
              base_url.dicom_store_api_version,
              base_url.gcp_project_id,
              base_url.location,
              base_url.dataset_id,
              base_url.dicom_store,
              dcm.StudyInstanceUID,
              dcm.SOPInstanceUID,
          )
      )
    url = f'{root}/tile/{base_url}/studies/{dcm.StudyInstanceUID}/series/{dcm.SeriesInstanceUID}/instances/{dcm.SOPInstanceUID}/rendered'
    self.assertIn(url.encode('utf-8'), response.data)

  @parameterized.named_parameters([
      dict(
          testcase_name='frame_request', is_rendered=False, expected_suffix=''
      ),
      dict(
          testcase_name='rendered_frame_request',
          is_rendered=True,
          expected_suffix='/rendered',
      ),
  ])
  def test_redirect_frame_instance_missing_series_query(
      self, is_rendered, expected_suffix
  ):
    root = 'https://proxy.com'
    dcm = shared_test_util.jpeg_encoded_dicom_instance()
    base_url = _MOCK_DICOMWEBBASE_URL
    with _MissingSeriesRedirectTestContext(root, base_url, dcm, '', True):
      response = (
          dicom_proxy_blueprint._redirect_frame_instance_missing_series_query(
              base_url.dicom_store_api_version,
              base_url.gcp_project_id,
              base_url.location,
              base_url.dataset_id,
              base_url.dicom_store,
              dcm.StudyInstanceUID,
              dcm.SOPInstanceUID,
              '1',
              is_rendered,
          )
      )
    url = f'{root}/tile/{base_url}/studies/{dcm.StudyInstanceUID}/series/{dcm.SeriesInstanceUID}/instances/{dcm.SOPInstanceUID}/frames/1{expected_suffix}'
    self.assertIn(url.encode('utf-8'), response.data)


if __name__ == '__main__':
  absltest.main()
