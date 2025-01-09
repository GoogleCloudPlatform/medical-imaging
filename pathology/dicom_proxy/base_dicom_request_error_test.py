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
"""Tests for base dicom request error."""
from absl.testing import absltest
from absl.testing import parameterized
import requests
import requests_mock

from pathology.dicom_proxy import base_dicom_request_error

_TEST_URL = 'http://foo.com'
_MOCK_REQUEST_METHOD = 'GET'


class BaseDicomRequestErrorTest(parameterized.TestCase):

  def test_none_raw_response(self):
    error = base_dicom_request_error.BaseDicomRequestError(
        'BaseDicomRequestError', requests.Response()
    )
    self.assertEqual(
        str(error),
        (
            'BaseDicomRequestError; status:None; headers:{}; '
            'data:Request.response.raw is None. No data returned.'
        ),
    )

  def _setup_mock_request(self, mk_request):
    mk_request.register_uri(
        _MOCK_REQUEST_METHOD,
        _TEST_URL,
        text='Mock_Data',
        headers={'foo': 'bar'},
        status_code=404,
    )

  @parameterized.parameters([True, False])
  @requests_mock.Mocker()
  def test_mk_response(self, stream, mk_request):
    self._setup_mock_request(mk_request)
    with requests.Session() as session:
      response = session.get(_TEST_URL, stream=stream)
      error = base_dicom_request_error.BaseDicomRequestError(
          'BaseDicomRequestError', response
      )
    self.assertEqual(
        str(error),
        (
            "BaseDicomRequestError; status:404; headers:{'foo': 'bar'}; "
            'data:Mock_Data'
        ),
    )

  @requests_mock.Mocker()
  def test_mk_response_decoder_error(self, mk_request):
    mk_request.register_uri(
        _MOCK_REQUEST_METHOD, _TEST_URL, content=b'\xC2\xC2', status_code=404
    )
    with requests.Session() as session:
      response = session.get(_TEST_URL, stream=True)
      error = base_dicom_request_error.BaseDicomRequestError(
          'BaseDicomRequestError', response
      )
    self.assertEqual(
        str(error),
        (
            'BaseDicomRequestError; status:404; headers:{}; data:Unicode error'
            ' occurred decoding response; response may be binary.'
        ),
    )

  @requests_mock.Mocker()
  def test_mk_response_with_msg(self, mk_request):
    msg = 'test_123'
    self._setup_mock_request(mk_request)
    with requests.Session() as session:
      response = session.get(_TEST_URL, stream=True)
      error = base_dicom_request_error.BaseDicomRequestError(
          'BaseDicomRequestError', response, msg
      )
    self.assertEqual(
        str(error),
        (
            f'BaseDicomRequestError {msg}; '
            "status:404; headers:{'foo': 'bar'}; data:Mock_Data"
        ),
    )

  @requests_mock.Mocker()
  def test_mk_response_with_named_msg(self, mk_request):
    msg = 'test_123'
    self._setup_mock_request(mk_request)
    with requests.Session() as session:
      response = session.get(_TEST_URL, stream=True)
      error = base_dicom_request_error.BaseDicomRequestError(
          'BaseDicomRequestError', response, msg=msg
      )
    self.assertEqual(
        str(error),
        (
            f'BaseDicomRequestError {msg}; '
            "status:404; headers:{'foo': 'bar'}; data:Mock_Data"
        ),
    )

  @requests_mock.Mocker()
  def test_flask_response(self, mk_request):
    msg = 'test_123'
    self._setup_mock_request(mk_request)
    with requests.Session() as session:
      response = session.get(_TEST_URL, stream=True)
      error = base_dicom_request_error.BaseDicomRequestError(
          'BaseDicomRequestError', response, msg=msg
      )
    response = error.flask_response()
    self.assertEqual(response.status_code, 404)
    self.assertLen(response.headers, 1)
    self.assertEqual(response.headers['foo'], 'bar')
    self.assertEqual(response.data, b'Mock_Data')


if __name__ == '__main__':
  absltest.main()
