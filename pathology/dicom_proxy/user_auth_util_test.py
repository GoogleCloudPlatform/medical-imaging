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
"""Tests for user auth util."""
import http
import json
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import cachetools
import google.auth.transport.requests
import requests_mock

from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import user_auth_util


class _MockDefaultCredential:

  def __init__(self, token: str):
    self._token = token
    self._refreshed = False

  def refresh(self, request: google.auth.transport.requests.Request) -> None:
    del request
    self._refreshed = True

  @property
  def token(self) -> str:
    if not self._refreshed:
      raise ValueError('Error token is invalid. Has not been refreshed.')
    return self._token


class UserAuthUtilTest(parameterized.TestCase):

  @parameterized.parameters(
      [('A', {'A': 1}), ('a', {'a': 1}), ('z', {}), ('Z', {})]
  )
  def test_add_key_if_defined_in_source(self, source_key, expected):
    source_dict = {'a': 1, 'b': 2, 'c': 3, 'd': 4}
    dest_dict = {}
    user_auth_util._add_key_if_defined_in_source(
        dest_dict, source_dict, source_key
    )
    self.assertEqual(dest_dict, expected)

  @parameterized.parameters([
      ({}, {}),
      (
          {proxy_const.HeaderKeywords.AUTH_HEADER_KEY.lower(): '1.2.3'},
          {proxy_const.HeaderKeywords.AUTH_HEADER_KEY.lower(): '1.2.3'},
      ),
      (
          {proxy_const.HeaderKeywords.AUTH_HEADER_KEY.upper(): '1.2.3'},
          {proxy_const.HeaderKeywords.AUTH_HEADER_KEY.lower(): '1.2.3'},
      ),
      (
          {
              proxy_const.HeaderKeywords.AUTH_HEADER_KEY.lower(): '1.2.3',
              proxy_const.HeaderKeywords.AUTHORITY_HEADER_KEY.lower(): 'abc',
          },
          {
              proxy_const.HeaderKeywords.AUTH_HEADER_KEY.lower(): '1.2.3',
              proxy_const.HeaderKeywords.AUTHORITY_HEADER_KEY.lower(): 'abc',
          },
      ),
      (
          {proxy_const.HeaderKeywords.AUTHORITY_HEADER_KEY: 'abc'},
          {proxy_const.HeaderKeywords.AUTHORITY_HEADER_KEY: 'abc'},
      ),
  ])
  @flagsaver.flagsaver(enable_app_default_credentials=False)
  def test_auth_session_add_to_header(self, auth_header, expected_header):
    test_header = {}
    user_auth = user_auth_util.AuthSession(auth_header)
    self.assertEqual(user_auth.add_to_header(test_header), expected_header)
    self.assertEqual(
        user_auth.authorization,
        expected_header.get(proxy_const.HeaderKeywords.AUTH_HEADER_KEY, ''),
    )
    self.assertEqual(
        user_auth.authority,
        expected_header.get(
            proxy_const.HeaderKeywords.AUTHORITY_HEADER_KEY, ''
        ),
    )

  @flagsaver.flagsaver(validate_iap=True)
  def test_auth_session_iap_user_email(self):
    user_auth = user_auth_util.AuthSession(
        {
            proxy_const.HeaderKeywords.IAP_USER_ID_KEY: 'foo',
            proxy_const.HeaderKeywords.IAP_EMAIL_KEY: 'bar',
        },
    )
    self.assertEqual(user_auth.email, 'bar')

  @flagsaver.flagsaver(validate_iap=True)
  def test_auth_session_iap_user_id(self):
    user_auth = user_auth_util.AuthSession(
        {
            proxy_const.HeaderKeywords.IAP_USER_ID_KEY: 'foo',
            proxy_const.HeaderKeywords.IAP_EMAIL_KEY: 'bar',
        },
    )
    self.assertEqual(user_auth.iap_user_id, 'foo')

  @flagsaver.flagsaver(validate_iap=True)
  def test_auth_session_iap_missing_user_email_raises(self):
    user_auth = user_auth_util.AuthSession(
        {
            proxy_const.HeaderKeywords.IAP_USER_ID_KEY: 'foo',
        },
    )
    with self.assertRaises(user_auth_util.UserEmailRetrievalError):
      _ = user_auth.email

  @mock.patch.object(google.auth, 'default', autospec=True)
  @flagsaver.flagsaver(enable_app_default_credentials=True, validate_iap=True)
  def test_auth_session_application_default_credentials(self, mock_auth_def):
    mock_token = 'mock_token'
    service_account_email = 'service@account.org'
    mock_auth_def.return_value = (
        _MockDefaultCredential(mock_token),
        'mock_gcp',
    )
    with requests_mock.Mocker() as mock_request:
      mock_request.get(
          user_auth_util._USER_INFO_REQUEST_URL,
          status_code=http.client.OK,
          text=json.dumps(
              {'email': service_account_email, 'verified_email': True}
          ),
      )
      auth = user_auth_util.AuthSession(None)
      self.assertEqual(auth.authorization, f'Bearer {mock_token}')
      self.assertEqual(auth.email, service_account_email)

  def test_init_fork_module_state(self):
    user_auth_util._auth_cache = None
    user_auth_util._auth_cache_lock = None
    user_auth_util._init_fork_module_state()
    self.assertIsInstance(user_auth_util._auth_cache, cachetools.LRUCache)
    self.assertIsNotNone(user_auth_util._auth_cache_lock)


if __name__ == '__main__':
  absltest.main()
