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
"""Tests for auth."""
from unittest import mock

from absl.testing import absltest
from google.auth.transport import requests

from pathology.shared_libs.iap_auth_lib import auth

_AUDIENCE = '/projects/project_id/global/backendServices/service_id'


class AuthTest(absltest.TestCase):

  @mock.patch.object(requests, 'Request')
  @mock.patch('google.oauth2.id_token.verify_token', autospec=True)
  def test_valid_jwt(self, mock_verify_token, mock_request):
    """Verify a call with a valid JWT."""
    mock_verify_token.return_value = {'email': 'foobar@abc.com'}

    valid = auth._is_valid(mock.sentinel.token, _AUDIENCE)

    mock_verify_token.assert_called_once_with(
        mock.sentinel.token,
        mock_request.return_value,
        audience=_AUDIENCE,
        certs_url='https://www.gstatic.com/iap/verify/public_key',
    )
    self.assertTrue(valid)

  @mock.patch.object(requests, 'Request')
  @mock.patch('google.oauth2.id_token.verify_token', autospec=True)
  def test_no_email_jwt(self, mock_verify_token, mock_request):
    """Verify a call with a valid JWT without email."""
    mock_verify_token.return_value = {}

    with self.assertLogs(level='ERROR') as log:
      valid = auth._is_valid(mock.sentinel.token, _AUDIENCE)

    mock_verify_token.assert_called_once_with(
        mock.sentinel.token,
        mock_request.return_value,
        audience=_AUDIENCE,
        certs_url='https://www.gstatic.com/iap/verify/public_key',
    )
    self.assertFalse(valid)
    self.assertEqual('No email in IAP JWT.', log[0][0].message)

  @mock.patch.object(requests, 'Request')
  @mock.patch('google.oauth2.id_token.verify_token', autospec=True)
  def test_invalid_jwt(self, mock_verify_token, mock_request):
    """Verify a call with an invalid JWT."""
    mock_verify_token.side_effect = ValueError(
        'Could not verify token signature.'
    )

    with self.assertLogs(level='ERROR') as log:
      valid = auth._is_valid(mock.sentinel.token, _AUDIENCE)

    mock_verify_token.assert_called_once_with(
        mock.sentinel.token,
        mock_request.return_value,
        audience=_AUDIENCE,
        certs_url='https://www.gstatic.com/iap/verify/public_key',
    )
    self.assertFalse(valid)
    self.assertEqual('IAP JWT validation failed.', log[0][0].message)


if __name__ == '__main__':
  absltest.main()
