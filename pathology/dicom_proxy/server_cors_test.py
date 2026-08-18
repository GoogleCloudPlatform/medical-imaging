# Copyright 2026 Google LLC
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
"""Tests for server.py CORS middleware (handle_preflight_methods and add_security_headers)."""

import http
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized

# Patch flask_compress before importing the server module so that the decorator
# applied at import time is a no-op in tests.
mock.patch('flask_compress.Compress.compressed', lambda x: lambda x: x).start()

from pathology.dicom_proxy import server  # pylint: disable=g-import-not-at-top

_ALLOWED_ORIGIN = 'https://allowed-client.example.com'
_BLOCKED_ORIGIN = 'https://blocked-client.example.com'


class HandlePreflightMethodsTest(parameterized.TestCase):
  """Tests for the before_request CORS preflight handler."""

  def setUp(self):
    super().setUp()
    self.client = server.flask_app.test_client()

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN], cors_max_age=3600)
  def test_options_approved_origin_returns_204(self):
    response = self.client.options(
        '/healthcheck',
        headers={
            'Origin': _ALLOWED_ORIGIN,
            'Access-Control-Request-Method': 'GET',
            'Access-Control-Request-Headers': 'accept',
        },
    )
    self.assertEqual(response.status_code, http.HTTPStatus.NO_CONTENT)

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN], cors_max_age=3600)
  def test_options_approved_origin_has_allow_origin_header(self):
    response = self.client.options(
        '/healthcheck',
        headers={
            'Origin': _ALLOWED_ORIGIN,
            'Access-Control-Request-Method': 'GET',
        },
    )
    self.assertEqual(
        response.headers.get('Access-Control-Allow-Origin'), _ALLOWED_ORIGIN
    )

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN], cors_max_age=3600)
  def test_options_approved_origin_has_max_age_header(self):
    response = self.client.options(
        '/healthcheck',
        headers={
            'Origin': _ALLOWED_ORIGIN,
            'Access-Control-Request-Method': 'GET',
        },
    )
    self.assertEqual(response.headers.get('Access-Control-Max-Age'), '3600')

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN], cors_max_age=3600)
  def test_options_approved_origin_has_allow_methods_header(self):
    response = self.client.options(
        '/healthcheck',
        headers={
            'Origin': _ALLOWED_ORIGIN,
            'Access-Control-Request-Method': 'GET',
        },
    )
    self.assertIn('GET', response.headers.get('Access-Control-Allow-Methods'))

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN], cors_max_age=3600)
  def test_options_approved_origin_reflects_requested_headers(self):
    response = self.client.options(
        '/healthcheck',
        headers={
            'Origin': _ALLOWED_ORIGIN,
            'Access-Control-Request-Method': 'GET',
            'Access-Control-Request-Headers': 'authorization,accept',
        },
    )
    self.assertEqual(
        response.headers.get('Access-Control-Allow-Headers'),
        'authorization,accept',
    )

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN], cors_max_age=3600)
  def test_options_blocked_origin_returns_403(self):
    response = self.client.options(
        '/healthcheck',
        headers={
            'Origin': _BLOCKED_ORIGIN,
            'Access-Control-Request-Method': 'GET',
        },
    )
    self.assertEqual(response.status_code, http.HTTPStatus.FORBIDDEN)

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN], cors_max_age=3600)
  def test_options_blocked_origin_has_no_cors_headers(self):
    response = self.client.options(
        '/healthcheck',
        headers={
            'Origin': _BLOCKED_ORIGIN,
            'Access-Control-Request-Method': 'GET',
        },
    )
    self.assertIsNone(response.headers.get('Access-Control-Allow-Origin'))

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN], cors_max_age=3600)
  def test_options_no_origin_returns_403(self):
    response = self.client.options(
        '/healthcheck',
        headers={'Access-Control-Request-Method': 'GET'},
    )
    self.assertEqual(response.status_code, http.HTTPStatus.FORBIDDEN)

  @flagsaver.flagsaver(
      origins=[_ALLOWED_ORIGIN],
      cors_max_age=3600,
      allow_credentials=True,
  )
  def test_options_approved_origin_allow_credentials_header_present(self):
    response = self.client.options(
        '/healthcheck',
        headers={
            'Origin': _ALLOWED_ORIGIN,
            'Access-Control-Request-Method': 'GET',
        },
    )
    self.assertEqual(
        response.headers.get('Access-Control-Allow-Credentials'), 'true'
    )

  @flagsaver.flagsaver(
      origins=[_ALLOWED_ORIGIN],
      cors_max_age=3600,
      allow_credentials=False,
  )
  def test_options_approved_origin_no_credentials_header_when_disabled(self):
    response = self.client.options(
        '/healthcheck',
        headers={
            'Origin': _ALLOWED_ORIGIN,
            'Access-Control-Request-Method': 'GET',
        },
    )
    self.assertIsNone(response.headers.get('Access-Control-Allow-Credentials'))

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN], cors_max_age=3600)
  def test_options_unsupported_method_returns_405(self):
    response = self.client.options(
        '/healthcheck',
        headers={
            'Origin': _ALLOWED_ORIGIN,
            'Access-Control-Request-Method': 'PATCH',
        },
    )
    self.assertEqual(response.status_code, http.HTTPStatus.METHOD_NOT_ALLOWED)

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN], cors_max_age=0)
  def test_options_max_age_zero(self):
    response = self.client.options(
        '/healthcheck',
        headers={
            'Origin': _ALLOWED_ORIGIN,
            'Access-Control-Request-Method': 'GET',
        },
    )
    self.assertEqual(response.headers.get('Access-Control-Max-Age'), '0')

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN.upper()], cors_max_age=3600)
  def test_options_origin_matching_is_case_insensitive(self):
    """Origin header matching should be case-insensitive."""
    response = self.client.options(
        '/healthcheck',
        headers={
            'Origin': _ALLOWED_ORIGIN.lower(),
            'Access-Control-Request-Method': 'GET',
        },
    )
    self.assertEqual(response.status_code, http.HTTPStatus.NO_CONTENT)


class AddSecurityHeadersTest(parameterized.TestCase):
  """Tests for the after_request security headers hook."""

  def setUp(self):
    super().setUp()
    self.client = server.flask_app.test_client()

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN])
  def test_get_approved_origin_returns_allow_origin(self):
    response = self.client.get('/', headers={'Origin': _ALLOWED_ORIGIN})
    self.assertEqual(
        response.headers.get('Access-Control-Allow-Origin'), _ALLOWED_ORIGIN
    )

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN])
  def test_get_approved_origin_returns_expose_headers(self):
    response = self.client.get('/', headers={'Origin': _ALLOWED_ORIGIN})
    expose = response.headers.get('Access-Control-Expose-Headers')
    self.assertIsNotNone(expose)
    self.assertIn('Content-Location', expose)
    self.assertIn('Warning', expose)

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN])
  def test_get_blocked_origin_no_allow_origin_header(self):
    response = self.client.get('/', headers={'Origin': _BLOCKED_ORIGIN})
    self.assertIsNone(response.headers.get('Access-Control-Allow-Origin'))

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN])
  def test_get_blocked_origin_no_expose_headers(self):
    response = self.client.get('/', headers={'Origin': _BLOCKED_ORIGIN})
    self.assertIsNone(response.headers.get('Access-Control-Expose-Headers'))

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN])
  def test_response_has_x_frame_options(self):
    response = self.client.get('/', headers={'Origin': _ALLOWED_ORIGIN})
    self.assertEqual(response.headers.get('X-Frame-Options'), 'SAMEORIGIN')

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN])
  def test_response_has_x_xss_protection(self):
    response = self.client.get('/', headers={'Origin': _ALLOWED_ORIGIN})
    self.assertEqual(response.headers.get('X-XSS-Protection'), '0')

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN], allow_credentials=True)
  def test_get_approved_origin_allow_credentials_when_enabled(self):
    response = self.client.get('/', headers={'Origin': _ALLOWED_ORIGIN})
    self.assertEqual(
        response.headers.get('Access-Control-Allow-Credentials'), 'true'
    )

  @flagsaver.flagsaver(origins=[_ALLOWED_ORIGIN], allow_credentials=False)
  def test_get_approved_origin_no_credentials_when_disabled(self):
    response = self.client.get('/', headers={'Origin': _ALLOWED_ORIGIN})
    self.assertIsNone(response.headers.get('Access-Control-Allow-Credentials'))


if __name__ == '__main__':
  absltest.main()
