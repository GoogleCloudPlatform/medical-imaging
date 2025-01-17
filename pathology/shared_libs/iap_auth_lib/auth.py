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
"""Validates the JWT header generated from IAP.

See https://cloud.google.com/iap/docs/signed-headers-howto and
https://cloud.google.com/python/docs/getting-started/authenticate-users.
"""
import functools
import http
from typing import Any, Callable, Mapping, Union

from absl import flags
from absl import logging
import flask
from google.auth import exceptions
from google.auth.transport import requests
from google.oauth2 import id_token

from pathology.shared_libs.flags import secret_flag_utils

VALIDATE_IAP_FLG = flags.DEFINE_bool(
    'validate_iap',
    secret_flag_utils.get_bool_secret_or_env('VALIDATE_IAP', True),
    'Whether to validate IAP.',
)
JWT_AUD_FLG = flags.DEFINE_string(
    'jwt_audience',
    # Backend service default/dpas-orchestrator-service
    # Project dpas-digipat in organization cloudflyer.info
    secret_flag_utils.get_secret_or_env(
        'JWT_AUDIENCE',
        None
    ),
    'JWT audience of this backend service.',
)

# The URL for public key used in JWT. Please see
# https://cloud.google.com/iap/docs/signed-headers-howto#verifying_the_jwt_header
# for more details.
_CERTS_URL = 'https://www.gstatic.com/iap/verify/public_key'
# The header to store signed JWT. Please see
# https://cloud.google.com/python/docs/getting-started/authenticate-users
# for more details.
IAP_JWT_HEADER = 'X-Goog-IAP-JWT-Assertion'


def _get_flask_headers() -> Mapping[str, str]:
  return flask.request.headers


def validate_iap(request_handler: Callable[..., Any]) -> Callable[..., Any]:
  """Decorator used in the endpoints to validate IAP JWT assertion header."""

  @functools.wraps(request_handler)
  def wrapper(*args, **kwargs):
    """Obtains the IAP JWT assertion from the header and validates it.

    Args:
      *args: from the parent function.
      **kwargs: from the parent function.

    Returns:
      The function it wraps if the token is valid. Otherwise, returns an
      UNAUTHORIZED http status code.
    """
    if not VALIDATE_IAP_FLG.value:
      return request_handler(*args, **kwargs)

    token = _get_flask_headers().get(IAP_JWT_HEADER, None)
    if _is_valid(token, JWT_AUD_FLG.value):
      return request_handler(*args, **kwargs)

    return flask.abort(http.HTTPStatus.UNAUTHORIZED)

  return wrapper


def _is_valid(iap_jwt_token: Union[str, bytes], expected_audience: str) -> bool:
  """Checks whether an IAP JWT is valid.

  Implemented based on
  https://cloud.google.com/iap/docs/signed-headers-howto#retrieving_the_user_identity.

  Args:
    iap_jwt_token: The contents of the X-Goog-IAP-JWT-Assertion header.
    expected_audience: The Signed Header JWT audience. See
      https://cloud.google.com/iap/docs/signed-headers-howto for details on how
        to get this value.

  Returns:
    Bool to indicate whether the iap_jwt_token is valid.
  """

  try:
    decoded_jwt = id_token.verify_token(
        iap_jwt_token,
        requests.Request(),
        audience=expected_audience,
        certs_url=_CERTS_URL,
    )
  except (ValueError, exceptions.TransportError):
    logging.exception('IAP JWT validation failed.')
    return False

  if decoded_jwt.get('email', None):
    return True

  logging.error('No email in IAP JWT.')
  return False
