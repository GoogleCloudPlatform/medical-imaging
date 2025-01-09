# Copyright 2024 Google LLC
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

"""Utility to process gRPC context and authenticate user with OAuth."""

import http
import json
from typing import Dict, Optional

import google.auth
import google.auth.transport.requests
import grpc
import requests

from pathology.shared_libs.logging_lib import cloud_logging_client

_METADATA_AUTHORIZATION_KEY = 'authorization'
_BEARER_KEYWORD = 'Bearer'
_REQUEST_PATH = 'https://www.googleapis.com/userinfo/v2/me'

_GRPC_TO_HTTP_CODE_MAP = {
    grpc.StatusCode.OK: http.HTTPStatus.OK,
    grpc.StatusCode.PERMISSION_DENIED: http.HTTPStatus.FORBIDDEN,
    grpc.StatusCode.NOT_FOUND: http.HTTPStatus.NOT_FOUND,
    grpc.StatusCode.ALREADY_EXISTS: http.HTTPStatus.CONFLICT,
    grpc.StatusCode.INVALID_ARGUMENT: http.HTTPStatus.BAD_REQUEST,
    grpc.StatusCode.FAILED_PRECONDITION: http.HTTPStatus.BAD_REQUEST,
    grpc.StatusCode.INTERNAL: http.HTTPStatus.INTERNAL_SERVER_ERROR,
    grpc.StatusCode.UNKNOWN: http.HTTPStatus.INTERNAL_SERVER_ERROR
}


def _add_auth_to_header(msg_headers: Dict[str, str]) -> Dict[str, str]:
  """Updates credentials and adds to header for HTTP requests.

  Args:
    msg_headers : Header to pass to HTTP request.

  Returns:
    header with authentication added
  """
  auth_credentials = google.auth.default(scopes=[
      'https://www.googleapis.com/auth/userinfo.email',
      'https://www.googleapis.com/auth/userinfo.profile'
  ])[0]
  auth_credentials.apply(msg_headers)
  if not auth_credentials.valid:
    auth_req = google.auth.transport.requests.Request()
    auth_credentials.refresh(auth_req)
  return msg_headers


def get_email(context: grpc.ServicerContext) -> Optional[str]:
  """Extracts the email of the rpc caller.

  Args:
    context: grpc ServicerContext to extract caller from.

  Returns:
    str - email of caller.
  """
  token = get_token(context)
  session = requests.Session()
  headers = {'Authorization': f'{_BEARER_KEYWORD} {token}'}
  headers = _add_auth_to_header(headers)
  try:
    response = session.get(_REQUEST_PATH, headers=headers).json()
    return response['email']
  except (requests.HTTPError, json.JSONDecodeError, KeyError) as ex:
    cloud_logging_client.logger().info(
        f'Exception retrieving user profile info {ex}.', ex)
    return None


def get_token(context: grpc.ServicerContext) -> Optional[str]:
  """Extracts OAuth2 access token from ServicerContext.

  Args:
    context: grpc ServicerContext to extract token from.

  Returns:
    str - Oauth token
  """
  for key, value in context.invocation_metadata():
    if key == _METADATA_AUTHORIZATION_KEY:
      tokenized_tokens = value.split()
      if len(tokenized_tokens) == 2 and tokenized_tokens[0] == _BEARER_KEYWORD:
        return tokenized_tokens[1]
  return None


def convert_grpc_code_to_http(code: grpc.StatusCode) -> http.HTTPStatus:
  if code in _GRPC_TO_HTTP_CODE_MAP.keys():
    return _GRPC_TO_HTTP_CODE_MAP[code]
  else:
    return http.HTTPStatus.INTERNAL_SERVER_ERROR
