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
"""Util methods for logging in DICOM Proxy."""

import time
from typing import Callable, Mapping

import flask

from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import flask_util
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.logging_lib import cloud_logging_client

_REMOVED_FROM_LOG = 'removed from log'


def mask_privileged_header_values(
    headers: Mapping[str, str]
) -> Mapping[str, str]:
  """Returns a copy of headers dictionary with privlaged values masked."""
  log_headers = dict(headers)
  header_key_index = {key.strip().lower(): key for key in log_headers}
  for (
      search_key
  ) in dicom_proxy_flags.KEYWORDS_TO_MASK_FROM_CONNECTION_HEADER_LOG_FLG.value:
    log_key = header_key_index.get(search_key.strip().lower(), '')
    if log_key:
      log_headers[log_key] = _REMOVED_FROM_LOG
  return log_headers


def log_exceptions(
    func: Callable[..., flask.Response]
) -> Callable[..., flask.Response]:
  """Decorator for endpoints to add traceability and log uncaught exceptions.

  Args:
    func: Function to decorate.

  Returns:
    Decorated function.
  """

  def inner1(*args, **kwargs) -> flask.Response:
    try:
      try:
        credentials = user_auth_util.AuthSession(flask_util.get_headers())
        iap_user_id = credentials.iap_user_id
        email = credentials.email
      except user_auth_util.UserEmailRetrievalError:
        email = 'unable_to_retrieve_email'
        iap_user_id = ''
      request_time = str(time.time())
      url_request = flask_util.get_full_request_url()
      log_signature = {
          proxy_const.LogKeywords.HTTP_REQUEST: (
              f'{flask_util.get_method()}: {url_request}'
          ),
      }
      if iap_user_id:
        instance = f'{url_request}_{email}_{iap_user_id}_{request_time}'
        log_signature.update({
            proxy_const.LogKeywords.DICOM_PROXY_SESSION_KEY: instance,
            proxy_const.HeaderKeywords.IAP_EMAIL_KEY: email,
            proxy_const.HeaderKeywords.IAP_USER_ID_KEY: iap_user_id,
        })
      else:
        instance = f'{url_request}_{email}_{request_time}'
        log_signature.update({
            proxy_const.LogKeywords.DICOM_PROXY_SESSION_KEY: instance,
            proxy_const.LogKeywords.USER_BEARER_TOKEN_EMAIL: email,
        })
      cloud_logging_client.set_log_signature(log_signature)
      return func(*args, **kwargs)
    except Exception as exp:
      cloud_logging_client.error('An unexpected exception occurred.', exp)
      raise

  return inner1


def log_exceptions_in_health_check(
    func: Callable[..., flask.Response]
) -> Callable[..., flask.Response]:
  """Decorator for healthcheck to add traceability and log uncaught exceptions.

  Args:
    func: Function to decorate.

  Returns:
    Decorated function.
  """

  def inner1(*args, **kwargs) -> flask.Response:
    try:
      request_time = str(time.time())
      url_request = flask_util.get_full_request_url()
      instance = f'{url_request}_{request_time}'
      cloud_logging_client.set_log_signature({
          proxy_const.LogKeywords.DICOM_PROXY_SESSION_KEY: instance,
          proxy_const.LogKeywords.HTTP_REQUEST: url_request,
      })
      return func(*args, **kwargs)
    except Exception as exp:
      cloud_logging_client.error(
          'An unexpected exception occurred in the health check.',
          exp,
      )
      raise

  return inner1
