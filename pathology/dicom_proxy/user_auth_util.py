# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

"""Utility for user auth and session creation."""
import os
import threading
from typing import Any, Mapping, MutableMapping, Optional

import cachetools
import google.auth
import google.auth.transport.requests
import requests

from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import flask_util
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import redis_cache
from pathology.shared_libs.logging_lib import cloud_logging_client

_USER_INFO_REQUEST_URL = 'https://www.googleapis.com/userinfo/v2/me'
_EMAIL_DERIVED_FROM_BEARER_TOKEN = 'EMAIL_DERIVED_FROM_BEARER_TOKEN'


class UserEmailRetrievalError(Exception):
  """Unable to determine user email."""


_auth_cache = cachetools.LRUCache(maxsize=1)
_auth_cache_lock = threading.Lock()


def _init_fork_module_state() -> None:
  global _auth_cache
  global _auth_cache_lock
  _auth_cache = cachetools.LRUCache(maxsize=1)
  _auth_cache_lock = threading.Lock()


def _add_key_if_defined_in_source(
    dest: MutableMapping[str, Any], source: Mapping[str, Any], key: str
) -> None:
  value = source.get(key.lower())
  if value is not None:
    dest[key] = value


@cachetools.cached(_auth_cache, lock=_auth_cache_lock)
def _get_email_from_bearer_token(bearer_token: str) -> str:
  """Return Email address associated with OAuth bearer token.

  Args:
    bearer_token: OAuth Bearer Token.

  Returns:
    Email address.

  Raises:
    UserEmailRetrievalError: Unable to resolve email address.
  """
  cache = redis_cache.RedisCache()
  token_key = f'email:{bearer_token}'
  email = cache.get(token_key)
  if email is not None and email.value is not None:
    return email.value.decode('utf-8')
  response = None
  try:
    response = requests.get(
        _USER_INFO_REQUEST_URL,
        headers={
            proxy_const.HeaderKeywords.AUTH_HEADER_KEY: bearer_token,
        },
    )
    json_response = response.json()
    email = json_response['email']
    if not json_response['verified_email']:
      msg = f'Email: {email} is not verified.'
      cloud_logging_client.error(msg, {'e-mail': email})
      raise UserEmailRetrievalError(msg)
    cache.set(
        token_key,
        email,
        ttl_sec=dicom_proxy_flags.USER_LEVEL_METADATA_TTL_FLG.value,
    )
    return email
  except (
      requests.exceptions.RequestException,
      requests.exceptions.JSONDecodeError,
      KeyError,
  ) as exc:
    cloud_logging_client.error(
        'Unable to retrieve user email.',
        exc,
        {'response_received': response.text, 'token': bearer_token}
        if response is not None
        else {'token': bearer_token},
    )
    raise UserEmailRetrievalError('Unable to retrieve user email.') from exc


class AuthSession:
  """Wraps credentials passed to initiate a DICOM Proxy downsampling request."""

  def __init__(
      self,
      authorization_header: Optional[Mapping[str, str]],
  ):
    self._auth_dict = {}
    self._userid_dict = {}
    if authorization_header is not None and authorization_header:
      norm_dict = flask_util.norm_dict_keys(
          authorization_header,
          [
              proxy_const.HeaderKeywords.AUTH_HEADER_KEY,
              proxy_const.HeaderKeywords.AUTHORITY_HEADER_KEY,
              proxy_const.HeaderKeywords.IAP_EMAIL_KEY,
              proxy_const.HeaderKeywords.IAP_USER_ID_KEY,
          ],
      )
      _add_key_if_defined_in_source(
          self._auth_dict, norm_dict, proxy_const.HeaderKeywords.AUTH_HEADER_KEY
      )
      _add_key_if_defined_in_source(
          self._auth_dict,
          norm_dict,
          proxy_const.HeaderKeywords.AUTHORITY_HEADER_KEY,
      )
      if dicom_proxy_flags.VALIDATE_IAP_FLG.value:
        _add_key_if_defined_in_source(
            self._userid_dict,
            norm_dict,
            proxy_const.HeaderKeywords.IAP_EMAIL_KEY,
        )
        _add_key_if_defined_in_source(
            self._userid_dict,
            norm_dict,
            proxy_const.HeaderKeywords.IAP_USER_ID_KEY,
        )
      else:
        bearer_token = self._auth_dict.get(
            proxy_const.HeaderKeywords.AUTH_HEADER_KEY
        )
        if bearer_token is not None:
          self._userid_dict[_EMAIL_DERIVED_FROM_BEARER_TOKEN] = (
              _get_email_from_bearer_token(bearer_token)
          )
    if (
        dicom_proxy_flags.ENABLE_APPLICATION_DEFAULT_CREDENTIALS_FLG.value
        and proxy_const.HeaderKeywords.AUTH_HEADER_KEY not in self._auth_dict
    ):
      # User default credentials returns the credentials of the GKE service
      # account and not the calling user. The purpose of this code path is to
      # enable e2e testing of the IAP enabled application in chrome in dev.
      # For this pathway to work the tile-server service account must have
      # read/write permissions on the DICOM store. When enabled the pathway can
      # be activated via chrome without the need to supply a valid user bearer
      # token. When deployed in the customers environment (production) the
      # tile-server service account should not have permission to access the
      # DICOM Store and the ENABLE_APPLICATION_DEFAULT_CREDENTIALS_FLG.value
      # should be set to false.
      self._init_to_service_account_credentials()

  def _init_to_service_account_credentials(self) -> None:
    """Initializes service account default credentials."""
    cloud_logging_client.info('Retrieving service account credentials.')
    credentials, _ = google.auth.default()
    # Credentials need to be refreshed to return a bearer token.
    credentials.refresh(google.auth.transport.requests.Request())
    bearer_token = f'Bearer {credentials.token}'
    self._auth_dict[proxy_const.HeaderKeywords.AUTH_HEADER_KEY] = bearer_token
    self._auth_dict[proxy_const.HeaderKeywords.AUTHORITY_HEADER_KEY] = ''
    try:
      self._userid_dict[_EMAIL_DERIVED_FROM_BEARER_TOKEN] = (
          _get_email_from_bearer_token(bearer_token)
      )
    except UserEmailRetrievalError:
      self._userid_dict[_EMAIL_DERIVED_FROM_BEARER_TOKEN] = (
          'proxy-service-account'
      )

  @property
  def email(self) -> str:
    """Returns email associated with token."""
    email = self._userid_dict.get(_EMAIL_DERIVED_FROM_BEARER_TOKEN)
    if email is not None:
      return email
    email = self._userid_dict.get(proxy_const.HeaderKeywords.IAP_EMAIL_KEY)
    if email is not None:
      return email
    raise UserEmailRetrievalError('User email is unknown.')

  @property
  def iap_user_id(self) -> str:
    """Returns IAP user id."""
    return self._userid_dict.get(proxy_const.HeaderKeywords.IAP_USER_ID_KEY, '')

  @property
  def authorization(self) -> str:
    return self._auth_dict.get(proxy_const.HeaderKeywords.AUTH_HEADER_KEY, '')

  @property
  def authority(self) -> str:
    return self._auth_dict.get(
        proxy_const.HeaderKeywords.AUTHORITY_HEADER_KEY, ''
    )

  def add_to_header(
      self, request_header: Optional[Mapping[str, str]]
  ) -> Mapping[str, str]:
    """Adds authentication token and scope to request header.

    Args:
      request_header: header to add auth to.

    Returns:
      header with auth
    """
    if request_header is None:
      request_header = {}
    return_header = dict(request_header)
    return_header.update(self._auth_dict)
    return return_header


# The digitial_pathology_dicom proxy runs using gunicorn, which forks worker
# processes. Forked processes do not re-init global state and assume their
# values at the time of the fork. This can result in forked modules being
# started with invalid global state, e.g., acquired locks that will not release
# or references state. os.register at fork, defines a function run in child
# forked processes following the fork to re-initalize the forked global module
# state.
os.register_at_fork(after_in_child=_init_fork_module_state)
