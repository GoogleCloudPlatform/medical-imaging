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
"""Utilities to access flask context globals."""

import http
from typing import Any, List, Mapping, MutableMapping, Optional, Set, Union

import flask
import pydicom

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import enum_types
from pathology.shared_libs.flags import flag_utils
from pathology.shared_libs.logging_lib import cloud_logging_client


# Constants
ACCEPT_HEADER_KEY = 'accept'
CONTENT_TYPE_HEADER_KEY = 'content-type'
CONTENT_LENGTH_HEADER_KEY = 'content-length'
ACCEPT_ENCODING_KEY = 'accept-encoding'
DELETE = 'DELETE'
GET = 'GET'
POST = 'POST'
DELETE_METHOD = [DELETE]
POST_METHOD = [POST]
GET_AND_POST_METHODS = [POST, GET]
GET_POST_AND_DELETE_METHODS = [POST, GET, DELETE]
_BINARY_TAG_VR_CODES = frozenset(['OB', 'OD', 'OF', 'OW'])


def get_dicom_proxy_request_args() -> Mapping[str, str]:
  return norm_dict_keys(
      get_first_key_args(),
      [en.value.name for en in enum_types.TileServerHttpParams],
  )


def get_parameter_value(
    args: Mapping[str, str], param: enum_types.TileServerHttpParams
) -> str:
  return args.get(param.value.name, param.value.default).strip().lower()


def parse_cache_enabled(
    args: Mapping[str, str],
) -> cache_enabled_type.CachingEnabled:
  """Returns True if caching is enabled.

  Args:
    args: HTTP Request Args.

  Raises:
    Value error if parameter cannot be converted to bool.
  """
  result = flag_utils.str_to_bool(
      get_parameter_value(args, enum_types.TileServerHttpParams.DISABLE_CACHING)
  )
  if result:
    cloud_logging_client.info('Caching disabled')
  return cache_enabled_type.CachingEnabled(not result)


def parse_downsample(args: Mapping[str, str]) -> float:
  """Returns request image downsample.

  Args:
    args: HTTP Request Args.

  Raise:
    ValueError: Invalid downsample value or cannot parse to float.
  """
  downsample = float(
      get_parameter_value(args, enum_types.TileServerHttpParams.DOWNSAMPLE)
  )
  if downsample < 1.0:
    raise ValueError('Invalid downsample')
  return downsample


def exception_flask_response(exp: Union[Exception, str]) -> flask.Response:
  """Returns flask response for python exception."""
  return flask.Response(
      response=str(exp),
      status=http.HTTPStatus.BAD_REQUEST.value,
      content_type='text/plain',
  )


def get_request() -> flask.Request:
  return flask.request


def get_json() -> MutableMapping[str, Any]:
  return get_request().get_json()


def get_headers() -> Mapping[str, str]:
  return get_request().headers


def get_method() -> str:
  return get_request().method.upper()


def get_accept_header() -> Optional[str]:
  headers = get_request().headers
  if ACCEPT_HEADER_KEY in headers:
    return headers[ACCEPT_HEADER_KEY]
  return None


def get_first_key_args() -> Mapping[str, str]:
  """Returns first key mapping to an arg."""
  return get_request().args


def get_key_args_list() -> Mapping[str, List[str]]:
  """Returns key mapping to full list of defined args."""
  return get_request().args.to_dict(flat=False)


def get_includefields() -> set[str]:
  for key, tag_list in get_key_args_list().items():
    if key.lower() == 'includefield':
      includefields = set()
      for tags in tag_list:
        for tag in tags.split(','):
          includefields.add(tag.strip())
      return includefields
  return set()


def includefield_binary_tags() -> bool:
  """Return true if dicomWeb includefield param requests all tags or binary tag."""
  tags = get_includefields()
  for tag in tags:
    if tag.lower() == 'all':
      return True
    try:
      tag_vr_code = set(pydicom.datadict.dictionary_VR(tag).split(' or '))
      if tag_vr_code.intersection(_BINARY_TAG_VR_CODES):
        return True
    except (ValueError, KeyError, TypeError) as exp:
      cloud_logging_client.warning(f'DICOM tag: {tag} is not recognized.', exp)
  return False


def get_data() -> bytes:
  return get_request().get_data()


def get_url_root() -> str:
  """Returns root url for flask request.

  Example:  "https://myserver.com"
  """
  return get_request().url_root.rstrip('/')


def get_path() -> str:
  """Returns url path for flask request.

  Example: http://foo.bar/abc/133?test=yes -> /abc/133
  """
  return get_request().path.rstrip('/')


def get_base_url() -> str:
  """Returns base_url for flask request, omits query parameters.

  Example: http://foo.bar/abc/133?test=yes -> http://foo.bar/abc/133
  """
  return get_request().base_url.rstrip('/')


def get_full_request_url() -> str:
  """Returns full url to flask request path.

  Example: http://foo.bar/abc/133?test=yes -> http://foo.bar/abc/133?test=yes
  """
  return get_request().url


def get_parameters() -> str:
  """Returns full url to flask request path.

  Example: http://foo.bar/abc/133?test=yes -> ?test=yes
  """
  full_url = get_full_request_url()
  if '?' not in full_url:
    return ''
  return full_url[full_url.index('?') :]


def norm_dict_keys(
    args: Mapping[str, Any], requested: Union[List[str], Set[str]]
) -> Mapping[str, Any]:
  """Normalizes dictionary for requested keys.

  The purpose is to enable dict key requests to query for values without
  worrying about key case.  Keys normalized to lowercase with any bounding
  whitespace removed.

  Args:
    args: Arguments[key : value pairs]
    requested: List of keys to return in normalized dictionary.

  Returns:
    Normalize dictionary
  """
  key_count_requested = len(requested)
  requested_set = {key.lower() for key in requested}
  return_dict = {}
  for key, value in args.items():
    norm_key = key.strip().lower()
    if norm_key not in requested_set:
      continue
    return_dict[norm_key] = value
    if len(return_dict) == key_count_requested:
      break
  return return_dict


def add_key_value_to_dict(
    headers: MutableMapping[str, str], search_key: str, value: str
):
  search_key = search_key.lower()
  for key in list(headers):
    if key.lower() == search_key:
      headers[key] = value
      return
  headers[search_key] = value


def get_key_value(
    headers: Mapping[str, str],
    search_key: str,
    default_value: str,
) -> str:
  search_key = search_key.lower()
  for key, value in headers.items():
    if key.lower() == search_key:
      return value
  return default_value
