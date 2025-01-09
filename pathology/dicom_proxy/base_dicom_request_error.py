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
"""Exception base class for returning exceptions caught handling requests."""
import copy
from typing import Optional

import flask
import requests


class BaseDicomRequestError(Exception):
  """Exception base class for returning exceptions caught handling requests."""

  def __init__(
      self, name: str, response: requests.Response, msg: Optional[str] = None
  ):
    self._headers = copy.copy(response.headers)
    if response.raw is None:
      self._data = b'Request.response.raw is None. No data returned.'
    else:
      self._data = response.raw.read()
    if self._data:
      try:
        response_data_as_str = self._data.decode('utf-8')
      except (UnicodeError, UnicodeDecodeError) as _:
        response_data_as_str = (
            'Unicode error occurred decoding response; response may be binary.'
        )
    else:
      # Not a streaming request
      self._data = response.text
      response_data_as_str = self._data
    self._status_code = response.status_code
    if msg is None:
      msg = name
    else:
      msg = f'{name} {msg}'
    super().__init__(
        f'{msg}; status:{self._status_code}; headers:{self._headers};'
        f' data:{response_data_as_str}'
    )

  def flask_response(self) -> flask.Response:
    fl_response = flask.Response(self._data, status=self._status_code)
    fl_response.headers.clear()
    for key, value in self._headers.items():
      fl_response.headers[key] = value
    return fl_response
