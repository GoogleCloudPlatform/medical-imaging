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

"""Tests for grpc util."""

from unittest import mock

from absl.testing import absltest
import grpc

from pathology.orchestrator import grpc_util

_TOKEN = 'oauthToken'
_MOCK_INVOCATION_METADATA = [('authorization', f'Bearer {_TOKEN}')]


class GrpcUtilTest(absltest.TestCase):
  """Tests for grpc_util."""

  def test_get_token(self):
    mock_ctx = mock.create_autospec(grpc.ServicerContext)
    mock_ctx.invocation_metadata.return_value = _MOCK_INVOCATION_METADATA
    self.assertEqual(_TOKEN, grpc_util.get_token(mock_ctx))


if __name__ == '__main__':
  absltest.main()
