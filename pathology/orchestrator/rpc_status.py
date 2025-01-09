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
"""Dataclass for status return type used on rpc failure."""

import dataclasses
from typing import Any, Mapping, Optional

import grpc

from pathology.shared_libs.logging_lib import cloud_logging_client


@dataclasses.dataclass(frozen=True)
class RpcMethodStatus:
  """Defines the status for a method when there is an error.

  Used in rpc handler implementation when rpc fails.
  """

  code: grpc.StatusCode
  error_msg: str


class RpcFailureError(Exception):
  """Raised when the rpc fails."""

  def __init__(self, status: RpcMethodStatus):
    self.status = status
    super().__init__()


def build_rpc_method_status_and_log(
    code: grpc.StatusCode,
    error_msg: str,
    log_struct: Optional[Mapping[str, Any]] = None,
    exp: Optional[Exception] = None,
) -> RpcMethodStatus:
  """Builds RpcMethodStatus instance on rpc failure and logs details of error.

  Args:
    code: Grpc StatusCode of failure.
    error_msg: Error message to return to caller.
    log_struct: Details to output in structured logging format.
    exp: Exception that occurs to log.

  Returns:
    RpcMethodStatus instance.
  """
  if exp is None:
    exp = {}
  if log_struct is None:
    cloud_logging_client.error(error_msg, exp, {'error_code': code.name})
  else:
    cloud_logging_client.error(
        error_msg, exp, {'error_code': code.name}, log_struct
    )
  return RpcMethodStatus(code, error_msg)
