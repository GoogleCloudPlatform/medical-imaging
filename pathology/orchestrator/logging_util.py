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

"""Util constants and methods for Orchestrator logging."""

import enum
from typing import Any, Mapping, Optional


class RpcMethodName(enum.Enum):
  """Enum to define rpc method name used in log signature."""
  RPC_METHOD_UNSPECIFIED = 0
  COPY_PATHOLOGY_COHORT = 1
  CREATE_PATHOLOGY_COHORT = 2
  DELETE_PATHOLOGY_COHORT = 3
  EXPORT_PATHOLOGY_COHORT = 4
  GET_PATHOLOGY_COHORT = 5
  LIST_PATHOLOGY_COHORTS = 6
  SAVE_PATHOLOGY_COHORT = 7
  SHARE_PATHOLOGY_COHORT = 8
  TRANSFER_DEID_PATHOLOGY_COHORT = 9
  UNDELETE_PATHOLOGY_COHORT = 10
  UPDATE_PATHOLOGY_COHORT = 11
  UNSAVE_PATHOLOGY_COHORT = 12
  CREATE_PATHOLOGY_USER = 13
  GET_PATHOLOGY_USER = 14
  IDENTIFY_PATHOLOGY_USER = 15
  UPDATE_PATHOLOGY_USER = 16
  CREATE_PATHOLOGY_SLIDE = 17
  GET_PATHOLOGY_SLIDE = 18
  LIST_PATHOLOGY_SLIDES = 19
  GET_OPERATION = 20


def get_structured_log(
    rpc_name: RpcMethodName,
    resource_name: Optional[str] = None) -> Mapping[str, Any]:
  if resource_name is None:
    return {'rpc_method': rpc_name.name}
  else:
    return {'rpc_method': rpc_name.name, 'resource_name': resource_name}
