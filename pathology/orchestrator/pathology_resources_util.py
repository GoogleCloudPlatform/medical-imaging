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

"""Util methods for Orchestrator API resources."""

import os
from typing import Union


def convert_cohort_id_to_name(user_id: Union[int, str],
                              cohort_id: Union[int, str]) -> str:
  """Returns cohort resource name.

  Args:
    user_id: User Id.
    cohort_id: Cohort identifier.
  """
  return f'pathologyUsers/{user_id}/pathologyCohorts/{cohort_id}'


def convert_scan_uid_to_name(scan_uid: Union[int, str]) -> str:
  """Returns slide resource name.

  Args:
    scan_uid: Scan unique id.
  """
  return f'pathologySlides/{scan_uid}'


def convert_user_id_to_name(user_id: Union[int, str]) -> str:
  """Returns user resource name.

  Args:
    user_id: User Id.
  """
  return f'pathologyUsers/{user_id}'


def convert_operation_id_to_name(op_id: Union[int, str]) -> str:
  """Returns operation resource name.

  Args:
    op_id: Operation id.
  """
  return f'operations/{op_id}'


def get_id_from_name(resource_name: str) -> int:
  """Returns the id given the resource name.

  Args:
    resource_name: Resource name for instance.

  Returns:
    int id

  Raises:
    ValueError if it cannot convert to int.
  """
  return int(os.path.basename(resource_name))
