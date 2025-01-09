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

"""UID generator for internal Orchestrator ids."""
import hashlib
from typing import List
import uuid

from google.api_core import exceptions
from google.cloud import spanner


def _convert_to_spanner_key(id_value: int) -> int:
  """Converts a given 128 bit id value to fit within the Spanner Int key range.

  Spanner stores signed Int64 values.
  This method converts an id to be within the positive range of Int64.
  Right bit shift (>>) 65 is equivalent to dividing by 2^65 without remainder.

  Args:
    id_value: Id value to convert.

  Returns:
    int
  """
  return id_value >> 65


def generate_hash_id(name: str) -> int:
  """Generates a hash id that is used as primary key in Spanner.

  [:16] gets 16 bytes (128 bits) of the 256 bit hash value.

  Args:
    name: String to generate hash id from.

  Returns:
    int
  """
  return _convert_to_spanner_key(
      int.from_bytes(
          hashlib.sha256(name.encode()).digest()[:16], 'big', signed=False))


class OrchestratorIdGenerator(object):
  """Generates internal uids for Orchestrator."""

  @staticmethod
  def get_orchestrator_id() -> int:
    """Generates random id using uuid.uuid4() to be used as Orchestrator ids.

    https://docs.python.org/3/library/uuid.html#uuid.uuid4
    Converts 128-bit result to fit in Spanner key positive int range.

    Returns:
       int
    """
    return _convert_to_spanner_key(
        int.from_bytes(uuid.uuid4().bytes, byteorder='big', signed=False))

  @staticmethod
  def _check_id_exists(transaction, table_name: str, cols: List[str],
                       id_value: int) -> bool:
    """Checks if an id value is present in the table.

    Args:
      transaction: Transaction to run on.
      table_name: Name of table to check for id in.
      cols: Columns of table.
      id_value: Id to verify existence.

    Returns:
        Returns True if id exists in table.
    """
    # Check if id is in table.
    results = transaction.read(table_name, cols,
                               spanner.KeySet(keys=[[id_value]]))
    try:
      return results.one()
    except exceptions.NotFound:
      return False

  @staticmethod
  def generate_new_id_for_table(transaction, table_name: str,
                                table_cols: List[str]) -> int:
    """Generates a new Orchestrator internal id value.

     Ensures that the value is not currently in use in the specified table.

    Args:
      transaction: Transaction to run on.
      table_name: Name of table to check for id in.
      table_cols: Columns of table.

    Returns:
        int of the id value
    """
    while True:
      id_value = OrchestratorIdGenerator.get_orchestrator_id()
      if not OrchestratorIdGenerator._check_id_exists(transaction, table_name,
                                                      table_cols, id_value):
        return id_value
