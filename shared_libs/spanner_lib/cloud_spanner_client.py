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
"""Wrapper for Cloud Spanner for use with DPAS database.

Provides the utilities for read/write access to Cloud Spanner.
"""
from typing import Any, Dict, List, Optional, TypeVar

from absl import flags
from google.cloud import spanner
from google.cloud.spanner_v1 import database
from google.cloud.spanner_v1 import streamed

from shared_libs.flags import secret_flag_utils
from shared_libs.logging_lib import cloud_logging_client

INSTANCE_ID_FLG = flags.DEFINE_string(
    'instance_id',
    secret_flag_utils.get_secret_or_env('INSTANCE_ID', None),
    'Instance Id of instance containing database.',
)
DATABASE_NAME_FLG = flags.DEFINE_string(
    'database_name',
    secret_flag_utils.get_secret_or_env('DATABASE_NAME', None),
    'Name of database.',
)

CloudSpannerClientType = TypeVar(
    'CloudSpannerClientType', bound='CloudSpannerClient'
)


class CloudSpannerClientInstanceExceptionError(Exception):
  pass


class CloudSpannerClient:
  """Wrapper for Cloud Spanner for use with DPAS database."""

  def __init__(self):
    """Initializes a Cloud Spanner Client wrapper."""
    cloud_logging_client.info(
        'Connecting to Spanner instance.',
        {
            'instance_id': INSTANCE_ID_FLG.value,
            'database_name': DATABASE_NAME_FLG.value,
        },
    )
    self._database = (
        spanner.Client()
        .instance(INSTANCE_ID_FLG.value)
        .database(DATABASE_NAME_FLG.value)
    )

  def execute_sql(
      self,
      sql: str,
      params: Optional[Dict[str, Any]] = None,
      param_types: Optional[Dict[str, Any]] = None,
  ) -> streamed.StreamedResultSet:
    """Executes an sql statement on a snapshot of the database.

    Params and param_types fields should be specified in non-test usage to
    sanitize user data.

    Args:
      sql: SQL statement to execute
      params: Key value pairs of names to params. {str -> Any}.
      param_types: Key value pairs of param types corresponding to params {str
        -> spanner.param_types.Type}.

    Returns:
      StreamedResultSet which can be used to consume rows
    """
    with self._database.snapshot() as snapshot:
      return snapshot.execute_sql(sql, params, param_types)

  def execute_dml(self, dml: List[str]) -> List[int]:
    """Executes DML in a read-write transaction.

    DML supports read after write transactions allowing uncommitted data to be
    read in the same transaction it was written.

    Args:
      dml: DML statements to execute in order

    Returns:
      List[int] of rows updated
    """

    def execute_update(transaction, statement):
      return transaction.execute_update(statement)

    row_ct = []
    for st in dml:
      row_ct.append(self._database.run_in_transaction(execute_update, st))

    return row_ct

  def run_in_transaction(self, func, *args, **kw):
    """Runs a process of work in a transaction.

    Ensures that read/writes in a function occur in the same transaction.

    Args:
      func: Function to run.
      *args: Additional arguments to be passed to func.
      **kw: Keyword arguments to be passed to func.

    Returns:
      return of func
    """
    return self._database.run_in_transaction(func, *args, **kw)

  def get_database_snapshot(self) -> database.SnapshotCheckout:
    """Returns a snapshot of the Spanner database."""
    return self._database.snapshot(multi_use=True)

  def read_data(
      self,
      table: str,
      columns: List[str],
      keys: spanner.KeySet,
      index: Optional[str] = None,
  ) -> streamed.StreamedResultSet:
    """Reads sample data from a snapshot of the database.

    Args:
      table: Name of table to read from.
      columns: Column names of table.
      keys: KeySet defining keys to read.
      index: Secondary index to use instead of primary key.

    Returns:
      StreamedResultSet which can be used to consume rows
    """
    with self._database.snapshot() as snapshot:
      return snapshot.read(table, columns, keys, index)

  def insert_data(
      self, table: str, columns: List[str], values: List[List[Any]]
  ) -> None:
    """Inserts data using mutations.

    A mutation is a sequence of inserts, updates, and deletes that get applied
    atomically. All mutations in a single batch are applied atomically. Does not
    allow for read of uncommitted data.

    Args:
      table: Name of table to insert into.
      columns: Column names.
      values: Values to insert.

    Returns:
      None
    """
    with self._database.batch() as batch:
      batch.insert(table, columns, values)

  def update_data(
      self, table: str, columns: List[str], values: List[List[Any]]
  ) -> None:
    """Updates data using mutations.

    A mutation is a sequence of inserts, updates, and deletes that get applied
    atomically. All mutations in a single batch are applied atomically. Does not
    allow for read of uncommitted data.

    Args:
      table: Name of table to update
      columns: Column names
      values: Values to update

    Returns:
      None
    """
    with self._database.batch() as batch:
      batch.update(table, columns, values)

  def insert_or_update_data(
      self, table: str, columns: List[str], values: List[List[Any]]
  ) -> None:
    """Inserts or updates data using mutations.

    A mutation is a sequence of inserts, updates, and deletes that get applied
    atomically. All mutations in a single batch are applied atomically. Does not
    allow for read of uncommitted data.

    Args:
      table: Name of table to update
      columns: Column names
      values: Values to update

    Returns:
      None
    """
    with self._database.batch() as batch:
      batch.insert_or_update(table, columns, values)

  def delete_data(
      self,
      table: str,
      keys: Optional[List[List[Any]]] = None,
      all_keys: bool = False,
  ) -> None:
    """Deletes data using mutations.

    A mutation is a sequence of inserts, updates, and deletes that get applied
    atomically. All mutations in a single batch are applied atomically. Does not
    allow for read of uncommitted data.

    Args:
      table: Name of table to update
      keys: List of primary key values of rows to delete
      all_keys: True to delete all keys

    Returns:
      None
    """

    if all_keys:
      with self._database.batch() as batch:
        batch.delete(table, spanner.KeySet(all_=all_keys))
    else:
      with self._database.batch() as batch:
        batch.delete(table, spanner.KeySet(keys=keys))
