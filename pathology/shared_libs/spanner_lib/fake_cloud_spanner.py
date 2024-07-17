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
"""Fake CloudSpanner implementation for use in tests.

Connects to locally run Spanner emulator.
third_party/cloud_spanner_emulator/public/py:emulator
"""
from typing import Any, List

from google.cloud.spanner_v1 import database
from google.cloud.spanner_v1 import keyset
from google.cloud.spanner_v1 import streamed

from third_party.cloud_spanner_emulator.public.py import emulator

INSTANCE_ID = 'test-instance'
DATABASE_NAME = 'testdb'
GCLOUD_PROJECT = 'test-project'


class FakeCloudSpanner:
  """Fake CloudSpanner implementation for use in tests.

  Attributes:
    emulator: Spanner emulator instance.
    _db: Database instance that should only be accessed from tests through
      exposed methods.
  """

  emulator = None
  _db = None

  @classmethod
  def set_up_emulator(cls, schema_ddl: List[str]):
    """Sets up a local Cloud Spanner emulator.

    Emulator is set up as a class method, shared between all test cases in a
    test file as it is slow to start up.

    Args:
      schema_ddl: List of ddl statements to create database.

    Returns:
      None
    """
    cls.emulator = emulator.Emulator(suppress_logs=True)
    emulator_client = cls.emulator.get_client(project=GCLOUD_PROJECT)
    emulator_config = list(emulator_client.list_instance_configs())[0]
    emulator_instance = emulator_client.instance(
        INSTANCE_ID, configuration_name=emulator_config.name
    )
    emulator_instance.create().result()
    cls._db = emulator_instance.database(DATABASE_NAME, schema_ddl)
    cls._db.create().result()

  @classmethod
  def tear_down_emulator(cls):
    cls.emulator.stop()  # pytype: disable=attribute-error

  def clear_table(self, table_name) -> None:
    """Clears all data in table.

    Args:
      table_name: Name of table to clear data from.

    Returns:
      None
    """
    if self._db is None:
      return None
    with self._db.batch() as batch:
      batch.delete(table_name, keyset.KeySet(all_=True))

  def insert_data(
      self, table: str, columns: List[str], values: List[List[Any]]
  ) -> None:
    """Inserts data to seed fake Spanner table using mutations.

    A mutation is a sequence of inserts, updates, and deletes that get applied
    atomically.

    Args:
      table: Name of table to insert into.
      columns: Column names.
      values: Values to insert.

    Returns:
      None
    """
    if self._db is None:
      return None
    with self._db.batch() as batch:
      batch.insert(table, columns, values)

  def execute_sql(self, sql: str) -> streamed.StreamedResultSet:
    """Executes an sql statement on a snapshot of the database.

    Args:
      sql: SQL statement to execute.

    Returns:
      StreamedResultSet which can be used to consume rows.
    """
    with self._db.snapshot() as snapshot:  # pytype: disable=attribute-error
      return snapshot.execute_sql(sql)

  def get_database_snapshot(self) -> database.SnapshotCheckout:
    """Returns a snapshot of the Spanner database."""
    return self._db.snapshot(multi_use=True)  # pytype: disable=attribute-error

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
    return self._db.run_in_transaction(func, *args, **kw)  # pytype: disable=attribute-error
