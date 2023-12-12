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
"""Integration tests for CloudSpannerClient.

Creates a test Spanner instance.
Test must be run as a binary to use GCP credentials.
Test takes ~25s to run and will timeout if instance creation or database
creation exceeds the timeout value.

To run:
gcloud auth login
gcloud config configurations activate default
blaze run
//spanner_lib:cloud_spanner_client_integration_test
"""
from absl import logging
from absl.testing import absltest
from absl.testing import flagsaver
from google.api_core import exceptions
from google.cloud import spanner

from shared_libs.spanner_lib import cloud_spanner_client

_INSTANCE_ID = 'test-instance'
_DISPLAY_NAME = 'Test Instance'
_DATABASE_NAME = 'test'
_TABLE_NAME = 'Singers'
_COLS = ['SingerId', 'FirstName', 'LastName']
_SCHEMA_DDL = (
    'CREATE TABLE Singers (SingerId INT64 NOT NULL, FirstName '
    'STRING(1024), LastName STRING(1024),) PRIMARY KEY(SingerId)'
)
_OPERATION_TIMEOUT_SECONDS = 240


class CloudSpannerClientIntegrationTest(absltest.TestCase):
  """Integration Tests for CloudSpannerClient."""

  # Uses standard spanner client to create instance and database.
  _spanner_client = None
  _instance = None

  @classmethod
  def _create_database(cls):
    # pytype: disable=attribute-error
    config_name = '{}/instanceConfigs/regional-us-west2'.format(
        cls._spanner_client.project_name
    )
    cls._instance = cls._spanner_client.instance(
        instance_id=_INSTANCE_ID,
        configuration_name=config_name,
        display_name=_DISPLAY_NAME,
        node_count=1,
    )
    operation = cls._instance.create()
    logging.info('Creating Spanner instance %s...', _INSTANCE_ID)
    operation.result(_OPERATION_TIMEOUT_SECONDS)

    database = cls._instance.database(_DATABASE_NAME, [_SCHEMA_DDL])
    operation = database.create()
    logging.info('Creating Spanner database %s...', _DATABASE_NAME)
    operation.result(_OPERATION_TIMEOUT_SECONDS)
    # pytype: enable=attribute-error

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls._spanner_client = spanner.Client()
    cls._create_database()

  @classmethod
  def tearDownClass(cls):
    super().tearDownClass()
    cls._instance.delete()  # pytype: disable=attribute-error
    logging.info('Deleting Spanner instance %s...', _INSTANCE_ID)

  @flagsaver.flagsaver(instance_id=_INSTANCE_ID)
  @flagsaver.flagsaver(database_name=_DATABASE_NAME)
  def setUp(self):
    super().setUp()
    # Initialize CloudSpannerClient for test.
    self._client = cloud_spanner_client.CloudSpannerClient()

  @flagsaver.flagsaver(instance_id=_INSTANCE_ID)
  @flagsaver.flagsaver(database_name=_DATABASE_NAME)
  def tearDown(self):
    super().tearDown()
    self._client.delete_data(_TABLE_NAME, all_keys=True)

  @flagsaver.flagsaver(instance_id=_INSTANCE_ID)
  @flagsaver.flagsaver(database_name=_DATABASE_NAME)
  def test_insert_row(self):
    values = [[1, 'SingerA', 'TestA'], [2, 'SingerB', 'TestB']]
    self._client.insert_data(_TABLE_NAME, _COLS, values)

    results = self._client.execute_sql(
        f'SELECT * FROM {_TABLE_NAME} WHERE {_COLS[0]}=1;'
    )
    results2 = self._client.execute_sql(
        f'SELECT * FROM {_TABLE_NAME} WHERE {_COLS[0]}=2;'
    )
    self.assertEqual(results.one(), values[0])
    self.assertEqual(results2.one(), values[1])

  @flagsaver.flagsaver(instance_id=_INSTANCE_ID)
  @flagsaver.flagsaver(database_name=_DATABASE_NAME)
  def test_read_one_col(self):
    values = [[1, 'SingerA', 'TestA'], [2, 'SingerB', 'TestB']]
    self._client.insert_data(_TABLE_NAME, _COLS, values)

    results = self._client.read_data(
        _TABLE_NAME, ['FirstName'], spanner.KeySet(keys=[[1]])
    )
    self.assertEqual(results.one()[0], 'SingerA')

  @flagsaver.flagsaver(instance_id=_INSTANCE_ID)
  @flagsaver.flagsaver(database_name=_DATABASE_NAME)
  def test_run_in_transaction(self):
    values = [[1, 'SingerA', 'TestA'], [2, 'SingerB', 'TestB']]
    self._client.insert_data(_TABLE_NAME, _COLS, values)

    updated_values = [[1, 'SingerC', 'TestC'], [2, 'SingerB', 'TestB']]

    def update(transaction, values):
      transaction.update(_TABLE_NAME, _COLS, [[1, 'SingerC', 'TestC']])
      results = transaction.read(_TABLE_NAME, _COLS, spanner.KeySet(keys=[[1]]))
      # write is not visible until transaction commits.
      for r, v in zip(results, values):
        self.assertEqual(r, v)

    self._client.run_in_transaction(update, values)
    # Verify update was written.
    results = self._client.read_data(
        _TABLE_NAME, _COLS, spanner.KeySet(keys=[[1]])
    )
    for r, v in zip(results, updated_values):
      self.assertEqual(r, v)

  @flagsaver.flagsaver(instance_id=_INSTANCE_ID)
  @flagsaver.flagsaver(database_name=_DATABASE_NAME)
  def test_update_row(self):
    values = [[1, 'SingerA', 'TestA'], [2, 'SingerB', 'TestB']]
    self._client.insert_data(_TABLE_NAME, _COLS, values)

    updated_values = [[1, 'SingerC', 'TestC']]
    self._client.update_data(_TABLE_NAME, _COLS, updated_values)

    results = self._client.execute_sql(
        f'SELECT * FROM {_TABLE_NAME} WHERE {_COLS[0]}=1;'
    )
    results2 = self._client.execute_sql(
        f'SELECT * FROM {_TABLE_NAME} WHERE {_COLS[0]}=2;'
    )
    self.assertEqual(results.one(), updated_values[0])
    self.assertEqual(results2.one(), values[1])

  @flagsaver.flagsaver(instance_id=_INSTANCE_ID)
  @flagsaver.flagsaver(database_name=_DATABASE_NAME)
  def test_insert_or_update_row(self):
    values = [[1, 'SingerA', 'TestA'], [2, 'SingerB', 'TestB']]
    self._client.insert_data(_TABLE_NAME, _COLS, values)

    updated_values = [[1, 'SingerC', 'TestC'], [4, 'SingerD', 'TestD']]
    self._client.insert_or_update_data(_TABLE_NAME, _COLS, updated_values)

    results = self._client.execute_sql(
        f'SELECT * FROM {_TABLE_NAME} WHERE {_COLS[0]}=1;'
    )
    results2 = self._client.execute_sql(
        f'SELECT * FROM {_TABLE_NAME} WHERE {_COLS[0]}=2;'
    )
    results3 = self._client.execute_sql(
        f'SELECT * FROM {_TABLE_NAME} WHERE {_COLS[0]}=4;'
    )
    self.assertEqual(results.one(), updated_values[0])
    self.assertEqual(results2.one(), values[1])
    self.assertEqual(results3.one(), updated_values[1])

  @flagsaver.flagsaver(instance_id=_INSTANCE_ID)
  @flagsaver.flagsaver(database_name=_DATABASE_NAME)
  def test_delete_row(self):
    values = [[1, 'SingerA', 'TestA'], [2, 'SingerB', 'TestB']]
    self._client.insert_data(_TABLE_NAME, _COLS, values)

    self._client.delete_data(_TABLE_NAME, keys=[[1]])

    results = self._client.execute_sql(
        f'SELECT * FROM {_TABLE_NAME} WHERE {_COLS[0]}=1;'
    )
    results2 = self._client.execute_sql(
        f'SELECT * FROM {_TABLE_NAME} WHERE {_COLS[0]}=2;'
    )
    with self.assertRaises(exceptions.NotFound):
      results.one()
    self.assertEqual(results2.one(), values[1])

  @flagsaver.flagsaver(instance_id=_INSTANCE_ID)
  @flagsaver.flagsaver(database_name=_DATABASE_NAME)
  def test_delete_nonexistent_row(self):
    values = [[1, 'SingerA', 'TestA'], [2, 'SingerB', 'TestB']]
    self._client.insert_data(_TABLE_NAME, _COLS, values)

    self._client.delete_data(_TABLE_NAME, keys=[[3]])

    results = self._client.execute_sql(
        f'SELECT * FROM {_TABLE_NAME} WHERE {_COLS[0]}=1;'
    )
    results2 = self._client.execute_sql(
        f'SELECT * FROM {_TABLE_NAME} WHERE {_COLS[0]}=2;'
    )
    self.assertEqual(results.one(), values[0])
    self.assertEqual(results2.one(), values[1])

  @flagsaver.flagsaver(instance_id=_INSTANCE_ID)
  @flagsaver.flagsaver(database_name=_DATABASE_NAME)
  def test_execute_dml(self):
    values = [[1, 'SingerA', 'TestA'], [2, 'SingerB', 'TestB']]
    self._client.insert_data(_TABLE_NAME, _COLS, values)

    # Update SingerId = 1 to have FirstName = SingerC.
    # Then read and store that value in a new row.
    row_ct = self._client.execute_dml([
        f'UPDATE {_TABLE_NAME} SET {_COLS[1]} = "SingerC" WHERE {_COLS[0]} = 1',
        (
            f'INSERT INTO {_TABLE_NAME} ({_COLS[0]}, {_COLS[1]},'
            f' {_COLS[2]}) VALUES (3, (SELECT {_COLS[1]} from'
            f' {_TABLE_NAME} WHERE {_COLS[0]} = 1), "TestC")'
        ),
    ])

    results = self._client.execute_sql(
        f'SELECT * FROM {_TABLE_NAME} WHERE SingerId=1;'
    )
    results2 = self._client.execute_sql(
        f'SELECT * FROM {_TABLE_NAME} WHERE SingerId=2;'
    )
    results3 = self._client.execute_sql(
        f'SELECT * FROM {_TABLE_NAME} WHERE SingerId=3;'
    )
    self.assertEqual(results.one(), [1, 'SingerC', 'TestA'])
    self.assertEqual(results2.one(), values[1])
    self.assertEqual(results3.one(), [3, 'SingerC', 'TestC'])
    self.assertEqual(row_ct, [1, 1])

  @flagsaver.flagsaver(instance_id=_INSTANCE_ID)
  @flagsaver.flagsaver(database_name=_DATABASE_NAME)
  def test_execute_sql_multiple_rows(self):
    values = [[1, 'SingerA', 'TestA'], [2, 'SingerB', 'TestB']]
    self._client.insert_data(_TABLE_NAME, _COLS, values)

    results = self._client.execute_sql(
        f'SELECT * from {_TABLE_NAME} WHERE {_COLS[0]} >= 1'
    )

    i = 0
    for r in results:
      self.assertEqual(r, values[i])
      i += 1


if __name__ == '__main__':
  absltest.main()
