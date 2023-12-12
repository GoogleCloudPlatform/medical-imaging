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
"""Unit tests for CloudSpannerClient.

Connects to locally run Spanner emulator.
third_party/cloud_spanner_emulator/public/py:emulator

Must be run in an emulator environment. To set up, follow:
https://cloud.google.com/spanner/docs/emulator#using_the_gcloud_cli_with_the_emulator

To run:
gcloud emulators spanner start
gcloud config configurations activate emulator
blaze test
//spanner_lib:cloud_spanner_client_test
"""
import os

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from google.api_core.exceptions import NotFound
from google.cloud import spanner

from shared_libs.spanner_lib import cloud_spanner_client
from shared_libs.spanner_lib import fake_cloud_spanner

FLAGS = flags.FLAGS

_TABLE_NAME = 'Singers'
_COLS = ['SingerId', 'FirstName', 'LastName']
_SCHEMA_DDL = (
    'CREATE TABLE Singers (SingerId INT64 NOT NULL, FirstName '
    'STRING(1024), LastName STRING(1024),) PRIMARY KEY(SingerId)'
)


class CloudSpannerClientTest(
    fake_cloud_spanner.FakeCloudSpanner, absltest.TestCase
):
  """Unit Tests for CloudSpannerClient.

  Creates a new Spanner instance and database run locally on the emulator.
  """

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls.set_up_emulator([_SCHEMA_DDL])

  @classmethod
  def tearDownClass(cls):
    super().tearDownClass()
    cls.tear_down_emulator()

  def setUp(self):
    super().setUp()
    # Set flags to init spanner client.
    self._saved_flags = flagsaver.save_flag_values()
    FLAGS.instance_id = fake_cloud_spanner.INSTANCE_ID
    FLAGS.database_name = fake_cloud_spanner.DATABASE_NAME
    self._client = cloud_spanner_client.CloudSpannerClient()

  def tearDown(self):
    super().tearDown()
    self._client.delete_data(_TABLE_NAME, all_keys=True)
    flagsaver.restore_flag_values(self._saved_flags)

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
    with self.assertRaises(NotFound):
      results.one()
    self.assertEqual(results2.one(), values[1])

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

  def test_execute_sql_multiple_rows(self):
    values = [[1, 'SingerA', 'TestA'], [2, 'SingerB', 'TestB']]
    self._client.insert_data(_TABLE_NAME, _COLS, values)

    results = self._client.execute_sql(
        f'SELECT * from {_TABLE_NAME} WHERE SingerId >= @id_value',
        params={'id_value': 1},
        param_types={'id_value': spanner.param_types.INT64},
    )

    i = 0
    for r in results:
      self.assertEqual(r, values[i])
      i += 1


if __name__ == '__main__':
  # Specify fake Google Cloud Project for spanner emulator.
  os.environ.setdefault('GCLOUD_PROJECT', fake_cloud_spanner.GCLOUD_PROJECT)
  absltest.main()
