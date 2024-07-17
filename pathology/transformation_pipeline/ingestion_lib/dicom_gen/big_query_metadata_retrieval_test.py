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
"""Tests for BigQuery metadata retrieval.

Generates a BigQuery dataset on test project.
To run:
blaze run //transformation_pipeline/ingestion_lib/
dicom_gen:big_query_metadata_retrieval_test
"""

import time

from absl import logging
from absl.testing import absltest
from absl.testing import flagsaver
from google.api_core import exceptions
from google.cloud import bigquery

from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client

_PROJECT_ID = 'test-project'
_DATASET_NAME = 'test_dataset'
_DATASET_ID = f'{_PROJECT_ID}.{_DATASET_NAME}'
_TABLE_NAME = 'test_table'
_FULL_TABLE_NAME = f'{_DATASET_ID}.{_TABLE_NAME}'
_BAR_CODE_VALUE = '123'
_COLUMNS = [
    'BARCODEVALUE',
    'FIRSTNAME',
    'LASTNAME',
]
_SCHEMA = [
    bigquery.SchemaField(_COLUMNS[0], 'STRING', mode='REQUIRED'),
    bigquery.SchemaField(_COLUMNS[1], 'STRING', mode='REQUIRED'),
    bigquery.SchemaField(_COLUMNS[2], 'STRING', mode='REQUIRED'),
]


class BigQueryMetadataRetrievalTest(absltest.TestCase):
  """Tests for BigQuery metadata retrieval."""

  _bq_client = bigquery.Client(project=_PROJECT_ID)

  @classmethod
  def _create_table(cls):
    dataset = bigquery.Dataset(_DATASET_ID)
    dataset.location = 'us-west1'
    logging.info('Creating BigQuery Dataset %s...', _DATASET_NAME)
    cls._bq_client.create_dataset(dataset, timeout=30)

    table_id = _FULL_TABLE_NAME
    table = bigquery.Table(table_id, schema=_SCHEMA)
    logging.info('Creating BigQuery Table %s...', _TABLE_NAME)
    cls._bq_client.create_table(table)
    try:
      cls._bq_client.get_table(table_id)
    except exceptions.NotFound:
      time.sleep(30)

    rows_to_insert = [{
        _COLUMNS[0]: _BAR_CODE_VALUE,
        _COLUMNS[1]: 'John',
        _COLUMNS[2]: 'Doe',
    }]

    errors = cls._bq_client.insert_rows_json(
        table_id,
        rows_to_insert,
    )
    if not errors:
      logging.info('Data added to table %s.', table_id)
    else:
      logging.info('Encountered errors while inserting rows: %s', errors)

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls._create_table()

  @classmethod
  def tearDownClass(cls):
    super().tearDownClass()
    cls._bq_client.delete_dataset(
        _DATASET_ID, delete_contents=True, not_found_ok=True
    )
    logging.info('Deleting BigQuery Dataset %s...', _DATASET_NAME)

  def setUp(self):
    super().setUp()
    # Initialize BigQuery wrapper for test.
    self._bq_wrapper = metadata_storage_client._BigQueryMetadataTableUtil(
        _PROJECT_ID, _DATASET_NAME, _TABLE_NAME
    )

  def test_column_names(self):
    columns = self._bq_wrapper.column_names

    self.assertEqual(columns, _COLUMNS)

  def test_find_data_frame_column_name_exact_match_succeeds(self):
    col_name = self._bq_wrapper._find_bq_column_name('BarCodeValue')

    self.assertEqual(col_name, _COLUMNS[0])

  def test_find_data_frame_column_name_returns_none(self):
    col_name = self._bq_wrapper._find_bq_column_name('Test')

    self.assertIsNone(col_name)

  def test_find_data_frame_column_name_with_space_succeeds(self):
    col_name = self._bq_wrapper._find_bq_column_name('Bar Code Value')

    self.assertEqual(col_name, _COLUMNS[0])

  def test_get_slide_metadata_succeeds(self):
    result = self._bq_wrapper.get_slide_metadata(_BAR_CODE_VALUE)

    self.assertEqual(result.total_rows, 1)
    for row in result:
      self.assertSameElements(row, [_BAR_CODE_VALUE, 'John', 'Doe'])

  def test_get_slide_metadata_no_result(self):
    result = self._bq_wrapper.get_slide_metadata('456')

    self.assertEqual(result.total_rows, 0)

  @flagsaver.flagsaver(metadata_primary_key='First Name')
  def test_get_slide_metadata_with_different_key_succeeds(self):
    result = self._bq_wrapper.get_slide_metadata('John')

    self.assertEqual(result.total_rows, 1)
    for row in result:
      self.assertSameElements(row, [_BAR_CODE_VALUE, 'John', 'Doe'])


if __name__ == '__main__':
  absltest.main()
