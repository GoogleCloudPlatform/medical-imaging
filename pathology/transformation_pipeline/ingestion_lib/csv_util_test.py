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
"""Tests for csv_util."""
import os

from absl.testing import absltest
import pandas

from transformation_pipeline.ingestion_lib import csv_util
from transformation_pipeline.ingestion_lib import gen_test_util


class CsvUtilTest(absltest.TestCase):

  def test_count_csv_header_lines(self):
    test = '\n'.join(['', ' #comment', '\t #', '# comment', 'line #'])
    csv_dir = self.create_tempdir()
    csv_file_path = os.path.join(csv_dir, 'tmp.csv')
    with open(csv_file_path, 'wt') as outfile:
      outfile.write(test)
    self.assertEqual(csv_util._csv_header_lines(csv_file_path), 4)

  def test_count_csv_header_no_lines(self):
    test = '\n'.join(['line #', 'line ', '"line"'])
    csv_dir = self.create_tempdir()
    csv_file_path = os.path.join(csv_dir, 'tmp.csv')
    with open(csv_file_path, 'wt') as outfile:
      outfile.write(test)
    self.assertEqual(csv_util._csv_header_lines(csv_file_path), 0)

  def test_read_chunked_csv(self):
    csv_file = []
    csv_file_path = gen_test_util.test_file_path('csv_reader_test.csv')
    with open(csv_file_path, 'rt') as infile:
      for line in infile:
        csv_file.append([ch.lstrip(' ').rstrip('\n') for ch in line.split(',')])

    csv_file_offset = csv_util._csv_header_lines(csv_file_path)
    chunk_read = csv_util.read_csv(csv_file_path, 1)
    test_row = 1
    for chunk in chunk_read:
      for idx, col_name in enumerate(chunk.columns):
        self.assertEqual(csv_file[csv_file_offset][idx], col_name)
      for row in range(chunk.shape[0]):
        for col in range(chunk.shape[1]):
          val = chunk.iloc[row, col]
          if col >= len(csv_file[test_row + csv_file_offset]):
            # if csv has more rows then the test data
            # pandas should return value as nan or None
            self.assertTrue(pandas.isna(val))
            continue

          # if csv reader is stripping leading spaces
          expected_val = csv_file[test_row + csv_file_offset][col]
          if not expected_val:
            # if test data is empty missing value
            self.assertTrue(pandas.isna(val))
            continue

          # if test data is quoted remove quotes
          if (
              len(expected_val) >= 2
              and expected_val[0] == '"'
              and expected_val[-1] == '"'
          ):
            expected_val = expected_val[1:-1]
          # remove escape character from test data
          expected_val = expected_val.replace('\\', '')
          self.assertEqual(expected_val, val)
        test_row += 1


if __name__ == '__main__':
  absltest.main()
