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
"""Tests for ingest_flags."""

import os
import sys
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized

from transformation_pipeline import ingest_flags


class IngestFlagsTest(parameterized.TestCase):

  @mock.patch.dict(os.environ, {'SOME_ENV_VAR': 'env_value'})
  def test_get_value_from_env_var(self):
    value = ingest_flags.get_value(
        env_var='SOME_ENV_VAR',
        default_value='default',
        test_default_value='test_default',
    )
    self.assertEqual(value, 'env_value')

  def test_get_value_from_test_default(self):
    value = ingest_flags.get_value(
        env_var='SOME_ENV_VAR',
        default_value='default',
        test_default_value='test_default',
    )
    self.assertEqual(value, 'test_default')

  @mock.patch.dict(sys.modules, {}, clear=True)
  @mock.patch.dict(os.environ, {}, clear=True)
  def test_get_value_from_default(self):
    value = ingest_flags.get_value(
        env_var='SOME_ENV_VAR',
        default_value='default',
        test_default_value='test_default',
    )
    self.assertEqual(value, 'default')

  def test_uid_source_default_value(self):
    self.assertIsNone(
        ingest_flags.GCS_INGEST_STUDY_INSTANCE_UID_SOURCE_FLG.value
    )

  def test_get_int_value_from_default_getenv_value(self):
    self.assertEqual(ingest_flags.DICOM_QUOTA_ERROR_RETRY_FLG.value, 600)

  def test_wsi2dcm_compression_enums(self):
    compression = {
        item.name: item.value for item in ingest_flags.Wsi2DcmCompression
    }
    first_level_compression = {
        item.name: item.value
        for item in ingest_flags.Wsi2DcmFirstLevelCompression
        if item.value is not None
    }
    # Test only differences between Wsi2DcmFirstLevelCompression and
    # Wsi2DcmCompression is the addtional None enumeration in
    # Wsi2DcmFirstLevelCompression.
    self.assertEqual(compression, first_level_compression)
    self.assertIsNone(ingest_flags.Wsi2DcmFirstLevelCompression.NONE.value)
    self.assertLen(
        ingest_flags.Wsi2DcmFirstLevelCompression,
        len(ingest_flags.Wsi2DcmCompression) + 1,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='none',
          val=None,
          expected=None,
      ),
      dict(
          testcase_name='single_string',
          val='test',
          expected='test',
      ),
      dict(
          testcase_name='multiple_strings',
          val='["test1","test2"]',
          expected=['test1', 'test2'],
      ),
  ])
  def test_load_multi_string(self, val, expected):
    self.assertEqual(ingest_flags._load_multi_string(val), expected)


if __name__ == '__main__':
  absltest.main()
