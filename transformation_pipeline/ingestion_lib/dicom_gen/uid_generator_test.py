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
"""Tests for uid_generator."""
from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized

from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator

FLAGS = flags.FLAGS


class UidGeneratorTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    uid_generator._UIDGenerator.max_length = 64
    uid_generator._UIDGenerator.counter = 0
    uid_generator._UIDGenerator.rand_fraction = None
    uid_generator._UIDGenerator.clip_pod = 0
    uid_generator._UIDGenerator.prefix = ''
    uid_generator._UIDGenerator.pod = ''
    uid_generator._UIDGenerator.last_time_str_len = None

  def test_validate_uid_block_empty(self):
    self.assertFalse(uid_generator.is_uid_block_correctly_formatted(''))

  def test_validate_uid_block_valid(self):
    self.assertTrue(uid_generator.is_uid_block_correctly_formatted('1343'))

  def test_validate_uid_block_cannot_with_zero(self):
    self.assertFalse(uid_generator.is_uid_block_correctly_formatted('01343'))

  def test_validate_uid_block_can_start_zero(self):
    self.assertTrue(uid_generator.is_uid_block_correctly_formatted('0'))

  def test_validate_uid_block_contains_invalid_char(self):
    self.assertFalse(uid_generator.is_uid_block_correctly_formatted('1ABC'))

  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_validate_uid_prefix(self):
    self.assertTrue(uid_generator.validate_uid_prefix())

  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX[:-2]
  )
  def test_get_prefix_user_defined_short(self):
    self.assertEqual(uid_generator._get_prefix(), '1.3.6.1.4.1.11129.5.7.0')

  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_get_prefix_user_defined_long(self):
    self.assertEqual(uid_generator._get_prefix(), '1.3.6.1.4.1.11129.5.7.0.0')

  @flagsaver.flagsaver(
      pod_hostname='image-ingest-748f88c5d6-6spv8',
      dicom_guid_prefix=uid_generator.TEST_UID_PREFIX,
  )
  def test_get_pod(self):
    self.assertEqual(uid_generator._get_pod(), '233864984120')

  @flagsaver.flagsaver(pod_hostname='')
  def test_get_pod_missing_hostname(self):
    with self.assertRaises(ValueError):
      uid_generator._get_pod()

  @flagsaver.flagsaver(
      pod_hostname='image-ingest-748f88c5d6-6spv8',
      dicom_guid_prefix=uid_generator.TEST_UID_PREFIX,
  )
  def test_get_pod_clipped(self):
    uid_generator._UIDGenerator.clip_pod = 1
    self.assertEqual(uid_generator._get_pod(), '23386498412')

  @flagsaver.flagsaver(
      pod_hostname='image-ingest-748f88c5d6-6spv8',
      dicom_guid_prefix=uid_generator.TEST_UID_PREFIX,
  )
  def test_get_time(self):
    with mock.patch('time.time') as mock_method:
      mock_method.return_value = 1635400079.014863
      self.assertEqual(uid_generator._get_time(), '1635400079014')

  @flagsaver.flagsaver(
      pod_hostname='image-ingest-748f88c5d6-6spv8',
      dicom_guid_prefix=uid_generator.TEST_UID_PREFIX,
  )
  def test_get_counter(self):
    self.assertEqual(uid_generator._get_counter(), '001')

  @flagsaver.flagsaver(
      pod_hostname='image-ingest-748f88c5d6-6spv8',
      dicom_guid_prefix=f'{ingest_const.DPAS_UID_PREFIX}.9',
  )
  def test_get_counter_rollover(self):
    uid_generator._UIDGenerator.counter = 999
    self.assertEqual(uid_generator._get_counter(), '001')

  @flagsaver.flagsaver(
      pod_hostname='image-ingest-748f88c5d6-6spv8',
      dicom_guid_prefix=f'{ingest_const.DPAS_UID_PREFIX}.9',
  )
  def test_for_timer_roll_over(self):
    mock_time_stamp = 9.999
    with mock.patch('time.time') as mock_method:
      mock_method.return_value = mock_time_stamp
      uid_generator._init_random_fraction()
      uid_generator._UIDGenerator.counter = 1
      uid = uid_generator.generate_uid()
    self.assertLen(uid, 64)
    mock_time_stamp = 10.999
    with mock.patch('time.time') as mock_method:
      mock_method.return_value = mock_time_stamp
      uid = uid_generator.generate_uid()
    self.assertLen(uid, 64)

  @flagsaver.flagsaver(
      pod_hostname='image-ingest-748f88c5d6-6spv8',
      dicom_guid_prefix=uid_generator.TEST_UID_PREFIX,
  )
  def test_generate_uid(self):
    mock_time_stamp = 1635400079.014863
    with mock.patch('time.time') as mock_method:
      mock_method.return_value = mock_time_stamp
      uid_generator._init_random_fraction()
      uid_generator._UIDGenerator.counter = 1
      uid = uid_generator.generate_uid()
    self.assertLen(uid, 64)
    uid_parts = uid.split('.')
    self.assertLen(uid_parts, 14)

    dicom_prefix_parts = uid_generator.TEST_UID_PREFIX.split('.')
    for idx, _ in enumerate(dicom_prefix_parts):
      self.assertEqual(uid_parts[idx], dicom_prefix_parts[idx])
    # 233864984120  is generated from pod_hostname
    gen_base_index = len(dicom_prefix_parts)
    self.assertEqual(uid_parts[gen_base_index], '233864984120')
    # random component
    self.assertNotEqual(uid_parts[gen_base_index + 1][0], '0')
    # DateTime Counter component.
    mock_time_stamp = str(int(mock_time_stamp * 1000))
    counter_str = '002'
    self.assertEqual(
        uid_parts[gen_base_index + 2], mock_time_stamp + counter_str
    )

  @parameterized.parameters([
      None,
      f'{ingest_const.DPAS_UID_PREFIX}.A',
      f'{ingest_const.DPAS_UID_PREFIX}.01',
      f'{ingest_const.DPAS_UID_PREFIX}..1',
      f'{ingest_const.DPAS_UID_PREFIX}.1.1.1',
      ingest_const.DPAS_UID_PREFIX,
      '1.2.3',
      f'{ingest_const.DPAS_UID_PREFIX}.1234',
      f'{ingest_const.DPAS_UID_PREFIX}.',
  ])
  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_validate_uid_prefix_invalid_prefix(self, test_prefix):
    FLAGS.dicom_guid_prefix = test_prefix
    uid_generator._UIDGenerator.prefix = ''
    with self.assertRaises(uid_generator.InvalidUIDPrefixError):
      uid_generator.validate_uid_prefix()


if __name__ == '__main__':
  absltest.main()
