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
from typing import Optional
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized

from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator


def _get_uid_gen_state() -> uid_generator._UIDGeneratorState:
  if uid_generator._uid_generator_state is None:
    raise ValueError('UID generator state is not initialized.')
  return uid_generator._uid_generator_state


def _init_uid_generator_state(
    seed: Optional[int] = None, counter: int = 1
) -> None:
  state = uid_generator._UIDGeneratorState(seed)
  state._counter = counter
  uid_generator._uid_generator_state = state


class UidGeneratorTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    uid_generator._init_fork_module_state()

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

  @parameterized.named_parameters(
      dict(
          testcase_name='long',
          uid_prefix=uid_generator.TEST_UID_PREFIX,
          expected='1.3.6.1.4.1.11129.5.7.0.0',
      ),
      dict(
          testcase_name='short',
          uid_prefix=uid_generator.TEST_UID_PREFIX[:-2],
          expected='1.3.6.1.4.1.11129.5.7.0',
      ),
  )
  def test_get_prefix_user_defined_long(self, uid_prefix, expected):
    with flagsaver.flagsaver(pod_hostname='1234', dicom_guid_prefix=uid_prefix):
      _init_uid_generator_state()
      self.assertEqual(_get_uid_gen_state().get_prefix_str(), expected)

  @flagsaver.flagsaver(
      pod_hostname='image-ingest-748f88c5d6-6spv8',
      dicom_guid_prefix=uid_generator.TEST_UID_PREFIX,
  )
  def test_get_hostname(self):
    _init_uid_generator_state()
    self.assertEqual(_get_uid_gen_state().get_hostname_str(), '233864984120')

  @parameterized.named_parameters(
      dict(
          testcase_name='one_char',
          hostname='i',
          expected_length=3,
      ),
      dict(
          testcase_name='short',
          hostname='image-ingest-748f88c5d6-v8',
          expected_length=5,
      ),
      dict(
          testcase_name='expected',
          hostname='image-ingest-748f88c5d6-6spv8',
          expected_length=12,
      ),
      dict(
          testcase_name='long',
          hostname='imageingest748f88c5d66spv8',
          expected_length=12,
      ),
  )
  @flagsaver.flagsaver(dicom_guid_prefix=uid_generator.TEST_UID_PREFIX)
  def test_get_host_name_component_length(self, hostname, expected_length):
    with flagsaver.flagsaver(pod_hostname=hostname):
      _init_uid_generator_state()
      self.assertLen(_get_uid_gen_state().get_hostname_str(), expected_length)

  @flagsaver.flagsaver(
      pod_hostname='image-ingest-748f88c5d6-6spv8',
      dicom_guid_prefix=uid_generator.TEST_UID_PREFIX,
  )
  def test_get_time(self):
    _init_uid_generator_state()
    with mock.patch('time.time', return_value=1635400079.014863):
      self.assertEqual(_get_uid_gen_state().get_time_str(), '1635400079014')

  @flagsaver.flagsaver(
      pod_hostname='image-ingest-748f88c5d6-6spv8',
      dicom_guid_prefix=uid_generator.TEST_UID_PREFIX,
  )
  def test_get_counter(self):
    _init_uid_generator_state()
    self.assertEqual(_get_uid_gen_state().get_counter_str(), '002')

  @flagsaver.flagsaver(
      pod_hostname='image-ingest-748f88c5d6-6spv8',
      dicom_guid_prefix=f'{ingest_const.DPAS_UID_PREFIX}.9',
  )
  def test_get_counter_rollover(self):
    _init_uid_generator_state(counter=999)
    self.assertEqual(_get_uid_gen_state().get_counter_str(), '000')

  @flagsaver.flagsaver(
      pod_hostname='image-ingest-748f88c5d6-6spv8',
      dicom_guid_prefix=f'{ingest_const.DPAS_UID_PREFIX}.9',
  )
  def test_for_timer_roll_over(self):
    _init_uid_generator_state()
    with mock.patch('time.time', return_value=9.999):
      self.assertLen(uid_generator.generate_uid(), 64)
    with mock.patch('time.time', return_value=10.999):
      self.assertLen(uid_generator.generate_uid(), 64)

  @flagsaver.flagsaver(
      pod_hostname='image-ingest-748f88c5d6-6spv8',
      dicom_guid_prefix=uid_generator.TEST_UID_PREFIX,
  )
  def test_generate_uid(self):
    _init_uid_generator_state()
    mock_time_stamp = 1635400079.014863
    with mock.patch('time.time', return_value=mock_time_stamp):
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
      '',
      f'{ingest_const.DPAS_UID_PREFIX}.A',
      f'{ingest_const.DPAS_UID_PREFIX}.01',
      f'{ingest_const.DPAS_UID_PREFIX}..1',
      '1.2.3',
      f'{ingest_const.DPAS_UID_PREFIX}.12345678',
  ])
  def test_validate_uid_prefix_invalid_prefix(self, test_prefix):
    with flagsaver.flagsaver(dicom_guid_prefix=test_prefix):
      with self.assertRaises(uid_generator.InvalidUIDPrefixError):
        uid_generator.validate_uid_prefix()

  @parameterized.named_parameters([
      dict(
          testcase_name='smallest',
          counter_start=uid_generator._MIN_UID_COUNTER_VALUE,
          expected=(
              '1.3.6.1.4.1.11129.5.7.9.233864984120.7604876475.1635400079014001'
          ),
      ),
      dict(
          testcase_name='second_largest',
          counter_start=uid_generator._MAX_UID_COUNTER_VALUE - 1,
          expected=(
              '1.3.6.1.4.1.11129.5.7.9.233864984120.7604876475.1635400079014999'
          ),
      ),
      dict(
          testcase_name='largest',
          counter_start=uid_generator._MAX_UID_COUNTER_VALUE,
          expected=(
              '1.3.6.1.4.1.11129.5.7.9.233864984120.7604876475.1635400079014000'
          ),
      ),
  ])
  @mock.patch('time.time', return_value=1635400079.014863)
  @flagsaver.flagsaver(
      pod_hostname='image-ingest-748f88c5d6-6spv8',
      dicom_guid_prefix=f'{ingest_const.DPAS_UID_PREFIX}.9',
  )
  def test_init_at_counter_extremes(
      self, *unused_mocks, counter_start, expected
  ):
    _init_uid_generator_state(seed=0, counter=counter_start)
    self.assertEqual(uid_generator.generate_uid(), expected)

  def test_init_random_fraction_raises_if_uid_exceeds_max_length(self):
    _init_uid_generator_state()
    _get_uid_gen_state()._hostname_uid = 'imageingest748f88c5d66spv8'
    with self.assertRaises(ValueError):
      _get_uid_gen_state()._init_random_fraction()

  def test_generate_uid_raises_if_uid_exceeds_max_length(self):
    _init_uid_generator_state()
    _get_uid_gen_state()._hostname_uid = 'imageingest748f88c5d66spv8'
    with self.assertRaises(ValueError):
      uid_generator.generate_uid()

  @parameterized.named_parameters(
      dict(testcase_name='empty', uid='', expected=True),
      dict(testcase_name='to_long', uid=f'{"1"*65}', expected=False),
      dict(testcase_name='max_ones', uid=f'{"1"*64}', expected=True),
      dict(testcase_name='empty_block', uid='1..1', expected=False),
      dict(testcase_name='starts_with_zero', uid='1.023.1', expected=False),
      dict(testcase_name='contains_non_number', uid='1.2A3.1', expected=False),
      dict(testcase_name='valid_small', uid='1.2.3', expected=True),
      dict(
          testcase_name='valid_multi_number_blocks',
          uid='1.0.1232.3',
          expected=True,
      ),
  )
  def test_validate_uid_format(self, uid, expected):
    self.assertEqual(uid_generator.validate_uid_format(uid), expected)

  @flagsaver.flagsaver(pod_hostname='')
  def test_validate_empty_hostname(self):
    _init_uid_generator_state()
    self.assertTrue(
        uid_generator.validate_uid_format(uid_generator.generate_uid())
    )

  def test_get_uid(self):
    self.assertEqual(uid_generator._get_uid(['abc', '', 'def']), 'abc.def')


if __name__ == '__main__':
  absltest.main()
