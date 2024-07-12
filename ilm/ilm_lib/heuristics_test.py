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
#
# ==============================================================================
"""Tests for ILM storage class changes heuristics."""

import dataclasses
import datetime
import logging

from absl.testing import absltest
from absl.testing import parameterized
import mock

import ilm_config
import ilm_types
from ilm_lib import heuristics
from test_utils import test_const


_UPGRADE_MOVE_RULE = ilm_config.MoveRule(
    from_storage_class=ilm_config.StorageClass.ARCHIVE,
    to_storage_class=ilm_config.StorageClass.STANDARD,
    upgrade_conditions=[
        ilm_config.ToHigherAvailabilityCondition(modality='SM'),
        ilm_config.ToHigherAvailabilityCondition(sop_class_uid='4.5.600'),
        ilm_config.ToHigherAvailabilityCondition(
            pixel_spacing_range=ilm_config.PixelSpacingRange(min=0.1, max=0.5)
        ),
        ilm_config.ToHigherAvailabilityCondition(date_after='20221231'),
        ilm_config.ToHigherAvailabilityCondition(size_bytes_lower_than=1024),
        ilm_config.ToHigherAvailabilityCondition(
            access_count_higher_or_equal_to=ilm_config.AccessCount(
                count=20.0, num_days=7
            )
        ),
        ilm_config.ToHigherAvailabilityCondition(
            num_days_in_current_storage_class_higher_than=365
        ),
        ilm_config.ToHigherAvailabilityCondition(
            image_type={'ORIGINAL/PRIMARY', 'ORIGINAL/SECONDARY'}
        ),
    ],
)
_ADDITIONAL_UPGRADE_MOVE_RULE = ilm_config.MoveRule(
    from_storage_class=ilm_config.StorageClass.ARCHIVE,
    to_storage_class=ilm_config.StorageClass.STANDARD,
    upgrade_conditions=[
        ilm_config.ToHigherAvailabilityCondition(modality='CT'),
    ],
)
_ADDITIONAL_UPGRADE_MOVE_RULE_MULTIPLE_CONDITIONS = ilm_config.MoveRule(
    from_storage_class=ilm_config.StorageClass.ARCHIVE,
    to_storage_class=ilm_config.StorageClass.STANDARD,
    upgrade_conditions=[
        ilm_config.ToHigherAvailabilityCondition(
            modality='GM', sop_class_uid='7.8.900'
        ),
        ilm_config.ToHigherAvailabilityCondition(
            date_after='20201231', size_bytes_lower_than=2000
        ),
    ],
)
_DOWNGRADE_MOVE_RULE = ilm_config.MoveRule(
    from_storage_class=ilm_config.StorageClass.STANDARD,
    to_storage_class=ilm_config.StorageClass.ARCHIVE,
    downgrade_conditions=[
        ilm_config.ToLowerAvailabilityCondition(modality='SM'),
        ilm_config.ToLowerAvailabilityCondition(sop_class_uid='4.5.600'),
        ilm_config.ToLowerAvailabilityCondition(
            pixel_spacing_range=ilm_config.PixelSpacingRange(min=0.1, max=0.5)
        ),
        ilm_config.ToLowerAvailabilityCondition(date_before='18001231'),
        ilm_config.ToLowerAvailabilityCondition(size_bytes_larger_than=4096),
        ilm_config.ToLowerAvailabilityCondition(
            access_count_lower_or_equal_to=ilm_config.AccessCount(
                count=2.0, num_days=7
            )
        ),
        ilm_config.ToLowerAvailabilityCondition(
            num_days_in_current_storage_class_higher_than=365
        ),
        ilm_config.ToLowerAvailabilityCondition(
            image_type={'ORIGINAL/PRIMARY', 'ORIGINAL/SECONDARY'}
        ),
    ],
)
_ADDITIONAL_DOWNGRADE_MOVE_RULE_MULTIPLE_CONDITIONS = ilm_config.MoveRule(
    from_storage_class=ilm_config.StorageClass.STANDARD,
    to_storage_class=ilm_config.StorageClass.ARCHIVE,
    downgrade_conditions=[
        ilm_config.ToLowerAvailabilityCondition(
            modality='GM', sop_class_uid='7.8.900'
        ),
        ilm_config.ToLowerAvailabilityCondition(
            date_before='20001231', size_bytes_larger_than=3000
        ),
    ],
)
_ADDITIONAL_DOWNGRADE_MOVE_RULE_ZERO_ACCESS_CONDITION = ilm_config.MoveRule(
    from_storage_class=ilm_config.StorageClass.STANDARD,
    to_storage_class=ilm_config.StorageClass.ARCHIVE,
    downgrade_conditions=[
        ilm_config.ToLowerAvailabilityCondition(
            access_count_lower_or_equal_to=ilm_config.AccessCount(
                count=0.0, num_days=5
            )
        ),
    ],
)

_STORAGE_CLASS_CONFIG = ilm_config.StorageClassConfig(
    move_rules=[
        _UPGRADE_MOVE_RULE,
        _DOWNGRADE_MOVE_RULE,
        _ADDITIONAL_UPGRADE_MOVE_RULE,
        _ADDITIONAL_UPGRADE_MOVE_RULE_MULTIPLE_CONDITIONS,
        _ADDITIONAL_DOWNGRADE_MOVE_RULE_MULTIPLE_CONDITIONS,
        _ADDITIONAL_DOWNGRADE_MOVE_RULE_ZERO_ACCESS_CONDITION,
    ],
    date_priority=[
        ilm_config.DateTags.ACQUISITION_DATE,
        ilm_config.DateTags.STUDY_DATE,
        ilm_config.DateTags.SERIES_DATE,
        ilm_config.DateTags.ACQUISITION_DATE,
    ],
)
_ILM_CONFIG = dataclasses.replace(
    test_const.ILM_CONFIG,
    storage_class_config=_STORAGE_CLASS_CONFIG,
)


class HeuristicsTest(parameterized.TestCase):

  def test_heuristics_dofn_process_instance_missing_access_metadata_fails(self):
    instance_metadata = dataclasses.replace(
        test_const.INSTANCE_METADATA_ARCHIVE, **{'access_metadata': None}
    )
    heuristics_dofn = heuristics.ComputeStorageClassChangesDoFn(_ILM_CONFIG)
    with self.assertRaises(ValueError):
      list(heuristics_dofn.process(instance_metadata))

  @parameterized.named_parameters(
      ('modality', {'modality': 'SM'}, ilm_types.MoveRuleId(0, 0)),
      ('sop_class', {'sop_class_uid': '4.5.600'}, ilm_types.MoveRuleId(0, 1)),
      ('pixel_spacing', {'pixel_spacing': 0.3}, ilm_types.MoveRuleId(0, 2)),
      (
          'acquisition_date',
          {'acquisition_date': datetime.date(year=2023, month=12, day=31)},
          ilm_types.MoveRuleId(0, 3),
      ),
      ('size', {'size_bytes': 512}, ilm_types.MoveRuleId(0, 4)),
      (
          'access',
          {
              'access_metadata': ilm_types.AccessMetadata([
                  ilm_config.AccessCount(count=10.0, num_days=2),
                  ilm_config.AccessCount(count=100.0, num_days=5),
              ])
          },
          ilm_types.MoveRuleId(0, 5),
      ),
      (
          'days_in_storage',
          {'num_days_in_current_storage_class': 366},
          ilm_types.MoveRuleId(0, 6),
      ),
      (
          'image_type',
          {'image_type': 'ORIGINAL/SECONDARY'},
          ilm_types.MoveRuleId(0, 7),
      ),
      (
          'multiple_rules_same_storage_class',
          {'modality': 'CT'},
          ilm_types.MoveRuleId(2, 0),
      ),
      (
          'multiple_conditions_modality_and_sop_class',
          {'modality': 'GM', 'sop_class_uid': '7.8.900'},
          ilm_types.MoveRuleId(3, 0),
      ),
      (
          'multiple_conditions_acquisition_date_and_size',
          {
              'acquisition_date': datetime.date(year=2021, month=12, day=31),
              'size_bytes': 1500,
          },
          ilm_types.MoveRuleId(3, 1),
      ),
  )
  def test_heuristics_dofn_process_upgrade_succeeds(
      self,
      new_instance_metadata_params,
      expected_move_rule_id,
  ):
    instance_metadata = dataclasses.replace(
        test_const.INSTANCE_METADATA_ARCHIVE, **new_instance_metadata_params
    )
    expected_result = ilm_types.StorageClassChange(
        instance=test_const.INSTANCE_0,
        new_storage_class=ilm_config.StorageClass.STANDARD,
        move_rule_id=expected_move_rule_id,
    )
    heuristics_dofn = heuristics.ComputeStorageClassChangesDoFn(_ILM_CONFIG)
    self.assertEqual(
        list(heuristics_dofn.process(instance_metadata)), [expected_result]
    )

  @parameterized.named_parameters(
      ('no_upgrade', {}),
      ('no_upgrade_modality_missing', {'modality': None}),
      (
          'no_upgrade_dates_missing',
          {
              'acquisition_date': None,
              'content_date': None,
              'series_date': None,
              'study_date': None,
          },
      ),
      ('no_upgrade_pixel_spacing_missing', {'pixel_spacing': None}),
  )
  def test_heuristics_dofn_process_no_upgrade_succeeds(
      self, new_instance_metadata_params
  ):
    instance_metadata = dataclasses.replace(
        test_const.INSTANCE_METADATA_ARCHIVE, **new_instance_metadata_params
    )
    heuristics_dofn = heuristics.ComputeStorageClassChangesDoFn(_ILM_CONFIG)
    self.assertEmpty(list(heuristics_dofn.process(instance_metadata)))

  @parameterized.named_parameters(
      ('modality', {'modality': 'SM'}, ilm_types.MoveRuleId(1, 0)),
      ('sop_class', {'sop_class_uid': '4.5.600'}, ilm_types.MoveRuleId(1, 1)),
      ('pixel_spacing', {'pixel_spacing': 0.3}, ilm_types.MoveRuleId(1, 2)),
      (
          'acquisition_date',
          {'acquisition_date': datetime.date(year=1700, month=12, day=31)},
          ilm_types.MoveRuleId(1, 3),
      ),
      ('size', {'size_bytes': 8192}, ilm_types.MoveRuleId(1, 4)),
      (
          'access',
          {
              'access_metadata': ilm_types.AccessMetadata([
                  ilm_config.AccessCount(count=1.0, num_days=2),
                  ilm_config.AccessCount(count=2.0, num_days=5),
              ])
          },
          ilm_types.MoveRuleId(1, 5),
      ),
      (
          'access_zero',
          {'access_metadata': ilm_types.AccessMetadata([])},
          ilm_types.MoveRuleId(1, 5),
      ),
      (
          'access_zero_past_five_days',
          {
              'access_metadata': ilm_types.AccessMetadata(
                  [ilm_config.AccessCount(count=3.0, num_days=7)]
              )
          },
          ilm_types.MoveRuleId(5, 0),
      ),
      (
          'days_in_storage',
          {'num_days_in_current_storage_class': 366},
          ilm_types.MoveRuleId(1, 6),
      ),
      (
          'image_type',
          {'image_type': 'ORIGINAL/SECONDARY'},
          ilm_types.MoveRuleId(1, 7),
      ),
      (
          'multiple_conditions_modality_and_sop_class',
          {'modality': 'GM', 'sop_class_uid': '7.8.900'},
          ilm_types.MoveRuleId(4, 0),
      ),
      (
          'multiple_conditions_study_date_and_size',
          {
              'acquisition_date': None,
              'study_date': datetime.date(year=1900, month=12, day=31),
              'size_bytes': 4000,
          },
          ilm_types.MoveRuleId(4, 1),
      ),
  )
  def test_heuristics_dofn_process_downgrade_succeeds(
      self,
      new_instance_metadata_params,
      expected_move_rule_id,
  ):
    instance_metadata = dataclasses.replace(
        test_const.INSTANCE_METADATA_STANDARD, **new_instance_metadata_params
    )
    expected_result = ilm_types.StorageClassChange(
        instance=test_const.INSTANCE_0,
        new_storage_class=ilm_config.StorageClass.ARCHIVE,
        move_rule_id=expected_move_rule_id,
    )
    heuristics_dofn = heuristics.ComputeStorageClassChangesDoFn(_ILM_CONFIG)
    self.assertEqual(
        list(heuristics_dofn.process(instance_metadata)), [expected_result]
    )

  @parameterized.named_parameters(
      ('no_downgrade', {}),
      ('no_downgrade_modality_missing', {'modality': None}),
      ('no_downgrade_acquisition_date_missing', {'acquisition_date': None}),
      ('no_downgrade_pixel_spacing_missing', {'pixel_spacing': None}),
  )
  def test_heuristics_dofn_process_no_downgrade_succeeds(
      self, new_instance_metadata_params
  ):
    instance_metadata = dataclasses.replace(
        test_const.INSTANCE_METADATA_STANDARD, **new_instance_metadata_params
    )
    heuristics_dofn = heuristics.ComputeStorageClassChangesDoFn(_ILM_CONFIG)
    self.assertEmpty(list(heuristics_dofn.process(instance_metadata)))

  @mock.patch.object(logging, 'warning', autospec=True)
  def test_dofn_process_multiple_rules_detected(self, mock_warning):
    instance_metadata = dataclasses.replace(
        test_const.INSTANCE_METADATA_STANDARD,
        pixel_spacing=0.2,
        access_metadata=ilm_types.AccessMetadata([]),
    )
    heuristics_dofn = heuristics.ComputeStorageClassChangesDoFn(_ILM_CONFIG)
    expected_result = ilm_types.StorageClassChange(
        instance=test_const.INSTANCE_0,
        new_storage_class=ilm_config.StorageClass.ARCHIVE,
        move_rule_id=ilm_types.MoveRuleId(1, 2),
    )
    self.assertEqual(
        list(heuristics_dofn.process(instance_metadata)), [expected_result]
    )
    mock_warning.assert_called_once()

  @parameterized.named_parameters(
      (
          'below_min',
          0.2,
          ilm_config.PixelSpacingRange(min=0.3, max=0.8),
          False,
      ),
      (
          'above_max',
          0.9,
          ilm_config.PixelSpacingRange(min=0.3, max=0.8),
          False,
      ),
      (
          'min_max_equal',
          0.5,
          ilm_config.PixelSpacingRange(min=0.5, max=0.5),
          True,
      ),
      (
          'min_equal',
          0.5,
          ilm_config.PixelSpacingRange(min=0.5, max=0.8),
          True,
      ),
      (
          'max_equal',
          0.5,
          ilm_config.PixelSpacingRange(min=0.3, max=0.5),
          True,
      ),
      (
          'between_min_max',
          0.5,
          ilm_config.PixelSpacingRange(min=0.3, max=0.8),
          True,
      ),
      (
          'min_none',
          0.5,
          ilm_config.PixelSpacingRange(max=0.8),
          True,
      ),
      (
          'max_none',
          0.5,
          ilm_config.PixelSpacingRange(min=0.3),
          True,
      ),
      (
          'instance_zero',
          0,
          ilm_config.PixelSpacingRange(min=0.3, max=0.5),
          False,
      ),
      ('range_none', 0.5, None, True),
      ('instance_zero_range_none', 0, None, True),
  )
  def test_pixel_spacing_is_in_range(
      self, pixel_spacing, pixel_spacing_range, expected_result
  ):
    self.assertEqual(
        heuristics._is_in_range(pixel_spacing, pixel_spacing_range),
        expected_result,
    )

  @parameterized.named_parameters(
      (
          'acquisition_date',
          {},
          [
              ilm_config.DateTags.ACQUISITION_DATE,
              ilm_config.DateTags.CONTENT_DATE,
              ilm_config.DateTags.SERIES_DATE,
              ilm_config.DateTags.STUDY_DATE,
          ],
          test_const.INSTANCE_METADATA_STANDARD.acquisition_date,
      ),
      (
          'content_date',
          {},
          [
              ilm_config.DateTags.CONTENT_DATE,
              ilm_config.DateTags.STUDY_DATE,
              ilm_config.DateTags.SERIES_DATE,
              ilm_config.DateTags.ACQUISITION_DATE,
          ],
          test_const.INSTANCE_METADATA_STANDARD.content_date,
      ),
      (
          'study_date_single_element',
          {},
          [
              ilm_config.DateTags.STUDY_DATE,
          ],
          test_const.INSTANCE_METADATA_STANDARD.study_date,
      ),
      (
          'lowest_priority',
          {'content_date': None, 'series_date': None, 'study_date': None},
          [
              ilm_config.DateTags.CONTENT_DATE,
              ilm_config.DateTags.STUDY_DATE,
              ilm_config.DateTags.SERIES_DATE,
              ilm_config.DateTags.ACQUISITION_DATE,
          ],
          test_const.INSTANCE_METADATA_STANDARD.acquisition_date,
      ),
      (
          'no_match',
          {'content_date': None, 'series_date': None, 'study_date': None},
          [
              ilm_config.DateTags.CONTENT_DATE,
              ilm_config.DateTags.STUDY_DATE,
              ilm_config.DateTags.SERIES_DATE,
          ],
          None,
      ),
  )
  def test_get_highest_priority_date(
      self, metadata_params, date_priority, expected_date
  ):
    instance_metadata = dataclasses.replace(
        test_const.INSTANCE_METADATA_STANDARD, **metadata_params
    )
    self.assertEqual(
        heuristics._get_highest_priority_date(instance_metadata, date_priority),
        expected_date,
    )


if __name__ == '__main__':
  absltest.main()
