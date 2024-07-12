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
"""ILM heuristics to determine storage class changes based on metadata."""

import collections
import datetime
import logging
from typing import Any, Iterable, List, Optional, Set

import apache_beam as beam

import ilm_config
import ilm_types


def _convert_to_date(date_str: Optional[str]) -> Optional[datetime.date]:
  if not date_str:
    return None
  return datetime.datetime.strptime(date_str, '%Y%m%d').date()


def _optional_equal(
    metadata_value: Optional[Any], condition_value: Optional[Any]
) -> bool:
  if condition_value is None:
    return True
  if metadata_value is None:
    return False
  return metadata_value == condition_value


def _optional_in(
    metadata_value: Optional[Any], condition_value: Optional[Set[Any]]
) -> bool:
  if condition_value is None:
    return True
  if metadata_value is None:
    return False
  return metadata_value in condition_value


def _optional_larger(
    metadata_value: Optional[Any], condition_value: Optional[Any]
) -> bool:
  if condition_value is None:
    return True
  if metadata_value is None:
    return False
  return metadata_value > condition_value


def _optional_larger_or_equal(
    metadata_value: Optional[Any], condition_value: Optional[Any]
) -> bool:
  if condition_value is None:
    return True
  if metadata_value is None:
    return False
  return metadata_value >= condition_value


def _optional_smaller(
    metadata_value: Optional[Any], condition_value: Optional[Any]
) -> bool:
  if condition_value is None:
    return True
  if metadata_value is None:
    return False
  return metadata_value < condition_value


def _optional_smaller_or_equal(
    metadata_value: Optional[Any], condition_value: Optional[Any]
) -> bool:
  if condition_value is None:
    return True
  if metadata_value is None:
    return False
  return metadata_value <= condition_value


def _is_in_range(
    pixel_spacing: float,
    pixel_spacing_range: Optional[ilm_config.PixelSpacingRange],
) -> bool:
  """Whether pixel spacing is in expected range (min, max inclusive)."""
  if pixel_spacing_range is None:
    return True
  if pixel_spacing is None:
    return False
  if pixel_spacing_range.min and pixel_spacing_range.min > pixel_spacing:
    return False
  if pixel_spacing_range.max and pixel_spacing > pixel_spacing_range.max:
    return False
  return True


def _should_upgrade(
    metadata: ilm_types.InstanceMetadata,
    metadata_date: Optional[datetime.date],
    upgrade_conditions: Optional[
        List[ilm_config.ToHigherAvailabilityCondition]
    ] = None,
) -> Optional[int]:
  """Whether instance should be upgraded based on metadata and conditions."""
  if not metadata.access_metadata:
    raise ValueError(f'Instance metadata missing access info: {metadata}')
  for condition_index, condition in enumerate(upgrade_conditions):
    condition_date_after = _convert_to_date(condition.date_after)
    if condition.access_count_higher_or_equal_to:
      condition_access_count_higher_or_equal_to = (
          condition.access_count_higher_or_equal_to.count
      )
      metadata_access_count = metadata.access_metadata.get_access_count(
          condition.access_count_higher_or_equal_to.num_days
      )
    else:
      metadata_access_count = None
      condition_access_count_higher_or_equal_to = None
    if all([
        _optional_equal(metadata.modality, condition.modality),
        _optional_equal(metadata.sop_class_uid, condition.sop_class_uid),
        _optional_in(metadata.image_type, condition.image_type),
        _is_in_range(metadata.pixel_spacing, condition.pixel_spacing_range),
        _optional_larger(metadata_date, condition_date_after),
        _optional_smaller(metadata.size_bytes, condition.size_bytes_lower_than),
        _optional_larger_or_equal(
            metadata_access_count, condition_access_count_higher_or_equal_to
        ),
        _optional_larger(
            metadata.num_days_in_current_storage_class,
            condition.num_days_in_current_storage_class_higher_than,
        ),
    ]):
      # All of criteria within single condition are satisfied
      return condition_index
  # None of the conditions are satisfied
  return None


def _should_downgrade(
    metadata: ilm_types.InstanceMetadata,
    metadata_date: Optional[datetime.date],
    downgrade_conditions: Optional[
        List[ilm_config.ToLowerAvailabilityCondition]
    ] = None,
) -> Optional[int]:
  """Whether instance should be downgraded based on metadata and conditions."""
  if not metadata.access_metadata:
    raise ValueError(f'Instance metadata missing access info: {metadata}')
  for condition_index, condition in enumerate(downgrade_conditions):
    condition_date_before = _convert_to_date(condition.date_before)
    if condition.access_count_lower_or_equal_to:
      condition_access_count_lower_or_equal_to = (
          condition.access_count_lower_or_equal_to.count
      )
      metadata_access_count = metadata.access_metadata.get_access_count(
          condition.access_count_lower_or_equal_to.num_days
      )
    else:
      metadata_access_count = None
      condition_access_count_lower_or_equal_to = None
    if all([
        _optional_equal(metadata.modality, condition.modality),
        _optional_equal(metadata.sop_class_uid, condition.sop_class_uid),
        _optional_in(metadata.image_type, condition.image_type),
        _is_in_range(metadata.pixel_spacing, condition.pixel_spacing_range),
        _optional_smaller(metadata_date, condition_date_before),
        _optional_larger(metadata.size_bytes, condition.size_bytes_larger_than),
        _optional_smaller_or_equal(
            metadata_access_count, condition_access_count_lower_or_equal_to
        ),
        _optional_larger(
            metadata.num_days_in_current_storage_class,
            condition.num_days_in_current_storage_class_higher_than,
        ),
    ]):
      # All of criteria within single condition are satisfied
      return condition_index
  # None of the conditions are satisfied
  return None


def _get_highest_priority_date(
    metadata: ilm_types.InstanceMetadata,
    dates_priority: List[ilm_config.DateTags],
) -> Optional[datetime.date]:
  """Returns highest priority date in metadata."""
  date_tag_to_value = {
      ilm_config.DateTags.ACQUISITION_DATE: metadata.acquisition_date,
      ilm_config.DateTags.CONTENT_DATE: metadata.content_date,
      ilm_config.DateTags.SERIES_DATE: metadata.series_date,
      ilm_config.DateTags.STUDY_DATE: metadata.study_date,
  }
  for date_tag in dates_priority:
    if date_tag_to_value[date_tag] is not None:
      return date_tag_to_value[date_tag]
  return None


class ComputeStorageClassChangesDoFn(beam.DoFn):
  """DoFn to determine instances' storage class changes based on move rules."""

  def __init__(self, ilm_cfg: ilm_config.ImageLifecycleManagementConfig):
    self._from_storage_class_to_rules = collections.defaultdict(list)
    for rule_index, rule in enumerate(ilm_cfg.storage_class_config.move_rules):
      self._from_storage_class_to_rules[rule.from_storage_class].append(
          (rule_index, rule)
      )
    self._dates_priority = ilm_cfg.storage_class_config.date_priority

  def process(
      self, metadata: ilm_types.InstanceMetadata
  ) -> Iterable[ilm_types.StorageClassChange]:
    """Applies config move rules and returns metadata with new storage class."""
    move_rules = self._from_storage_class_to_rules[metadata.storage_class]
    metadata_date = _get_highest_priority_date(metadata, self._dates_priority)
    detected_move_rules = []
    storage_class_change = None
    for rule_index, rule in move_rules:
      if rule.upgrade_conditions:
        condition_index = _should_upgrade(
            metadata, metadata_date, rule.upgrade_conditions
        )
      else:
        condition_index = _should_downgrade(
            metadata, metadata_date, rule.downgrade_conditions
        )
      if condition_index is not None:
        detected_move_rules.append(rule_index)
        if storage_class_change is None:
          storage_class_change = ilm_types.StorageClassChange(
              instance=metadata.instance,
              new_storage_class=rule.to_storage_class,
              move_rule_id=ilm_types.MoveRuleId(
                  rule_index=rule_index, condition_index=condition_index
              ),
          )
    if len(detected_move_rules) > 1:
      logging.warning(
          'Multiple move rules detected for instance %s: %s',
          metadata.instance,
          detected_move_rules,
      )
    if storage_class_change:
      yield storage_class_change
