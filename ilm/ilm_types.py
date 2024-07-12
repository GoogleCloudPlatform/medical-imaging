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
"""Types used in ILM pipeline."""

import bisect
import dataclasses
import datetime
from typing import List, Optional

import dataclasses_json

import ilm_config


@dataclasses.dataclass(frozen=True)
class AccessMetadata:
  """Instance access metadata."""

  # Total (cumulative) access count, sorted by number of days passed.
  cumulative_access_counts: List[ilm_config.AccessCount]

  def __post_init__(self):
    for i in range(len(self.cumulative_access_counts) - 1):
      if (
          self.cumulative_access_counts[i + 1].num_days
          <= self.cumulative_access_counts[i].num_days
      ):
        raise ValueError(
            'Cumulative access count list should be sorted:'
            f' {self.cumulative_access_counts}'
        )

  def get_access_count(self, num_days: int) -> float:
    """Returns total number of accesses for the past num_days."""
    i = (
        bisect.bisect_right(
            self.cumulative_access_counts, num_days, key=lambda x: x.num_days
        )
        - 1
    )
    if i < 0:
      # No access in past num_days.
      return 0
    return self.cumulative_access_counts[i].count


@dataclasses.dataclass(frozen=True)
class LogAccessCount:
  frames_access_count: Optional[ilm_config.AccessCount] = None
  instance_access_count: Optional[ilm_config.AccessCount] = None


@dataclasses.dataclass(frozen=True)
class LogAccessMetadata:
  frames_access_counts: List[ilm_config.AccessCount]
  instance_access_counts: List[ilm_config.AccessCount]


@dataclasses.dataclass(frozen=True)
class InstanceMetadata:
  """Instance metadata to be considered when moving between storage classes."""

  # DICOM metadata
  instance: str  # Instance DICOM web path.
  modality: Optional[str]
  num_frames: int
  pixel_spacing: Optional[float]
  sop_class_uid: str
  acquisition_date: Optional[datetime.date]
  content_date: Optional[datetime.date]
  series_date: Optional[datetime.date]
  study_date: Optional[datetime.date]
  image_type: Optional[str]

  # Storage metadata
  size_bytes: int
  storage_class: ilm_config.StorageClass
  num_days_in_current_storage_class: int

  # Access metadata
  access_metadata: Optional[AccessMetadata] = None


@dataclasses.dataclass(frozen=True)
class MoveRuleId:
  rule_index: int
  condition_index: int


@dataclasses.dataclass(frozen=True)
class StorageClassChange:
  instance: str
  new_storage_class: ilm_config.StorageClass
  move_rule_id: MoveRuleId


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class SetStorageClassRequestMetadata:
  move_rule_ids: List[MoveRuleId]
  # Only populated if generating detailed report of updates. See report config.
  instances: List[str]
  filter_file_gcs_uri: str
  new_storage_class: ilm_config.StorageClass


@dataclasses.dataclass(frozen=False)
class SetStorageClassOperationMetadata:
  move_rule_ids: List[MoveRuleId]
  instances: List[str]
  operation_name: str
  filter_file_gcs_uri: Optional[str] = None
  start_time: Optional[float] = None
  # None indicates unfinished operation.
  succeeded: Optional[bool] = None
