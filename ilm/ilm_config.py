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
"""Image Lifecycle Management config."""

import dataclasses
import enum
from typing import List, Optional, Set

import dataclasses_json


class InvalidConfigError(Exception):
  pass


class StorageClass(enum.Enum):
  STANDARD = 'STANDARD'
  NEARLINE = 'NEARLINE'
  COLDLINE = 'COLDLINE'
  ARCHIVE = 'ARCHIVE'


_STORAGE_CLASS_AVAILABILITY = {
    StorageClass.STANDARD: 3,
    StorageClass.NEARLINE: 2,
    StorageClass.COLDLINE: 1,
    StorageClass.ARCHIVE: 0,
}


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class AccessCount:
  count: float
  num_days: int

  def __post_init__(self):
    if self.count < 0:
      raise InvalidConfigError('Access count must be >= 0.')
    if self.num_days < 0:
      raise InvalidConfigError('Number of days in access count must be >= 0.')


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class PixelSpacingRange:
  min: Optional[float] = None
  max: Optional[float] = None

  def __post_init__(self):
    if not self.min and not self.max:
      raise InvalidConfigError(
          'At least one of (min, max) in pixel spacing range must be set.'
      )


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class ToLowerAvailabilityCondition:
  """Condition for moving to a lower availability storage class."""

  modality: Optional[str] = None
  sop_class_uid: Optional[str] = None
  # Min, max range inclusive, i.e.,
  # range min <= instance pixel spacing <= range max
  pixel_spacing_range: Optional[PixelSpacingRange] = None
  # Date format: YYYYMMDD. See StorageClassConfig for relative date priority.
  date_before: Optional[str] = None
  size_bytes_larger_than: Optional[int] = None
  access_count_lower_or_equal_to: Optional[AccessCount] = None
  num_days_in_current_storage_class_higher_than: Optional[int] = None
  # Set of concatenated image type array values to match with, e.g.
  # {"DERIVED/PRIMARY/VOLUME/RESAMPLED", "ORIGINAL/PRIMARY/VOLUME"}
  # Please note /NONE suffixes should be added as a separate value if
  # applicable, i.e. for example
  # image_type = {"ORIGINAL/PRIMARY/VOLUME"} in the ILM config will NOT
  # automatically match with instances that have
  # image type = [ORIGINAL, PRIMARY, VOLUME, NONE] and vice versa, so
  # both {"ORIGINAL/PRIMARY/VOLUME", "ORIGINAL/PRIMARY/VOLUME/NONE"}
  # should be added to the config if this is desirable.
  image_type: Optional[Set[str]] = None

  def __str__(self):
    params = []
    if self.modality:
      params.append(f'modality={self.modality}')
    if self.sop_class_uid:
      params.append(f'sop_class_uid={self.sop_class_uid}')
    if self.pixel_spacing_range:
      params.append(f'pixel_spacing_range={self.pixel_spacing_range}')
    if self.date_before:
      params.append(f'date_before={self.date_before}')
    if self.size_bytes_larger_than:
      params.append(f'size_bytes_larger_than={self.size_bytes_larger_than}')
    if self.access_count_lower_or_equal_to:
      params.append(
          f'access_count_lower_or_equal_to={self.access_count_lower_or_equal_to}'
      )
    if self.num_days_in_current_storage_class_higher_than:
      params.append(
          f'num_days_in_current_storage_class_higher_than={self.num_days_in_current_storage_class_higher_than}'
      )
    if self.image_type:
      params.append(f'image_type={self.image_type}')
    return 'ToLowerAvailabilityCondition(' + ', '.join(params) + ')'


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class ToHigherAvailabilityCondition:
  """Condition for moving to a higher availability storage class."""

  modality: Optional[str] = None
  sop_class_uid: Optional[str] = None
  # Min, max range inclusive, i.e.,
  # range min <= instance pixel spacing <= range max
  pixel_spacing_range: Optional[PixelSpacingRange] = None
  # Date format: YYYYMMDD. See StorageClassConfig for relative date priority.
  date_after: Optional[str] = None
  size_bytes_lower_than: Optional[int] = None
  access_count_higher_or_equal_to: Optional[AccessCount] = None
  num_days_in_current_storage_class_higher_than: Optional[int] = None
  # Set of concatenated image type array values to match with, e.g.
  # {"DERIVED/PRIMARY/VOLUME/RESAMPLED", "ORIGINAL/PRIMARY/VOLUME"}
  # Please note /NONE suffixes should be added as a separate value if
  # applicable, i.e. for example
  # image_type = {"ORIGINAL/PRIMARY/VOLUME"} in the ILM config will NOT
  # automatically match with instances that have
  # image type = [ORIGINAL, PRIMARY, VOLUME, NONE] and vice versa, so
  # both {"ORIGINAL/PRIMARY/VOLUME", "ORIGINAL/PRIMARY/VOLUME/NONE"}
  # should be added to the config if this is desirable.
  image_type: Optional[Set[str]] = None

  def __str__(self):
    params = []
    if self.modality:
      params.append(f'modality={self.modality}')
    if self.sop_class_uid:
      params.append(f'sop_class_uid={self.sop_class_uid}')
    if self.pixel_spacing_range:
      params.append(f'pixel_spacing_range={self.pixel_spacing_range}')
    if self.date_after:
      params.append(f'date_after={self.date_after}')
    if self.size_bytes_lower_than:
      params.append(f'size_bytes_lower_than={self.size_bytes_lower_than}')
    if self.access_count_higher_or_equal_to:
      params.append(
          f'access_count_higher_or_equal_to={self.access_count_higher_or_equal_to}'
      )
    if self.num_days_in_current_storage_class_higher_than:
      params.append(
          f'num_days_in_current_storage_class_higher_than={self.num_days_in_current_storage_class_higher_than}'
      )
    if self.image_type:
      params.append(f'image_type={self.image_type}')
    return 'ToHigherAvailabilityCondition(' + ', '.join(params) + ')'


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class MoveRule:
  """Rule for moving an instance from one storage class to another.

  If moving from a lower to a higher availability storage class,
  upgrade_conditions must be populated. Conversely, if moving from a higher
  to a lower availability storage class, downgrade_conditions must be
  populated.

  Instances are moved if ANY of the conditions in the downgrade/upgrade
  list are met. For a condition to be satisfied, ALL criteria within it must
  be satisfied.

  E.g.: Given a move rule:
    from_storage_class = STANDARD
    to_storage_class = ARCHIVE
    downgrade_conditions = [
      (modality = 'SM'),
      (modality = 'MR', access_count_lower_or_equal_to = (count = 0, days = 10))
      (modality = 'MR', access_count_lower_or_equal_to = (count = 5, days = 30))
    ]

    Instances will be moved if:
      (modality = SM)
    OR
      (modality = MR AND they were not accessed in last 10 days)
    OR
      (modality = MR AND they were accessed 5 times or less in last 30 days)
  """

  # Current storage class for DICOM instance.
  from_storage_class: StorageClass
  # New storage class for DICOM instance.
  to_storage_class: StorageClass
  # List of conditions to move instances between storage classes above.
  upgrade_conditions: Optional[List[ToHigherAvailabilityCondition]] = None
  downgrade_conditions: Optional[List[ToLowerAvailabilityCondition]] = None

  def __post_init__(self):
    """Validates rule parameters."""
    if self.from_storage_class == self.to_storage_class:
      raise InvalidConfigError(
          'Current storage class and new storage class must be different.'
      )
    if self.upgrade_conditions and self.downgrade_conditions:
      raise InvalidConfigError(
          'Only one of {upgrade_conditions, downgrade_conditions} can be set.'
      )
    from_availability = _STORAGE_CLASS_AVAILABILITY[self.from_storage_class]
    to_availability = _STORAGE_CLASS_AVAILABILITY[self.to_storage_class]
    if from_availability > to_availability and not self.downgrade_conditions:
      raise InvalidConfigError(
          'Moving to a lower availabilitiy storage class. Expected '
          'downgrade_conditions to be set.'
      )
    elif from_availability < to_availability and not self.upgrade_conditions:
      raise InvalidConfigError(
          'Moving to a higher availabilitiy storage class. Expected '
          'upgrade_conditions to be set.'
      )

  @property
  def conditions(
      self,
  ) -> List[ToHigherAvailabilityCondition | ToLowerAvailabilityCondition]:
    if self.upgrade_conditions:
      return self.upgrade_conditions
    if self.downgrade_conditions:
      return self.downgrade_conditions
    return []


class DateTags(enum.Enum):
  ACQUISITION_DATE = 'ACQUISITION_DATE'
  CONTENT_DATE = 'CONTENT_DATE'
  SERIES_DATE = 'SERIES_DATE'
  STUDY_DATE = 'STUDY_DATE'


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class StorageClassConfig:
  """Heuristic config for moving instances between storage classes.

  Rules are applied in order, i.e. rule[i] has a higher priority than rule[i+1]
  when both apply to the same origin storage class.

  E.g. Given a storage class config
    move_rules = (
        (from_storage_class: STANDARD, to_storage_class: ARCHIVE, conditions0),
        (from_storage_class: ARCHIVE, to_storage_class: COLDLINE, conditions1),
        (from_storage_class: STANDARD, to_storage_class: NEARLINE, conditions2))
    )

    And an instance in STANDARD storage, if both conditions0 and conditions2 are
    met, the instance is moved to ARCHIVE since rule[0] has a higher priority
    than rule[2].
  """

  move_rules: List[MoveRule]

  # Relative priority for date tags in case some dates are not present,
  # from highest to lowest.
  # E.g. if date_priority = ['STUDY_DATE', 'ACQUISITION_DATE', 'SERIES_DATE'],
  # the study date will be used. Acquisition date will be used if study date is
  # not defined, and so on.
  date_priority: List[DateTags] = dataclasses.field(
      default_factory=lambda: [
          DateTags.CONTENT_DATE,
          DateTags.SERIES_DATE,
          DateTags.STUDY_DATE,
          DateTags.ACQUISITION_DATE,
      ]
  )

  def __post_init__(self):
    if not self.move_rules:
      raise InvalidConfigError('move_rules in StorageClassConfig must be set.')
    if not self.date_priority:
      raise InvalidConfigError(
          'date_priority in StorageClassConfig must be defined.'
      )


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class DicomStoreConfig:
  """DICOM store configuration."""

  # DICOM Store to perform storage class changes on.
  # Expects full resource name format, i.e.
  # projects/<project>/locations/<location>/datasets/<dataset>/dicomStores/<ds>
  dicom_store_path: str
  # BigQuery table streaming from DICOM Store above
  # Format: <project id>.<dataset id>.<table id>
  dicom_store_bigquery_table: str
  # Maximum number of instances to batch into a single
  # SetBlobStorageSettingsRequest using the filter config.
  set_storage_class_max_num_instances: int = 10_000
  # Timeout in minutes for SetBlobStorageSettingsRequest operations.
  set_storage_class_timeout_min: int = 60
  # Whether to delete SetBlobStorageSettingsRequest filter files written to GCS
  # after timeout defined above.
  set_storage_class_delete_filter_files: bool = True
  # Maximum QPS for DICOM store requests for each beam worker when sending
  # SetBlobStorageSettings requests.
  # Consider adjusting in accordance with beam pipeline WorkerOptions.
  max_dicom_store_qps: float = 2.0

  def __post_init__(self):
    if not self.dicom_store_path:
      raise InvalidConfigError('dicom_store_path must be set.')
    if not self.dicom_store_bigquery_table:
      raise InvalidConfigError('dicom_store_bigquery_table must be set.')
    if self.set_storage_class_max_num_instances <= 0:
      raise InvalidConfigError(
          'set_storage_class_max_num_instances must be positive.'
      )
    if self.set_storage_class_timeout_min <= 0:
      raise InvalidConfigError(
          'set_storage_class_timeout_min must be positive.'
      )
    if self.max_dicom_store_qps <= 0:
      raise InvalidConfigError('max_dicom_store_qps must be positive.')


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class DataAccessLogsConfiguration:
  """Data Access audit logs configuration."""

  # Bigquery table for Data Access audit logs sink.
  # Format: <project id>.<dataset id>.<table id>
  # May also include wildcard for multiple tables,
  # e.g. <project id>.<dataset id>.<table prefix>*
  logs_bigquery_table: str
  # Ignore access log entries before this date. Date format: YYYYMMDD.
  log_entries_date_equal_or_after: Optional[str] = None
  # Maximum QPS for DICOM store requests for each beam worker when parsing logs.
  # Consider adjusting in accordance with beam pipeline WorkerOptions.
  # DICOM Store is queried to fetch all instances accessed in the case of
  # RetrieveSeries or RetrieveStudy requests.
  max_dicom_store_qps: float = 2.0

  def __post_init__(self):
    if not self.logs_bigquery_table:
      raise InvalidConfigError('logs_bigquery_table must be set.')
    if self.max_dicom_store_qps <= 0:
      raise InvalidConfigError('max_dicom_store_qps must be positive.')


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class ReportConfiguration:
  """Configuration for generating storage class updates report."""

  # GCS file to write summarized report of storage class updates to. Includes
  # counters of number of instances updated per move rule condition.
  summarized_results_report_gcs_uri: str
  # GCS file to write detailed report of storage class updates to. Includes
  # the actual instances updated per move rule condition.
  detailed_results_report_gcs_uri: Optional[str] = None

  def __post_init__(self):
    if not self.summarized_results_report_gcs_uri:
      raise InvalidConfigError('summarized_results_report_gcs_uri must be set.')
    if '{}' not in self.summarized_results_report_gcs_uri:
      raise InvalidConfigError(
          'summarized_results_report_gcs_uri must include "{}" placeholder in '
          'filename for timestamp, e.g. '
          '"gs://<your-bucket>/<some-dir>/summarized_report_{}.csv"'
      )
    if (
        self.detailed_results_report_gcs_uri
        and '{}' not in self.detailed_results_report_gcs_uri
    ):
      raise InvalidConfigError(
          'detailed_results_report_gcs_uri must include "{}" placeholder in '
          'filename for timestamp, e.g. '
          '"gs://<your-bucket>/<some-dir>/detailed_report_{}.csv"'
      )


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class ImageLifecycleManagementConfig:
  """Image Lifecycle Management configuration."""

  # Whether to enable dry-run mode. If true, DICOM store changes are skipped.
  dry_run: bool
  # DICOM store config, including bigquery table and request options.
  dicom_store_config: DicomStoreConfig
  # Data Access audit logs configuration.
  logs_config: DataAccessLogsConfiguration
  # List of DICOM instances to exclude from ILM processing.
  # Instances should be in the format
  # studies/<study UID>/series/<series UID>/instances/<SOP instance UID>,
  # e.g.
  #   instances_disallow_list = ["studies/1.2/series/3.4/instances/5.6", (...)]
  instances_disallow_list: Set[str]
  # Rules based configuration for changing instances' storage classes.
  storage_class_config: StorageClassConfig
  # GCS URI to write temporary results to.
  tmp_gcs_uri: str
  # Configuration for report of storage class updates, which is generated at
  # the end of the pipeline execution.
  report_config: ReportConfiguration

  def __post_init__(self):
    if not self.tmp_gcs_uri:
      raise InvalidConfigError('tmp_gcs_uri must be set.')
