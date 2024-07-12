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
"""Tests for ilm_config."""

import dataclasses

from absl.testing import absltest
from absl.testing import parameterized

import ilm_config

_ACCESS_COUNT = ilm_config.AccessCount(count=3, num_days=10)
_MOVE_RULE_DOWNGRADE = ilm_config.MoveRule(
    from_storage_class=ilm_config.StorageClass.STANDARD,
    to_storage_class=ilm_config.StorageClass.ARCHIVE,
    downgrade_conditions=[
        ilm_config.ToLowerAvailabilityCondition(
            modality='MR',
            sop_class_uid='1.2.840.10008.5.1.4.1.1.77.1.6',
            pixel_spacing_range=ilm_config.PixelSpacingRange(min=0.05, max=0.1),
            date_before='20231231',
            size_bytes_larger_than=1024,
            access_count_lower_or_equal_to=ilm_config.AccessCount(
                count=3, num_days=10
            ),
            num_days_in_current_storage_class_higher_than=0,
            image_type={'ORIGINAL/PRIMARY'},
        )
    ],
)
_MOVE_RULE_UPGRADE = ilm_config.MoveRule(
    from_storage_class=ilm_config.StorageClass.ARCHIVE,
    to_storage_class=ilm_config.StorageClass.STANDARD,
    upgrade_conditions=[
        ilm_config.ToHigherAvailabilityCondition(
            modality='MR',
            sop_class_uid='1.2.840.10008.5.1.4.1.1.77.1.6',
            pixel_spacing_range=ilm_config.PixelSpacingRange(min=0.05, max=0.1),
            date_after='20231231',
            size_bytes_lower_than=1024,
            access_count_higher_or_equal_to=ilm_config.AccessCount(
                count=3, num_days=10
            ),
            num_days_in_current_storage_class_higher_than=365,
            image_type={'ORIGINAL/PRIMARY'},
        ),
    ],
)
_DICOM_STORE_CONFIG = ilm_config.DicomStoreConfig(
    dicom_store_path='projects/my-proj/locations/us-west1/datasets/my-dataset/dicomStores/my-store',
    dicom_store_bigquery_table='my-proj.my_bq_dicom_dataset.my-bq-dicom-table',
)
_LOGS_CONFIG = ilm_config.DataAccessLogsConfiguration(
    logs_bigquery_table=(
        'my-proj.my_bq_logs_dataset.cloudaudit_googleapis_com_data_access_*'
    ),
    max_dicom_store_qps=1.0,
)
_ILM_CONFIG = ilm_config.ImageLifecycleManagementConfig(
    dry_run=True,
    dicom_store_config=_DICOM_STORE_CONFIG,
    logs_config=_LOGS_CONFIG,
    instances_disallow_list=['instance0', 'instance1'],
    storage_class_config=ilm_config.StorageClassConfig(
        move_rules=[_MOVE_RULE_DOWNGRADE, _MOVE_RULE_UPGRADE],
    ),
    tmp_gcs_uri='gs://my-bucket/temp-location/',
    report_config=ilm_config.ReportConfiguration(
        summarized_results_report_gcs_uri='gs://my-bucket/results-{}.txt'
    ),
)


class IlmConfigTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('negative_count', {'count': -3}, 'Access count'),
      ('zero_num_days', {'num_days': -1}, 'Number of days'),
  )
  def test_invalid_access_count_raises_error(self, new_params, error_message):
    with self.assertRaisesRegex(ilm_config.InvalidConfigError, error_message):
      _ = dataclasses.replace(_ACCESS_COUNT, **new_params)

  def test_zero_valid_access_count_succeeds(self):
    _ = ilm_config.AccessCount(count=0, num_days=0)

  def test_invalid_pixel_range_raises_error(self):
    with self.assertRaises(ilm_config.InvalidConfigError):
      _ = ilm_config.PixelSpacingRange()

  @parameterized.named_parameters(
      (
          'same_storage_class',
          _MOVE_RULE_DOWNGRADE,
          {'from_storage_class': 'STANDARD', 'to_storage_class': 'STANDARD'},
          'must be different',
      ),
      (
          'upgrade_and_downgrade_both_set',
          _MOVE_RULE_DOWNGRADE,
          {'upgrade_conditions': _MOVE_RULE_UPGRADE.upgrade_conditions},
          'Only one of {upgrade_conditions, downgrade_conditions}',
      ),
      (
          'downgrade_conditions_missing',
          _MOVE_RULE_DOWNGRADE,
          {'downgrade_conditions': []},
          'Expected downgrade_conditions',
      ),
      (
          'upgrade_conditions_missing',
          _MOVE_RULE_UPGRADE,
          {'upgrade_conditions': []},
          'Expected upgrade_conditions',
      ),
  )
  def test_invalid_move_rule_raises_error(
      self, move_rules, new_params, error_message
  ):
    with self.assertRaisesRegex(ilm_config.InvalidConfigError, error_message):
      _ = dataclasses.replace(move_rules, **new_params)

  @parameterized.named_parameters(
      ('missing_move_rule', {'move_rules': []}, 'move_rules.*must be set'),
      (
          'missing_date_priority',
          {'date_priority': []},
          'date_priority.*must be defined',
      ),
  )
  def test_invalid_storage_class_config_raises_error(
      self, new_params, error_message
  ):
    with self.assertRaisesRegex(ilm_config.InvalidConfigError, error_message):
      _ = dataclasses.replace(_ILM_CONFIG.storage_class_config, **new_params)

  @parameterized.named_parameters(
      (
          'missing_dicom_store_path',
          {'dicom_store_path': ''},
          'dicom_store_path must be set',
      ),
      (
          'missing_dicom_store_bigquery_table',
          {'dicom_store_bigquery_table': ''},
          'dicom_store_bigquery_table must be set',
      ),
      (
          'invalid_set_storage_class_max_num_instances',
          {'set_storage_class_max_num_instances': 0},
          'set_storage_class_max_num_instances must be positive',
      ),
      (
          'invalid_set_storage_class_timeout_min',
          {'set_storage_class_timeout_min': -3},
          'set_storage_class_timeout_min must be positive',
      ),
      (
          'invalid_max_dicom_store_qps',
          {'max_dicom_store_qps': -0.5},
          'max_dicom_store_qps must be positive',
      ),
  )
  def test_invalid_dicom_store_config_raises_error(
      self, new_params, error_message
  ):
    with self.assertRaisesRegex(ilm_config.InvalidConfigError, error_message):
      _ = dataclasses.replace(_DICOM_STORE_CONFIG, **new_params)

  @parameterized.named_parameters(
      (
          'missing_logs_bigquery_table',
          {'logs_bigquery_table': ''},
          'logs_bigquery_table must be set',
      ),
      (
          'invalid_max_dicom_store_qps',
          {'max_dicom_store_qps': -0.5},
          'max_dicom_store_qps must be positive',
      ),
  )
  def test_invalid_logs_config_raises_error(self, new_params, error_message):
    with self.assertRaisesRegex(ilm_config.InvalidConfigError, error_message):
      _ = dataclasses.replace(_LOGS_CONFIG, **new_params)

  @parameterized.named_parameters(
      (
          'missing_summarized_results',
          '',
          None,
          'summarized_results_report_gcs_uri must be set',
      ),
      (
          'missing_summarized_results_placeholder',
          'invalid-summarized-report',
          '',
          'summarized_results_report_gcs_uri must include "{}"',
      ),
      (
          'missing_detailed_results_placeholder',
          'summarized-report-{}',
          'invalid-detailed-report',
          'detailed_results_report_gcs_uri must include "{}"',
      ),
  )
  def test_invalid_report_config_raises_error(
      self, summarized_report, detailed_report, error_message
  ):
    with self.assertRaisesRegex(ilm_config.InvalidConfigError, error_message):
      _ = ilm_config.ReportConfiguration(
          summarized_results_report_gcs_uri=summarized_report,
          detailed_results_report_gcs_uri=detailed_report,
      )

  @parameterized.named_parameters(
      (
          'missing_tmp_gcs_uri',
          {'tmp_gcs_uri': ''},
          'tmp_gcs_uri must be set',
      ),
  )
  def test_invalid_ilm_config_raises_error(self, new_params, error_message):
    with self.assertRaisesRegex(ilm_config.InvalidConfigError, error_message):
      _ = dataclasses.replace(_ILM_CONFIG, **new_params)

  def test_config_to_str(self):
    self.assertEqual(
        str(_ILM_CONFIG),
        'ImageLifecycleManagementConfig(dry_run=True,'
        " dicom_store_config=DicomStoreConfig(dicom_store_path='projects/my-proj/locations/us-west1/datasets/my-dataset/dicomStores/my-store',"
        " dicom_store_bigquery_table='my-proj.my_bq_dicom_dataset.my-bq-dicom-table',"
        ' set_storage_class_max_num_instances=10000,'
        ' set_storage_class_timeout_min=60,'
        ' set_storage_class_delete_filter_files=True, max_dicom_store_qps=2.0),'
        " logs_config=DataAccessLogsConfiguration(logs_bigquery_table='my-proj.my_bq_logs_dataset.cloudaudit_googleapis_com_data_access_*',"
        ' log_entries_date_equal_or_after=None, max_dicom_store_qps=1.0),'
        " instances_disallow_list=['instance0', 'instance1'],"
        ' storage_class_config=StorageClassConfig(move_rules=[MoveRule(from_storage_class=<StorageClass.STANDARD:'
        " 'STANDARD'>, to_storage_class=<StorageClass.ARCHIVE: 'ARCHIVE'>,"
        ' upgrade_conditions=None,'
        " downgrade_conditions=[ToLowerAvailabilityCondition(modality='MR',"
        " sop_class_uid='1.2.840.10008.5.1.4.1.1.77.1.6',"
        ' pixel_spacing_range=PixelSpacingRange(min=0.05, max=0.1),'
        " date_before='20231231', size_bytes_larger_than=1024,"
        ' access_count_lower_or_equal_to=AccessCount(count=3, num_days=10),'
        ' num_days_in_current_storage_class_higher_than=0,'
        " image_type={'ORIGINAL/PRIMARY'})]),"
        " MoveRule(from_storage_class=<StorageClass.ARCHIVE: 'ARCHIVE'>,"
        " to_storage_class=<StorageClass.STANDARD: 'STANDARD'>,"
        " upgrade_conditions=[ToHigherAvailabilityCondition(modality='MR',"
        " sop_class_uid='1.2.840.10008.5.1.4.1.1.77.1.6',"
        ' pixel_spacing_range=PixelSpacingRange(min=0.05, max=0.1),'
        " date_after='20231231', size_bytes_lower_than=1024,"
        ' access_count_higher_or_equal_to=AccessCount(count=3, num_days=10),'
        ' num_days_in_current_storage_class_higher_than=365,'
        " image_type={'ORIGINAL/PRIMARY'})], downgrade_conditions=None)],"
        " date_priority=[<DateTags.CONTENT_DATE: 'CONTENT_DATE'>,"
        " <DateTags.SERIES_DATE: 'SERIES_DATE'>, <DateTags.STUDY_DATE:"
        " 'STUDY_DATE'>, <DateTags.ACQUISITION_DATE: 'ACQUISITION_DATE'>]),"
        " tmp_gcs_uri='gs://my-bucket/temp-location/',"
        " report_config=ReportConfiguration(summarized_results_report_gcs_uri='gs://my-bucket/results-{}.txt',"
        ' detailed_results_report_gcs_uri=None))',
    )


if __name__ == '__main__':
  absltest.main()
