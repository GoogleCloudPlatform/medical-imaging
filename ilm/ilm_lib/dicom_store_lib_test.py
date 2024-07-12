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
"""Tests for dicom_store_lib."""

import dataclasses
import datetime
import time
import uuid

from absl.testing import absltest
from absl.testing import parameterized
from google.cloud import bigquery
import mock

import ilm_config
import ilm_types
from ilm_lib import dicom_store_lib
from ilm_lib import pipeline_util
from test_utils import test_const


_DICOM_STORE_TABLE_RAW_METADATA = {
    'InstanceDicomWebPath': f'{test_const.INSTANCE_0}',
    'BlobStorageSize': 12345,
    'Modality': 'SM',
    'NumberOfFrames': '10',
    'PixelSpacing': None,
    'ImageType': 'ORIGINAL/PRIMARY/LABEL/NONE',
    'SOPClassUID': '1.2.300',
    'StorageClass': 'STANDARD',
    'AcquisitionDate': datetime.date(1899, 12, 28),
    'ContentDate': datetime.date(1899, 12, 29),
    'SeriesDate': datetime.date(1899, 12, 30),
    'StudyDate': datetime.date(1899, 12, 31),
    'Type': 'CREATED',
    'LastUpdated': datetime.datetime(
        2023, 12, 30, tzinfo=datetime.timezone.utc
    ),
    'LastUpdatedStorageClass': datetime.datetime(
        2023, 12, 29, tzinfo=datetime.timezone.utc
    ),
}
_TODAY = datetime.datetime(2023, 12, 31, tzinfo=datetime.timezone.utc)


class DicomStoreLibTest(parameterized.TestCase):

  @mock.patch.object(bigquery, 'Client', autospec=True)
  def test_get_dicom_store_query_empty_optional_metadata_columns_succeeds(
      self, mk
  ):
    mk.return_value.get_table.return_value.schema = []
    query = dicom_store_lib.get_dicom_store_query(test_const.ILM_CONFIG)
    self.assertIn(
        test_const.ILM_CONFIG.dicom_store_config.dicom_store_bigquery_table,
        query,
    )
    self.assertIn('BlobStorageSize', query)
    mk.assert_called_once_with('my-proj')

  @mock.patch.object(bigquery, 'Client', autospec=True)
  def test_get_dicom_store_query_succeeds(self, mk):
    mk.return_value.get_table.return_value.schema = [
        bigquery.SchemaField('StudyDate', 'DATE'),
        bigquery.SchemaField('Modality', 'STRING'),
        bigquery.SchemaField('SomeOtherColumn', 'STRING'),
    ]
    query = dicom_store_lib.get_dicom_store_query(test_const.ILM_CONFIG)
    self.assertIn(
        test_const.ILM_CONFIG.dicom_store_config.dicom_store_bigquery_table,
        query,
    )
    self.assertIn('BlobStorageSize', query)
    self.assertIn('StudyDate', query)
    self.assertIn('Modality', query)
    self.assertNotIn('SomeOtherColumn', query)

  def test_parse_dicom_metadata_empty_fails(self):
    with self.assertRaises(RuntimeError):
      dicom_store_lib.parse_dicom_metadata({})

  def test_parse_dicom_metadata_missing_metadata_succeeds(self):
    raw_metadata = _DICOM_STORE_TABLE_RAW_METADATA.copy()
    del raw_metadata['Modality']
    del raw_metadata['NumberOfFrames']
    self.assertEqual(
        dicom_store_lib.parse_dicom_metadata(raw_metadata, _TODAY),
        (
            test_const.INSTANCE_0,
            ilm_types.InstanceMetadata(
                instance=test_const.INSTANCE_0,
                modality=None,
                num_frames=0,
                pixel_spacing=None,
                sop_class_uid='1.2.300',
                acquisition_date=datetime.date(1899, 12, 28),
                content_date=datetime.date(1899, 12, 29),
                series_date=datetime.date(1899, 12, 30),
                study_date=datetime.date(1899, 12, 31),
                image_type='ORIGINAL/PRIMARY/LABEL/NONE',
                size_bytes=12345,
                storage_class=ilm_config.StorageClass.STANDARD,
                num_days_in_current_storage_class=2,
            ),
        ),
    )

  def test_parse_dicom_metadata_num_frames_none_succeeds(self):
    raw_metadata = _DICOM_STORE_TABLE_RAW_METADATA.copy()
    raw_metadata['NumberOfFrames'] = None
    self.assertEqual(
        dicom_store_lib.parse_dicom_metadata(raw_metadata, _TODAY),
        (
            test_const.INSTANCE_0,
            ilm_types.InstanceMetadata(
                instance=test_const.INSTANCE_0,
                modality='SM',
                num_frames=0,
                pixel_spacing=None,
                sop_class_uid='1.2.300',
                acquisition_date=datetime.date(1899, 12, 28),
                content_date=datetime.date(1899, 12, 29),
                series_date=datetime.date(1899, 12, 30),
                study_date=datetime.date(1899, 12, 31),
                image_type='ORIGINAL/PRIMARY/LABEL/NONE',
                size_bytes=12345,
                storage_class=ilm_config.StorageClass.STANDARD,
                num_days_in_current_storage_class=2,
            ),
        ),
    )

  def test_parse_dicom_metadata_unsupported_storage_class_fails(self):
    raw_metadata = _DICOM_STORE_TABLE_RAW_METADATA.copy()
    raw_metadata['StorageClass'] = 'UNSUPPORTED'
    with self.assertRaises(RuntimeError):
      dicom_store_lib.parse_dicom_metadata(raw_metadata)

  def test_parse_dicom_metadata_missing_column_fails(
      self,
  ):
    raw_metadata = _DICOM_STORE_TABLE_RAW_METADATA.copy()
    del raw_metadata['InstanceDicomWebPath']
    with self.assertRaises(RuntimeError):
      dicom_store_lib.parse_dicom_metadata(raw_metadata, _TODAY)

  def test_parse_dicom_metadata_wrong_type_fails(
      self,
  ):
    raw_metadata = _DICOM_STORE_TABLE_RAW_METADATA.copy()
    raw_metadata['LastUpdatedStorageClass'] = 'invalid type'
    with self.assertRaises(RuntimeError):
      dicom_store_lib.parse_dicom_metadata(raw_metadata, _TODAY)

  def test_parse_dicom_metadata_succeeds(self):
    self.assertEqual(
        dicom_store_lib.parse_dicom_metadata(
            _DICOM_STORE_TABLE_RAW_METADATA, _TODAY
        ),
        (
            test_const.INSTANCE_0,
            ilm_types.InstanceMetadata(
                instance=test_const.INSTANCE_0,
                modality='SM',
                num_frames=10,
                pixel_spacing=None,
                sop_class_uid='1.2.300',
                acquisition_date=datetime.date(1899, 12, 28),
                content_date=datetime.date(1899, 12, 29),
                series_date=datetime.date(1899, 12, 30),
                study_date=datetime.date(1899, 12, 31),
                image_type='ORIGINAL/PRIMARY/LABEL/NONE',
                size_bytes=12345,
                storage_class=ilm_config.StorageClass.STANDARD,
                num_days_in_current_storage_class=2,
            ),
        ),
    )

  def test_parse_dicom_metadata_with_pixel_spacing_succeeds(self):
    raw_metadata = _DICOM_STORE_TABLE_RAW_METADATA.copy()
    raw_metadata['PixelSpacing'] = '0.0005'
    self.assertEqual(
        dicom_store_lib.parse_dicom_metadata(raw_metadata, _TODAY),
        (
            test_const.INSTANCE_0,
            ilm_types.InstanceMetadata(
                instance=test_const.INSTANCE_0,
                modality='SM',
                num_frames=10,
                pixel_spacing=0.0005,
                sop_class_uid='1.2.300',
                acquisition_date=datetime.date(1899, 12, 28),
                content_date=datetime.date(1899, 12, 29),
                series_date=datetime.date(1899, 12, 30),
                study_date=datetime.date(1899, 12, 31),
                image_type='ORIGINAL/PRIMARY/LABEL/NONE',
                size_bytes=12345,
                storage_class=ilm_config.StorageClass.STANDARD,
                num_days_in_current_storage_class=2,
            ),
        ),
    )

  def test_parse_dicom_metadata_missing_last_updated_storage_class_succeeds(
      self,
  ):
    raw_metadata = _DICOM_STORE_TABLE_RAW_METADATA.copy()
    raw_metadata['LastUpdatedStorageClass'] = None
    _, instance_metadata = dicom_store_lib.parse_dicom_metadata(
        raw_metadata, _TODAY
    )
    self.assertEqual(instance_metadata.num_days_in_current_storage_class, 1)

  @parameterized.named_parameters(
      (
          'include_instances',
          'gs://my-proj-bucket/detailed-results-{}.txt',
          [test_const.INSTANCE_0],
      ),
      (
          'exclude_instances',
          '',
          [],
      ),
  )
  @mock.patch.object(uuid, 'uuid4', return_value='some-uuid', autospec=True)
  @mock.patch.object(pipeline_util, 'write_gcs_file', autospec=True)
  def test_generate_filter_files_dry_run_succeeds(
      self, detailed_report_gcs_uri, expected_instances, mock_write, _
  ):
    ilm_cfg = dataclasses.replace(
        test_const.ILM_CONFIG,
        dry_run=True,
        report_config=ilm_config.ReportConfiguration(
            summarized_results_report_gcs_uri=(
                'gs://my-proj-bucket/results-{}.txt'
            ),
            detailed_results_report_gcs_uri=detailed_report_gcs_uri,
        ),
    )
    storage_class_to_changes = (
        ilm_config.StorageClass.NEARLINE,
        [
            ilm_types.StorageClassChange(
                test_const.INSTANCE_0,
                ilm_config.StorageClass.NEARLINE,
                ilm_types.MoveRuleId(3, 4),
            ),
        ],
    )
    files_dofn = dicom_store_lib.GenerateFilterFilesDoFn(ilm_cfg)
    storage_request_metadata = files_dofn.process(storage_class_to_changes)

    expected_storage_request_metadata = (
        ilm_types.SetStorageClassRequestMetadata(
            move_rule_ids=[
                ilm_types.MoveRuleId(3, 4),
            ],
            instances=expected_instances,
            filter_file_gcs_uri=(
                'gs://my-proj-bucket/tmp/instances-filter-some-uuid.txt'
            ),
            new_storage_class=ilm_config.StorageClass.NEARLINE,
        )
    )
    storage_request_metadata = list(storage_request_metadata)
    self.assertLen(storage_request_metadata, 1)
    self.assertEqual(
        storage_request_metadata[0], expected_storage_request_metadata
    )
    mock_write.assert_not_called()

  @parameterized.named_parameters(
      (
          'include_instances',
          'gs://my-proj-bucket/detailed-results-{}.txt',
          [test_const.INSTANCE_0, test_const.INSTANCE_1],
      ),
      (
          'exclude_instances',
          '',
          [],
      ),
  )
  @mock.patch.object(uuid, 'uuid4', return_value='some-uuid', autospec=True)
  @mock.patch.object(pipeline_util, 'write_gcs_file', autospec=True)
  def test_generate_filter_files_succeeds(
      self, detailed_report_gcs_uri, expected_instances, mock_write, _
  ):
    ilm_cfg = dataclasses.replace(
        test_const.ILM_CONFIG,
        dry_run=False,
        report_config=ilm_config.ReportConfiguration(
            summarized_results_report_gcs_uri=(
                'gs://my-proj-bucket/results-{}.txt'
            ),
            detailed_results_report_gcs_uri=detailed_report_gcs_uri,
        ),
    )
    storage_class_to_changes = (
        ilm_config.StorageClass.NEARLINE,
        [
            ilm_types.StorageClassChange(
                test_const.INSTANCE_0,
                ilm_config.StorageClass.NEARLINE,
                ilm_types.MoveRuleId(3, 4),
            ),
            ilm_types.StorageClassChange(
                test_const.INSTANCE_1,
                ilm_config.StorageClass.NEARLINE,
                ilm_types.MoveRuleId(2, 0),
            ),
        ],
    )

    files_dofn = dicom_store_lib.GenerateFilterFilesDoFn(ilm_cfg)
    storage_request_metadata = files_dofn.process(storage_class_to_changes)

    expected_storage_request_metadata = (
        ilm_types.SetStorageClassRequestMetadata(
            move_rule_ids=[
                ilm_types.MoveRuleId(3, 4),
                ilm_types.MoveRuleId(2, 0),
            ],
            instances=expected_instances,
            filter_file_gcs_uri=(
                'gs://my-proj-bucket/tmp/instances-filter-some-uuid.txt'
            ),
            new_storage_class=ilm_config.StorageClass.NEARLINE,
        )
    )
    storage_request_metadata = list(storage_request_metadata)
    self.assertLen(storage_request_metadata, 1)
    self.assertEqual(
        storage_request_metadata[0], expected_storage_request_metadata
    )
    mock_write.assert_called_once_with(
        f'{test_const.INSTANCE_0}\n{test_const.INSTANCE_1}',
        'gs://my-proj-bucket/tmp/instances-filter-some-uuid.txt',
        None,
    )

  @parameterized.parameters(
      ['dicom/store/resource', 'dicom/store/resource', '/dicom/store/resource/']
  )
  def test_dicom_store_client_dicom_store_path(self, dicom_store_resource):
    dicom_store_config = dataclasses.replace(
        test_const.ILM_CONFIG.dicom_store_config,
        dicom_store_path=dicom_store_resource,
    )
    ilm_cfg = dataclasses.replace(
        test_const.ILM_CONFIG, dicom_store_config=dicom_store_config
    )
    client = dicom_store_lib.DicomStoreClient(ilm_cfg)
    self.assertEqual(
        client._dicom_store_path,
        'https://healthcare.googleapis.com/v1beta1/dicom/store/resource',
    )

  @mock.patch.object(
      dicom_store_lib.DicomStoreClient,
      'set_blob_storage_settings',
      autospec=True,
  )
  def test_update_storage_classes_dry_run_succeeds(self, mock_set_storage):
    request = ilm_types.SetStorageClassRequestMetadata(
        move_rule_ids=[ilm_types.MoveRuleId(0, 1), ilm_types.MoveRuleId(0, 1)],
        instances=[],
        filter_file_gcs_uri=(
            'gs://my-proj-bucket/tmp/instances-filter-uuid0.txt'
        ),
        new_storage_class=ilm_config.StorageClass.ARCHIVE,
    )
    updater = dicom_store_lib.UpdateStorageClassesDoFn(test_const.ILM_CONFIG)
    updater.setup()
    operation = updater.process(request)
    operation = list(operation)
    self.assertLen(operation, 1)
    self.assertEqual(
        operation[0],
        ilm_types.SetStorageClassOperationMetadata(
            move_rule_ids=[
                ilm_types.MoveRuleId(rule_index=0, condition_index=1),
                ilm_types.MoveRuleId(rule_index=0, condition_index=1),
            ],
            instances=[],
            operation_name='',
            filter_file_gcs_uri=None,
            start_time=None,
            succeeded=True,
        ),
    )
    mock_set_storage.assert_not_called()

  @mock.patch.object(
      dicom_store_lib.DicomStoreClient, 'is_operation_done', autospec=True
  )
  @mock.patch.object(pipeline_util, 'delete_gcs_file', autospec=True)
  @mock.patch.object(pipeline_util, 'write_gcs_file', autospec=True)
  def test_generate_report_dry_run_succeeds(
      self, mock_write, mock_gcs_delete, mock_operation_done
  ):
    operation_0 = ilm_types.SetStorageClassOperationMetadata(
        move_rule_ids=[
            ilm_types.MoveRuleId(rule_index=0, condition_index=1),
            ilm_types.MoveRuleId(rule_index=0, condition_index=1),
        ],
        instances=[],
        operation_name='',
        filter_file_gcs_uri=None,
        start_time=None,
        succeeded=True,
    )
    operation_1 = ilm_types.SetStorageClassOperationMetadata(
        move_rule_ids=[
            ilm_types.MoveRuleId(rule_index=1, condition_index=0),
        ],
        instances=[],
        operation_name='',
        filter_file_gcs_uri=None,
        start_time=None,
        succeeded=True,
    )
    reporter = dicom_store_lib.GenerateReportDoFn(test_const.ILM_CONFIG)
    reporter.setup()
    reporter.process((0, [operation_0, operation_1]))

    mock_operation_done.assert_not_called()
    mock_gcs_delete.assert_not_called()
    mock_write.assert_called_once()
    _, kwargs = mock_write.call_args
    file_content = kwargs['file_content']
    gcs_uri = kwargs['gcs_uri']
    self.assertRegex(gcs_uri, 'gs://my-proj-bucket/results-.*.txt')
    self.assertIn(
        'STANDARD,ARCHIVE,ToLowerAvailabilityCondition(sop_class_uid=1.2.300),0,1,2,0,0',
        file_content,
    )
    self.assertIn(
        'ARCHIVE,STANDARD,ToLowerAvailabilityCondition(modality=CR),1,0,1,0,0',
        file_content,
    )

  @mock.patch.object(
      dicom_store_lib.DicomStoreClient,
      'set_blob_storage_settings',
      return_value='operation0',
      autospec=True,
  )
  @mock.patch.object(pipeline_util.Throttler, 'wait', autospec=True)
  @mock.patch.object(time, 'time', return_value=123.45, autospec=True)
  def test_update_storage_classes_succeeds(
      self,
      _,
      mock_throttler,
      mock_ds_set_storage,
  ):
    ilm_cfg = dataclasses.replace(test_const.ILM_CONFIG, dry_run=False)
    request = ilm_types.SetStorageClassRequestMetadata(
        move_rule_ids=[ilm_types.MoveRuleId(0, 1), ilm_types.MoveRuleId(0, 1)],
        instances=[test_const.INSTANCE_0, test_const.INSTANCE_1],
        filter_file_gcs_uri=(
            'gs://my-proj-bucket/tmp/instances-filter-uuid0.txt'
        ),
        new_storage_class=ilm_config.StorageClass.ARCHIVE,
    )
    updater = dicom_store_lib.UpdateStorageClassesDoFn(ilm_cfg)
    updater.setup()
    operation = updater.process(request)
    operation = list(operation)
    self.assertLen(operation, 1)
    self.assertEqual(
        operation[0],
        ilm_types.SetStorageClassOperationMetadata(
            move_rule_ids=[
                ilm_types.MoveRuleId(rule_index=0, condition_index=1),
                ilm_types.MoveRuleId(rule_index=0, condition_index=1),
            ],
            instances=[test_const.INSTANCE_0, test_const.INSTANCE_1],
            operation_name='operation0',
            filter_file_gcs_uri=(
                'gs://my-proj-bucket/tmp/instances-filter-uuid0.txt'
            ),
            start_time=123.45,
        ),
    )
    mock_ds_set_storage.assert_called_once()
    self.assertEqual(mock_throttler.call_count, 1)

  @mock.patch.object(
      dicom_store_lib.DicomStoreClient,
      'is_operation_done',
      return_value=True,
      autospec=True,
  )
  @mock.patch.object(pipeline_util, 'delete_gcs_file', autospec=True)
  @mock.patch.object(pipeline_util, 'write_gcs_file', autospec=True)
  @mock.patch.object(pipeline_util.Throttler, 'wait', autospec=True)
  def test_generate_report_succeeds(
      self,
      mock_throttler,
      mock_gcs_write,
      mock_gcs_delete,
      mock_ds_operation,
  ):
    ilm_cfg = dataclasses.replace(
        test_const.ILM_CONFIG,
        dry_run=False,
        report_config=ilm_config.ReportConfiguration(
            summarized_results_report_gcs_uri=(
                'gs://my-proj-bucket/results-{}.txt'
            ),
            detailed_results_report_gcs_uri=(
                'gs://my-proj-bucket/detailed-results-{}.txt'
            ),
        ),
    )
    operation_0 = ilm_types.SetStorageClassOperationMetadata(
        move_rule_ids=[ilm_types.MoveRuleId(0, 1), ilm_types.MoveRuleId(0, 1)],
        instances=[test_const.INSTANCE_0, test_const.INSTANCE_1],
        operation_name='operation0',
        filter_file_gcs_uri=(
            'gs://my-proj-bucket/tmp/instances-filter-uuid0.txt'
        ),
        start_time=time.time(),
    )
    operation_1 = ilm_types.SetStorageClassOperationMetadata(
        move_rule_ids=[ilm_types.MoveRuleId(1, 0)],
        instances=[test_const.INSTANCE_2],
        operation_name='operation1',
        filter_file_gcs_uri=(
            'gs://my-proj-bucket/tmp/instances-filter-uuid1.txt'
        ),
        start_time=time.time(),
    )
    reporter = dicom_store_lib.GenerateReportDoFn(ilm_cfg)
    reporter.setup()
    reporter.process((0, [operation_0, operation_1]))

    mock_ds_operation.assert_has_calls(
        [mock.call(mock.ANY, 'operation0'), mock.call(mock.ANY, 'operation1')],
        any_order=True,
    )
    mock_gcs_delete.assert_has_calls(
        [
            mock.call('gs://my-proj-bucket/tmp/instances-filter-uuid0.txt'),
            mock.call('gs://my-proj-bucket/tmp/instances-filter-uuid1.txt'),
        ],
        any_order=True,
    )
    self.assertEqual(mock_gcs_write.call_count, 2)
    _, kwargs = mock_gcs_write.call_args
    file_content = kwargs['file_content']
    gcs_uri = kwargs['gcs_uri']
    self.assertRegex(gcs_uri, 'gs://my-proj-bucket/detailed-results-.*.txt')
    self.assertIn(
        "STANDARD,ARCHIVE,ToLowerAvailabilityCondition(sop_class_uid=1.2.300),0,1,\"['studies/1.22.333/series/1.44.555/instances/6.77.888',"
        ' \'studies/1.22.333/series/1.44.555/instances/6.77.999\']","[]","[]"',
        file_content,
    )
    self.assertIn(
        'ARCHIVE,STANDARD,ToLowerAvailabilityCondition(modality=CR),1,0,"[\'studies/1.22.333/series/1.44.555/instances/6.77.000\']","[]","[]"',
        file_content,
    )
    self.assertEqual(mock_throttler.call_count, 2)

  @mock.patch.object(
      dicom_store_lib.DicomStoreClient,
      'set_blob_storage_settings',
      side_effect=dicom_store_lib.DicomStoreError(),
      autospec=True,
  )
  @mock.patch.object(pipeline_util.Throttler, 'wait', autospec=True)
  def test_update_storage_classes_set_storage_request_fails(
      self,
      mock_throttler,
      mock_ds_set_storage,
  ):
    ilm_cfg = dataclasses.replace(test_const.ILM_CONFIG, dry_run=False)
    request = ilm_types.SetStorageClassRequestMetadata(
        move_rule_ids=[ilm_types.MoveRuleId(0, 1), ilm_types.MoveRuleId(0, 1)],
        instances=[],
        filter_file_gcs_uri=(
            'gs://my-proj-bucket/tmp/instances-filter-uuid0.txt'
        ),
        new_storage_class=ilm_config.StorageClass.ARCHIVE,
    )
    updater = dicom_store_lib.UpdateStorageClassesDoFn(ilm_cfg)
    updater.setup()
    operation = updater.process(request)
    operation = list(operation)
    self.assertLen(operation, 1)
    self.assertEqual(
        operation[0],
        ilm_types.SetStorageClassOperationMetadata(
            move_rule_ids=[
                ilm_types.MoveRuleId(rule_index=0, condition_index=1),
                ilm_types.MoveRuleId(rule_index=0, condition_index=1),
            ],
            instances=[],
            operation_name='',
            filter_file_gcs_uri=(
                'gs://my-proj-bucket/tmp/instances-filter-uuid0.txt'
            ),
            start_time=None,
            succeeded=False,
        ),
    )

    mock_ds_set_storage.assert_called_once()
    self.assertEqual(mock_throttler.call_count, 1)

  @mock.patch.object(
      dicom_store_lib.DicomStoreClient, 'is_operation_done', autospec=True
  )
  @mock.patch.object(pipeline_util, 'delete_gcs_file', autospec=True)
  @mock.patch.object(pipeline_util, 'write_gcs_file', autospec=True)
  @mock.patch.object(pipeline_util.Throttler, 'wait', autospec=True)
  def test_generate_report_set_storage_request_fails(
      self,
      mock_throttler,
      mock_gcs_write,
      mock_gcs_delete,
      mock_ds_operation,
  ):
    ilm_cfg = dataclasses.replace(test_const.ILM_CONFIG, dry_run=False)
    operation = ilm_types.SetStorageClassOperationMetadata(
        move_rule_ids=[ilm_types.MoveRuleId(0, 1), ilm_types.MoveRuleId(0, 1)],
        instances=[],
        operation_name='',
        filter_file_gcs_uri=(
            'gs://my-proj-bucket/tmp/instances-filter-uuid0.txt'
        ),
        start_time=None,
        succeeded=False,
    )
    reporter = dicom_store_lib.GenerateReportDoFn(ilm_cfg)
    reporter.setup()
    reporter.process((0, [operation]))

    mock_ds_operation.assert_not_called()
    mock_gcs_delete.assert_called_once()
    mock_gcs_write.assert_called_once()
    _, kwargs = mock_gcs_write.call_args
    file_content = kwargs['file_content']
    gcs_uri = kwargs['gcs_uri']
    self.assertRegex(gcs_uri, 'gs://my-proj-bucket/results-.*.txt')
    self.assertIn(
        'STANDARD,ARCHIVE,ToLowerAvailabilityCondition(sop_class_uid=1.2.300),0,1,0,2,0',
        file_content,
    )
    mock_throttler.assert_not_called()

  @mock.patch.object(
      dicom_store_lib.DicomStoreClient,
      'is_operation_done',
      side_effect=dicom_store_lib.DicomStoreError(),
      autospec=True,
  )
  @mock.patch.object(pipeline_util, 'delete_gcs_file', autospec=True)
  @mock.patch.object(pipeline_util, 'write_gcs_file', autospec=True)
  @mock.patch.object(pipeline_util.Throttler, 'wait', autospec=True)
  def test_generate_report_operation_fails(
      self,
      mock_throttler,
      mock_gcs_write,
      mock_gcs_delete,
      mock_ds_operation,
  ):
    ilm_cfg = dataclasses.replace(test_const.ILM_CONFIG, dry_run=False)
    operation = ilm_types.SetStorageClassOperationMetadata(
        move_rule_ids=[ilm_types.MoveRuleId(0, 1), ilm_types.MoveRuleId(0, 1)],
        instances=[],
        operation_name='operation0',
        filter_file_gcs_uri=(
            'gs://my-proj-bucket/tmp/instances-filter-uuid0.txt'
        ),
        start_time=123.45,
    )
    reporter = dicom_store_lib.GenerateReportDoFn(ilm_cfg)
    reporter.setup()
    reporter.process((0, [operation]))

    mock_ds_operation.assert_called_once()
    mock_gcs_delete.assert_called_once()
    mock_gcs_write.assert_called_once()
    _, kwargs = mock_gcs_write.call_args
    file_content = kwargs['file_content']
    gcs_uri = kwargs['gcs_uri']
    self.assertRegex(gcs_uri, 'gs://my-proj-bucket/results-.*.txt')
    self.assertIn(
        'STANDARD,ARCHIVE,ToLowerAvailabilityCondition(sop_class_uid=1.2.300),0,1,0,2,0',
        file_content,
    )
    self.assertEqual(mock_throttler.call_count, 1)

  @mock.patch.object(
      dicom_store_lib.DicomStoreClient,
      'is_operation_done',
      return_value=False,
      autospec=True,
  )
  @mock.patch.object(pipeline_util, 'delete_gcs_file', autospec=True)
  @mock.patch.object(pipeline_util, 'write_gcs_file', autospec=True)
  @mock.patch.object(pipeline_util.Throttler, 'wait', autospec=True)
  @mock.patch.object(
      time,
      'time',
      side_effect=[3700 * i for i in range(10)],  # Timeout is 3600 seconds.
      autospec=True,
  )
  def test_dicom_store_updater_operation_timeout(
      self,
      _,
      mock_throttler,
      mock_gcs_write,
      mock_gcs_delete,
      mock_ds_operation,
  ):
    dicom_store_cfg = dataclasses.replace(
        test_const.ILM_CONFIG.dicom_store_config,
        set_storage_class_delete_filter_files=False,
    )
    ilm_cfg = dataclasses.replace(
        test_const.ILM_CONFIG, dry_run=False, dicom_store_config=dicom_store_cfg
    )
    operation = ilm_types.SetStorageClassOperationMetadata(
        move_rule_ids=[ilm_types.MoveRuleId(0, 1), ilm_types.MoveRuleId(0, 1)],
        instances=[],
        operation_name='operation0',
        filter_file_gcs_uri=(
            'gs://my-proj-bucket/tmp/instances-filter-uuid0.txt'
        ),
        start_time=1.23,
    )
    reporter = dicom_store_lib.GenerateReportDoFn(ilm_cfg)
    reporter.setup()
    reporter.process((0, [operation]))

    mock_ds_operation.assert_called_once()
    mock_gcs_delete.assert_not_called()
    mock_gcs_write.assert_called_once()
    _, kwargs = mock_gcs_write.call_args
    file_content = kwargs['file_content']
    gcs_uri = kwargs['gcs_uri']
    self.assertRegex(gcs_uri, 'gs://my-proj-bucket/results-.*.txt')
    self.assertIn(
        'STANDARD,ARCHIVE,ToLowerAvailabilityCondition(sop_class_uid=1.2.300),0,1,0,0,2',
        file_content,
    )
    self.assertEqual(mock_throttler.call_count, 1)

  # TODO: Add tests for DICOM store client.


if __name__ == '__main__':
  absltest.main()
