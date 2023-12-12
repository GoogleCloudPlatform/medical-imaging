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
"""Unit tests for IngestBase."""

import copy
import json
import os
from typing import Any, Dict, Mapping, Optional, Sequence, Set, Union
from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import pydicom

from shared_libs.logging_lib import cloud_logging_client
from shared_libs.test_utils.dicom_store_mock import dicom_store_mock
from shared_libs.test_utils.gcs_mock import gcs_mock
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import abstract_polling_client
from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_json_util
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_image_extractor
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import barcode_reader
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client


_METADATA_PATH = 'metadata.csv'
_SCHEMA_PATH = 'example_schema_slide_coordinates.json'
_SLIDE_ID_1 = 'MD-03-2-A1-1'
_SLIDE_ID_2 = 'MD-03-2-A1-2'

_SCHEMA = {
    '0x00100010': {'Keyword': 'PatientName', 'Meta': 'Patient Name'},
    '0x00100020': {'Keyword': 'PatientID', 'Meta': 'Patient ID'},
}

_TEST_BUCKET_PATH = 'transformation_pipeline/testdata/bucket'


_EXPECTED_DEFAULT_WSI2DCM_PARAMS = {
    '--tileHeight': '256',
    '--tileWidth': '256',
    '--compression': 'jpeg',
    '--jpegCompressionQuality': '95',
    '--jpegSubsampling': '444',
    '--opencvDownsampling': 'AREA',
    '--progressiveDownsample': None,
    '--floorCorrectOpenslideLevelDownsamples': None,
    '--SVSImportPreferScannerTileingForLargestLevel': None,
}


def _test_dicomfile_set(val: Union[Sequence[int], Set[int]]) -> Set[str]:
  return {f'/foo/bar/downsample-{ds}-4-6.dcm' for ds in val}


def _parse_wsi2dcm_params(param_str: str) -> Mapping[str, Optional[str]]:
  parsed_params = {}
  for param in [item.split('=') for item in param_str.split(' ')]:
    if len(param) == 1:
      parsed_params[param[0]] = None
    elif len(param) == 2:
      parsed_params[param[0]] = param[1]
    else:
      raise ValueError(f'Invalid Parameter: {"=".join(param)}')
  return parsed_params


def _tag_val(value: str, vr_code: str) -> Dict[str, Any]:
  return {
      ingest_const.VALUE: [value],
      ingest_const.VR: vr_code,
  }


def _dcm_json(
    study_instance_uid: str = '',
    series_instance_uid: str = '',
    sop_instance_uid: str = '',
    accession: str = '',
    patient_id: str = '',
) -> Dict[str, Any]:
  result = {}
  if study_instance_uid:
    result[ingest_const.DICOMTagAddress.STUDY_INSTANCE_UID] = _tag_val(
        study_instance_uid, ingest_const.DICOMVRCodes.UI
    )
  if series_instance_uid:
    result[ingest_const.DICOMTagAddress.SERIES_INSTANCE_UID] = _tag_val(
        series_instance_uid, ingest_const.DICOMVRCodes.UI
    )
  if sop_instance_uid:
    result[ingest_const.DICOMTagAddress.SOP_INSTANCE_UID] = _tag_val(
        sop_instance_uid, ingest_const.DICOMVRCodes.UI
    )
  if accession:
    result[ingest_const.DICOMTagAddress.ACCESSION_NUMBER] = _tag_val(
        accession, ingest_const.DICOMVRCodes.SH
    )
  if patient_id:
    result[ingest_const.DICOMTagAddress.PATIENT_ID] = _tag_val(
        patient_id, ingest_const.DICOMVRCodes.LO
    )
  return result


def _create_pydicom_file_dataset_from_json(
    data: Dict[str, Any],
) -> pydicom.FileDataset:
  ds = pydicom.Dataset().from_json(data)
  ds = pydicom.FileDataset(
      filename_or_obj='',
      dataset=ds,
      file_meta=pydicom.dataset.FileMetaDataset(),
      preamble=b'\0' * 128,
  )
  ds.is_little_endian = True
  ds.is_implicit_VR = False
  return ds


class DicomInstanceIngestionSetsTest(parameterized.TestCase):

  @mock.patch.object(
      cloud_logging_client.CloudLoggingClient, 'warning', autospec=True
  )
  def test_get_downsampled_dicom_files(self, mock_logger):
    downsamples = {5, 7, 9}
    dicom_files = _test_dicomfile_set(range(10))
    expected = _test_dicomfile_set(downsamples)

    filepaths = ingest_base._get_downsampled_dicom_files(
        dicom_files, downsamples, False
    )

    self.assertEqual(filepaths, expected)
    # validates when len(downsamples) == len(file_list)
    # does not log a warning.
    mock_logger.assert_not_called()

  @mock.patch.object(
      cloud_logging_client.CloudLoggingClient, 'warning', autospec=True
  )
  def test_get_downsampled_dicom_files_include_largest_downsample(
      self, mock_logger
  ):
    dicom_files = _test_dicomfile_set(range(10))
    expected = _test_dicomfile_set([5, 9])

    filepaths = ingest_base._get_downsampled_dicom_files(dicom_files, {5}, True)

    self.assertEqual(filepaths, expected)
    # validates when len(downsamples) == len(file_list)
    # does not log a warning.
    mock_logger.assert_not_called()

  def test_get_downsampled_dicom_files_no_files(self):
    downsamples = {5, 7, 9}
    dicom_files = ['/foo/bar/magic.dcm']
    filepaths = ingest_base._get_downsampled_dicom_files(
        dicom_files, downsamples
    )
    self.assertEmpty(filepaths)

  @mock.patch.object(
      cloud_logging_client.CloudLoggingClient, 'warning', autospec=True
  )
  def test_get_downsampled_dicom_files_missing_file_warning(self, mock_logger):
    downsamples = set(range(10))
    dicom_files = _test_dicomfile_set(range(5, 10))
    expected_result = copy.copy(dicom_files)

    self.assertEqual(
        ingest_base._get_downsampled_dicom_files(dicom_files, downsamples),
        expected_result,
    )
    # validates when len(downsamples) != len(file_list)
    # a logs a warning.
    mock_logger.assert_called_once_with(
        cloud_logging_client.CloudLoggingClient.logger(),
        'Wsi2Dcm failed to produce expected downsample.',
        {
            'files_found': str(sorted(expected_result)),
            'downsamples_requested': str(sorted(downsamples)),
        },
    )

  def test_dicom_instance_ingestion_lists_class_dowsamples_none(self):
    dicom_files = _test_dicomfile_set(range(10))
    expected_dicom_files = copy.copy(dicom_files)

    test = ingest_base.DicomInstanceIngestionSets(dicom_files, None)

    self.assertEqual(test.main_store_instances, expected_dicom_files)
    self.assertEmpty(test.oof_store_instances)

  def test_dicom_instance_ingestion_lists_class_oof_dowsamples_empty(self):
    dicom_files = _test_dicomfile_set(range(10))
    expected_dicom_files = _test_dicomfile_set([1, 9])
    ds_layer_config = ingest_base.WsiDownsampleLayers(main_store={1}, oof=set())

    test = ingest_base.DicomInstanceIngestionSets(dicom_files, ds_layer_config)

    self.assertEqual(test.main_store_instances, expected_dicom_files)
    self.assertEmpty(test.oof_store_instances)

  def test_dicom_instance_ingestion_lists_class_oof_gen_full_pyramid(self):
    downsamples = {2, 3}
    dicom_files = _test_dicomfile_set(range(10))
    expected_main_files = copy.copy(dicom_files)
    expected_oof_files = _test_dicomfile_set(downsamples)
    ds_layer_config = ingest_base.WsiDownsampleLayers(None, downsamples)

    test = ingest_base.DicomInstanceIngestionSets(dicom_files, ds_layer_config)

    self.assertEqual(test.main_store_instances, expected_main_files)
    self.assertEqual(test.oof_store_instances, expected_oof_files)

  def test_dicom_instance_ingestion_lists_class_oof_select_layers(self):
    main_ds = {1, 4, 5, 6, 9}
    oof_ds = {2, 3}
    expected_main_files = _test_dicomfile_set(main_ds)
    expected_oof_files = _test_dicomfile_set(oof_ds)
    ds_layer_config = ingest_base.WsiDownsampleLayers(main_ds, oof_ds)
    dicom_files = _test_dicomfile_set(range(10))

    test = ingest_base.DicomInstanceIngestionSets(dicom_files, ds_layer_config)

    self.assertEqual(test.main_store_instances, expected_main_files)
    self.assertEqual(test.oof_store_instances, expected_oof_files)

  def test_combine_main_and_oof_ingestion_and_main_store_sets(self):
    main_ds = {1, 4, 5, 6, 9}
    oof_ds = {2, 3, 5, 6}
    expected_main_files = _test_dicomfile_set([1, 2, 3, 4, 5, 6, 9])
    ds_layer_config = ingest_base.WsiDownsampleLayers(main_ds, oof_ds)
    dicom_files = _test_dicomfile_set(range(10))

    test = ingest_base.DicomInstanceIngestionSets(dicom_files, ds_layer_config)
    test.combine_main_and_oof_ingestion_and_main_store_sets()
    self.assertEqual(test.main_store_instances, expected_main_files)
    self.assertEmpty(test.oof_store_instances)

  def test_dicom_instance_force_upload_to_main_store_list(self):
    main_ds = {1, 4, 6}
    oof_ds = {2, 3}
    files_to_force_upload = {'foo', 'bar'}
    # Smallest downsampling included automatically
    expected_main_files = _test_dicomfile_set(main_ds.union({9}))
    expected_oof_files = _test_dicomfile_set(oof_ds)

    ds_layer_config = ingest_base.WsiDownsampleLayers(main_ds, oof_ds)
    dicom_files = _test_dicomfile_set(range(10))
    test = ingest_base.DicomInstanceIngestionSets(
        dicom_files, ds_layer_config, files_to_force_upload
    )
    expected_main_files = expected_main_files.union(files_to_force_upload)
    self.assertEqual(test.main_store_instances, expected_main_files)
    self.assertEqual(test.oof_store_instances, expected_oof_files)


class _IngestBaseTestDriver(ingest_base.IngestBase):

  def generate_dicom(
      self,
      dicom_gen_dir: str,
      dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
      polling_client: abstract_polling_client.AbstractPollingClient,
      abstract_dicom_handler: abstract_dicom_generation.AbstractDicomGeneration,
  ) -> ingest_base.GenDicomResult:
    return ingest_base.GenDicomResult(
        dicom_gen, 'None', ingest_base.DicomInstanceIngestionSets([]), False
    )


class IngestBaseTest(parameterized.TestCase):
  """Tests IngestBase."""

  def setUp(self):
    super().setUp()
    self._layer_config_path = gen_test_util.test_file_path(
        'layer_config_valid.yaml'
    )
    self._wsi_schema_path = gen_test_util.test_file_path(
        'example_schema_wsi.json'
    )

  @parameterized.named_parameters([
      dict(testcase_name='default', param_flags={}, expected_mod={}),
      dict(
          testcase_name='frame_height',
          param_flags={'wsi2dcm_dicom_frame_height': '512'},
          expected_mod={'--tileHeight': '512'},
      ),
      dict(
          testcase_name='frame_width',
          param_flags={'wsi2dcm_dicom_frame_width': '512'},
          expected_mod={'--tileWidth': '512'},
      ),
      dict(
          testcase_name='jpeg2000',
          param_flags={
              'wsi2dcm_compression': ingest_flags.Wsi2DcmCompression.JPEG2000
          },
          expected_mod={'--compression': 'jpeg2000'},
      ),
      dict(
          testcase_name='raw',
          param_flags={
              'wsi2dcm_compression': ingest_flags.Wsi2DcmCompression.RAW
          },
          expected_mod={'--compression': 'raw'},
      ),
      dict(
          testcase_name='quality',
          param_flags={'wsi2dcm_jpeg_compression_quality': '50'},
          expected_mod={'--jpegCompressionQuality': '50'},
      ),
      dict(
          testcase_name='first_level_compression',
          param_flags={
              'wsi2dcm_first_level_compression': (
                  ingest_flags.Wsi2DcmFirstLevelCompression.JPEG
              )
          },
          expected_mod={'--firstLevelCompression': 'jpeg'},
      ),
      dict(
          testcase_name='jpeg_compression_subsampling_444',
          param_flags={
              'wsi2dcm_jpeg_compression_subsampling': (
                  ingest_flags.Wsi2DcmJpegCompressionSubsample.SUBSAMPLE_444
              )
          },
          expected_mod={'--jpegSubsampling': '444'},
      ),
      dict(
          testcase_name='jpeg_compression_subsampling_440',
          param_flags={
              'wsi2dcm_jpeg_compression_subsampling': (
                  ingest_flags.Wsi2DcmJpegCompressionSubsample.SUBSAMPLE_440
              )
          },
          expected_mod={'--jpegSubsampling': '440'},
      ),
      dict(
          testcase_name='jpeg_compression_subsampling_442',
          param_flags={
              'wsi2dcm_jpeg_compression_subsampling': (
                  ingest_flags.Wsi2DcmJpegCompressionSubsample.SUBSAMPLE_442
              )
          },
          expected_mod={'--jpegSubsampling': '442'},
      ),
      dict(
          testcase_name='jpeg_compression_subsampling_420',
          param_flags={
              'wsi2dcm_jpeg_compression_subsampling': (
                  ingest_flags.Wsi2DcmJpegCompressionSubsample.SUBSAMPLE_420
              )
          },
          expected_mod={'--jpegSubsampling': '420'},
      ),
  ])
  def test_build_wsi2dcm_param_string(self, param_flags, expected_mod):
    expected_param_set = copy.copy(_EXPECTED_DEFAULT_WSI2DCM_PARAMS)
    expected_param_set.update(expected_mod)

    with flagsaver.flagsaver(**param_flags):
      wsi2dcm_params = ingest_base._build_wsi2dcm_param_string()

    self.assertEqual(_parse_wsi2dcm_params(wsi2dcm_params), expected_param_set)

  @parameterized.named_parameters([
      dict(
          testcase_name='all_levels',
          param_flags={
              'wsi2dcm_pixel_equivalent_transform': (
                  ingest_flags.Wsi2DcmPixelEquivalentTransform.ALL_LEVELS
              )
          },
          expected_mod={'--SVSImportPreferScannerTileingForAllLevels': None},
      ),
      dict(
          testcase_name='highest_magnification',
          param_flags={
              'wsi2dcm_pixel_equivalent_transform': (
                  ingest_flags.Wsi2DcmPixelEquivalentTransform.HIGHEST_MAGNIFICATION
              )
          },
          expected_mod={'--SVSImportPreferScannerTileingForLargestLevel': None},
      ),
      dict(
          testcase_name='no_levels',
          param_flags={
              'wsi2dcm_pixel_equivalent_transform': (
                  ingest_flags.Wsi2DcmPixelEquivalentTransform.DISABLED
              )
          },
          expected_mod={},
      ),
  ])
  def test_build_wsi2dcm_param_set_pixel_equal_transform(
      self, param_flags, expected_mod
  ):
    expected_param_set = copy.copy(_EXPECTED_DEFAULT_WSI2DCM_PARAMS)
    del expected_param_set['--SVSImportPreferScannerTileingForLargestLevel']
    expected_param_set.update(expected_mod)

    with flagsaver.flagsaver(**param_flags):
      wsi2dcm_params = ingest_base._build_wsi2dcm_param_string()

    self.assertEqual(_parse_wsi2dcm_params(wsi2dcm_params), expected_param_set)

  @mock.patch.object(cloud_logging_client.CloudLoggingClient, 'error')
  def test_log_and_get_failure_bucket_path_empty(self, mock_logger):
    ingest = _IngestBaseTestDriver()
    self.assertEmpty(
        ingest.log_and_get_failure_bucket_path(Exception('error_msg'))
    )
    mock_logger.assert_called_once()

  @mock.patch.object(cloud_logging_client.CloudLoggingClient, 'error')
  def test_log_and_get_failure_bucket_path(self, mock_logger):
    ingest = _IngestBaseTestDriver(
        ingest_base.GcsIngestionBuckets(
            success_uri='gs://success-bucket', failure_uri='gs://failure-bucket'
        )
    )
    self.assertEqual(
        ingest.log_and_get_failure_bucket_path(Exception('error_msg')),
        'gs://failure-bucket/error_msg',
    )
    mock_logger.assert_called_once()

  @mock.patch.object(cloud_logging_client.CloudLoggingClient, 'error')
  def test_log_and_get_failure_bucket_path_additional_arg(self, mock_logger):
    ingest = _IngestBaseTestDriver(
        ingest_base.GcsIngestionBuckets(
            success_uri='gs://success-bucket', failure_uri='gs://failure-bucket'
        )
    )
    self.assertEqual(
        ingest.log_and_get_failure_bucket_path(
            Exception('Detailed error message.', 'simplified_error_msg')
        ),
        'gs://failure-bucket/simplified_error_msg',
    )
    mock_logger.assert_called_once()

  @parameterized.named_parameters(
      dict(
          testcase_name='whole_slide_image',
          sop_class_name='VL Whole Slide Microscopy Image Storage',
      ),
      dict(
          testcase_name='slide_coordinates_image',
          sop_class_name='VL Slide-Coordinates Microscopic Image Storage',
      ),
  )
  @flagsaver.flagsaver(metadata_bucket='test')
  @mock.patch.multiple(ingest_base.IngestBase, __abstractmethods__=set())
  @mock.patch.object(
      metadata_storage_client.MetadataStorageClient,
      'get_dicom_schema',
      return_value=_SCHEMA,
      autospec=True,
  )
  def test_get_dicom_metadata_schema(self, mock_metadata, sop_class_name):
    ingest = _IngestBaseTestDriver()
    self.assertEqual(ingest._get_dicom_metadata_schema(sop_class_name), _SCHEMA)
    mock_metadata.assert_called_once_with(
        ingest._metadata_storage_client, {'SOPClassUID_Name': sop_class_name}
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='whole_slide_image',
          sop_class_name='VL Whole Slide Microscopy Image Storage',
      ),
      dict(
          testcase_name='slide_coordinates_image',
          sop_class_name='VL Slide-Coordinates Microscopic Image Storage',
      ),
  )
  @flagsaver.flagsaver(metadata_bucket='test_bucket')
  @mock.patch.multiple(ingest_base.IngestBase, __abstractmethods__=set())
  def test_get_slide_dicom_json_formatted_metadata(self, sop_class_name):
    ingest = _IngestBaseTestDriver()
    test_bucket_path = os.path.join(flags.FLAGS.test_srcdir, _TEST_BUCKET_PATH)
    with open(self._wsi_schema_path, 'rt') as infile:
      schema_json = json.load(infile)
    del schema_json['DICOMSchemaDef']
    filename = sop_class_name.replace(' ', '_').lower()
    filename = os.path.join(test_bucket_path, f'expected_{filename}')
    with open(f'{filename}.json', 'rt') as infile:
      expected_json = json.load(infile)
    with open(f'{filename}.csv', 'rt') as infile:
      expected_metadata = infile.read()
    slide_id = 'GO-1675974754377-1-A-0'

    with gcs_mock.GcsMock({'test_bucket': test_bucket_path}):
      ingest.metadata_storage_client.update_metadata()
      dcm_json, csv_metadata = ingest._get_slide_dicom_json_formatted_metadata(
          sop_class_name,
          slide_id,
          dicom_schema=schema_json,
          dicom_client=dicom_store_client.DicomStoreClient(
              'mock_dicom_store_client'
          ),
          test_metadata_for_missing_study_instance_uid=False,
      )
    self.assertEqual(dcm_json, expected_json)
    self.assertEqual(
        csv_metadata.slide_metadata.to_csv(index=False),
        expected_metadata,
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='whole_slide_image',
          sop_class_name='VL Whole Slide Microscopy Image Storage',
      ),
      dict(
          testcase_name='slide_coordinates_image',
          sop_class_name='VL Slide-Coordinates Microscopic Image Storage',
      ),
  )
  @flagsaver.flagsaver(
      metadata_bucket='test_bucket',
      require_type1_dicom_tag_metadata_is_defined=True,
  )
  @mock.patch.multiple(ingest_base.IngestBase, __abstractmethods__=set())
  def test_get_slide_dicom_json_formatted_metadata_missing_required_metadata_raises(
      self,
      sop_class_name,
  ):
    ingest = _IngestBaseTestDriver()
    test_bucket_path = os.path.join(flags.FLAGS.test_srcdir, _TEST_BUCKET_PATH)
    with open(self._wsi_schema_path, 'rt') as infile:
      schema_json = json.load(infile)
    del schema_json['DICOMSchemaDef']
    slide_id = 'GO-1675974754377-1-A-1'
    with gcs_mock.GcsMock({'test_bucket': test_bucket_path}):
      with self.assertRaises(ingest_base.GenDicomFailedError):
        ingest.metadata_storage_client.update_metadata()
        ingest._get_slide_dicom_json_formatted_metadata(
            sop_class_name,
            slide_id,
            dicom_schema=schema_json,
            dicom_client=dicom_store_client.DicomStoreClient(
                'mock_dicom_store_client'
            ),
            test_metadata_for_missing_study_instance_uid=False,
        )

  @flagsaver.flagsaver(
      metadata_bucket='test_bucket',
      require_type1_dicom_tag_metadata_is_defined=False,
  )
  @mock.patch.multiple(ingest_base.IngestBase, __abstractmethods__=set())
  def test_get_slide_dicom_json_formatted_metadata_missing_study_instance_uid_raises(
      self,
  ):
    sop_class_name = 'VL Slide-Coordinates Microscopic Image Storage'
    ingest = _IngestBaseTestDriver()
    test_bucket_path = os.path.join(flags.FLAGS.test_srcdir, _TEST_BUCKET_PATH)
    with open(self._wsi_schema_path, 'rt') as infile:
      schema_json = json.load(infile)
    del schema_json['DICOMSchemaDef']
    del schema_json['0x0020000D']
    slide_id = 'GO-1675974754377-1-A-1'
    with gcs_mock.GcsMock({'test_bucket': test_bucket_path}):
      with self.assertRaises(ingest_base.GenDicomFailedError):
        ingest.metadata_storage_client.update_metadata()
        ingest._get_slide_dicom_json_formatted_metadata(
            sop_class_name,
            slide_id,
            dicom_schema=schema_json,
            dicom_client=dicom_store_client.DicomStoreClient(
                'mock_dicom_store_client'
            ),
            test_metadata_for_missing_study_instance_uid=True,
        )

  @parameterized.named_parameters(
      [('oof_enabled', True, {2, 32}), ('oof_disabled', False, set())]
  )
  def test_get_downsamples_generate_from_yaml_oof_enabled(
      self, oof_enabled, expected_oof
  ):
    with flagsaver.flagsaver(
        ingestion_pyramid_layer_generation_config_path=self._layer_config_path
    ):
      result = _IngestBaseTestDriver().get_downsamples_to_generate(
          0.00025, oof_enabled
      )
    self.assertEqual(result.main_store, {1, 8, 32})
    self.assertEqual(result.oof, expected_oof)
    self.assertFalse(result.generate_full_pyramid)

  @parameterized.named_parameters(
      [('oof_enabled', True, {2, 32}), ('oof_disabled', False, set())]
  )
  def test_get_downsamples_to_generate_no_yaml(self, oof_enabled, expected_oof):
    with flagsaver.flagsaver(ingestion_pyramid_layer_generation_config_path=''):
      result = _IngestBaseTestDriver().get_downsamples_to_generate(
          0.00025, oof_enabled
      )
    self.assertIsNone(result.main_store)
    self.assertEqual(result.oof, expected_oof)
    self.assertTrue(result.generate_full_pyramid)

  def test_get_generated_dicom_files_sorted_by_size(self):
    temp_dir = self.create_tempdir()
    file_list = []
    for fnum in range(5):
      path = os.path.join(temp_dir, f'downsample-{fnum}-.dcm')
      with open(path, 'wt') as outfile:
        outfile.write('*' * fnum)
      file_list.append(path)
      path = os.path.join(temp_dir, f'temp_{fnum}.other')
      with open(path, 'wt') as outfile:
        outfile.write('*' * fnum)
    file_list.reverse()

    self.assertEqual(
        ingest_base._get_generated_dicom_files_sorted_by_size(
            temp_dir.full_path
        ),
        file_list,
    )

  def test_get_generated_dicom_files_raises_finding_invalid_dicom(self):
    temp_dir = self.create_tempdir()
    for fnum in range(5):
      path = os.path.join(temp_dir, f'temp-{fnum}-.dcm')
      with open(path, 'wt') as outfile:
        outfile.write('*' * fnum)
      path = os.path.join(temp_dir, f'downsample-{fnum}-.dcm')
      with open(path, 'wt') as outfile:
        outfile.write('*' * fnum)

    with self.assertRaises(ingest_base.Wsi2DcmFileNameFormatError):
      ingest_base._get_generated_dicom_files_sorted_by_size(temp_dir.full_path)

  @parameterized.parameters(
      ('1.2.3', '1.2.3.4'),
      ('1.2.4', ''),
      ('1.2.5', None),
      (None, None),
      ('', '1.2.3.5'),
      ('', ''),
      (None, '1.2.3.6'),
  )
  def test_get_wsi2dcm_cmdline_study_seriesuid(self, studyuid, seriesuid):
    downsample_config = ingest_base.WsiDownsampleLayers(None, {2, 32})
    expected = (
        '/wsi-to-dicom-converter/build/wsi2dcm --input="convert.svs" '
        '--outFolder="output_path"  default_params --levels=999 '
        '--stopDownsamplingAtSingleFrame --singleFrameDownsample'
    )
    if studyuid is not None and studyuid:
      expected = f'{expected} --studyId={studyuid}'
    if seriesuid is not None and seriesuid:
      expected = f'{expected} --seriesId={seriesuid}'

    result = ingest_base._get_wsi2dcm_cmdline(
        'convert.svs',
        downsample_config,
        'output_path',
        studyuid,
        seriesuid,
        'default_params',
    )

    self.assertEqual(result, expected)

  @parameterized.parameters([
      (
          ingest_base.WsiDownsampleLayers({1, 2, 8}, {2, 32}),
          (
              '/wsi-to-dicom-converter/build/wsi2dcm --input="convert.svs"'
              ' --outFolder="output_path"  default_params --downsamples 1 2'
              ' 8 32 --singleFrameDownsample'
          ),
      ),
      (
          ingest_base.WsiDownsampleLayers(None, {2, 32}),
          (
              '/wsi-to-dicom-converter/build/wsi2dcm --input="convert.svs" '
              '--outFolder="output_path"  default_params --levels=999 '
              '--stopDownsamplingAtSingleFrame --singleFrameDownsample'
          ),
      ),
  ])
  def test_get_wsi2dcm_cmdline(self, downsample_config, expected):
    result = ingest_base._get_wsi2dcm_cmdline(
        'convert.svs',
        downsample_config,
        'output_path',
        '',
        '',
        'default_params',
    )
    self.assertEqual(result, expected)

  def test_correct_missing_study_instance_uid_in_metadata_has_uid_succees(self):
    study_instance_uid = '1.2.3.4'
    metadata = {}
    dicom_json_util.set_study_instance_uid_in_metadata(
        metadata, study_instance_uid
    )

    self.assertEqual(
        ingest_base._correct_missing_study_instance_uid_in_metadata(
            metadata,
            mock.create_autospec(
                dicom_store_client.DicomStoreClient, instance=True
            ),
        ),
        study_instance_uid,
    )
    self.assertEqual(
        dicom_json_util.get_study_instance_uid(metadata),
        study_instance_uid,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='find_by_accession',
          store_metadata=[_dcm_json('1.2', '1.2.1', '1.2.1.1', 'A1')],
          test_metadata=_dcm_json(accession='A1'),
          expected='1.2',
      ),
      dict(
          testcase_name='find_by_accession_and_patient_id',
          store_metadata=[
              _dcm_json('1.2', '1.2.1', '1.2.1.1', 'A1', 'P1'),
              _dcm_json('1.3', '1.2.1', '1.2.1.1', 'A1', 'P2'),
          ],
          test_metadata=_dcm_json(accession='A1', patient_id='P2'),
          expected='1.3',
      ),
      dict(
          testcase_name='not_found',
          store_metadata=[
              _dcm_json('1.2', '1.2.1', '1.2.1.1', 'A1'),
          ],
          test_metadata=_dcm_json(accession='A3'),
          expected='5.6.7',
      ),
      dict(
          testcase_name='not_found_using_full_match',
          store_metadata=[
              _dcm_json('1.2', '1.2.1', '1.2.1.1', 'A1'),
          ],
          test_metadata=_dcm_json(accession='A1', patient_id='p2'),
          expected='5.6.7',
      ),
      dict(
          testcase_name='study_missing_patient_id_not_found',
          store_metadata=[
              _dcm_json('1.2', '1.2.1', '1.2.1.1', 'A1', 'p1'),
          ],
          test_metadata=_dcm_json(accession='A1'),
          expected='5.6.7',
      ),
  ])
  @mock.patch.object(
      uid_generator, 'generate_uid', autospec=True, return_value='5.6.7'
  )
  @flagsaver.flagsaver(enable_create_missing_study_instance_uid=True)
  def test_correct_missing_study_instance_uid_in_metadata_from_accession(
      self, unused_mock, store_metadata, test_metadata, expected
  ):
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url) as mk_store:
      for dicom_metadata in store_metadata:
        mk_store[mock_dicomweb_url].add_instance(
            _create_pydicom_file_dataset_from_json(dicom_metadata)
        )

      dicom_client = dicom_store_client.DicomStoreClient(mock_dicomweb_url)
      result = ingest_base._correct_missing_study_instance_uid_in_metadata(
          test_metadata, dicom_client
      )
    self.assertEqual(result, expected)
    self.assertEqual(
        dicom_json_util.get_study_instance_uid(test_metadata),
        expected,
    )

  @mock.patch.object(
      uid_generator, 'generate_uid', autospec=True, return_value='5.6.7'
  )
  @flagsaver.flagsaver(enable_create_missing_study_instance_uid=True)
  def test_correct_missing_study_instance_uid_raises_if_multiple_accession_found(
      self, unused_mock
  ):
    store_metadata = [
        _dcm_json('1.2', '1.2.1', '1.2.1.1', 'A1'),
        _dcm_json('1.3', '1.2.1', '1.2.1.1', 'A1'),
    ]
    test_metadata = _dcm_json(accession='A1')
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url) as mk_store:
      for dicom_metadata in store_metadata:
        mk_store[mock_dicomweb_url].add_instance(
            _create_pydicom_file_dataset_from_json(dicom_metadata)
        )

      dicom_client = dicom_store_client.DicomStoreClient(mock_dicomweb_url)
      with self.assertRaises(
          ingest_base._AccessionNumberAssociatedWithMultipleStudyInstanceUIDError
      ):
        ingest_base._correct_missing_study_instance_uid_in_metadata(
            test_metadata, dicom_client
        )

  @parameterized.named_parameters([
      dict(
          testcase_name='missing_study_uid_create_disabled',
          store_metadata=[_dcm_json('1.2', '1.2.1', '1.2.1.1', 'A1')],
          test_metadata=_dcm_json(accession='A1'),
          enable_create_missing_study_instance_uid=False,
      ),
      dict(
          testcase_name='missing_acccession_number',
          store_metadata=[
              _dcm_json('1.2', '1.2.1', '1.2.1.1', 'A1', 'P1'),
          ],
          test_metadata=_dcm_json(patient_id='p1'),
          enable_create_missing_study_instance_uid=True,
      ),
  ])
  def test_correct_missing_study_instance_uid_raises_if_cannot_create(
      self,
      store_metadata,
      test_metadata,
      enable_create_missing_study_instance_uid,
  ):
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url) as mk_store:
      for dicom_metadata in store_metadata:
        mk_store[mock_dicomweb_url].add_instance(
            _create_pydicom_file_dataset_from_json(dicom_metadata)
        )

      dicom_client = dicom_store_client.DicomStoreClient(mock_dicomweb_url)
      with flagsaver.flagsaver(
          enable_create_missing_study_instance_uid=enable_create_missing_study_instance_uid
      ):
        with self.assertRaisesRegex(
            dicom_json_util.MissingStudyUIDInMetadataError,
            ingest_const.ErrorMsgs.MISSING_STUDY_UID,
        ):
          ingest_base._correct_missing_study_instance_uid_in_metadata(
              test_metadata, dicom_client
          )

  @parameterized.named_parameters([
      dict(
          testcase_name='filename_ingestion',
          filename=f'{_SLIDE_ID_2}.svs',
          candidate_barcode_values=[],
          expected=_SLIDE_ID_2,
      ),
      dict(
          testcase_name='barcode_ingestion',
          filename='foo.svs',
          candidate_barcode_values=[_SLIDE_ID_1],
          expected=_SLIDE_ID_1,
      ),
  ])
  @flagsaver.flagsaver(metadata_bucket='test')
  def test_determine_slideid_success(
      self, filename, candidate_barcode_values, expected
  ):
    ingest_base_test_driver = _IngestBaseTestDriver()
    metadata_client = metadata_storage_client.MetadataStorageClient()
    metadata_client.set_debug_metadata([
        gen_test_util.test_file_path(_METADATA_PATH),
        gen_test_util.test_file_path(_SCHEMA_PATH),
    ])
    ingest_base_test_driver._metadata_storage_client = metadata_client
    with mock.patch.object(
        barcode_reader,
        'read_barcode_in_files',
        autospec=True,
        return_value={
            bar_code: {'unused'} for bar_code in candidate_barcode_values
        },
    ):
      self.assertEqual(
          ingest_base_test_driver._determine_slideid(
              filename,
              [ancillary_image_extractor.AncillaryImage('foo.png', 'RGB', True)]
              * len(candidate_barcode_values),
          ),
          expected,
      )

  @flagsaver.flagsaver(metadata_bucket='test')
  def test_determine_slideid_fails_to_find_slide_id_raises(self):
    ingest_base_test_driver = _IngestBaseTestDriver()
    metadata_client = metadata_storage_client.MetadataStorageClient()
    metadata_client.set_debug_metadata([
        gen_test_util.test_file_path(_METADATA_PATH),
        gen_test_util.test_file_path(_SCHEMA_PATH),
    ])
    ingest_base_test_driver._metadata_storage_client = metadata_client
    with mock.patch.object(
        barcode_reader, 'read_barcode_in_files', autospec=True, return_value={}
    ):
      with self.assertRaisesRegex(
          ingest_base.GenDicomFailedError,
          r"\('Failed to determine slide id.',"
          r" 'slide_id_error__file_name_does_not_contain_slide_id_candidates'\)",
      ):
        ingest_base_test_driver._determine_slideid('foo.svs', [])


if __name__ == '__main__':
  absltest.main()
