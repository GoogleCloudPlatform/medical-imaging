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

import contextlib
import copy
import json
import os
import shutil
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Union
from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import pydicom
import requests

from shared_libs.logging_lib import cloud_logging_client
from shared_libs.test_utils.dicom_store_mock import dicom_store_mock
from shared_libs.test_utils.gcs_mock import gcs_mock
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import abstract_polling_client
from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib import redis_client
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_json_util
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_private_tag_generator
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client


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


def _schema_paths() -> List[str]:
  return [
      gen_test_util.test_file_path('example_schema_wsi.json'),
      gen_test_util.test_file_path('example_schema_microscopic_image.json'),
      gen_test_util.test_file_path('example_schema_slide_coordinates.json'),
  ]


def _get_test_metadata_bucket(path: absltest._TempDir) -> absltest._TempDir:
  metadata_path = os.path.join(flags.FLAGS.test_srcdir, _TEST_BUCKET_PATH)
  for fname in os.listdir(metadata_path):
    shutil.copyfile(
        os.path.join(metadata_path, fname), os.path.join(path, fname)
    )
  return path


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


class _MockAbstractDicomGenerationForTest(
    abstract_dicom_generation.AbstractDicomGeneration
):
  """AbstractDicomGeneration with abstract methods implemented."""

  def get_slide_transform_lock(
      self,
      unused_ingest_file: abstract_dicom_generation.GeneratedDicomFiles,
      unused_polling_client: abstract_polling_client.AbstractPollingClient,
  ) -> abstract_dicom_generation.TransformationLock:
    return abstract_dicom_generation.TransformationLock('mock_lock')

  def generate_dicom_and_push_to_store(self, ingest_file_list, polling_client):
    pass

  def decode_pubsub_msg(self, msg):
    return None

  def handle_unexpected_exception(self, polling_client, ingest_file, exp):
    pass


def _mock_abstract_dicom_gen_handler(
    mock_dicomweb_url: str, context_stack: contextlib.ExitStack
) -> abstract_dicom_generation.AbstractDicomGeneration:
  abstract_dicom_gen = mock.create_autospec(
      abstract_dicom_generation.AbstractDicomGeneration, instance=True
  )
  abstract_dicom_gen.dcm_store_client = dicom_store_client.DicomStoreClient(
      mock_dicomweb_url
  )
  abstract_dicom_gen._process_message_context_block = context_stack
  return abstract_dicom_gen


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

  @mock.patch.object(cloud_logging_client, 'warning', autospec=True)
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

  def __init__(
      self,
      slide_id: str = 'undefined',
      ingest_buckets: Optional[ingest_base.GcsIngestionBuckets] = None,
  ):
    super().__init__(
        ingest_buckets, metadata_storage_client.MetadataStorageClient()
    )
    self._slide_id = slide_id

  def init_handler_for_ingestion(self) -> None:
    return

  def _generate_metadata_free_slide_metadata(
      self,
      slide_id: str,
      client: dicom_store_client.DicomStoreClient,
  ) -> ingest_base.DicomMetadata:
    return ingest_base.generate_metadata_free_slide_metadata(slide_id, client)

  def get_slide_id(
      self,
      dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
      abstract_dicom_handler: abstract_dicom_generation.AbstractDicomGeneration,
  ) -> str:
    return self._slide_id

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

  @flagsaver.flagsaver(metadata_bucket='test_bucket')
  @mock.patch.object(cloud_logging_client.CloudLoggingClient, 'error')
  def test_log_and_get_failure_bucket_path_empty(self, mock_logger):
    ingest = _IngestBaseTestDriver()
    self.assertEmpty(
        ingest.log_and_get_failure_bucket_path(Exception('error_msg'))
    )
    mock_logger.assert_called_once()

  @flagsaver.flagsaver(metadata_bucket='test_bucket')
  @mock.patch.object(cloud_logging_client.CloudLoggingClient, 'error')
  def test_log_and_get_failure_bucket_path(self, mock_logger):
    ingest = _IngestBaseTestDriver(
        ingest_buckets=ingest_base.GcsIngestionBuckets(
            success_uri='gs://success-bucket', failure_uri='gs://failure-bucket'
        )
    )
    self.assertEqual(
        ingest.log_and_get_failure_bucket_path(Exception('error_msg')),
        'gs://failure-bucket/error_msg',
    )
    mock_logger.assert_called_once()

  @flagsaver.flagsaver(metadata_bucket='test_bucket')
  @mock.patch.object(cloud_logging_client.CloudLoggingClient, 'error')
  def test_log_and_get_failure_bucket_path_additional_arg(self, mock_logger):
    ingest = _IngestBaseTestDriver(
        ingest_buckets=ingest_base.GcsIngestionBuckets(
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
    self.assertEqual(ingest.get_dicom_metadata_schema(sop_class_name), _SCHEMA)
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
  @flagsaver.flagsaver(
      metadata_bucket='test_bucket',
      metadata_tag_length_validation=ingest_flags.MetadataTagLengthValidation.LOG_WARNING_AND_CLIP,
  )
  @mock.patch.multiple(ingest_base.IngestBase, __abstractmethods__=set())
  def test_get_slide_dicom_json_formatted_metadata(self, sop_class_name):
    ingest = _IngestBaseTestDriver()
    test_bucket_path = _get_test_metadata_bucket(self.create_tempdir())
    for path in _schema_paths():
      shutil.copyfile(
          path, os.path.join(test_bucket_path, os.path.basename(path))
      )
    filename = sop_class_name.replace(' ', '_').lower()
    filename = os.path.join(test_bucket_path, f'expected_{filename}')
    with open(f'{filename}.json', 'rt') as infile:
      expected_json = json.load(infile)
    with open(f'{filename}.csv', 'rt') as infile:
      expected_metadata = infile.read()
    slide_id = 'GO-1675974754377-1-A-0'
    with gcs_mock.GcsMock({'test_bucket': test_bucket_path}):
      ingest.metadata_storage_client.update_metadata()
      ingest.set_slide_id(slide_id, False)
      dcm_metadata = ingest.get_slide_dicom_json_formatted_metadata(
          sop_class_name,
          slide_id,
          dicom_client=dicom_store_client.DicomStoreClient(
              'mock_dicom_store_client'
          ),
      )
    self.assertEqual(dcm_metadata.dicom_json, expected_json)
    self.assertEqual(
        dcm_metadata.metadata_table_row.slide_metadata.to_csv(index=False),
        expected_metadata,
    )

  @flagsaver.flagsaver(metadata_bucket='test_bucket')
  def test_get_slide_dicom_json_formatted_metadata_removes_sopinstance_uid(
      self,
  ):
    sop_class_name = 'VL Whole Slide Microscopy Image Storage'
    ingest = _IngestBaseTestDriver()
    test_bucket_path = _get_test_metadata_bucket(self.create_tempdir())
    for path in _schema_paths():
      with open(path, 'rt') as infile:
        schema = json.load(infile)
      schema['0x00080018'] = {
          'Keyword': 'SOPInstanceUID',
          'Static_Value': '1.2.3',
      }
      with open(
          os.path.join(test_bucket_path, os.path.basename(path)), 'wt'
      ) as outfile:
        json.dump(schema, outfile)
    slide_id = 'GO-1675974754377-1-A-0'
    with gcs_mock.GcsMock({'test_bucket': test_bucket_path}):
      ingest.metadata_storage_client.update_metadata()
      ingest.set_slide_id(slide_id, False)
      dcm_metadata = ingest.get_slide_dicom_json_formatted_metadata(
          sop_class_name,
          slide_id,
          dicom_client=dicom_store_client.DicomStoreClient(
              'mock_dicom_store_client'
          ),
      )
      self.assertNotIn('00080018', dcm_metadata.dicom_json)

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
    test_bucket_path = _get_test_metadata_bucket(self.create_tempdir())
    for path in _schema_paths():
      shutil.copyfile(
          path, os.path.join(test_bucket_path, os.path.basename(path))
      )
    slide_id = 'GO-1675974754377-1-A-1'
    with gcs_mock.GcsMock({'test_bucket': test_bucket_path}):
      with self.assertRaises(ingest_base.GenDicomFailedError):
        ingest.metadata_storage_client.update_metadata()
        ingest.set_slide_id(slide_id, False)
        ingest.get_slide_dicom_json_formatted_metadata(
            sop_class_name,
            slide_id,
            dicom_client=dicom_store_client.DicomStoreClient(
                'mock_dicom_store_client'
            ),
        )

  @flagsaver.flagsaver(
      metadata_bucket='test_bucket',
      require_type1_dicom_tag_metadata_is_defined=True,
      enable_metadata_free_ingestion=True,
  )
  @mock.patch.multiple(ingest_base.IngestBase, __abstractmethods__=set())
  def test_get_slide_dicom_json_formatted_metadata_missing_returns_metadata_free(
      self,
  ):
    expected = {
        '00100020': {'vr': 'LO', 'Value': ['GO-1675974754377-1-A-1-5']},
        '0020000D': {'vr': 'UI', 'Value': ['1.2.3']},
        '00400512': {'vr': 'LO', 'Value': ['GO-1675974754377-1-A-1-5']},
    }
    ingest = _IngestBaseTestDriver()
    test_bucket_path = _get_test_metadata_bucket(self.create_tempdir())
    for path in _schema_paths():
      shutil.copyfile(
          path, os.path.join(test_bucket_path, os.path.basename(path))
      )
    slide_id = 'GO-1675974754377-1-A-1-5'
    with gcs_mock.GcsMock({'test_bucket': test_bucket_path}):
      ingest.metadata_storage_client.update_metadata()
      ingest.set_slide_id(slide_id, True)
      mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
      with dicom_store_mock.MockDicomStores(mock_dicomweb_url):
        dicom_client = dicom_store_client.DicomStoreClient(mock_dicomweb_url)
        with mock.patch.object(
            uid_generator, 'generate_uid', autospec=True, return_value='1.2.3'
        ):
          dcm_metadata = ingest.get_slide_dicom_json_formatted_metadata(
              'VL Whole Slide Microscopy Image Storage',
              slide_id,
              dicom_client=dicom_client,
          )
    self.assertEqual(dcm_metadata.dicom_json, expected)
    self.assertIsNotNone(dcm_metadata.metadata_table_row)

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
      enable_metadata_free_ingestion=False,
  )
  @mock.patch.multiple(ingest_base.IngestBase, __abstractmethods__=set())
  def test_get_slide_dicom_json_formatted_metadata_missing_raises_if_metadata_free_is_false(
      self,
      sop_class_name,
  ):
    ingest = _IngestBaseTestDriver()
    test_bucket_path = _get_test_metadata_bucket(self.create_tempdir())
    for path in _schema_paths():
      shutil.copyfile(
          path, os.path.join(test_bucket_path, os.path.basename(path))
      )
    slide_id = 'GO-1675974754377-1-A-1-5'
    with gcs_mock.GcsMock({'test_bucket': test_bucket_path}):
      ingest.metadata_storage_client.update_metadata()
      mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
      with dicom_store_mock.MockDicomStores(mock_dicomweb_url):
        dicom_client = dicom_store_client.DicomStoreClient(mock_dicomweb_url)
        with mock.patch.object(
            uid_generator, 'generate_uid', autospec=True, return_value='1.2.3'
        ):
          with self.assertRaises(
              metadata_storage_client.MetadataNotFoundExceptionError
          ):
            ingest.get_slide_dicom_json_formatted_metadata(
                sop_class_name,
                slide_id,
                dicom_client=dicom_client,
            )

  @parameterized.named_parameters(
      [('oof_enabled', True, {2, 32}), ('oof_disabled', False, set())]
  )
  @flagsaver.flagsaver(metadata_bucket='test_bucket')
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
  @flagsaver.flagsaver(metadata_bucket='test_bucket')
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

      result = ingest_base._correct_missing_study_instance_uid_in_metadata(
          test_metadata,
          _mock_abstract_dicom_gen_handler(
              mock_dicomweb_url,
              self.enter_context(contextlib.ExitStack()),
          ),
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

      with self.assertRaises(ingest_base.GenDicomFailedError):
        ingest_base._correct_missing_study_instance_uid_in_metadata(
            test_metadata,
            _mock_abstract_dicom_gen_handler(
                mock_dicomweb_url,
                self.enter_context(contextlib.ExitStack()),
            ),
        )

  @parameterized.named_parameters([
      dict(
          testcase_name='missing_study_uid_create_disabled',
          store_metadata=[_dcm_json('1.2', '1.2.1', '1.2.1.1', 'A1')],
          test_metadata=_dcm_json(accession='A1'),
          enable_create_missing_study_instance_uid=False,
          error_msg=ingest_const.ErrorMsgs.MISSING_STUDY_UID,
      ),
      dict(
          testcase_name='missing_acccession_number',
          store_metadata=[
              _dcm_json('1.2', '1.2.1', '1.2.1.1', 'A1', 'P1'),
          ],
          test_metadata=_dcm_json(patient_id='p1'),
          enable_create_missing_study_instance_uid=True,
          error_msg=ingest_const.ErrorMsgs.MISSING_ACCESSION_NUMBER_UNABLE_TO_CREATE_STUDY_INSTANCE_UID,
      ),
  ])
  def test_correct_missing_study_instance_uid_raises_if_cannot_create(
      self,
      store_metadata,
      test_metadata,
      enable_create_missing_study_instance_uid,
      error_msg,
  ):
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url) as mk_store:
      for dicom_metadata in store_metadata:
        mk_store[mock_dicomweb_url].add_instance(
            _create_pydicom_file_dataset_from_json(dicom_metadata)
        )
      with flagsaver.flagsaver(
          enable_create_missing_study_instance_uid=enable_create_missing_study_instance_uid
      ):
        with self.assertRaisesRegex(ingest_base.GenDicomFailedError, error_msg):
          ingest_base._correct_missing_study_instance_uid_in_metadata(
              test_metadata,
              _mock_abstract_dicom_gen_handler(
                  mock_dicomweb_url,
                  self.enter_context(contextlib.ExitStack()),
              ),
          )

  @parameterized.parameters(
      [requests.HTTPError, dicom_store_client.StudyInstanceUIDSearchError]
  )
  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'study_instance_uid_search',
      autospec=True,
  )
  @flagsaver.flagsaver(enable_create_missing_study_instance_uid=True)
  def test_correct_missing_study_instance_uid_in_metadata_search_dicom_store_failure_raises(
      self, exp, mock_study_instance_uid_search
  ):
    mock_study_instance_uid_search.side_effect = exp
    with self.assertRaises(ingest_base.GenDicomFailedError):
      ingest_base._correct_missing_study_instance_uid_in_metadata(
          _dcm_json(accession='A1'),
          _mock_abstract_dicom_gen_handler(
              'https://mock.dicomstore.com/dicomWeb',
              self.enter_context(contextlib.ExitStack()),
          ),
      )

  @mock.patch.object(
      uid_generator, 'generate_uid', autospec=True, return_value='5.6.7'
  )
  @mock.patch.object(
      redis_client.RedisClient,
      'has_redis_client',
      autospec=True,
      return_value=True,
  )
  @mock.patch.object(
      redis_client,
      'redis_client',
      autospec=True,
  )
  @flagsaver.flagsaver(enable_create_missing_study_instance_uid=True)
  def test_correct_missing_study_instance_locks_if_study_instance_uid_gen(
      self, mk_redis_client, *unused_mocks
  ):
    mk_client = mock.create_autospec(redis_client.RedisClient, instance=True)
    mk_client.redis_ip = '1.2.3'
    mk_client.redis_port = '555'
    mk_redis_client.return_value = mk_client
    store_metadata = [_dcm_json('1.2', '1.2.1', '1.2.1.1', 'A3', 'p1')]
    test_metadata = _dcm_json(accession='A1')
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url) as mk_store:
      for dicom_metadata in store_metadata:
        mk_store[mock_dicomweb_url].add_instance(
            _create_pydicom_file_dataset_from_json(dicom_metadata)
        )
      handler = _MockAbstractDicomGenerationForTest(mock_dicomweb_url)
      handler._process_message_context_block = contextlib.ExitStack()
      ingest_base._correct_missing_study_instance_uid_in_metadata(
          test_metadata,
          handler,
      )
    mk_client.acquire_non_blocking_lock.assert_called_once_with(
        'DICOM_ACCESSION_NUMBER:A1',
        None,
        600,
        handler._process_message_context_block,
        mock.ANY,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='no_prior_dicom',
          dicom_patient_id='other',
          expected_dicom_json={
              '00100020': {'vr': 'LO', 'Value': ['foo']},
              '00400512': {'vr': 'LO', 'Value': ['foo']},
              '0020000D': {'vr': 'UI', 'Value': ['1.2.3']},
          },
          expected_csv={
              'ContainerIdentifier': 'foo',
              'PatientID': 'foo',
              'StudyInstanceUID': '1.2.3',
          },
      ),
      dict(
          testcase_name='store_has_prior_patient_id',
          dicom_patient_id='foo',
          expected_dicom_json={
              '00100020': {'vr': 'LO', 'Value': ['foo']},
              '00400512': {'vr': 'LO', 'Value': ['foo']},
              '0020000D': {'vr': 'UI', 'Value': ['1.2']},
          },
          expected_csv={
              'ContainerIdentifier': 'foo',
              'PatientID': 'foo',
              'StudyInstanceUID': '1.2',
          },
      ),
  ])
  @mock.patch.object(
      uid_generator, 'generate_uid', autospec=True, return_value='1.2.3'
  )
  @flagsaver.flagsaver(
      enable_metadata_free_ingestion=True,
  )
  def test_generate_metadata_free_slide_metadata(
      self,
      unused_mock,
      dicom_patient_id,
      expected_dicom_json,
      expected_csv,
  ):
    slide_id = 'foo'
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url) as mk_store:
      mk_store[mock_dicomweb_url].add_instance(
          _create_pydicom_file_dataset_from_json(
              _dcm_json('1.2', '1.2.1', '1.2.1.1', 'A1', dicom_patient_id)
          )
      )
      dicom_client = dicom_store_client.DicomStoreClient(mock_dicomweb_url)
      dcm_metadata = ingest_base.generate_metadata_free_slide_metadata(
          slide_id, dicom_client
      )
    metadata_row = dcm_metadata.metadata_table_row
    csv_style_metadata = {
        name: metadata_row.lookup_metadata_value(name)
        for name in metadata_row.column_names
    }
    self.assertEqual(dcm_metadata.dicom_json, expected_dicom_json)
    self.assertEqual(csv_style_metadata, expected_csv)

  def test_generate_empty_slide_metadata(self):
    dcm_metadata = ingest_base.generate_empty_slide_metadata()
    metadata_row = dcm_metadata.metadata_table_row
    csv_style_metadata = {
        name: metadata_row.lookup_metadata_value(name)
        for name in metadata_row.column_names
    }
    self.assertEqual(dcm_metadata.dicom_json, {})
    self.assertEqual(csv_style_metadata, {})

  def test_initialize_metadata_study_and_series_uids_for_dicom_triggered_ingestion(
      self,
  ):
    input_study_instance_uid = '1.2.3'
    input_series_instance_uid = '1.2.3.4'
    ds = pydicom.Dataset()
    ds.SOPClassUID = '4.5.6'
    ds.AccessionNumber = 'mock_accession'
    metadata = ds.to_json_dict()
    ingest_base.initialize_metadata_study_and_series_uids_for_dicom_triggered_ingestion(
        input_study_instance_uid,
        input_series_instance_uid,
        metadata,
        _mock_abstract_dicom_gen_handler(
            'https://mock.dicomstore.com/dicomWeb',
            self.enter_context(contextlib.ExitStack()),
        ),
        set_study_instance_uid_from_metadata=False,
        set_series_instance_uid_from_metadata=False,
    )
    self.assertEqual(
        dicom_json_util.get_study_instance_uid(metadata),
        input_study_instance_uid,
    )
    self.assertEqual(
        dicom_json_util.get_series_instance_uid(metadata),
        input_series_instance_uid,
    )
    self.assertNotIn('00080016', metadata)

  def test_initialize_metadata_study_and_series_uids_for_dicom_triggered_ingestion_from_def_metadata(
      self,
  ):
    ds = pydicom.Dataset()
    ds.SOPClassUID = '4.5.6'
    ds.StudyInstanceUID = '1.2.5'
    ds.SeriesInstanceUID = '1.2.3.5'
    metadata = ds.to_json_dict()
    ingest_base.initialize_metadata_study_and_series_uids_for_dicom_triggered_ingestion(
        '1.2.3',
        '1.2.3.4',
        metadata,
        mock.create_autospec(
            dicom_store_client.DicomStoreClient, instance=True
        ),
        set_study_instance_uid_from_metadata=True,
        set_series_instance_uid_from_metadata=True,
    )
    self.assertEqual(
        dicom_json_util.get_study_instance_uid(metadata),
        ds.StudyInstanceUID,
    )
    self.assertEqual(
        dicom_json_util.get_series_instance_uid(metadata),
        ds.SeriesInstanceUID,
    )
    self.assertNotIn('00080016', metadata)

  @parameterized.named_parameters([
      dict(testcase_name='series', init_series=True, init_study=False),
      dict(testcase_name='study', init_series=False, init_study=True),
  ])
  def test_initialize_metadata_study_and_series_uids_for_dicom_triggered_ingestion_missing_uid_raises(
      self, init_series, init_study
  ):
    ds = pydicom.Dataset()
    ds.SOPClassUID = '4.5.6'
    metadata = ds.to_json_dict()
    with self.assertRaises(ingest_base.GenDicomFailedError):
      ingest_base.initialize_metadata_study_and_series_uids_for_dicom_triggered_ingestion(
          '1.2.3',
          '1.2.3.4',
          metadata,
          mock.create_autospec(
              dicom_store_client.DicomStoreClient, instance=True
          ),
          set_study_instance_uid_from_metadata=init_study,
          set_series_instance_uid_from_metadata=init_series,
      )

  @mock.patch.object(
      uid_generator, 'generate_uid', autospec=True, return_value='9.9.9'
  )
  @flagsaver.flagsaver(enable_create_missing_study_instance_uid=True)
  def test_initialize_metadata_study_and_series_dicom_triggered_ingestion_from_metadata_study_uid_generated(
      self, *unused_mock
  ):
    ds = pydicom.Dataset()
    ds.SOPClassUID = '4.5.6'
    ds.AccessionNumber = 'mock_accession'
    metadata = ds.to_json_dict()
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url):
      ingest_base.initialize_metadata_study_and_series_uids_for_dicom_triggered_ingestion(
          '1.2.3',
          '1.2.3.4',
          metadata,
          _mock_abstract_dicom_gen_handler(
              mock_dicomweb_url, self.enter_context(contextlib.ExitStack())
          ),
          set_study_instance_uid_from_metadata=True,
          set_series_instance_uid_from_metadata=False,
      )
    self.assertEqual(
        dicom_json_util.get_study_instance_uid(metadata),
        '9.9.9',
    )
    self.assertEqual(
        dicom_json_util.get_series_instance_uid(metadata),
        '1.2.3.4',
    )
    self.assertNotIn('00080016', metadata)

  @mock.patch.object(
      uid_generator, 'generate_uid', autospec=True, return_value='9.9.9'
  )
  @flagsaver.flagsaver(enable_create_missing_study_instance_uid=True)
  def test_initialize_metadata_study_and_series_dicom_triggered_ingestion_missing_accesison_raises(
      self, *unused_mock
  ):
    ds = pydicom.Dataset()
    ds.SOPClassUID = '4.5.6'
    metadata = ds.to_json_dict()
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url):
      with self.assertRaisesRegex(
          ingest_base.GenDicomFailedError,
          ingest_const.ErrorMsgs.MISSING_ACCESSION_NUMBER_UNABLE_TO_CREATE_STUDY_INSTANCE_UID,
      ):
        ingest_base.initialize_metadata_study_and_series_uids_for_dicom_triggered_ingestion(
            '1.2.3',
            '1.2.3.4',
            metadata,
            _mock_abstract_dicom_gen_handler(
                mock_dicomweb_url, self.enter_context(contextlib.ExitStack())
            ),
            set_study_instance_uid_from_metadata=True,
            set_series_instance_uid_from_metadata=False,
        )

  @mock.patch.object(
      uid_generator, 'generate_uid', autospec=True, return_value='5.6.7'
  )
  def test_initialize_metadata_study_and_series_uids_for_non_dicom_image_triggered_ingestion(
      self, unused_mock
  ):
    study_uid = '1.2.3.4.5.6.7'
    ds = pydicom.Dataset()
    ds.StudyInstanceUID = study_uid
    metadata = ds.to_json_dict()
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    dicom_gen = abstract_dicom_generation.GeneratedDicomFiles(
        '/tmp.dcm', 'gs://tmp.dcm'
    )
    dicom_gen.hash = 'mock_hash'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url):
      rs = ingest_base.initialize_metadata_study_and_series_uids_for_non_dicom_image_triggered_ingestion(
          _mock_abstract_dicom_gen_handler(
              mock_dicomweb_url, self.enter_context(contextlib.ExitStack())
          ),
          dicom_gen,
          metadata,
          initialize_series_uid_from_metadata=False,
      )
    self.assertEqual(
        dicom_json_util.get_study_instance_uid(metadata), study_uid
    )
    self.assertEqual(dicom_json_util.get_series_instance_uid(metadata), '5.6.7')
    self.assertTrue(rs)

  def test_initialize_metadata_study_and_series_uids_for_non_dicom_image_triggered_ingestion_missing_study_uid_raises(
      self,
  ):
    ds = pydicom.Dataset()
    ds.AccessionNumber = 'A1'
    metadata = ds.to_json_dict()
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    dicom_gen = abstract_dicom_generation.GeneratedDicomFiles(
        '/tmp.dcm', 'gs://tmp.dcm'
    )
    dicom_gen.hash = 'mock_hash'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url):
      with self.assertRaises(ingest_base.GenDicomFailedError):
        ingest_base.initialize_metadata_study_and_series_uids_for_non_dicom_image_triggered_ingestion(
            _mock_abstract_dicom_gen_handler(
                mock_dicomweb_url, self.enter_context(contextlib.ExitStack())
            ),
            dicom_gen,
            metadata,
            initialize_series_uid_from_metadata=False,
        )

  @mock.patch.object(
      uid_generator, 'generate_uid', autospec=True, return_value='5.6.7'
  )
  @flagsaver.flagsaver(enable_create_missing_study_instance_uid=True)
  def test_initialize_metadata_study_and_series_uids_for_non_dicom_image_triggered_ingestion_generated_study_uid(
      self, unused_mock
  ):
    ds = pydicom.Dataset()
    ds.AccessionNumber = '123'
    metadata = ds.to_json_dict()
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    dicom_gen = abstract_dicom_generation.GeneratedDicomFiles(
        '/tmp.dcm', 'gs://tmp.dcm'
    )
    dicom_gen.hash = 'mock_hash'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url):
      rs = ingest_base.initialize_metadata_study_and_series_uids_for_non_dicom_image_triggered_ingestion(
          _mock_abstract_dicom_gen_handler(
              mock_dicomweb_url, self.enter_context(contextlib.ExitStack())
          ),
          dicom_gen,
          metadata,
          initialize_series_uid_from_metadata=False,
      )
    self.assertEqual(dicom_json_util.get_study_instance_uid(metadata), '5.6.7')
    self.assertEqual(dicom_json_util.get_series_instance_uid(metadata), '5.6.7')
    self.assertTrue(rs)

  @mock.patch.object(
      uid_generator, 'generate_uid', autospec=True, return_value='5.6.7'
  )
  @flagsaver.flagsaver(enable_create_missing_study_instance_uid=True)
  def test_initialize_metadata_study_and_series_uids_for_non_dicom_image_triggered_ingestion_gen_series_from_metadata(
      self, unused_mock
  ):
    ds = pydicom.Dataset()
    ds.AccessionNumber = '123'
    ds.SeriesInstanceUID = '1.2.3'
    metadata = ds.to_json_dict()
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    dicom_gen = abstract_dicom_generation.GeneratedDicomFiles(
        '/tmp.dcm', 'gs://tmp.dcm'
    )
    dicom_gen.hash = 'mock_hash'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url):
      rs = ingest_base.initialize_metadata_study_and_series_uids_for_non_dicom_image_triggered_ingestion(
          _mock_abstract_dicom_gen_handler(
              mock_dicomweb_url, self.enter_context(contextlib.ExitStack())
          ),
          dicom_gen,
          metadata,
          initialize_series_uid_from_metadata=True,
      )
    self.assertEqual(dicom_json_util.get_study_instance_uid(metadata), '5.6.7')
    self.assertEqual(dicom_json_util.get_series_instance_uid(metadata), '1.2.3')
    self.assertFalse(rs)

  @mock.patch.object(
      uid_generator, 'generate_uid', autospec=True, return_value='5.6.7'
  )
  @flagsaver.flagsaver(enable_create_missing_study_instance_uid=True)
  def test_initialize_metadata_study_and_series_uids_for_non_dicom_image_triggered_ingestion_gen_series_from_metadata_missing_raises(
      self, unused_mock
  ):
    ds = pydicom.Dataset()
    ds.AccessionNumber = '123'
    metadata = ds.to_json_dict()
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    dicom_gen = abstract_dicom_generation.GeneratedDicomFiles(
        '/tmp.dcm', 'gs://tmp.dcm'
    )
    dicom_gen.hash = 'mock_hash'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url):
      with self.assertRaises(ingest_base.GenDicomFailedError):
        ingest_base.initialize_metadata_study_and_series_uids_for_non_dicom_image_triggered_ingestion(
            _mock_abstract_dicom_gen_handler(
                mock_dicomweb_url, self.enter_context(contextlib.ExitStack())
            ),
            dicom_gen,
            metadata,
            initialize_series_uid_from_metadata=True,
        )

  @mock.patch.object(
      uid_generator, 'generate_uid', autospec=True, return_value='5.6.7'
  )
  def test_initialize_metadata_study_and_series_uids_for_non_dicom_image_triggered_ingestion_init_series_from_hash(
      self, unused_mock
  ):
    study_instance_uid = '2.3.4'
    series_instance_uid = '1.2.3'
    hash_value = 'mock_hash'
    path = os.path.join(self.create_tempdir(), 'mk.dcm')
    ds = pydicom.Dataset()
    ds.StudyInstanceUID = study_instance_uid
    ds.SeriesInstanceUID = series_instance_uid
    ds.SOPInstanceUID = '5.6.7'
    tag = dicom_private_tag_generator.DicomPrivateTag(
        ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG, 'LT', hash_value
    )
    dicom_private_tag_generator.DicomPrivateTagGenerator.add_dicom_private_tags(
        [tag], ds
    )
    gen_test_util.write_test_dicom(path, ds)

    ds = pydicom.Dataset()
    ds.StudyInstanceUID = study_instance_uid
    metadata = ds.to_json_dict()
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    dicom_gen = abstract_dicom_generation.GeneratedDicomFiles(
        '/tmp.dcm', 'gs://tmp.dcm'
    )
    dicom_gen.hash = hash_value
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url) as mk_store:
      mk_store[mock_dicomweb_url].add_instance(path)
      rs = ingest_base.initialize_metadata_study_and_series_uids_for_non_dicom_image_triggered_ingestion(
          _mock_abstract_dicom_gen_handler(
              mock_dicomweb_url, self.enter_context(contextlib.ExitStack())
          ),
          dicom_gen,
          metadata,
          initialize_series_uid_from_metadata=True,
      )
      self.assertEqual(
          dicom_json_util.get_study_instance_uid(metadata), study_instance_uid
      )
      self.assertEqual(
          dicom_json_util.get_series_instance_uid(metadata), series_instance_uid
      )
      self.assertFalse(rs)


if __name__ == '__main__':
  absltest.main()
