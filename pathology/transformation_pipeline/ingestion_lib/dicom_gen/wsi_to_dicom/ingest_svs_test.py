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
"""Unit tests for IngestSVS."""

from __future__ import annotations

import contextlib
import copy
import datetime
import json
import math
import os
import shutil
import subprocess
import time
import typing
from typing import Any, Dict, List, Set
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import pydicom
import requests

from shared_libs.logging_lib import cloud_logging_client
from shared_libs.test_utils.dicom_store_mock import dicom_store_mock
from shared_libs.test_utils.gcs_mock import gcs_mock
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_general_equipment
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_image_extractor
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import barcode_reader
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_gcs_handler
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_svs
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client
from transformation_pipeline.ingestion_lib.dicom_util import dicom_test_util


_METADATA_PATH = 'metadata.csv'
_SCHEMA_PATH = 'example_schema_slide_coordinates.json'
_SLIDE_ID_1 = 'MD-03-2-A1-1'
_SLIDE_ID_2 = 'MD-03-2-A1-2'
_TEST_JSON_METADATA = {
    '00100010': {'Value': [{'Alphabetic': 'test'}], 'vr': 'PN'}
}
_MOCK_DICOM_GEN_UID = '1.2.3.9'


class _IngestOpenslideTest(contextlib.ExitStack):

  def __init__(
      self,
      test_instance: parameterized.TestCase,
      input_file_path: str,
      ingest_filename: str,
      schema='example_schema_wsi.json',
      metadata='metadata.csv',
      dicom_store_url: str = 'https://mock.dicom.store.com/dicomWeb',
  ):
    super().__init__()
    self._test_instance = test_instance
    self._dicom_store_url = dicom_store_url
    self._input_bucket_path = self._test_instance.create_tempdir()
    self._filename = ingest_filename
    self._input_file_container_path = os.path.join(
        self._test_instance.create_tempdir(), self._filename
    )
    shutil.copyfile(
        input_file_path, os.path.join(self._input_bucket_path, self._filename)
    )
    shutil.copyfile(input_file_path, self._input_file_container_path)
    metadata_path = self._test_instance.create_tempdir()
    shutil.copyfile(
        gen_test_util.test_file_path(schema),
        os.path.join(metadata_path, schema),
    )
    if metadata:
      shutil.copyfile(
          gen_test_util.test_file_path(metadata),
          os.path.join(metadata_path, metadata),
      )
    self.enter_context(mock.patch.object(subprocess, 'run', autospec=True))
    self.enter_context(
        gcs_mock.GcsMock({
            'input': self._input_bucket_path,
            'output': self._test_instance.create_tempdir(),
            'metadata': metadata_path,
        })
    )
    self.enter_context(
        flagsaver.flagsaver(
            gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.METADATA
        )
    )
    self.enter_context(dicom_store_mock.MockDicomStores(self._dicom_store_url))
    self.dicom_gen = abstract_dicom_generation.GeneratedDicomFiles(
        self._input_file_container_path, f'gs://input/{ self._filename}'
    )
    self.handler: ingest_gcs_handler.IngestGcsPubSubHandler = None

  def __enter__(self) -> _IngestOpenslideTest:
    super().__enter__()
    self.handler = ingest_gcs_handler.IngestGcsPubSubHandler(
        'gs://output/success',
        'gs://output/failure',
        self._dicom_store_url,
        frozenset(),
        metadata_client=metadata_storage_client.MetadataStorageClient(),
        oof_trigger_config=None,
    )
    self.handler.root_working_dir = (
        self._test_instance.create_tempdir().full_path
    )
    os.mkdir(self.handler.img_dir)
    return self


@flagsaver.flagsaver(metadata_bucket='metadata')
def _ingest_svs_generate_dicom_test_shim(
    test_instance: parameterized.TestCase,
    source_image_path: str,
    mock_dicom: pydicom.FileDataset,
    downsampled_factors: List[int],
    slide_id: str = 'MD-04-3-A1-2',
) -> ingest_base.GenDicomResult:
  filename = os.path.basename(source_image_path)
  with _IngestOpenslideTest(
      test_instance,
      source_image_path,
      f'{slide_id}_{filename}',
      metadata='metadata.csv',
  ) as ingest_test:
    ingest = ingest_svs.IngestSVS(
        ingest_test.handler._ingest_buckets,
        is_oof_ingestion_enabled=False,
        metadata_client=metadata_storage_client.MetadataStorageClient(),
    )
    ingest.init_handler_for_ingestion()
    ingest.update_metadata()
    ingest.get_slide_id(ingest_test.dicom_gen, ingest_test.handler)
    dicom_gen_dir = os.path.join(
        ingest_test.handler.root_working_dir, 'gen_dicom'
    )
    os.mkdir(dicom_gen_dir)
    downsampled_dicom = copy.deepcopy(mock_dicom)
    for factor in downsampled_factors:
      output_path = os.path.join(dicom_gen_dir, f'downsample-{factor}-foo.dcm')
      if factor > 1:
        # Mock downsampling by just changing metadata.
        # actual pixels are not downsampled.
        height = int(mock_dicom.TotalPixelMatrixRows / factor)
        width = int(mock_dicom.TotalPixelMatrixColumns / factor)
        number_of_frames = int(
            math.ceil(height / mock_dicom.Rows)
            * math.ceil(width / mock_dicom.Columns)
        )
        downsampled_dicom.TotalPixelMatrixRows = height
        downsampled_dicom.TotalPixelMatrixColumns = width
        downsampled_dicom.NumberOfFrames = number_of_frames
      downsampled_dicom.save_as(output_path)
    return ingest.generate_dicom(
        dicom_gen_dir,
        ingest_test.dicom_gen,
        'mock_pubsub_msg_id',
        ingest_test.handler,
    )


def _get_instances_json(dicom_paths: Set[str]) -> List[Dict[str, Any]]:
  sorted_dicoms = sorted(
      [pydicom.dcmread(path) for path in dicom_paths],
      key=lambda x: ''.join(x.ImageType),
  )
  for dicom in sorted_dicoms:
    del dicom['SOPInstanceUID']
    del dicom['StudyInstanceUID']
    del dicom['SeriesInstanceUID']
    del dicom['PixelData']
    try:
      del dicom['30211001']
    except KeyError:
      pass
  return [dicom.to_json_dict() for dicom in sorted_dicoms]


class IngestSVSTest(parameterized.TestCase):
  """Tests IngestSVS."""

  def test_add_burned_in_annotation_and_spec_label_already_defined(self):
    dcm = pydicom.Dataset()
    dcm.BurnedInAnnotation = 'YES'
    dcm.SpecimenLabelInImage = 'YES'
    (
        ingest_svs._add_burned_in_annotation_and_specimen_label_in_image_if_not_def(
            dcm
        )
    )
    self.assertEqual(dcm.BurnedInAnnotation, 'YES')
    self.assertEqual(dcm.SpecimenLabelInImage, 'YES')

  def test_add_burned_in_annotation_and_spec_label_not_defined(self):
    dcm = pydicom.Dataset()
    (
        ingest_svs._add_burned_in_annotation_and_specimen_label_in_image_if_not_def(
            dcm
        )
    )
    self.assertEqual(dcm.BurnedInAnnotation, 'NO')
    self.assertEqual(dcm.SpecimenLabelInImage, 'NO')

  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_add_metadata_to_wsi_dicom_files_no_files(self):
    dcm_gen = abstract_dicom_generation.GeneratedDicomFiles('', None)
    dcm_gen.generated_dicom_files = []
    svs_metadata = {}
    scan_datetime = datetime.datetime.now(datetime.timezone.utc)
    ac_date = datetime.datetime.strftime(scan_datetime, '%Y%m%d')
    svs_metadata['AcquisitionDate'] = pydicom.DataElement(
        '00080022', 'DA', ac_date
    )
    additional_metadata = pydicom.Dataset()
    additional_metadata.FrameOfReferenceUID = '1.2.3'
    additional_metadata.PositionReferenceIndicator = 'Side'

    self.assertFalse(
        ingest_svs.add_metadata_to_wsi_dicom_files(
            dcm_gen,
            [],
            additional_metadata,
            svs_metadata,
            _TEST_JSON_METADATA,
        )
    )

  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_add_metadata_to_wsi_dicom_files(self, unused_mock):
    out_dir = self.create_tempdir()
    dcm = typing.cast(
        pydicom.FileDataset,
        dicom_test_util.create_test_dicom_instance(
            dcm_json=dicom_test_util.create_metadata_dict()
        ),
    )
    dcm_file_path = os.path.join(out_dir, 'test.dcm')
    dcm.ImageType = 'test_dicom'
    dcm.save_as(dcm_file_path, write_like_original=False)

    dcm_gen = abstract_dicom_generation.GeneratedDicomFiles(
        gen_test_util.test_file_path('ndpi_test.ndpi'), None
    )
    dcm_gen.generated_dicom_files = [dcm_file_path]
    svs_metadata = {}
    scan_datetime = datetime.datetime.now(datetime.timezone.utc)
    ac_date = datetime.datetime.strftime(scan_datetime, '%Y%m%d')
    svs_metadata['AcquisitionDate'] = pydicom.DataElement(
        '00080022', 'DA', ac_date
    )
    dt = datetime.datetime(
        2022, 5, 3, 3, 34, 24, 884733, tzinfo=datetime.timezone.utc
    )
    additional_metadata = pydicom.Dataset()
    additional_metadata.FrameOfReferenceUID = '1.2.3'
    additional_metadata.PositionReferenceIndicator = 'Side'
    save_func = datetime.datetime.strftime
    with mock.patch('datetime.datetime') as mk:
      mk.now = mock.Mock(return_value=dt)
      mk.strftime = save_func
      ingest_svs.add_metadata_to_wsi_dicom_files(
          dcm_gen,
          [],
          additional_metadata,
          svs_metadata,
          _TEST_JSON_METADATA,
      )
    dcm = pydicom.dcmread(dcm_file_path)
    self.assertTrue(
        dcm.SOPInstanceUID.startswith(uid_generator.TEST_UID_PREFIX)
    )
    self.assertEqual(dcm.ContentDate, '20220503')
    # content time component is derived current time in dicom_util.
    # Dicom_util converts time to UTC formatted DICOM string.
    self.assertEqual(dcm.ContentTime, '033424.884733')
    self.assertEqual(dcm.PatientName, 'test')
    self.assertEqual(dcm.Manufacturer, 'GOOGLE')
    self.assertEqual(dcm.ManufacturerModelName, 'DPAS_transformation_pipeline')
    self.assertEqual(
        dcm.SoftwareVersions,
        cloud_logging_client.get_build_version(
            dicom_general_equipment._MAX_STRING_LENGTH_DICOM_SOFTWARE_VERSION_TAG
        ),
    )
    self.assertEqual(dcm.FrameOfReferenceUID, '1.2.3')
    self.assertEqual(dcm.PositionReferenceIndicator, 'Side')
    self.assertEqual(dcm.AcquisitionDate, ac_date)
    self.assertEqual(dcm.BurnedInAnnotation, 'NO')
    self.assertEqual(dcm.SpecimenLabelInImage, 'NO')
    self.assertEqual(str(dcm.OpticalPathSequence[0].ObjectiveLensPower), '20')
    self.assertEqual(
        str(
            dcm.TotalPixelMatrixOriginSequence[0].XOffsetInSlideCoordinateSystem
        ),
        '0.0',
    )
    self.assertEqual(
        str(
            dcm.TotalPixelMatrixOriginSequence[0].YOffsetInSlideCoordinateSystem
        ),
        '0.0',
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
  def test_get_slide_id_success(
      self, filename, candidate_barcode_values, expected
  ):
    metadata_client = metadata_storage_client.MetadataStorageClient()
    ingest_base_test_driver = ingest_svs.IngestSVS(None, False, metadata_client)  # pytype: disable=wrong-arg-types
    metadata_client.set_debug_metadata([
        gen_test_util.test_file_path(_METADATA_PATH),
        gen_test_util.test_file_path(_SCHEMA_PATH),
    ])
    with mock.patch.object(
        barcode_reader,
        'read_barcode_in_files',
        autospec=True,
        return_value={
            bar_code: {'unused'} for bar_code in candidate_barcode_values
        },
    ):
      with mock.patch.object(
          ancillary_image_extractor,
          'get_ancillary_images_from_svs',
          autospec=True,
          return_value=[
              ancillary_image_extractor.AncillaryImage('foo.png', 'RGB', True)
          ]
          * len(candidate_barcode_values),
      ):
        handler = mock.create_autospec(
            ingest_gcs_handler.IngestGcsPubSubHandler, instance=True
        )
        handler.img_dir = '/foo'
        self.assertEqual(
            ingest_base_test_driver.get_slide_id(
                abstract_dicom_generation.GeneratedDicomFiles(
                    filename, f'gs://bar/{filename}'
                ),
                handler,
            ),
            expected,
        )

  @flagsaver.flagsaver(metadata_bucket='test')
  def test_get_slide_id_fails_to_find_slide_id_raises(self):
    metadata_client = metadata_storage_client.MetadataStorageClient()
    ingest_base_test_driver = ingest_svs.IngestSVS(
        ingest_base.GcsIngestionBuckets('gs://success', 'gs://failure'),
        False,
        metadata_client,
    )  # pytype: disable=wrong-arg-types
    metadata_client.set_debug_metadata([
        gen_test_util.test_file_path(_METADATA_PATH),
        gen_test_util.test_file_path(_SCHEMA_PATH),
    ])
    with mock.patch.object(
        barcode_reader, 'read_barcode_in_files', autospec=True, return_value={}
    ):
      dest_uri = ''
      try:
        with mock.patch.object(
            ancillary_image_extractor,
            'get_ancillary_images_from_svs',
            autospec=True,
            return_value=[],
        ):
          handler = mock.create_autospec(
              ingest_gcs_handler.IngestGcsPubSubHandler, instance=True
          )
          handler.img_dir = '/foo'
          ingest_base_test_driver.get_slide_id(
              abstract_dicom_generation.GeneratedDicomFiles(
                  'foo.svs', 'gs://bar/foo.svs'
              ),
              handler,
          )
      except ingest_base.DetermineSlideIDError as exp:
        dest_uri = exp._dest_uri
      self.assertEqual(
          dest_uri,
          'gs://failure/slide_id_error__filename_missing_slide_metadata_primary_key',
      )

  @flagsaver.flagsaver(
      metadata_bucket='test', enable_metadata_free_ingestion=True
  )
  def test_get_slide_id_triggers_metadata_free_get_slide_id(self):
    metadata_client = metadata_storage_client.MetadataStorageClient()
    ingest_base_test_driver = ingest_svs.IngestSVS(
        ingest_base.GcsIngestionBuckets('gs://success', 'gs://failure'),
        False,
        metadata_client,
    )  # pytype: disable=wrong-arg-types
    metadata_client.set_debug_metadata([
        gen_test_util.test_file_path(_METADATA_PATH),
        gen_test_util.test_file_path(_SCHEMA_PATH),
    ])
    with mock.patch.object(
        barcode_reader, 'read_barcode_in_files', autospec=True, return_value={}
    ):
      with mock.patch.object(
          ancillary_image_extractor,
          'get_ancillary_images_from_svs',
          autospec=True,
          return_value=[],
      ):
        handler = mock.create_autospec(
            ingest_gcs_handler.IngestGcsPubSubHandler, instance=True
        )
        handler.img_dir = '/foo'
        self.assertEqual(
            ingest_base_test_driver.get_slide_id(
                abstract_dicom_generation.GeneratedDicomFiles(
                    'foo.svs', 'gs://bar/foo.svs'
                ),
                handler,
            ),
            'foo',
        )

  @parameterized.named_parameters([
      dict(
          testcase_name='full_metadata',
          slide_id='MD-04-3-A1-2',
          flags={},
          expected_output='openslide_dicom_json/full_metadata.json',
      ),
      dict(
          testcase_name='metadata_free',
          slide_id='not_found',
          flags=dict(enable_metadata_free_ingestion=True),
          expected_output='openslide_dicom_json/metadata_free.json',
      ),
  ])
  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  @mock.patch.object(
      dicom_util,
      '_get_srgb_iccprofile',
      autospec=True,
      return_value='SRGB_ICCPROFILE_BYTES'.encode('utf-8'),
  )
  @mock.patch.object(
      cloud_logging_client,
      'get_build_version',
      autospec=True,
      return_value='Build_Version:123',
  )
  @mock.patch.object(
      dicom_util,
      '_dicom_formatted_date',
      autospec=True,
      return_value='20240312',
  )
  @mock.patch.object(
      dicom_util, '_dicom_formatted_time', autospec=True, return_value='012345'
  )
  @mock.patch.object(dicom_util, 'set_content_date_time_to_now', autospec=True)
  @mock.patch.object(
      time, 'time', autospec=True, return_value=1705894937.3804379
  )
  @mock.patch.object(
      uid_generator,
      'generate_uid',
      autospec=True,
      return_value=_MOCK_DICOM_GEN_UID,
  )
  def test_generate_dicom_full_metadata(
      self, *unused_mocks, slide_id, flags, expected_output
  ):
    path = gen_test_util.test_file_path('ndpi_test.ndpi')
    mock_dicom = pydicom.dcmread(
        gen_test_util.test_file_path('test_wikipedia.dcm')
    )
    with flagsaver.flagsaver(**flags):
      result = _ingest_svs_generate_dicom_test_shim(
          self, path, mock_dicom, [1], slide_id=slide_id
      )
    self.assertLen(result.files_to_upload.main_store_instances, 2)
    self.assertTrue(result.generated_series_instance_uid)
    self.assertEqual(result.dest_uri, 'gs://output/success')
    gen_instance = _get_instances_json(
        result.files_to_upload.main_store_instances
    )
    with open(gen_test_util.test_file_path(expected_output), 'rt') as infile:
      expected_instances = json.load(infile)
    self.assertLen(gen_instance, len(expected_instances))
    for index, expected_instance in enumerate(expected_instances):
      self.assertEqual(gen_instance[index], expected_instance)

  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  @mock.patch.object(
      uid_generator,
      'generate_uid',
      autospec=True,
      return_value=_MOCK_DICOM_GEN_UID,
  )
  @flagsaver.flagsaver(enable_create_missing_study_instance_uid=True)
  def test_generate_dicom_missing_study_instance_uid(self, *unused_mocks):
    slide_id = 'MD-05-3-A1-2'
    path = gen_test_util.test_file_path('ndpi_test.ndpi')
    mock_dicom = pydicom.dcmread(
        gen_test_util.test_file_path('test_wikipedia.dcm')
    )
    result = _ingest_svs_generate_dicom_test_shim(
        self, path, mock_dicom, [1], slide_id=slide_id
    )
    self.assertLen(result.files_to_upload.main_store_instances, 2)
    self.assertTrue(result.generated_series_instance_uid)
    self.assertEqual(result.dest_uri, 'gs://output/success')
    study_instance_uid = {
        pydicom.dcmread(pth).StudyInstanceUID
        for pth in result.files_to_upload.main_store_instances
    }
    series_instance_uid = {
        pydicom.dcmread(pth).SeriesInstanceUID
        for pth in result.files_to_upload.main_store_instances
    }
    self.assertEqual(study_instance_uid, {_MOCK_DICOM_GEN_UID})
    self.assertEqual(series_instance_uid, {_MOCK_DICOM_GEN_UID})

  @parameterized.named_parameters([
      dict(
          testcase_name='missing_study_uid',
          slide_id='MD-05-3-A1-2',
          flags=dict(enable_create_missing_study_instance_uid=True),
          dest='gs://output/failure/error_occurred_querying_dicom_store_unable_to_create_study_instance_uid',
      ),
      dict(
          testcase_name='metadata_free',
          slide_id='not_found',
          flags=dict(enable_metadata_free_ingestion=True),
          dest='gs://output/failure/error_querying_dicom_store',
      ),
  ])
  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'study_instance_uid_search',
      autospec=True,
      side_effect=requests.HTTPError,
  )
  @flagsaver.flagsaver(enable_metadata_free_ingestion=True)
  def test_failure_to_search_dicom_store_for_study_uid(
      self,
      *unused_mocks,
      slide_id,
      flags,
      dest,
  ):
    path = gen_test_util.test_file_path('ndpi_test.ndpi')
    mock_dicom = pydicom.dcmread(
        gen_test_util.test_file_path('test_wikipedia.dcm')
    )
    with flagsaver.flagsaver(**flags):
      result = _ingest_svs_generate_dicom_test_shim(
          self, path, mock_dicom, [1], slide_id=slide_id
      )
    self.assertEmpty(result.files_to_upload.main_store_instances)
    self.assertTrue(result.generated_series_instance_uid)
    self.assertEqual(result.dest_uri, dest)

  def test_generate_dicom_errors_missing_study_instance_uid(
      self,
      *unused_mocks,
  ):
    path = gen_test_util.test_file_path('ndpi_test.ndpi')
    mock_dicom = pydicom.dcmread(
        gen_test_util.test_file_path('test_wikipedia.dcm')
    )
    result = _ingest_svs_generate_dicom_test_shim(
        self, path, mock_dicom, [1], slide_id='MD-05-3-A1-2'
    )
    self.assertEmpty(result.files_to_upload.main_store_instances)
    self.assertTrue(result.generated_series_instance_uid)
    self.assertEqual(
        result.dest_uri,
        'gs://output/failure/missing_study_instance_uid_in_metadata',
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='full_metadata',
          slide_id='MD-06-3-A1-2',
          flags={},
          expected_study_uid={'1.2.840.5555.184555.9588327844440923'},
          expected_series_uid={'1.2.3.4.5'},
      ),
      dict(
          testcase_name='missing_study_uid',
          slide_id='MD-07-3-A1-2',
          flags=dict(enable_create_missing_study_instance_uid=True),
          expected_study_uid={_MOCK_DICOM_GEN_UID},
          expected_series_uid={'1.2.3.4.5'},
      ),
  ])
  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  @mock.patch.object(
      uid_generator,
      'generate_uid',
      autospec=True,
      return_value=_MOCK_DICOM_GEN_UID,
  )
  @flagsaver.flagsaver(init_series_instance_uid_from_metadata=True)
  def test_generate_dicom_metadata_defined_series_uid(
      self,
      *unused_mocks,
      slide_id,
      flags,
      expected_study_uid,
      expected_series_uid,
  ):
    path = gen_test_util.test_file_path('ndpi_test.ndpi')
    mock_dicom = pydicom.dcmread(
        gen_test_util.test_file_path('test_wikipedia.dcm')
    )
    with flagsaver.flagsaver(**flags):
      result = _ingest_svs_generate_dicom_test_shim(
          self, path, mock_dicom, [1], slide_id=slide_id
      )
    self.assertLen(result.files_to_upload.main_store_instances, 2)
    self.assertFalse(result.generated_series_instance_uid)
    self.assertEqual(result.dest_uri, 'gs://output/success')
    study_instance_uid = {
        pydicom.dcmread(pth).StudyInstanceUID
        for pth in result.files_to_upload.main_store_instances
    }
    series_instance_uid = {
        pydicom.dcmread(pth).SeriesInstanceUID
        for pth in result.files_to_upload.main_store_instances
    }
    self.assertEqual(study_instance_uid, expected_study_uid)
    self.assertEqual(series_instance_uid, expected_series_uid)

  @parameterized.named_parameters([
      dict(
          testcase_name='missing_just_series_metadata',
          slide_id='MD-04-3-A1-2',
          flags={},
      ),
      dict(
          testcase_name='missing_study_and_series_uid',
          slide_id='MD-05-3-A1-2',
          flags=dict(enable_create_missing_study_instance_uid=True),
      ),
  ])
  @flagsaver.flagsaver(init_series_instance_uid_from_metadata=True)
  def test_generate_dicom_metadata_defined_series_uid_errors_metadata(
      self, *unused_mocks, slide_id, flags
  ):
    path = gen_test_util.test_file_path('ndpi_test.ndpi')
    mock_dicom = pydicom.dcmread(
        gen_test_util.test_file_path('test_wikipedia.dcm')
    )
    with flagsaver.flagsaver(**flags):
      result = _ingest_svs_generate_dicom_test_shim(
          self, path, mock_dicom, [1], slide_id=slide_id
      )
    self.assertEmpty(result.files_to_upload.main_store_instances, 2)
    self.assertTrue(result.generated_series_instance_uid)
    self.assertEqual(
        result.dest_uri,
        'gs://output/failure/missing_series_instance_uid_in_metadata',
    )

  def test_generate_dicom_error(
      self,
      *unused_mocks,
  ):
    path = gen_test_util.test_file_path('ndpi_test.ndpi')
    mock_dicom = pydicom.dcmread(
        gen_test_util.test_file_path('test_wikipedia.dcm')
    )
    with self.assertRaises(ingest_base.DetermineSlideIDError):
      _ingest_svs_generate_dicom_test_shim(
          self, path, mock_dicom, [1], slide_id='not_found'
      )

  @flagsaver.flagsaver(metadata_bucket='metadata')
  def test_generate_dicom_raises_if_slide_id_not_set(self):
    with _IngestOpenslideTest(
        self,
        gen_test_util.test_file_path('ndpi_test.ndpi'),
        'ingest_filename',
        metadata='metadata.csv',
    ) as ingest_test:
      ingest = ingest_svs.IngestSVS(
          ingest_test.handler._ingest_buckets,
          is_oof_ingestion_enabled=False,
          metadata_client=metadata_storage_client.MetadataStorageClient(),
      )
      ingest.init_handler_for_ingestion()
      with self.assertRaises(ValueError):
        ingest.generate_dicom(
            'mock_dicom_gen_dir',
            ingest_test.dicom_gen,
            'mock_pubsub_msg_id',
            ingest_test.handler,
        )

  @flagsaver.flagsaver(metadata_bucket='metadata')
  def test_generate_dicom_generates_bad_dicom_errors(self):
    with _IngestOpenslideTest(
        self,
        gen_test_util.test_file_path('ndpi_test.ndpi'),
        'MD-04-3-A1-2_ndpi_test.ndpi',
        metadata='metadata.csv',
    ) as ingest_test:
      ingest = ingest_svs.IngestSVS(
          ingest_test.handler._ingest_buckets,
          is_oof_ingestion_enabled=False,
          metadata_client=metadata_storage_client.MetadataStorageClient(),
      )
      ingest.init_handler_for_ingestion()
      ingest.update_metadata()
      ingest.get_slide_id(ingest_test.dicom_gen, ingest_test.handler)
      dicom_gen_dir = os.path.join(
          ingest_test.handler.root_working_dir, 'gen_dicom'
      )
      os.mkdir(dicom_gen_dir)
      with open(
          os.path.join(dicom_gen_dir, 'downsample-1-foo.dcm'), 'wb'
      ) as output:
        output.write(b'1234')
      result = ingest.generate_dicom(
          dicom_gen_dir,
          ingest_test.dicom_gen,
          'mock_pubsub_msg_id',
          ingest_test.handler,
      )
    self.assertEmpty(result.files_to_upload.main_store_instances)
    self.assertTrue(result.generated_series_instance_uid)
    self.assertEqual(
        result.dest_uri,
        'gs://output/failure/wsi_to_dicom_invalid_dicom_generated',
    )

  @flagsaver.flagsaver(metadata_bucket='test')
  @mock.patch.object(
      uid_generator,
      'generate_uid',
      autospec=True,
      return_value=_MOCK_DICOM_GEN_UID,
  )
  def test_generate_metadata_free_slide_metadata(self, *unused_mocks):
    dicom_store_url = 'https://mock.dicom.store.com/dicomWeb'
    with _IngestOpenslideTest(
        self,
        gen_test_util.test_file_path('ndpi_test.ndpi'),
        'MD-04-3-A1-2_ndpi_test.ndpi',
        metadata='metadata.csv',
        dicom_store_url=dicom_store_url,
    ) as ingest_test:
      ingest = ingest_svs.IngestSVS(
          ingest_test.handler._ingest_buckets,
          is_oof_ingestion_enabled=False,
          metadata_client=metadata_storage_client.MetadataStorageClient(),
      )
      dicom_client = dicom_store_client.DicomStoreClient(dicom_store_url)
      result = ingest._generate_metadata_free_slide_metadata(
          'mock_slide_id', dicom_client
      )
    self.assertEqual(
        result.dicom_json,
        {
            '00100020': {'vr': 'LO', 'Value': ['mock_slide_id']},
            '0020000D': {'vr': 'UI', 'Value': [_MOCK_DICOM_GEN_UID]},
            '00400512': {'vr': 'LO', 'Value': ['mock_slide_id']},
        },
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='both_factors_true',
          init_series_from_metadata=True,
          is_metadata_free=True,
          expected=False,
      ),
      dict(
          testcase_name='metadata_free',
          init_series_from_metadata=False,
          is_metadata_free=True,
          expected=False,
      ),
      dict(
          testcase_name='all_factors_false',
          init_series_from_metadata=False,
          is_metadata_free=False,
          expected=False,
      ),
      dict(
          testcase_name='init_series_from_metadata',
          init_series_from_metadata=True,
          is_metadata_free=False,
          expected=True,
      ),
  ])
  @flagsaver.flagsaver(metadata_bucket='test')
  def test_init_series_instance_uid_from_metadata(
      self, is_metadata_free, init_series_from_metadata, expected
  ):
    with _IngestOpenslideTest(
        self,
        gen_test_util.test_file_path('ndpi_test.ndpi'),
        'MD-04-3-A1-2_ndpi_test.ndpi',
        metadata='metadata.csv',
    ) as ingest_test:
      ingest = ingest_svs.IngestSVS(
          ingest_test.handler._ingest_buckets,
          is_oof_ingestion_enabled=False,
          metadata_client=metadata_storage_client.MetadataStorageClient(),
      )
      ingest.set_slide_id('mock_slide_id', is_metadata_free)
      with flagsaver.flagsaver(
          init_series_instance_uid_from_metadata=init_series_from_metadata
      ):
        self.assertEqual(
            ingest._init_series_instance_uid_from_metadata(), expected
        )


if __name__ == '__main__':
  absltest.main()
