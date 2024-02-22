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
"""Tests for ingest_dicom."""

import os
from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import pydicom

from shared_libs.test_utils.dicom_store_mock import dicom_store_mock
from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import decode_slideid
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_dicom
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client
from transformation_pipeline.ingestion_lib.dicom_util import dicom_test_util


FLAGS = flags.FLAGS

_TEST_PATH_DCM = 'test_slide_coordinates.dcm'
_METADATA_PATH = 'metadata.csv'
_SCHEMA_PATH = 'example_schema_slide_coordinates.json'

_SLIDE_ID_1 = 'MD-03-2-A1-1'
_SLIDE_ID_2 = 'MD-03-2-A1-2'
_STUDY_UID = '1.2.840.5555.184555.9488327844440923'
_SERIES_UID = '1.2.3.4.5.6'
_INSTANCE_UID = '1.2.3.4.5.6.7'
_MESSAGE_ID = 'message_id'


class IngestDicomTest(parameterized.TestCase):
  """Tests for DICOM files ingestion."""

  def _create_metadata_client(self):
    schema_path = gen_test_util.test_file_path(_SCHEMA_PATH)
    metadata_path = gen_test_util.test_file_path(_METADATA_PATH)
    metadata_client = metadata_storage_client.MetadataStorageClient()
    metadata_client.set_debug_metadata([metadata_path, schema_path])
    return metadata_client

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(metadata_bucket='test'))
    self.metadata_client = self._create_metadata_client()

  def _get_mock_dicom_handler(
      self,
  ) -> abstract_dicom_generation.AbstractDicomGeneration:
    mock_dicom_handler = self.enter_context(
        mock.patch.object(
            abstract_dicom_generation, 'AbstractDicomGeneration', autospec=True
        )
    )
    mock_dcm_client = self.enter_context(
        mock.patch.object(dicom_store_client, 'DicomStoreClient', autospec=True)
    )
    mock_dicom_handler.dcm_store_client = mock_dcm_client
    series_uid = dicom_store_client._SeriesUidAndDicomExistingInStore(
        series_instance_uid=None, preexisting_dicoms_in_store=[]
    )
    mock_dcm_client.get_series_uid_and_existing_dicom_for_study_and_hash.return_value = (
        series_uid
    )
    return mock_dicom_handler

  def test_metadata_client_initialized(self):
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=False,
        override_study_uid_with_metadata=True,
        metadata_client=metadata_storage_client.MetadataStorageClient(),
    )
    self.assertIsNotNone(ingest.metadata_storage_client)

  @parameterized.named_parameters([
      dict(
          testcase_name='_filename_preceeds_barcode',
          barcode=_SLIDE_ID_2,
          filename=_SLIDE_ID_1,
          slideid=_SLIDE_ID_1,
      ),
      dict(
          testcase_name='filename_not_found_determine_in_barcode',
          barcode=_SLIDE_ID_2,
          filename='not_found',
          slideid=_SLIDE_ID_2,
      ),
  ])
  def test_get_slide_id_succeeds(self, barcode, filename, slideid):
    dcm = pydicom.Dataset()
    dcm.BarcodeValue = barcode
    path = os.path.join(self.create_tempdir(), f'{filename}.dcm')
    gen_test_util.write_test_dicom(path, dcm)
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=False,
        override_study_uid_with_metadata=True,
        metadata_client=self.metadata_client,
    )
    self.assertEqual(
        ingest._determine_dicom_slideid(
            dcm,
            abstract_dicom_generation.GeneratedDicomFiles(
                f'path/{filename}.dcm', f'gs://foo/{filename}.dcm'
            ),
        ),
        slideid,
    )

  def test_get_slide_id_no_barcode_read_from_filename(self):
    path = os.path.join(self.create_tempdir(), f'{_SLIDE_ID_2}.dcm')
    gen_test_util.write_test_dicom(path, pydicom.Dataset())
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=False,
        override_study_uid_with_metadata=True,
        metadata_client=self.metadata_client,
    )
    self.assertEqual(
        ingest.get_slide_id(
            abstract_dicom_generation.GeneratedDicomFiles(
                path, f'gs://foo/{_SLIDE_ID_2}.dcm'
            ),
            mock.Mock(),
        ),
        _SLIDE_ID_2,
    )

  @flagsaver.flagsaver(enable_metadata_free_ingestion=True)
  def test_get_slide_id_metadata_free_returns_filename_based_slideid(
      self,
  ):
    filename = 'foo'
    path = os.path.join(self.create_tempdir(), f'{filename}.dcm')
    gen_test_util.write_test_dicom(path, pydicom.Dataset())
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=False,
        override_study_uid_with_metadata=True,
        metadata_client=self.metadata_client,
    )
    slide_id = ingest.get_slide_id(
        abstract_dicom_generation.GeneratedDicomFiles(
            path, f'gs://foo/{filename}.dcm'
        ),
        mock.Mock(),
    )
    self.assertEqual(slide_id, filename)

  @flagsaver.flagsaver(enable_metadata_free_ingestion=True)
  def test_get_slide_id_metadata_free_called_from_store_trigger_raises(
      self,
  ):
    filename = 'foo'
    path = os.path.join(self.create_tempdir(), f'{filename}.dcm')
    gen_test_util.write_test_dicom(path, pydicom.Dataset())
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=True,
        override_study_uid_with_metadata=True,
        metadata_client=self.metadata_client,
    )
    with self.assertRaises(ingest_base.DetermineSlideIDError):
      ingest.get_slide_id(
          abstract_dicom_generation.GeneratedDicomFiles(
              path, f'gs://foo/{filename}.dcm'
          ),
          mock.Mock(),
      )

  @parameterized.named_parameters([
      dict(
          testcase_name='do_not_read_barcode_from_filename',
          dicom_store_triggered_ingest=True,
          filename=_SLIDE_ID_2,
      ),
      dict(
          testcase_name='can_not_determine_from_filename',
          dicom_store_triggered_ingest=False,
          filename='path',
      ),
  ])
  def test_determine_dicom_slideid_raises(
      self, dicom_store_triggered_ingest, filename
  ):
    path = os.path.join(self.create_tempdir(), f'{filename}.dcm')
    gen_test_util.write_test_dicom(path, pydicom.Dataset())
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=dicom_store_triggered_ingest,
        override_study_uid_with_metadata=True,
        metadata_client=self.metadata_client,
    )
    with self.assertRaises(ingest_base.DetermineSlideIDError):
      ingest.get_slide_id(
          abstract_dicom_generation.GeneratedDicomFiles(
              path, f'gs://foo/{filename}.dcm'
          ),
          mock.Mock(),
      )

  def test_is_dicom_file_already_ingested_invalid_path(self):
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=False,
        override_study_uid_with_metadata=True,
        metadata_client=metadata_storage_client.MetadataStorageClient(),
    )
    self.assertFalse(ingest.is_dicom_file_already_ingested('invalid/path'))

  @parameterized.parameters(
      {'dcm_json': {}},
      {
          'dcm_json': {
              '00080005': {
                  ingest_const.VR: ingest_const.DICOMVRCodes.CS,
                  ingest_const.VALUE: ['ISO_IR 192'],
              }
          }
      },
  )
  def test_is_dicom_file_already_ingested_false(self, dcm_json):
    dcm_path = dicom_test_util.create_test_dicom_instance(
        self.create_tempdir(), dcm_json=dcm_json
    )
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=False,
        override_study_uid_with_metadata=True,
        metadata_client=metadata_storage_client.MetadataStorageClient(),
    )
    self.assertFalse(ingest.is_dicom_file_already_ingested(dcm_path))

  @parameterized.named_parameters([
      dict(
          testcase_name='google_private_creator_block',
          dcm_json={
              ingest_const.DICOMTagAddress.DICOM_GOOGLE_PRIVATE_CREATOR_BLOCK_TAG: {
                  ingest_const.VR: ingest_const.DICOMVRCodes.LO,
                  ingest_const.VALUE: ['GOOGLE'],
              }
          },
      ),
      dict(
          testcase_name='pubsub_message_id_tag',
          dcm_json={
              ingest_const.DICOMTagAddress.PUBSUB_MESSAGE_ID_TAG: {
                  ingest_const.VR: ingest_const.DICOMVRCodes.LT,
                  ingest_const.VALUE: ['message_id'],
              }
          },
      ),
      dict(
          testcase_name='ingest_filename_tag',
          dcm_json={
              ingest_const.DICOMTagAddress.INGEST_FILENAME_TAG: {
                  ingest_const.VR: ingest_const.DICOMVRCodes.LT,
                  ingest_const.VALUE: ['google.jpg'],
              }
          },
      ),
      dict(
          testcase_name='multiple_tags_including_one_dpas_created_tag',
          dcm_json={
              '00080005': {
                  ingest_const.VR: ingest_const.DICOMVRCodes.CS,
                  ingest_const.VALUE: ['ISO_IR 192'],
              },
              ingest_const.DICOMTagAddress.INGEST_FILENAME_TAG: {
                  ingest_const.VR: ingest_const.DICOMVRCodes.LT,
                  ingest_const.VALUE: ['google.jpg'],
              },
          },
      ),
  ])
  def test_is_dicom_file_already_ingested_private_tags_true(self, dcm_json):
    dcm_path = dicom_test_util.create_test_dicom_instance(
        self.create_tempdir(), dcm_json=dcm_json
    )
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=False,
        override_study_uid_with_metadata=True,
        metadata_client=metadata_storage_client.MetadataStorageClient(),
    )
    self.assertTrue(ingest.is_dicom_file_already_ingested(dcm_path))

  @mock.patch.object(
      decode_slideid,
      'get_slide_id_from_filename',
      return_value=_SLIDE_ID_1,
      autospec=True,
  )
  def test_is_dicom_file_already_ingested_gen_dicom_true(self, _):
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=False,
        override_study_uid_with_metadata=True,
        metadata_client=self.metadata_client,
    )
    dcm_path = gen_test_util.test_file_path(_TEST_PATH_DCM)
    dicom_gen = abstract_dicom_generation.GeneratedDicomFiles(dcm_path, None)
    handler = self._get_mock_dicom_handler()
    ingest.get_slide_id(dicom_gen, handler)
    gen_dicom_result = ingest.generate_dicom(
        dicom_gen_dir=FLAGS.test_tmpdir,
        dicom_gen=dicom_gen,
        message_id=_MESSAGE_ID,
        abstract_dicom_handler=handler,
    )
    dcm_path = gen_dicom_result.dicom_gen.generated_dicom_files[0]
    self.assertTrue(ingest.is_dicom_file_already_ingested(dcm_path))

  def test_generate_dicom_fails_unable_to_read_dicom(self):
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=False,
        override_study_uid_with_metadata=True,
        metadata_client=self.metadata_client,
        ingest_buckets=ingest_base.GcsIngestionBuckets(
            success_uri='gs://foo', failure_uri='gs://bar'
        ),
    )
    dcm_path = os.path.join(self.create_tempdir(), 'non-existent-path.dcm')
    dicom_gen = abstract_dicom_generation.GeneratedDicomFiles(dcm_path, None)
    dest_uri = ''
    try:
      ingest.get_slide_id(dicom_gen, mock.Mock())
    except ingest_base.DetermineSlideIDError as exp:
      dest_uri = exp.dest_uri
    self.assertEqual(dest_uri, 'gs://bar/invalid_dicom')

  @mock.patch.object(
      decode_slideid,
      'get_slide_id_from_filename',
      return_value=_SLIDE_ID_1,
      autospec=True,
  )
  def test_generate_dicom_succeeds_overrides_study_uid_ingest_from_gcs(self, _):
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=False,
        override_study_uid_with_metadata=True,
        metadata_client=self.metadata_client,
    )
    dcm_path = gen_test_util.test_file_path(
        gen_test_util.test_file_path(_TEST_PATH_DCM)
    )
    dicom_gen = abstract_dicom_generation.GeneratedDicomFiles(dcm_path, None)
    handler = self._get_mock_dicom_handler()
    ingest.get_slide_id(dicom_gen, handler)
    gen_dicom_result = ingest.generate_dicom(
        dicom_gen_dir=FLAGS.test_tmpdir,
        dicom_gen=dicom_gen,
        message_id=_MESSAGE_ID,
        abstract_dicom_handler=handler,
    )

    self.assertLen(gen_dicom_result.files_to_upload.main_store_instances, 1)
    self.assertFalse(gen_dicom_result.generated_series_instance_uid)
    dcm = pydicom.dcmread(
        list(gen_dicom_result.files_to_upload.main_store_instances)[0]
    )
    self.assertEqual(dcm.StudyInstanceUID, _STUDY_UID)
    self.assertEqual(dcm.SeriesInstanceUID, _SERIES_UID)
    self.assertEqual(dcm.SOPInstanceUID, _INSTANCE_UID)
    self.assertEqual(dcm.BarcodeValue, _SLIDE_ID_1)
    self.assertEqual(dcm.PatientName, 'Curie^Marie')
    self.assertIn(ingest_const.DICOMTagKeywords.INGEST_FILENAME_TAG, dcm)
    self.assertIsNotNone(dcm.PixelData)

  @parameterized.parameters([True, False])
  @mock.patch.object(
      decode_slideid,
      'get_slide_id_from_filename',
      return_value=_SLIDE_ID_1,
      autospec=True,
  )
  def test_generate_dicom_succeeds_overrides_study_uid_ingest_from_dicom_store(
      self, dicom_store_triggered_ingest, _
  ):
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=dicom_store_triggered_ingest,
        override_study_uid_with_metadata=True,
        metadata_client=self.metadata_client,
    )
    temp_dicom_path = os.path.join(self.create_tempdir(), 'tmp_dcm.dcm')
    with pydicom.dcmread(gen_test_util.test_file_path(_TEST_PATH_DCM)) as dcm:
      dcm.BarcodeValue = _SLIDE_ID_1
      dcm.save_as(temp_dicom_path)
    dcm_path = gen_test_util.test_file_path(temp_dicom_path)
    dicom_gen = abstract_dicom_generation.GeneratedDicomFiles(dcm_path, None)
    handler = self._get_mock_dicom_handler()
    ingest.get_slide_id(dicom_gen, handler)
    gen_dicom_result = ingest.generate_dicom(
        dicom_gen_dir=FLAGS.test_tmpdir,
        dicom_gen=dicom_gen,
        message_id=_MESSAGE_ID,
        abstract_dicom_handler=handler,
    )

    self.assertLen(gen_dicom_result.files_to_upload.main_store_instances, 1)
    self.assertFalse(gen_dicom_result.generated_series_instance_uid)
    dcm = pydicom.dcmread(
        list(gen_dicom_result.files_to_upload.main_store_instances)[0]
    )
    self.assertEqual(
        dcm.StudyInstanceUID == _STUDY_UID, not dicom_store_triggered_ingest
    )
    self.assertEqual(dcm.SeriesInstanceUID, _SERIES_UID)
    self.assertEqual(dcm.SOPInstanceUID, _INSTANCE_UID)
    self.assertEqual(dcm.BarcodeValue, _SLIDE_ID_1)
    self.assertEqual(dcm.PatientName, 'Curie^Marie')
    self.assertEqual(
        ingest_const.DICOMTagKeywords.INGEST_FILENAME_TAG in dcm,
        not dicom_store_triggered_ingest,
    )
    self.assertIsNotNone(dcm.PixelData)

  @mock.patch.object(
      decode_slideid,
      'get_slide_id_from_filename',
      return_value=_SLIDE_ID_1,
      autospec=True,
  )
  def test_generate_dicom_succeeds_does_not_override_study_uid(self, _):
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=False,
        override_study_uid_with_metadata=False,
        metadata_client=self.metadata_client,
    )
    dcm_path = gen_test_util.test_file_path(_TEST_PATH_DCM)
    dicom_gen = abstract_dicom_generation.GeneratedDicomFiles(dcm_path, None)
    handler = self._get_mock_dicom_handler()
    ingest.get_slide_id(dicom_gen, handler)
    gen_dicom_result = ingest.generate_dicom(
        dicom_gen_dir=FLAGS.test_tmpdir,
        dicom_gen=abstract_dicom_generation.GeneratedDicomFiles(dcm_path, None),
        message_id=_MESSAGE_ID,
        abstract_dicom_handler=handler,
    )

    self.assertLen(gen_dicom_result.files_to_upload.main_store_instances, 1)
    self.assertFalse(gen_dicom_result.generated_series_instance_uid)
    dcm = pydicom.dcmread(
        list(gen_dicom_result.files_to_upload.main_store_instances)[0]
    )
    self.assertEqual(dcm.StudyInstanceUID, '1.2.3.4.5')
    self.assertEqual(dcm.SeriesInstanceUID, _SERIES_UID)
    self.assertEqual(dcm.SOPInstanceUID, _INSTANCE_UID)
    self.assertEqual(dcm.BarcodeValue, _SLIDE_ID_1)
    self.assertEqual(dcm.PatientName, 'Curie^Marie')
    self.assertIsNotNone(dcm.PixelData)

  @parameterized.named_parameters(
      dict(
          testcase_name='google_private_creator_block',
          dcm_json={
              ingest_const.DICOMTagAddress.DICOM_GOOGLE_PRIVATE_CREATOR_BLOCK_TAG: {
                  ingest_const.VR: ingest_const.DICOMVRCodes.LO,
                  ingest_const.VALUE: ['GOOGLE'],
              }
          },
          expected_result=True,
      ),
      dict(
          testcase_name='pubsub_message_id_tag',
          dcm_json={
              ingest_const.DICOMTagAddress.PUBSUB_MESSAGE_ID_TAG: {
                  ingest_const.VR: ingest_const.DICOMVRCodes.LT,
                  ingest_const.VALUE: ['message_id'],
              }
          },
          expected_result=True,
      ),
      dict(
          testcase_name='ingest_filename_tag',
          dcm_json={
              ingest_const.DICOMTagAddress.INGEST_FILENAME_TAG: {
                  ingest_const.VR: ingest_const.DICOMVRCodes.LT,
                  ingest_const.VALUE: ['google.jpg'],
              }
          },
          expected_result=True,
      ),
      dict(
          testcase_name='multiple_tags_including_one_dpas_created_tag',
          dcm_json={
              '00080005': {
                  ingest_const.VR: ingest_const.DICOMVRCodes.CS,
                  ingest_const.VALUE: ['ISO_IR 192'],
              },
              ingest_const.DICOMTagAddress.INGEST_FILENAME_TAG: {
                  ingest_const.VR: ingest_const.DICOMVRCodes.LT,
                  ingest_const.VALUE: ['google.jpg'],
              },
          },
          expected_result=True,
      ),
      dict(
          testcase_name='no_dpas_created_tags',
          dcm_json={},
          expected_result=False,
      ),
  )
  def test_is_dicom_instance_already_ingested(self, dcm_json, expected_result):
    dcm_path = dicom_test_util.create_test_dicom_instance(
        self.create_tempdir(), dcm_json=dcm_json
    )
    with pydicom.dcmread(dcm_path) as dcm:
      study_instance_uid = dcm.StudyInstanceUID
      series_instance_uid = dcm.SeriesInstanceUID
      sop_instance_uid = dcm.SOPInstanceUID
    dcm_store = (
        'projects/proj/locations/loc/datasets/dat/dicomStores/store/dicomWeb'
    )
    dicom_store_url = f'https://healthcare.googleapis.com/v1/{dcm_store}'
    with dicom_store_mock.MockDicomStores(dicom_store_url) as mock_store:
      mock_store[dicom_store_url].add_instance(dcm_path)
      client = dicom_store_client.DicomStoreClient(dicom_store_url)
      ingest = ingest_dicom.IngestDicom(
          dicom_store_triggered_ingest=False,
          override_study_uid_with_metadata=True,
          metadata_client=metadata_storage_client.MetadataStorageClient(),
      )
      self.assertEqual(
          ingest.is_dicom_instance_already_ingested(
              client, study_instance_uid, series_instance_uid, sop_instance_uid
          ),
          expected_result,
      )

  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'get_instance_tags_json',
      autospec=True,
      return_value=[{}, {}],
  )
  def test_is_dicom_instance_already_ingested_for_invalid_dicom_response(
      self, _
  ):
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=False,
        override_study_uid_with_metadata=True,
        metadata_client=metadata_storage_client.MetadataStorageClient(),
    )
    with self.assertRaises(ingest_dicom.UnexpectedDicomMetadataError):
      ingest.is_dicom_instance_already_ingested(
          dicom_store_client.DicomStoreClient(
              'https://healthcare.googleapis.com/v1/projects/proj/locations/'
              'loc/datasets/dat/dicomStores/store/dicomWeb'
          ),
          '1',
          '1.2',
          '1.2.3',
      )

  def test_uninitialized_slide_id_raises_value_error(self):
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=False,
        override_study_uid_with_metadata=True,
        metadata_client=metadata_storage_client.MetadataStorageClient(),
    )
    ingest.init_handler_for_ingestion()
    handler = self._get_mock_dicom_handler()
    with self.assertRaises(ValueError):
      ingest.generate_dicom(
          '',
          abstract_dicom_generation.GeneratedDicomFiles('filename', 'uri'),
          '',
          handler,
      )

  def test_generate_metadata_free_slide_metadata(self):
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=False,
        override_study_uid_with_metadata=True,
        metadata_client=metadata_storage_client.MetadataStorageClient(),
    )
    ingest.init_handler_for_ingestion()
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    with dicom_store_mock.MockDicomStoreClient(mock_dicomweb_url):
      dicom_client = dicom_store_client.DicomStoreClient(mock_dicomweb_url)
      result = ingest._generate_metadata_free_slide_metadata(
          'mock_slide_id', dicom_client
      )
    self.assertEmpty(result.dicom_json)

  @parameterized.named_parameters([
      dict(
          testcase_name='All_factors_true',
          override_study_uid_with_metadata=True,
          dicom_store_triggered_ingest=True,
          is_metadata_free=True,
          expected=False,
      ),
      dict(
          testcase_name='metadata_free',
          override_study_uid_with_metadata=False,
          is_metadata_free=True,
          dicom_store_triggered_ingest=False,
          expected=False,
      ),
      dict(
          testcase_name='dicom_store_triggered_ingest',
          override_study_uid_with_metadata=False,
          is_metadata_free=False,
          dicom_store_triggered_ingest=True,
          expected=False,
      ),
      dict(
          testcase_name='dicom_store_triggered_ingest_and_metadata_free',
          override_study_uid_with_metadata=False,
          is_metadata_free=True,
          dicom_store_triggered_ingest=True,
          expected=False,
      ),
      dict(
          testcase_name='init_series_from_metadata',
          override_study_uid_with_metadata=True,
          is_metadata_free=False,
          dicom_store_triggered_ingest=False,
          expected=True,
      ),
      dict(
          testcase_name='all_factors_false',
          override_study_uid_with_metadata=False,
          is_metadata_free=False,
          dicom_store_triggered_ingest=False,
          expected=False,
      ),
  ])
  def test_set_study_instance_uid_from_metadata(
      self,
      override_study_uid_with_metadata,
      is_metadata_free,
      dicom_store_triggered_ingest,
      expected,
  ) -> bool:
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=dicom_store_triggered_ingest,
        override_study_uid_with_metadata=override_study_uid_with_metadata,
        metadata_client=metadata_storage_client.MetadataStorageClient(),
    )
    ingest.set_slide_id('mock_slide_id', is_metadata_free)
    self.assertEqual(ingest._set_study_instance_uid_from_metadata(), expected)

  @parameterized.named_parameters([
      dict(
          testcase_name='All_factors_true',
          init_series_from_metadata=True,
          dicom_store_triggered_ingest=True,
          is_metadata_free=True,
          expected=False,
      ),
      dict(
          testcase_name='metadata_free',
          init_series_from_metadata=False,
          is_metadata_free=True,
          dicom_store_triggered_ingest=False,
          expected=False,
      ),
      dict(
          testcase_name='dicom_store_triggered_ingest',
          init_series_from_metadata=False,
          is_metadata_free=False,
          dicom_store_triggered_ingest=True,
          expected=False,
      ),
      dict(
          testcase_name='dicom_store_triggered_ingest_and_metadata_free',
          init_series_from_metadata=False,
          is_metadata_free=True,
          dicom_store_triggered_ingest=True,
          expected=False,
      ),
      dict(
          testcase_name='init_series_from_metadata',
          init_series_from_metadata=True,
          is_metadata_free=False,
          dicom_store_triggered_ingest=False,
          expected=True,
      ),
      dict(
          testcase_name='all_factors_false',
          init_series_from_metadata=False,
          is_metadata_free=False,
          dicom_store_triggered_ingest=False,
          expected=False,
      ),
  ])
  def test_set_series_instance_uid_from_metadata(
      self,
      init_series_from_metadata,
      is_metadata_free,
      dicom_store_triggered_ingest,
      expected,
  ) -> bool:
    ingest = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=dicom_store_triggered_ingest,
        override_study_uid_with_metadata=False,
        metadata_client=metadata_storage_client.MetadataStorageClient(),
    )
    ingest.set_slide_id('mock_slide_id', is_metadata_free)
    with flagsaver.flagsaver(
        init_series_instance_uid_from_metadata=init_series_from_metadata
    ):
      self.assertEqual(
          ingest._set_series_instance_uid_from_metadata(), expected
      )


if __name__ == '__main__':
  absltest.main()
