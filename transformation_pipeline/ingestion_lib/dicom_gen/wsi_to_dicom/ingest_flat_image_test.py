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
"""Tests for ingest_flat_image."""

from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
import pydicom

from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import decode_slideid
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_flat_image
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client


_TEST_PATH_JPG = 'google.jpg'
_TEST_PATH_GIF = 'google.gif'
_METADATA_PATH = 'metadata.csv'
_SLIDE_COORDINATES_SCHEMA_FILENAME = 'example_schema_slide_coordinates.json'
_MICROSCOPIC_IMAGE_SCHEMA_FILENAME = 'example_schema_microscopic_image.json'

_SLIDE_ID = 'MD-03-2-A1-1'
_STUDY_UID = '1.2.840.5555.184555.9488327844440923'
_SERIES_UID = '1.2.3.4.5.6'
_INSTANCE_UID = '1.2.3.4.5.6.7'
_MESSAGE_ID = 'message_id'


class IngestFlatImageTest(absltest.TestCase):
  """Tests for flat image ingestion."""

  @flagsaver.flagsaver(metadata_bucket='test')
  def _create_metadata_client(
      self, schema_filename=_SLIDE_COORDINATES_SCHEMA_FILENAME
  ):
    schema_filename = gen_test_util.test_file_path(schema_filename)
    metadata_path = gen_test_util.test_file_path(_METADATA_PATH)
    metadata_client = metadata_storage_client.MetadataStorageClient()
    metadata_client.set_debug_metadata([metadata_path, schema_filename])
    return metadata_client

  def _create_flat_image_ingest(
      self, ingest_buckets=ingest_base.GcsIngestionBuckets('foo', 'bar')
  ):
    return ingest_flat_image.IngestFlatImage(ingest_buckets)

  def setUp(self):
    super().setUp()
    self.mock_slideid = self.enter_context(
        mock.patch.object(
            decode_slideid,
            'get_slide_id_from_filename',
            return_value=_SLIDE_ID,
            autospec=True,
        )
    )
    self.mock_uids = self.enter_context(
        mock.patch.object(
            uid_generator,
            'generate_uid',
            side_effect=[_SERIES_UID, _INSTANCE_UID],
            autospec=True,
        )
    )

    self.mock_dicom_handler = self.enter_context(
        mock.patch.object(
            abstract_dicom_generation, 'AbstractDicomGeneration', autospec=True
        )
    )
    mock_dcm_client = self.enter_context(
        mock.patch.object(dicom_store_client, 'DicomStoreClient', autospec=True)
    )
    self.mock_dicom_handler.dcm_store_client = mock_dcm_client
    series_uid = dicom_store_client._SeriesUidAndDicomExistingInStore(
        series_instance_uid=None, preexisting_dicoms_in_store=[]
    )
    mock_dcm_client.get_series_uid_and_existing_dicom_for_study_and_hash.return_value = (
        series_uid
    )

  @flagsaver.flagsaver(flat_images_vl_microscopic_image_iod=True)
  def test_generate_dicom_microscopic_image_succeeds(self):
    ingest = self._create_flat_image_ingest()
    ingest._metadata_storage_client = self._create_metadata_client(
        schema_filename=_MICROSCOPIC_IMAGE_SCHEMA_FILENAME
    )
    image_path = gen_test_util.test_file_path(_TEST_PATH_JPG)
    gen_dicom_result = ingest.generate_dicom(
        dicom_gen_dir=self.create_tempdir().full_path,
        dicom_gen=abstract_dicom_generation.GeneratedDicomFiles(
            image_path, None
        ),
        message_id=_MESSAGE_ID,
        abstract_dicom_handler=self.mock_dicom_handler,
    )

    self.assertLen(gen_dicom_result.files_to_upload.main_store_instances, 1)
    self.assertTrue(gen_dicom_result.generated_series_instance_uid)
    dcm = pydicom.dcmread(
        list(gen_dicom_result.files_to_upload.main_store_instances)[0]
    )
    self.assertEqual(dcm.StudyInstanceUID, _STUDY_UID)
    self.assertEqual(dcm.SeriesInstanceUID, _SERIES_UID)
    self.assertEqual(dcm.SOPInstanceUID, _INSTANCE_UID)
    self.assertEqual(dcm.PatientName, 'Curie^Marie')
    self.assertEqual(
        dcm.SOPClassUID, ingest_const.DicomSopClasses.MICROSCOPIC_IMAGE.uid
    )
    self.assertIsNotNone(dcm.PixelData)

  def test_generate_dicom_slide_coordinates_image_succeeds(self):
    ingest = self._create_flat_image_ingest()
    ingest._metadata_storage_client = self._create_metadata_client()
    image_path = gen_test_util.test_file_path(_TEST_PATH_JPG)
    gen_dicom_result = ingest.generate_dicom(
        dicom_gen_dir=self.create_tempdir().full_path,
        dicom_gen=abstract_dicom_generation.GeneratedDicomFiles(
            image_path, None
        ),
        message_id=_MESSAGE_ID,
        abstract_dicom_handler=self.mock_dicom_handler,
    )

    self.assertLen(gen_dicom_result.files_to_upload.main_store_instances, 1)
    self.assertTrue(gen_dicom_result.generated_series_instance_uid)
    dcm = pydicom.dcmread(
        list(gen_dicom_result.files_to_upload.main_store_instances)[0]
    )
    self.assertEqual(dcm.StudyInstanceUID, _STUDY_UID)
    self.assertEqual(dcm.SeriesInstanceUID, _SERIES_UID)
    self.assertEqual(dcm.SOPInstanceUID, _INSTANCE_UID)
    self.assertEqual(dcm.PatientName, 'Curie^Marie')
    self.assertEqual(
        dcm.SOPClassUID,
        ingest_const.DicomSopClasses.SLIDE_COORDINATES_IMAGE.uid,
    )
    self.assertIsNotNone(dcm.PixelData)

  def test_generate_dicom_fails_invalid_format(self):
    ingest = self._create_flat_image_ingest(
        ingest_base.GcsIngestionBuckets(
            'gs://success-bucket', 'gs://failure-bucket'
        )
    )
    ingest._metadata_storage_client = self._create_metadata_client()
    image_path = gen_test_util.test_file_path(_TEST_PATH_GIF)
    dcm_result = ingest.generate_dicom(
        dicom_gen_dir=self.create_tempdir().full_path,
        dicom_gen=abstract_dicom_generation.GeneratedDicomFiles(
            image_path, None
        ),
        message_id=_MESSAGE_ID,
        abstract_dicom_handler=mock.Mock(),
    )
    self.assertEqual(
        dcm_result.dest_uri, 'gs://failure-bucket/flat_image_unexpected_format'
    )

  def test_generate_dicom_fails_missing_slide_id(self):
    self.mock_slideid.side_effect = decode_slideid.SlideIdIdentificationError(
        'slideid_not_found', decode_slideid._SlideIdErrorLevel.BASE_ERROR_LEVEL
    )
    ingest = self._create_flat_image_ingest(
        ingest_base.GcsIngestionBuckets(
            'gs://success-bucket', 'gs://failure-bucket'
        )
    )
    ingest._metadata_storage_client = self._create_metadata_client()
    image_path = gen_test_util.test_file_path(_TEST_PATH_JPG)
    mock_dicom_handler = mock.Mock()
    dcm_result = ingest.generate_dicom(
        dicom_gen_dir=self.create_tempdir().full_path,
        dicom_gen=abstract_dicom_generation.GeneratedDicomFiles(
            image_path, None
        ),
        message_id=_MESSAGE_ID,
        abstract_dicom_handler=mock_dicom_handler,
    )
    self.assertEqual(
        dcm_result.dest_uri, 'gs://failure-bucket/slideid_not_found'
    )


if __name__ == '__main__':
  absltest.main()
