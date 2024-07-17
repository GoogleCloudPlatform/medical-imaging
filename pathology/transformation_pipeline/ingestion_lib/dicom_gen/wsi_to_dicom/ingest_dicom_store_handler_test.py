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
"""Tests for ingest_dicom_store_handler."""

import http
import itertools
import os
import shutil
from typing import Any, Mapping, MutableMapping, Optional
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
from google.cloud import pubsub_v1
import google.cloud.storage
import pydicom
import requests

from shared_libs.test_utils.dicom_store_mock import dicom_store_mock
from shared_libs.test_utils.gcs_mock import gcs_mock
from transformation_pipeline.ingestion_lib import cloud_storage_client
from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib import polling_client
from transformation_pipeline.ingestion_lib import redis_client
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen import wsi_dicom_file_ref
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_dicom
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_dicom_store_handler
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client


_RECOVERY_GCS_URI = 'gs://dcm-recover-bucket/'
_VIEWER_DEBUG_URL = 'https://debug_url'
_DCM_UPLOAD_WEBPATH = 'https://healthcare.googleapis.com/v1/projects/proj/locations/loc/datasets/dat/dicomStores/store/dicomWeb'
_SLIDE_ID_1 = 'MD-03-2-A1-1'


@flagsaver.flagsaver(dicom_guid_prefix=uid_generator.TEST_UID_PREFIX)
@flagsaver.flagsaver(dicom_store_ingest_gcs_uri=_RECOVERY_GCS_URI)
def _create_ingestion_handler() -> (
    ingest_dicom_store_handler.IngestDicomStorePubSubHandler
):
  return ingest_dicom_store_handler.IngestDicomStorePubSubHandler(
      metadata_storage_client.MetadataStorageClient()
  )


def _create_gen_dicom_result(dicom_files=None) -> ingest_base.GenDicomResult:
  if dicom_files is None:
    dicom_files = ['path/to/updated.dcm']
  return ingest_base.GenDicomResult(
      dicom_gen=abstract_dicom_generation.GeneratedDicomFiles(
          localfile='path/to/original.dcm', source_uri='dicom_store_uri'
      ),
      dest_uri='',
      files_to_upload=ingest_base.DicomInstanceIngestionSets(dicom_files),
      generated_series_instance_uid=False,
  )


def _create_test_dicom_instance_metadata(
    study_instance_uid: str,
    series_instanceuid: str,
    sop_instance_uid: str,
    metadata: Optional[Mapping[str, Any]] = None,
) -> MutableMapping[str, Any]:
  metadata = dict(metadata) if metadata is not None else {}
  metadata[ingest_const.DICOMTagAddress.SOP_CLASS_UID] = {
      ingest_const.VALUE: [ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid],
      ingest_const.VR: ingest_const.DICOMVRCodes.UI,
  }
  metadata[ingest_const.DICOMTagAddress.STUDY_INSTANCE_UID] = {
      ingest_const.VALUE: [study_instance_uid],
      ingest_const.VR: ingest_const.DICOMVRCodes.UI,
  }
  metadata[ingest_const.DICOMTagAddress.SERIES_INSTANCE_UID] = {
      ingest_const.VALUE: [series_instanceuid],
      ingest_const.VR: ingest_const.DICOMVRCodes.UI,
  }
  metadata[ingest_const.DICOMTagAddress.SOP_INSTANCE_UID] = {
      ingest_const.VALUE: [sop_instance_uid],
      ingest_const.VR: ingest_const.DICOMVRCodes.UI,
  }
  return metadata


class IngestDicomStorePubSubHandlerTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    cloud_storage_client.reset_storage_client('')
    self.enter_context(flagsaver.flagsaver(metadata_bucket='test_bucket'))

  @flagsaver.flagsaver(dicom_guid_prefix=uid_generator.TEST_UID_PREFIX)
  def test_create_ingestion_handler_gcs_flag_missing(self):
    with self.assertRaises(ValueError):
      _ = ingest_dicom_store_handler.IngestDicomStorePubSubHandler(
          metadata_storage_client.MetadataStorageClient()
      )

  def test_decode_pubsub_msg_no_data(self):
    ingest = _create_ingestion_handler()
    pubsub_msg = pubsub_v1.types.ReceivedMessage(
        ack_id='5',
        message=pubsub_v1.types.PubsubMessage(data=None, message_id='test'),
        delivery_attempt=1,
    )
    msg = ingest.decode_pubsub_msg(pubsub_msg)
    self.assertTrue(msg.ignore)

  def test_decode_pubsub_msg_invalid_format(self):
    ingest = _create_ingestion_handler()
    dcm = 'projects/proj/.../studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5'
    pubsub_msg = pubsub_v1.types.ReceivedMessage(
        ack_id='5',
        message=pubsub_v1.types.PubsubMessage(
            data=dcm.encode('utf-8'), message_id='test'
        ),
        delivery_attempt=1,
    )
    msg = ingest.decode_pubsub_msg(pubsub_msg)
    self.assertTrue(msg.ignore)

  def test_decode_pubsub_msg_already_ingested(self):
    ingest = _create_ingestion_handler()
    dcm = f'projects/proj/locations/loc/datasets/dat/dicomStores/store/dicomWeb/studies/1.2.3/series/1.2.3.4/instances/{ingest_const.DPAS_UID_PREFIX}.123'
    pubsub_msg = pubsub_v1.types.ReceivedMessage(
        ack_id='5',
        message=pubsub_v1.types.PubsubMessage(
            data=dcm.encode('utf-8'), message_id='test'
        ),
        delivery_attempt=1,
    )
    msg = ingest.decode_pubsub_msg(pubsub_msg)
    self.assertTrue(msg.ignore)

  @parameterized.named_parameters(
      dict(testcase_name='instance_not_in_store', mock_dicom_instances=[]),
      dict(
          testcase_name='untransformed_instance_in_store',
          mock_dicom_instances=[
              _create_test_dicom_instance_metadata(
                  '1.2.3', '1.2.3.4', '1.2.3.4.5'
              )
          ],
      ),
  )
  @flagsaver.flagsaver(viewer_debug_url=_VIEWER_DEBUG_URL)
  def test_decode_pubsub_msg_succeeds(self, mock_dicom_instances):
    study_uid = '1.2.3'
    series_uid = '1.2.3.4'
    instance_uid = '1.2.3.4.5'
    ingest = _create_ingestion_handler()
    dcm_store = (
        'projects/proj/locations/loc/datasets/dat/dicomStores/store/dicomWeb'
    )
    dcm = f'{dcm_store}/studies/{study_uid}/series/{series_uid}/instances/{instance_uid}'
    pubsub_msg = pubsub_v1.types.ReceivedMessage(
        ack_id='5',
        message=pubsub_v1.types.PubsubMessage(
            data=dcm.encode('utf-8'), message_id='test'
        ),
        delivery_attempt=1,
    )
    dicom_store_url = f'https://healthcare.googleapis.com/v1/{dcm_store}'
    with dicom_store_mock.MockDicomStores(dicom_store_url) as mock_store:
      for dcm_instance in mock_dicom_instances:
        mock_store[dicom_store_url].add_instance(dcm_instance)
      msg = ingest.decode_pubsub_msg(pubsub_msg)

    self.assertFalse(msg.ignore)
    self.assertEqual(
        ingest._current_instance.gcs_recovery_uri,
        f'gs://dcm-recover-bucket/{dcm}.dcm',
    )
    self.assertEqual(
        ingest._current_instance.viewer_debug_url, _VIEWER_DEBUG_URL
    )
    self.assertEqual(
        ingest._current_instance.dicom_store_client.dicomweb_path,
        f'https://healthcare.googleapis.com/v1/{dcm_store}',
    )
    self.assertEqual(ingest._viewer_debug_url, _VIEWER_DEBUG_URL)

  @parameterized.named_parameters([
      dict(
          testcase_name='json_reponse_error',
          http_status_code=http.HTTPStatus.OK,
          http_response='{a:1}',
      ),
      dict(
          testcase_name='unexpected_dicom_metadata_error',
          http_status_code=http.HTTPStatus.OK,
          http_response='[{}, {}]',
      ),
      dict(
          testcase_name='httperror_reponse',
          http_status_code=http.HTTPStatus.BAD_REQUEST,
          http_response='BadRequest',
      ),
  ])
  @flagsaver.flagsaver(viewer_debug_url=_VIEWER_DEBUG_URL)
  def test_decode_pubsub_msg_succeeds_if_metadata_fetch_fails(
      self, http_status_code, http_response
  ):
    study_uid = '1.2.3'
    series_uid = '1.2.3.4'
    instance_uid = '1.2.3.4.5'
    ingest = _create_ingestion_handler()
    mock_response = dicom_store_mock.MockHttpResponse(
        f'/studies/{study_uid}/series/{series_uid}/instances'
        r'\?'
        f'SOPInstanceUID={instance_uid}.*',
        dicom_store_mock.RequestMethod.GET,
        http_status_code,
        http_response,
        dicom_store_mock.ContentType.APPLICATION_DICOM_JSON,
    )
    dicom_store_path = (
        'projects/proj/locations/loc/datasets/dat/dicomStores/store/dicomWeb'
    )
    dcm = f'{dicom_store_path}/studies/{study_uid}/series/{series_uid}/instances/{instance_uid}'
    pubsub_msg = pubsub_v1.types.ReceivedMessage(
        ack_id='5',
        message=pubsub_v1.types.PubsubMessage(
            data=dcm.encode('utf-8'), message_id='test'
        ),
        delivery_attempt=1,
    )
    dicom_store_url = f'https://healthcare.googleapis.com/v1/{dicom_store_path}'
    with dicom_store_mock.MockDicomStores(dicom_store_url) as mock_store:
      mock_store[dicom_store_url].set_mock_response(mock_response)
      msg = ingest.decode_pubsub_msg(pubsub_msg)
    self.assertFalse(msg.ignore)
    self.assertEqual(
        ingest._current_instance.gcs_recovery_uri,
        f'gs://dcm-recover-bucket/{dcm}.dcm',
    )
    self.assertEqual(
        ingest._current_instance.viewer_debug_url, _VIEWER_DEBUG_URL
    )
    self.assertEqual(
        ingest._current_instance.dicom_store_client.dicomweb_path,
        f'https://healthcare.googleapis.com/v1/{dicom_store_path}',
    )
    self.assertEqual(ingest._viewer_debug_url, _VIEWER_DEBUG_URL)

  @parameterized.parameters(ingest_dicom.DICOM_ALREADY_INGESTED_TAG_ADDRESSES)
  def test_decode_pubsub_msg_skipped_updated_instance_in_store(
      self, tag_address
  ):
    study_uid = '1.2.3'
    series_uid = '1.2.3.4'
    instance_uid = '1.2.3.4.5'
    ingest = _create_ingestion_handler()
    dcm_store = (
        'projects/proj/locations/loc/datasets/dat/dicomStores/store/dicomWeb'
    )
    dcm = f'{dcm_store}/studies/{study_uid}/series/{series_uid}/instances/{instance_uid}'
    pubsub_msg = pubsub_v1.types.ReceivedMessage(
        ack_id='5',
        message=pubsub_v1.types.PubsubMessage(
            data=dcm.encode('utf-8'), message_id='test'
        ),
        delivery_attempt=1,
    )
    dicom_store_url = f'https://healthcare.googleapis.com/v1/{dcm_store}'
    with dicom_store_mock.MockDicomStores(dicom_store_url) as mock_store:
      mock_store[dicom_store_url].add_instance(
          _create_test_dicom_instance_metadata(
              study_uid,
              series_uid,
              instance_uid,
              {
                  tag_address: {
                      ingest_const.VALUE: ['123'],
                      ingest_const.VR: ingest_const.DICOMVRCodes.SH,
                  }
              },
          )
      )
      msg = ingest.decode_pubsub_msg(pubsub_msg)

    self.assertTrue(msg.ignore)

  def test_get_pubsub_file_dicom_store_no_dicom_store_client(self):
    ingest = _create_ingestion_handler()
    uri = 'https://healthcare/mock/instances/1.2.3'
    download_filepath = '/some/path/file.dcm'
    with self.assertRaisesRegex(
        abstract_dicom_generation.FileDownloadError,
        'DICOM store client undefined',
    ):
      _ = ingest.get_pubsub_file(uri, download_filepath)

  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'download_instance_from_uri',
      side_effect=requests.HTTPError(),
      autospec=True,
  )
  @mock.patch.object(
      cloud_storage_client,
      'download_to_container',
      return_value=False,
      autospec=True,
  )
  def test_get_pubsub_file_dicom_store_and_gcs_downloads_fail(
      self, unused_mk1, unused_mk2
  ):
    ingest = _create_ingestion_handler()
    ingest._current_instance = ingest_dicom_store_handler._IngestionInstance(
        gcs_recovery_uri=_RECOVERY_GCS_URI,
        viewer_debug_url=_VIEWER_DEBUG_URL,
        dicom_store_client=dicom_store_client.DicomStoreClient(
            _DCM_UPLOAD_WEBPATH
        ),
    )
    uri = 'https://healthcare/mock/instances/1.2.3'
    download_filepath = '/some/path/file.dcm'
    with self.assertRaisesRegex(
        abstract_dicom_generation.FileDownloadError,
        'Failed to download .* from both DICOM store and GCS',
    ):
      _ = ingest.get_pubsub_file(uri, download_filepath)

  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'download_instance_from_uri',
      side_effect=requests.HTTPError(),
      autospec=True,
  )
  @mock.patch.object(
      cloud_storage_client,
      'download_to_container',
      return_value=True,
      autospec=True,
  )
  @mock.patch.object(
      cloud_storage_client,
      'upload_blob_to_uri',
      return_value=True,
      autospec=True,
  )
  def test_get_pubsub_file_dicom_store_download_fails_and_gcs_download_succeeds(
      self, gcs_upload_mk, gcs_download_mk, dicom_mk
  ):
    ingest = _create_ingestion_handler()
    ingest._current_instance = ingest_dicom_store_handler._IngestionInstance(
        gcs_recovery_uri=_RECOVERY_GCS_URI,
        viewer_debug_url=_VIEWER_DEBUG_URL,
        dicom_store_client=dicom_store_client.DicomStoreClient(
            _DCM_UPLOAD_WEBPATH
        ),
    )
    uri = 'https://healthcare/mock/instances/1.2.3'
    download_filepath = '/some/path/file.dcm'
    dcm_file = ingest.get_pubsub_file(uri, download_filepath)
    self.assertEqual(dcm_file.localfile, download_filepath)
    self.assertEqual(dcm_file.source_uri, uri)
    dicom_mk.assert_called_once()
    gcs_download_mk.assert_called_once()
    gcs_upload_mk.assert_not_called()

  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'download_instance_from_uri',
      autospec=True,
  )
  @mock.patch.object(
      cloud_storage_client,
      'download_to_container',
      return_value=True,
      autospec=True,
  )
  @mock.patch.object(
      cloud_storage_client,
      'upload_blob_to_uri',
      return_value=True,
      autospec=True,
  )
  def test_get_pubsub_file_from_dicom_store_download_and_gcs_upload_succeed(
      self, gcs_upload_mk, gcs_download_mk, dicom_mk
  ):
    ingest = _create_ingestion_handler()
    ingest._current_instance = ingest_dicom_store_handler._IngestionInstance(
        gcs_recovery_uri=_RECOVERY_GCS_URI,
        viewer_debug_url=_VIEWER_DEBUG_URL,
        dicom_store_client=dicom_store_client.DicomStoreClient(
            _DCM_UPLOAD_WEBPATH
        ),
    )
    uri = 'https://healthcare/mock/instances/1.2.3'
    download_filepath = '/some/path/file.dcm'
    dcm_file = ingest.get_pubsub_file(uri, download_filepath)
    self.assertEqual(dcm_file.localfile, download_filepath)
    self.assertEqual(dcm_file.source_uri, uri)
    dicom_mk.assert_called_once()
    gcs_upload_mk.assert_called_once()
    gcs_download_mk.assert_not_called()

  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'download_instance_from_uri',
      autospec=True,
  )
  @mock.patch.object(
      cloud_storage_client,
      'upload_blob_to_uri',
      return_value=False,
      autospec=True,
  )
  def test_get_pubsub_file_from_dicom_store_download_succeeds_and_gcs_upload_fails(
      self, unused_mk1, unused_mk2
  ):
    ingest = _create_ingestion_handler()
    ingest._current_instance = ingest_dicom_store_handler._IngestionInstance(
        gcs_recovery_uri=_RECOVERY_GCS_URI,
        viewer_debug_url=_VIEWER_DEBUG_URL,
        dicom_store_client=dicom_store_client.DicomStoreClient(
            _DCM_UPLOAD_WEBPATH
        ),
    )
    uri = 'https://healthcare/mock/instances/1.2.3'
    download_filepath = '/some/path/file.dcm'
    with self.assertRaisesRegex(
        abstract_dicom_generation.FileDownloadError, 'Failed to write'
    ):
      _ = ingest.get_pubsub_file(uri, download_filepath)

  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'delete_resource_from_dicom_store',
      autospec=True,
  )
  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'upload_to_dicom_store',
      side_effect=requests.HTTPError(),
      autospec=True,
  )
  @mock.patch.object(cloud_storage_client, 'del_blob', autospec=True)
  def test_update_dicom_instance_in_dicom_store_no_instances_to_upload(
      self, gcs_delete_mk, dcm_upload_mk, dcm_delete_mk
  ):
    ingest = _create_ingestion_handler()
    ingest._current_instance = ingest_dicom_store_handler._IngestionInstance(
        gcs_recovery_uri=_RECOVERY_GCS_URI,
        viewer_debug_url=_VIEWER_DEBUG_URL,
        dicom_store_client=dicom_store_client.DicomStoreClient(
            _DCM_UPLOAD_WEBPATH
        ),
    )
    ingest._update_dicom_instance_in_dicom_store(
        dcm=_create_gen_dicom_result(dicom_files=[])
    )
    dcm_delete_mk.assert_not_called()
    dcm_upload_mk.assert_not_called()
    gcs_delete_mk.assert_not_called()

  @parameterized.parameters([True, False])
  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'delete_resource_from_dicom_store',
      autospec=True,
  )
  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'upload_to_dicom_store',
      side_effect=requests.HTTPError(),
      autospec=True,
  )
  @mock.patch.object(cloud_storage_client, 'del_blob', autospec=True)
  def test_update_dicom_instance_in_dicom_store_dcm_upload_fails(
      self, delete_result, gcs_delete_mk, dcm_upload_mk, dcm_delete_mk
  ):
    dcm_delete_mk.return_value = delete_result
    ingest = _create_ingestion_handler()
    ingest._current_instance = ingest_dicom_store_handler._IngestionInstance(
        gcs_recovery_uri=_RECOVERY_GCS_URI,
        viewer_debug_url=_VIEWER_DEBUG_URL,
        dicom_store_client=dicom_store_client.DicomStoreClient(
            _DCM_UPLOAD_WEBPATH
        ),
    )
    ingest._update_dicom_instance_in_dicom_store(dcm=_create_gen_dicom_result())
    dcm_delete_mk.assert_called_once()
    dcm_upload_mk.assert_called_once()
    gcs_delete_mk.assert_not_called()

  @parameterized.parameters([True, False])
  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'delete_resource_from_dicom_store',
      autospec=True,
  )
  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'upload_to_dicom_store',
      side_effect=dicom_store_client.DicomUploadToGcsError(),
      autospec=True,
  )
  @mock.patch.object(cloud_storage_client, 'del_blob', autospec=True)
  def test_update_dicom_instance_in_dicom_store_gcs_copy_fails(
      self, delete_result, gcs_delete_mk, dcm_upload_mk, dcm_delete_mk
  ):
    dcm_delete_mk.return_value = delete_result
    ingest = _create_ingestion_handler()
    ingest._current_instance = ingest_dicom_store_handler._IngestionInstance(
        gcs_recovery_uri=_RECOVERY_GCS_URI,
        viewer_debug_url=_VIEWER_DEBUG_URL,
        dicom_store_client=dicom_store_client.DicomStoreClient(
            _DCM_UPLOAD_WEBPATH
        ),
    )
    ingest._update_dicom_instance_in_dicom_store(dcm=_create_gen_dicom_result())
    dcm_delete_mk.assert_called_once()
    dcm_upload_mk.assert_called_once()
    gcs_delete_mk.assert_not_called()

  @parameterized.parameters([True, False])
  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'delete_resource_from_dicom_store',
      autospec=True,
  )
  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'upload_to_dicom_store',
      return_value=dicom_store_client.UploadSlideToDicomStoreResults(
          ingested=[],
          previously_ingested=[wsi_dicom_file_ref.WSIDicomFileRef()],
      ),
      autospec=True,
  )
  @mock.patch.object(cloud_storage_client, 'del_blob', autospec=True)
  def test_update_dicom_instance_in_dicom_store_dcm_upload_duplicate(
      self, delete_result, gcs_delete_mk, dcm_upload_mk, dcm_delete_mk
  ):
    dcm_delete_mk.return_value = delete_result
    ingest = _create_ingestion_handler()
    ingest._current_instance = ingest_dicom_store_handler._IngestionInstance(
        gcs_recovery_uri=_RECOVERY_GCS_URI,
        viewer_debug_url=_VIEWER_DEBUG_URL,
        dicom_store_client=dicom_store_client.DicomStoreClient(
            _DCM_UPLOAD_WEBPATH
        ),
    )
    ingest._update_dicom_instance_in_dicom_store(dcm=_create_gen_dicom_result())
    dcm_delete_mk.assert_called_once()
    dcm_upload_mk.assert_called_once()
    gcs_delete_mk.assert_not_called()

  @parameterized.parameters(itertools.product([True, False], [True, False]))
  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'delete_resource_from_dicom_store',
      autospec=True,
  )
  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      'upload_to_dicom_store',
      return_value=dicom_store_client.UploadSlideToDicomStoreResults(
          ingested=[wsi_dicom_file_ref.WSIDicomFileRef()],
          previously_ingested=[],
      ),
      autospec=True,
  )
  @mock.patch.object(cloud_storage_client, 'del_blob', autospec=True)
  def test_update_dicom_instance_in_dicom_store_dcm_upload_success(
      self,
      gcs_delete_result,
      dcm_delete_result,
      gcs_delete_mk,
      dcm_upload_mk,
      dcm_delete_mk,
  ):
    dcm_delete_mk.return_value = dcm_delete_result
    gcs_delete_mk.return_value = gcs_delete_result
    ingest = _create_ingestion_handler()
    ingest._current_instance = ingest_dicom_store_handler._IngestionInstance(
        gcs_recovery_uri=_RECOVERY_GCS_URI,
        viewer_debug_url=_VIEWER_DEBUG_URL,
        dicom_store_client=dicom_store_client.DicomStoreClient(
            _DCM_UPLOAD_WEBPATH
        ),
    )
    ingest._update_dicom_instance_in_dicom_store(dcm=_create_gen_dicom_result())
    dcm_delete_mk.assert_called_once()
    dcm_upload_mk.assert_called_once()
    gcs_delete_mk.assert_called_once()

  def test_handle_unexpected_exception(self):
    ingest_file = abstract_dicom_generation.GeneratedDicomFiles(
        localfile='path/to/original.dcm', source_uri='dicom_store_uri'
    )
    ingest = _create_ingestion_handler()
    ingest.handle_unexpected_exception(
        mock.Mock(), ingest_file, RuntimeError('unexpected error')
    )

  def _test_dicom_instance(self, barcode: str = _SLIDE_ID_1) -> str:
    path = os.path.join(self.create_tempdir().full_path, 'test_jpeg_dicom.dcm')
    with pydicom.dcmread(
        gen_test_util.test_file_path('test_jpeg_dicom.dcm')
    ) as dcm:
      dcm.BarcodeValue = barcode
      dcm.save_as(path)
    return path

  def _setup_handler(
      self,
      ingest_file: abstract_dicom_generation.GeneratedDicomFiles,
      ingest: ingest_dicom_store_handler.IngestDicomStorePubSubHandler,
      has_dicom_store_client: bool = True,
      has_gcs_recovery_uri: bool = True,
  ) -> str:
    ingest.root_working_dir = self.create_tempdir().full_path
    if has_dicom_store_client:
      ds_client = dicom_store_client.DicomStoreClient(_DCM_UPLOAD_WEBPATH)
    else:
      ds_client = None
    test_bucket_path = self.create_tempdir().full_path
    if has_gcs_recovery_uri:
      _, filename = os.path.split(ingest_file.localfile)
      gcs_recovery_uri = f'gs://test_bucket/{filename}'
      shutil.copyfile(
          ingest_file.localfile, os.path.join(test_bucket_path, filename)
      )
    else:
      gcs_recovery_uri = ''
    ingest._current_instance = ingest_dicom_store_handler._IngestionInstance(
        gcs_recovery_uri=gcs_recovery_uri,
        viewer_debug_url=_VIEWER_DEBUG_URL,
        dicom_store_client=ds_client,
    )
    shutil.copyfile(
        gen_test_util.test_file_path('metadata_duplicate.csv'),
        os.path.join(test_bucket_path, 'test.csv'),
    )
    return test_bucket_path

  @mock.patch.object(polling_client, 'PollingClient', autospec=True)
  def test_generate_dicom_and_push_to_store_no_dicom_store_client(
      self, mk_polling
  ):
    ingest_file = abstract_dicom_generation.GeneratedDicomFiles(
        localfile=self._test_dicom_instance(),
        source_uri='dicom_store_uri',
    )
    ingest = _create_ingestion_handler()
    with gcs_mock.GcsMock({
        'test_bucket': self._setup_handler(
            ingest_file, ingest, has_dicom_store_client=False
        )
    }):
      transform_lock = ingest.get_slide_transform_lock(ingest_file, mk_polling)
      self.assertFalse(
          google.cloud.storage.Blob.from_string(
              ingest._current_instance.gcs_recovery_uri,
              client=google.cloud.storage.Client(),
          ).exists()
      )
    self.assertEmpty(transform_lock.name)
    mk_polling.ack.assert_called_once()

  @mock.patch.object(polling_client, 'PollingClient', autospec=True)
  def test_generate_dicom_and_push_to_store_no_gcs_recovery_uri(
      self, mk_polling
  ):
    ingest_file = abstract_dicom_generation.GeneratedDicomFiles(
        localfile=self._test_dicom_instance(),
        source_uri='dicom_store_uri',
    )
    ingest = _create_ingestion_handler()
    with gcs_mock.GcsMock({
        'test_bucket': self._setup_handler(
            ingest_file, ingest, has_gcs_recovery_uri=False
        )
    }):
      transform_lock = ingest.get_slide_transform_lock(ingest_file, mk_polling)
    self.assertEmpty(transform_lock.name)
    mk_polling.ack.assert_called_once()

  @mock.patch.object(polling_client, 'PollingClient', autospec=True)
  @mock.patch.object(
      ingest_dicom.IngestDicom,
      'is_dicom_file_already_ingested',
      return_value=True,
      autospec=True,
  )
  def test_generate_dicom_and_push_to_store_already_ingested_succeeds(
      self, mk_ingested, mk_polling
  ):
    ingest_file = abstract_dicom_generation.GeneratedDicomFiles(
        localfile=self._test_dicom_instance(), source_uri='dicom_store_uri'
    )
    ingest = _create_ingestion_handler()
    with gcs_mock.GcsMock(
        {'test_bucket': self._setup_handler(ingest_file, ingest)}
    ):
      transform_lock = ingest.get_slide_transform_lock(ingest_file, mk_polling)
      self.assertFalse(
          google.cloud.storage.Blob.from_string(
              ingest._current_instance.gcs_recovery_uri,
              client=google.cloud.storage.Client(),
          ).exists()
      )
    self.assertEmpty(transform_lock.name)
    mk_ingested.assert_called_once()
    mk_polling.ack.assert_called_once()

  @mock.patch.object(polling_client, 'PollingClient', autospec=True)
  @mock.patch.object(
      ingest_dicom.IngestDicom,
      'is_dicom_file_already_ingested',
      return_value=False,
      autospec=True,
  )
  @mock.patch.object(
      ingest_dicom.IngestDicom,
      'update_metadata',
      side_effect=metadata_storage_client.MetadataDownloadExceptionError(),
      autospec=True,
  )
  @mock.patch.object(ingest_dicom.IngestDicom, 'generate_dicom', autospec=True)
  def test_generate_dicom_and_push_to_store_metadata_update_fails(
      self, mk_generate, mk_update, mk_ingested, mk_polling
  ):
    ingest_file = abstract_dicom_generation.GeneratedDicomFiles(
        localfile=self._test_dicom_instance(), source_uri='dicom_store_uri'
    )
    ingest = _create_ingestion_handler()
    with gcs_mock.GcsMock(
        {'test_bucket': self._setup_handler(ingest_file, ingest)}
    ):
      transform_lock = ingest.get_slide_transform_lock(ingest_file, mk_polling)
      self.assertFalse(
          google.cloud.storage.Blob.from_string(
              ingest._current_instance.gcs_recovery_uri,
              client=google.cloud.storage.Client(),
          ).exists()
      )
    self.assertEmpty(transform_lock.name)
    mk_ingested.assert_called_once()
    mk_update.assert_called_once()
    mk_generate.assert_not_called()
    mk_polling.ack.assert_called_once()

  @mock.patch.object(
      redis_client.RedisClient,
      'has_redis_client',
      autospec=True,
      return_value=True,
  )
  @mock.patch.object(polling_client, 'PollingClient', autospec=True)
  @mock.patch.object(
      ingest_dicom.IngestDicom,
      'is_dicom_file_already_ingested',
      return_value=False,
      autospec=True,
  )
  @mock.patch.object(
      ingest_dicom.IngestDicom,
      'generate_dicom',
      return_value=_create_gen_dicom_result(dicom_files=[]),
      autospec=True,
  )
  def test_generate_dicom_and_push_to_store_succeeds(
      self, mk_generate, mk_ingested, mk_polling, unused_has_redis_client
  ):
    is_owned = True
    ingest_file = abstract_dicom_generation.GeneratedDicomFiles(
        localfile=self._test_dicom_instance(), source_uri='dicom_store_uri'
    )
    ingest = _create_ingestion_handler()
    with gcs_mock.GcsMock(
        {'test_bucket': self._setup_handler(ingest_file, ingest)}
    ):
      transform_lock = ingest.get_slide_transform_lock(ingest_file, mk_polling)
      with mock.patch.object(
          redis_client.RedisClient,
          'is_lock_owned',
          autospec=True,
          return_value=is_owned,
      ):
        ingest.generate_dicom_and_push_to_store(
            transform_lock, ingest_file, mk_polling
        )
    mk_ingested.assert_called_once()
    mk_generate.assert_called_once()
    mk_polling.ack.assert_called_once()

  @mock.patch.object(
      redis_client.RedisClient,
      'has_redis_client',
      autospec=True,
      return_value=True,
  )
  @mock.patch.object(polling_client, 'PollingClient', autospec=True)
  @mock.patch.object(
      ingest_dicom.IngestDicom,
      'is_dicom_file_already_ingested',
      return_value=False,
      autospec=True,
  )
  @mock.patch.object(
      ingest_dicom.IngestDicom,
      'generate_dicom',
      return_value=_create_gen_dicom_result(dicom_files=[]),
      autospec=True,
  )
  def test_generate_dicom_and_push_to_store_lock_not_held_nacks_pubsub(
      self, mk_generate, mk_ingested, mk_polling, unused_has_redis_client
  ):
    is_owned = False
    ingest_file = abstract_dicom_generation.GeneratedDicomFiles(
        localfile=self._test_dicom_instance(), source_uri='dicom_store_uri'
    )
    ingest = _create_ingestion_handler()
    with gcs_mock.GcsMock(
        {'test_bucket': self._setup_handler(ingest_file, ingest)}
    ):
      transform_lock = ingest.get_slide_transform_lock(ingest_file, mk_polling)
      with mock.patch.object(
          redis_client.RedisClient,
          'is_lock_owned',
          autospec=True,
          return_value=is_owned,
      ):
        ingest.generate_dicom_and_push_to_store(
            transform_lock, ingest_file, mk_polling
        )
    mk_ingested.assert_called_once()
    mk_generate.assert_called_once()
    mk_polling.ack.assert_not_called()
    mk_polling.nack.assert_called_once()

  @mock.patch.object(
      redis_client.RedisClient,
      'has_redis_client',
      autospec=True,
      return_value=True,
  )
  @mock.patch.object(polling_client, 'PollingClient', autospec=True)
  @mock.patch.object(
      ingest_dicom.IngestDicom,
      'is_dicom_file_already_ingested',
      return_value=False,
      autospec=True,
  )
  @mock.patch.object(
      ingest_dicom.IngestDicom,
      'generate_dicom',
      return_value=_create_gen_dicom_result(dicom_files=[]),
      autospec=True,
  )
  def test_get_slide_transform_cannot_find_metadata_does_not_return_lock(
      self, mk_generate, mk_ingested, mk_polling, unused_has_redis_client
  ):
    ingest_file = abstract_dicom_generation.GeneratedDicomFiles(
        localfile=self._test_dicom_instance(barcode='not_found'),
        source_uri='dicom_store_uri',
    )
    ingest = _create_ingestion_handler()
    with gcs_mock.GcsMock(
        {'test_bucket': self._setup_handler(ingest_file, ingest)}
    ):
      transform_lock = ingest.get_slide_transform_lock(ingest_file, mk_polling)
    self.assertFalse(transform_lock.is_defined())
    mk_ingested.assert_called_once()
    mk_generate.assert_not_called()
    mk_polling.ack.assert_called_once()
    mk_polling.nack.assert_not_called()


if __name__ == '__main__':
  absltest.main()
