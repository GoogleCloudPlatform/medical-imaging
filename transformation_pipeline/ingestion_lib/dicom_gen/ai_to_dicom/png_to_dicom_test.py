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
"""Tests for PNG to DICOM conversion."""
import json
import os
import time
from typing import Any, Dict, Mapping, Optional, Tuple
from unittest import mock

from absl import logging
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
from hcls_imaging_ml_toolkit import dicom_json
from hcls_imaging_ml_toolkit import tags
from google.cloud import pubsub_v1
import pydicom

from shared_libs.test_utils.dicom_store_mock import dicom_store_mock
from transformation_pipeline.ingestion_lib import abstract_polling_client
from transformation_pipeline.ingestion_lib import cloud_storage_client
from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.ai_to_dicom import png_to_dicom
from transformation_pipeline.ingestion_lib.pubsub_msgs import abstract_pubsub_msg
from transformation_pipeline.ingestion_lib.pubsub_msgs import inference_pubsub_msg

PROJECT_ID = 'test'
STUDY_UID = '1.2.3.4.5'
SERIES_UID = '1.2.3.4.5.6'


def _mock_inference_pubsub_msg(
    dicomstore_path: str,
    pngfile: str,
    params: Mapping[str, Any],
    mock_source_dicom_instance: str,
) -> pubsub_v1.types.ReceivedMessage:
  with pydicom.dcmread(mock_source_dicom_instance) as dcm:
    study_instance_uid = dcm.StudyInstanceUID
    series_instance_uid = dcm.SeriesInstanceUID
  msg = {}
  msg['output_bucket'] = 'MOCK_BUCKET'
  msg['dicomstore_path'] = dicomstore_path
  msg['slide_uid'] = f'{study_instance_uid}:{series_instance_uid}'
  msg['whole_slide_score'] = '42'
  msg['model_name'] = 'oof_model'
  msg['model_version'] = 'MOCK_MODEL_VERSION'
  msg['instances'] = ['MOCK_OOF_SOURCE_INSTANCE']
  msg['uploaded_heatmaps'] = [os.path.basename(pngfile)]
  msg['additional_params'] = params
  now = time.time()
  seconds = int(now)
  nanos = int((now - seconds) * 10**9)
  timestamp = pubsub_v1.types.Timestamp(seconds=seconds, nanos=nanos)
  return pubsub_v1.types.ReceivedMessage(
      ack_id='ack_id',
      message=pubsub_v1.types.PubsubMessage(
          data=json.dumps(msg).encode('utf-8'),
          message_id='message_id',
          publish_time=timestamp,
      ),
  )


class _MockPollingClient(abstract_polling_client.AbstractPollingClient):

  def __init__(self, msg: inference_pubsub_msg.InferencePubSubMsg):
    self._ack = None
    self._current_msg = msg

  def nack(self, retry_ttl: int = 0):
    if self._ack is None:
      self._ack = 'NACK'

  def ack(self, retry_ttl: int = 0):
    if self._ack is None:
      self._ack = 'ACK'

  def is_acked(self) -> bool:
    if self._ack is None:
      return False
    return self._ack == 'ACK'

  @property
  def current_msg(self) -> Optional[abstract_pubsub_msg.AbstractPubSubMsg]:
    return self._current_msg


class AiPngtoDicomSecondaryCaptureTest(parameterized.TestCase):
  """Tests for PNG to DICOM conversion."""

  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def assertJsonTag(
      self, dicomjson: Dict[str, Any], tag: tags.DicomTag, expected: Any
  ) -> None:  # pytype: disable=invalid-annotation
    """Given a DICOM JSON dict asserts whether the tag's value matches the expected value.

    Args:
      dicomjson: Dict representing the DICOM JSON structure.
      tag: A tuple representing a DICOM Tag and its associated value.
      expected: Value that is expected.
    """
    self.assertEqual(dicom_json.GetValue(dicomjson, tag), expected)

  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_convert_png_to_dicom(self):
    png_to_dicom_converter = png_to_dicom.AiPngtoDicomSecondaryCapture('')

    # Tmp directory stores saved JPEG image and DICOM.
    result = png_to_dicom_converter._convert_png_to_dicom(
        gen_test_util.test_file_path('logo.png'),
        self.create_tempdir().full_path,
        STUDY_UID,
        SERIES_UID,
    )

    # Verify DICOM was generated
    self.assertNotEmpty(result)

    logging.info('Output Path: %s', result[0])
    ds = pydicom.dcmread(result[0])
    logging.info('Dataset: %s', ds)

    dicom_dict = ds.to_json_dict()
    # Verify metadata tags
    self.assertJsonTag(dicom_dict, tags.STUDY_INSTANCE_UID, STUDY_UID)
    self.assertJsonTag(dicom_dict, tags.SERIES_INSTANCE_UID, SERIES_UID)

  def test_handle_unexpected_exception(self):
    ingest_file = abstract_dicom_generation.GeneratedDicomFiles(
        localfile='path/to/original.dcm', source_uri='dicom_store_uri'
    )
    png_to_dicom_converter = png_to_dicom.AiPngtoDicomSecondaryCapture('')
    png_to_dicom_converter.handle_unexpected_exception(
        mock.Mock(), ingest_file, RuntimeError('unexpected error')
    )

  def get_client_convert_and_test_input(
      self,
      mock_dicomweb_url: str,
      pubsub_msg: pubsub_v1.types.ReceivedMessage,
      local_path: str,
  ) -> Tuple[
      _MockPollingClient,
      png_to_dicom.AiPngtoDicomSecondaryCapture,
      abstract_dicom_generation.GeneratedDicomFiles,
  ]:
    png_to_dicom_converter = png_to_dicom.AiPngtoDicomSecondaryCapture(
        mock_dicomweb_url
    )
    mock_triggering_msg = png_to_dicom_converter.decode_pubsub_msg(pubsub_msg)
    mock_polling_client = _MockPollingClient(mock_triggering_msg)
    abstract_gen_dicom_files = abstract_dicom_generation.GeneratedDicomFiles(
        local_path, mock_triggering_msg.uri
    )
    png_to_dicom_converter.root_working_dir = self.create_tempdir()
    return (
        mock_polling_client,
        png_to_dicom_converter,
        abstract_gen_dicom_files,
    )

  @parameterized.parameters([
      ({}, 'https://mock.oof_source.com/dicomWeb'),
      (
          {
              ingest_const.OofPassThroughKeywords.SOURCE_DICOM_IN_MAIN_STORE: (
                  True
              )
          },
          'https://mock.oof_source.com/dicomWeb',
      ),
      (
          {
              ingest_const.OofPassThroughKeywords.SOURCE_DICOM_IN_MAIN_STORE: (
                  False
              )
          },
          'mock.ai_ml_dest.store.com',
      ),
  ])
  @mock.patch.object(
      cloud_storage_client, 'del_blob', autospec=True, return_value=True
  )
  def test_png_to_dicom_pipeline_succeeds_and_does_not_delete_source_imaging(
      self, params, dicom_store_to_clean, mock_gcs_del
  ):
    mock_oof_source_dicomweb_url = 'https://mock.oof_source.com/dicomWeb'
    mock_oof_dest_dicomweb_url = 'https://mock.ai_ml_dest.store.com/dicomWeb'
    local_path = gen_test_util.test_file_path('logo.png')
    source_dicom_instance_path = gen_test_util.test_file_path(
        'test_png_to_dicom_source.dcm'
    )
    pubsub_msg = _mock_inference_pubsub_msg(
        mock_oof_source_dicomweb_url,
        local_path,
        params,
        source_dicom_instance_path,
    )

    with dicom_store_mock.MockDicomStores(
        mock_oof_source_dicomweb_url, mock_oof_dest_dicomweb_url
    ) as mk_store:
      mk_store[mock_oof_source_dicomweb_url].add_instance(
          source_dicom_instance_path
      )
      with flagsaver.flagsaver(dicom_store_to_clean=dicom_store_to_clean):
        rs = self.get_client_convert_and_test_input(
            mock_oof_dest_dicomweb_url, pubsub_msg, local_path
        )
        (
            mock_polling_client,
            png_to_dicom_converter,
            abstract_gen_dicom_files,
        ) = rs

        png_to_dicom_converter.generate_dicom_and_push_to_store(
            abstract_gen_dicom_files, mock_polling_client
        )

      # test source instance not deleted
      mk_store[mock_oof_source_dicomweb_url].assert_not_empty(self)
      # test_instance uploaded to store
      mk_store[mock_oof_dest_dicomweb_url].assert_not_empty(self)

      # pipeline deleted image
      mock_gcs_del.assert_called_once_with(
          uri='gs://MOCK_BUCKET/logo.png', ignore_file_not_found=True
      )
      # pipeline succeeded
      self.assertTrue(mock_polling_client.is_acked())

  @mock.patch.object(
      cloud_storage_client, 'del_blob', autospec=True, return_value=True
  )
  def test_png_to_dicom_pipeline_succeeds_and_deletes_source_imaging(
      self, mock_gcs_del
  ):
    mock_oof_source_dicomweb_url = 'https://mock.oof_source.com/dicomWeb'
    mock_oof_dest_dicomweb_url = 'https://mock.ai_ml_dest.store.com/dicomWeb'
    params = {
        ingest_const.OofPassThroughKeywords.SOURCE_DICOM_IN_MAIN_STORE: False
    }
    dicom_store_to_clean = 'https://mock.oof_source.com/dicomWeb'
    local_path = gen_test_util.test_file_path('logo.png')
    source_dicom_instance_path = gen_test_util.test_file_path(
        'test_png_to_dicom_source.dcm'
    )
    pubsub_msg = _mock_inference_pubsub_msg(
        mock_oof_source_dicomweb_url,
        local_path,
        params,
        source_dicom_instance_path,
    )

    with dicom_store_mock.MockDicomStores(
        mock_oof_source_dicomweb_url, mock_oof_dest_dicomweb_url
    ) as mk_store:
      mk_store[mock_oof_source_dicomweb_url].add_instance(
          source_dicom_instance_path
      )
      with flagsaver.flagsaver(dicom_store_to_clean=dicom_store_to_clean):
        rs = self.get_client_convert_and_test_input(
            mock_oof_dest_dicomweb_url, pubsub_msg, local_path
        )
        (
            mock_polling_client,
            png_to_dicom_converter,
            abstract_gen_dicom_files,
        ) = rs

        png_to_dicom_converter.generate_dicom_and_push_to_store(
            abstract_gen_dicom_files, mock_polling_client
        )

      # test source instance not deleted
      mk_store[mock_oof_source_dicomweb_url].assert_empty(self)
      # test_instance uploaded to store
      mk_store[mock_oof_dest_dicomweb_url].assert_not_empty(self)

      # pipeline deleted image
      mock_gcs_del.assert_called_once_with(
          uri='gs://MOCK_BUCKET/logo.png', ignore_file_not_found=True
      )
      # pipeline succeeded
      self.assertTrue(mock_polling_client.is_acked())


if __name__ == '__main__':
  absltest.main()
