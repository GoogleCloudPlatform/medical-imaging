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

import contextlib
import json
import os
import time
from typing import Any, Mapping
from unittest import mock

from absl import logging
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
from google.cloud import pubsub_v1
import pydicom

from shared_libs.test_utils.dicom_store_mock import dicom_store_mock
from transformation_pipeline.ingestion_lib import cloud_storage_client
from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib import mock_redis_client
from transformation_pipeline.ingestion_lib import polling_client
from transformation_pipeline.ingestion_lib import redis_client
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.ai_to_dicom import png_to_dicom

_STUDY_UID = '1.2.3.4.5'
_SERIES_UID = '1.2.3.4.5.6'


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


class AiPngtoDicomSecondaryCaptureTest(parameterized.TestCase):
  """Tests for PNG to DICOM conversion."""

  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_convert_png_to_dicom(self):
    png_to_dicom_converter = png_to_dicom.AiPngtoDicomSecondaryCapture('')

    # Tmp directory stores saved JPEG image and DICOM.
    result = png_to_dicom_converter._convert_png_to_dicom(
        gen_test_util.test_file_path('logo.png'),
        self.create_tempdir().full_path,
        _STUDY_UID,
        _SERIES_UID,
    )

    # Verify DICOM was generated
    self.assertNotEmpty(result)

    logging.info('Output Path: %s', result[0])
    ds = pydicom.dcmread(result[0])
    logging.info('Dataset: %s', ds)

    # Verify metadata tags
    self.assertEqual(ds.StudyInstanceUID, _STUDY_UID)
    self.assertEqual(ds.SeriesInstanceUID, _SERIES_UID)

  def test_handle_unexpected_exception(self):
    ingest_file = abstract_dicom_generation.GeneratedDicomFiles(
        localfile='path/to/original.dcm', source_uri='dicom_store_uri'
    )
    png_to_dicom_converter = png_to_dicom.AiPngtoDicomSecondaryCapture('')
    png_to_dicom_converter.handle_unexpected_exception(
        mock.Mock(), ingest_file, RuntimeError('unexpected error')
    )

  def get_png_to_dicom_converter(
      self,
      mock_dicomweb_url: str,
      pubsub_msg: pubsub_v1.types.ReceivedMessage,
      local_path: str,
  ) -> png_to_dicom.AiPngtoDicomSecondaryCapture:
    png_to_dicom_converter = png_to_dicom.AiPngtoDicomSecondaryCapture(
        mock_dicomweb_url
    )
    get_pubsub_file = self.enter_context(
        mock.patch.object(
            abstract_dicom_generation.AbstractDicomGeneration,
            'get_pubsub_file',
            autospec=True,
        )
    )
    mock_triggering_msg = png_to_dicom_converter.decode_pubsub_msg(pubsub_msg)
    get_pubsub_file.return_value = (
        abstract_dicom_generation.GeneratedDicomFiles(
            local_path, mock_triggering_msg.uri
        )
    )
    png_to_dicom_converter.root_working_dir = self.create_tempdir()
    return png_to_dicom_converter

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
  @mock.patch.object(polling_client, 'PollingClient', autospec=True)
  @mock.patch.object(
      cloud_storage_client, 'del_blob', autospec=True, return_value=True
  )
  def test_png_to_dicom_pipeline_succeeds_and_does_not_delete_source_imaging(
      self,
      params,
      dicom_store_to_clean,
      mock_gcs_del,
      mock_polling_client,
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
        png_to_dicom_converter = self.get_png_to_dicom_converter(
            mock_oof_dest_dicomweb_url, pubsub_msg, local_path
        )
        mock_polling_client.current_msg = (
            png_to_dicom_converter.decode_pubsub_msg(pubsub_msg)
        )
        with mock_redis_client.MockRedisClient('1.2.3.4'):
          png_to_dicom_converter.process_message(mock_polling_client)

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

  @parameterized.named_parameters([
      dict(testcase_name='redis_transform_lock_enabled', redis_ip='1.2.3.4'),
      dict(testcase_name='redis_transform_lock_disabled', redis_ip=None),
  ])
  @mock.patch.object(polling_client, 'PollingClient', autospec=True)
  @mock.patch.object(
      cloud_storage_client, 'del_blob', autospec=True, return_value=True
  )
  def test_png_to_dicom_pipeline_succeeds_and_deletes_source_imaging(
      self, mock_gcs_del, mock_polling_client, redis_ip
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
        png_to_dicom_converter = self.get_png_to_dicom_converter(
            mock_oof_dest_dicomweb_url, pubsub_msg, local_path
        )
        mock_polling_client.current_msg = (
            png_to_dicom_converter.decode_pubsub_msg(pubsub_msg)
        )
        with mock_redis_client.MockRedisClient(redis_ip):
          png_to_dicom_converter.process_message(mock_polling_client)
      # test source instance deleted
      mk_store[mock_oof_source_dicomweb_url].assert_empty(self)
      # test_instance uploaded to store
      mk_store[mock_oof_dest_dicomweb_url].assert_not_empty(self)

      # pipeline deleted image
      mock_gcs_del.assert_called_once_with(
          uri='gs://MOCK_BUCKET/logo.png', ignore_file_not_found=True
      )
      # pipeline succeeded
      self.assertTrue(mock_polling_client.is_acked())

  @mock.patch.object(polling_client, 'PollingClient', autospec=True)
  @mock.patch.object(
      cloud_storage_client, 'del_blob', autospec=True, return_value=True
  )
  def test_png_to_dicom_pipeline_delayed_due_to_lock(
      self, mock_gcs_del, mock_polling_client
  ):
    # Tests that AI transformation will do nothing and nack if redis lock
    # cannot be acquired.
    local_test_context_block = self.enter_context(contextlib.ExitStack())
    redis_ip = '1.2.3.4'
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
        png_to_dicom_converter = self.get_png_to_dicom_converter(
            mock_oof_dest_dicomweb_url, pubsub_msg, local_path
        )
        decoded_msg = png_to_dicom_converter.decode_pubsub_msg(pubsub_msg)
        mock_polling_client.current_msg = decoded_msg

        with mock_redis_client.MockRedisClient(redis_ip):
          # Simulate a lock by another pod on the images identiying lock name.
          png_transform_lock = png_to_dicom_converter.get_slide_transform_lock(
              abstract_dicom_generation.GeneratedDicomFiles(
                  local_path, decoded_msg.uri
              ),
              mock_polling_client,
          )
          redis_client.redis_client().acquire_non_blocking_lock(
              png_transform_lock.name,
              'ANOTHER_POD_UID',
              100,
              local_test_context_block,
          )

          # Now to try process the image with the acquired lock.
          png_to_dicom_converter.process_message(mock_polling_client)

      # test that nothing happend
      # source instance not deleted
      mk_store[mock_oof_source_dicomweb_url].assert_not_empty(self)
      # test_instance not uploaded to store
      mk_store[mock_oof_dest_dicomweb_url].assert_empty(self)
      # pipeline did not delete the image from the input gcs bucket
      mock_gcs_del.assert_not_called()
      # pub/sub message was nacked.
      self.assertTrue(mock_polling_client.is_nacked())

  @mock.patch.object(polling_client, 'PollingClient', autospec=True)
  def test_png_to_dicom_pipeline_get_slide_transform_lock(
      self, mock_polling_client
  ):
    params = {
        ingest_const.OofPassThroughKeywords.SOURCE_DICOM_IN_MAIN_STORE: False
    }
    local_path = 'mock_local_file_path'
    source_dicom_instance_path = gen_test_util.test_file_path(
        'test_png_to_dicom_source.dcm'
    )
    pubsub_msg = _mock_inference_pubsub_msg(
        'https://mock.oof_source.com/dicomWeb',
        'local_path',
        params,
        source_dicom_instance_path,
    )
    png_to_dicom_converter = self.get_png_to_dicom_converter(
        'https://mock.ai_ml_dest.store.com/dicomWeb',
        pubsub_msg,
        local_path,
    )
    decoded_msg = png_to_dicom_converter.decode_pubsub_msg(pubsub_msg)
    mock_polling_client.current_msg = decoded_msg

    # Now to try process the image with the acquired lock.
    lock = png_to_dicom_converter.get_slide_transform_lock(
        abstract_dicom_generation.GeneratedDicomFiles(
            local_path, decoded_msg.uri
        ),
        mock_polling_client,
    )
    self.assertEqual(
        lock.name,
        'ML_TRIGGERED'
        ' STUDY_INSTANCE_UID:1.2.276.0.7230010.3.1.2.296485376.46.1648688993.578030'
        ' SERIES_INSTANCE_UID:'
        ' 1.2.276.0.7230010.3.1.3.296485376.46.1648688993.578031',
    )


if __name__ == '__main__':
  absltest.main()
