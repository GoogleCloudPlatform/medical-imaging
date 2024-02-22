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
"""Tests for gcs_storage_util."""
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized

from transformation_pipeline.ingestion_lib import cloud_storage_client
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import gcs_storage_util
from transformation_pipeline.ingestion_lib.pubsub_msgs import ingestion_complete_pubsub


class GcsStorageUtilTest(parameterized.TestCase):

  @mock.patch.object(
      cloud_storage_client, 'copy_blob_to_uri', return_value=False
  )
  def test_move_ingested_dicom_and_publish_ingest_complete_copyfail(
      self, unused_mock1
  ):
    svs_path = ''

    with self.assertRaises(gcs_storage_util.CloudStorageBlobMoveError):
      gcs_storage_util.move_ingested_dicom_and_publish_ingest_complete(
          svs_path,
          None,
          'gs://dest',
          {},
          ingestion_complete_pubsub.create_ingest_complete_pubsub_msg(
              '', '', [], [], {}
          ),
      )

  @mock.patch.object(
      cloud_storage_client, 'copy_blob_to_uri', return_value=True
  )
  @mock.patch.object(cloud_storage_client, 'del_blob', return_value=False)
  def test_move_ingested_dicom_and_publish_ingest_complete_delfail(
      self, unused_mock1, unused_mock2
  ):
    svs_path = ''
    with self.assertRaises(gcs_storage_util.CloudStorageBlobMoveError):
      gcs_storage_util.move_ingested_dicom_and_publish_ingest_complete(
          svs_path,
          None,
          'gs://dest',
          {},
          ingestion_complete_pubsub.create_ingest_complete_pubsub_msg(
              '', '', [], [], {}
          ),
      )

  @mock.patch.object(
      cloud_storage_client, 'copy_blob_to_uri', return_value=True
  )
  @mock.patch.object(cloud_storage_client, 'del_blob', return_value=True)
  def test_move_ingested_dicom_and_publish_ingest_complete_succeeds(
      self, unused_mock1, unused_mock2
  ):
    svs_path = ''
    self.assertIsNone(
        gcs_storage_util.move_ingested_dicom_and_publish_ingest_complete(
            svs_path,
            None,
            'gs://dest',
            {},
            ingestion_complete_pubsub.create_ingest_complete_pubsub_msg(
                '', '', [], [], {}
            ),
        )
    )

  @parameterized.parameters(
      [ingestion_complete_pubsub.PubSubMsg('topic', b'message', None), None]
  )
  @mock.patch.object(
      cloud_storage_client, 'copy_blob_to_uri', autospec=True, return_value=True
  )
  @mock.patch.object(
      ingestion_complete_pubsub, 'publish_pubsubmsg', autospec=True
  )
  @mock.patch.object(
      cloud_storage_client, 'del_blob', autospec=True, return_value=True
  )
  def test_pubsub_message_published(
      self, files_copied_msg, mock_delete, mock_publish, mock_copy
  ):
    destination_uri = 'test_uri'
    dst_metadata = {}
    gcs_storage_util.move_ingested_dicom_and_publish_ingest_complete(
        '',
        None,
        destination_uri,
        dst_metadata,
        files_copied_msg,
        delete_file_in_ingestion_bucket_at_ingest_success_or_failure=True,
    )
    mock_copy.assert_called_once()
    if files_copied_msg is None:
      mock_publish.assert_not_called()
    else:
      mock_publish.assert_called_once()
    mock_delete.assert_called_once()

  @mock.patch.object(
      cloud_storage_client, 'copy_blob_to_uri', autospec=True, return_value=True
  )
  @mock.patch.object(
      ingestion_complete_pubsub, 'publish_pubsubmsg', autospec=True
  )
  @mock.patch.object(
      cloud_storage_client, 'del_blob', autospec=True, return_value=False
  )
  def test_del_blob_fail_throws(self, mock_delete, mock_publish, mock_copy):
    destination_uri = 'test_uri'
    files_copied_msg = ingestion_complete_pubsub.PubSubMsg(
        'topic', b'message', None
    )
    dst_metadata = {}
    with self.assertRaises(gcs_storage_util.CloudStorageBlobMoveError):
      gcs_storage_util.move_ingested_dicom_and_publish_ingest_complete(
          '',
          None,
          destination_uri,
          dst_metadata,
          files_copied_msg,
          delete_file_in_ingestion_bucket_at_ingest_success_or_failure=True,
      )
    mock_copy.assert_called_once()
    mock_publish.assert_called_once()
    mock_delete.assert_called_once()

  @mock.patch.object(
      cloud_storage_client,
      'copy_blob_to_uri',
      autospec=True,
      return_value=False,
  )
  @mock.patch.object(
      ingestion_complete_pubsub, 'publish_pubsubmsg', autospec=True
  )
  @mock.patch.object(
      cloud_storage_client, 'del_blob', autospec=True, return_value=True
  )
  def test_copy_fail_throws(self, mock_delete, mock_publish, mock_copy):
    destination_uri = 'test_uri'
    files_copied_msg = ingestion_complete_pubsub.PubSubMsg(
        'topic', b'message', None
    )
    dst_metadata = {}
    with self.assertRaises(gcs_storage_util.CloudStorageBlobMoveError):
      gcs_storage_util.move_ingested_dicom_and_publish_ingest_complete(
          '',
          None,
          destination_uri,
          dst_metadata,
          files_copied_msg,
          delete_file_in_ingestion_bucket_at_ingest_success_or_failure=True,
      )
    mock_copy.assert_called_once()
    mock_publish.assert_not_called()
    mock_delete.assert_not_called()

  @mock.patch.object(
      cloud_storage_client, 'copy_blob_to_uri', autospec=True, return_value=True
  )
  @mock.patch.object(
      ingestion_complete_pubsub, 'publish_pubsubmsg', autospec=True
  )
  @mock.patch.object(
      cloud_storage_client, 'del_blob', autospec=True, return_value=True
  )
  def test_del_blob_not_called_if_del_disabled(
      self, mock_delete, mock_publish, mock_copy
  ):
    destination_uri = 'test_uri'
    files_copied_msg = ingestion_complete_pubsub.PubSubMsg(
        'topic', b'message', None
    )
    dst_metadata = {}
    gcs_storage_util.move_ingested_dicom_and_publish_ingest_complete(
        '',
        None,
        destination_uri,
        dst_metadata,
        files_copied_msg,
        delete_file_in_ingestion_bucket_at_ingest_success_or_failure=False,
    )
    mock_copy.assert_called_once()
    mock_publish.assert_called_once()
    mock_delete.assert_not_called()


if __name__ == '__main__':
  absltest.main()
