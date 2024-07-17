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
"""Tests for dicom_store_pubsub_msg."""

from absl.testing import absltest
from google.cloud import pubsub_v1

from transformation_pipeline.ingestion_lib.pubsub_msgs import dicom_store_pubsub_msg


class DicomStorePubsubMsgTest(absltest.TestCase):

  def test_msg_missing_data(self):
    pubsub_msg = pubsub_v1.types.ReceivedMessage(
        ack_id='ack_id',
        message=pubsub_v1.types.PubsubMessage(message_id='message_id'),
    )
    dicom_msg = dicom_store_pubsub_msg.DicomStorePubSubMsg(pubsub_msg)
    self.assertTrue(dicom_msg.ignore)

  def test_valid_dicom_store_msg(self):
    dicom_instance = 'projects/123/locations/us-central1/datasets/some-dataset/dicomStores/some-datastore/dicomWeb/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5'
    pubsub_msg = pubsub_v1.types.ReceivedMessage(
        ack_id='ack_id',
        message=pubsub_v1.types.PubsubMessage(
            data=dicom_instance.encode('utf8'), message_id='message_id'
        ),
    )
    dicom_msg = dicom_store_pubsub_msg.DicomStorePubSubMsg(pubsub_msg)
    self.assertFalse(dicom_msg.ignore)
    self.assertEqual(dicom_msg.filename, dicom_instance)
    self.assertEqual(
        dicom_msg.uri, f'https://healthcare.googleapis.com/v1/{dicom_instance}'
    )


if __name__ == '__main__':
  absltest.main()
