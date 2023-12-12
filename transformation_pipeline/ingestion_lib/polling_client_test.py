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
"""Tests for polling_client."""
import json
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from google.cloud import pubsub_v1

from google.protobuf import duration_pb2
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import ack_timeout_monitor
from transformation_pipeline.ingestion_lib import polling_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_gcs_handler
from transformation_pipeline.ingestion_lib.pubsub_msgs import cloud_storage_pubsub_msg
from transformation_pipeline.ingestion_lib.pubsub_msgs import gcs_file_msg


class PollingClientTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    sub_def = pubsub_v1.types.Subscription()
    sub_def.ack_deadline_seconds = 600
    sub_def.expiration_policy.ttl = duration_pb2.Duration(seconds=0)
    self._mocks = [
        mock.patch.object(
            gcs_file_msg.GCSFileMsg,
            'gcs_file_exists',
            return_value=True,
            autospec=True,
        ),
        mock.patch('google.auth.default', autospec=True, return_value=(1, 1)),
        mock.patch('google.cloud.pubsub_v1.PublisherClient', autospec=True),
        mock.patch.object(
            pubsub_v1.SubscriberClient,
            'get_subscription',
            autospec=True,
            return_value=sub_def,
        ),
        mock.patch.object(pubsub_v1.SubscriberClient, 'acknowledge'),
        mock.patch.object(pubsub_v1.SubscriberClient, 'modify_ack_deadline'),
    ]
    self._mocked_classes = []
    for mk in self._mocks:
      self._mocked_classes.append(mk.start())
      self.addCleanup(mk.stop)

  @flagsaver.flagsaver(
      dicom_guid_prefix=uid_generator.TEST_UID_PREFIX,
      gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.METADATA,
  )
  def _create_dicom_handler(self):
    return ingest_gcs_handler.IngestGcsPubSubHandler(
        ingest_succeeded_uri='gs://mybucket/success',
        ingest_failed_uri='gs://mybucket/failed',
        dicom_store_web_path='dicom_weburl',
        ingest_ignore_root_dirs=frozenset(),
    )

  @flagsaver.flagsaver(pod_hostname='test_pod')
  def _create_gcs_message(
      self, gcs_file_path='gs://tst_bket/foo.svs'
  ) -> gcs_file_msg.GCSFileMsg:
    return gcs_file_msg.GCSFileMsg(gcs_file_path)

  def test_constructor_missing_project_id(self):
    with self.assertRaisesRegex(ValueError, 'Missing project id'):
      _ = polling_client.PollingClient(
          project_id='', pubsub_subscription_to_handler={}
      )

  def test_constructor_no_subscriptions(self):
    with self.assertRaisesRegex(ValueError, 'Missing ingestion subscriptions'):
      _ = polling_client.PollingClient(
          project_id='foo', pubsub_subscription_to_handler={}
      )

  @flagsaver.flagsaver(gcs_file_to_ingest_list=['gs://bk/foo', 'gs://bk/bar'])
  def test_constructor_multiple_subscriptions_for_gcs_list_ingest(self):
    with self.assertRaisesRegex(
        ValueError, 'Unexpected subscriptions for GCS list ingestion'
    ):
      _ = polling_client.PollingClient(
          project_id='foo',
          pubsub_subscription_to_handler={
              'bar': self._create_dicom_handler(),
              'baz': self._create_dicom_handler(),
          },
      )

  @mock.patch.object(
      ack_timeout_monitor.PubSubAckTimeoutMonitor, 'start', autospec=True
  )
  def test_constructor_ack_monitor_started(self, mk):
    _ = polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    )
    mk.assert_called_once()

  @flagsaver.flagsaver(gcs_file_to_ingest_list=['gs://bk/foo', 'gs://bk/bar'])
  @mock.patch.object(
      ack_timeout_monitor.PubSubAckTimeoutMonitor, 'start', autospec=True
  )
  def test_constructor_ack_monitor_started_for_gcs_list_ingest(self, mk):
    _ = polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    )
    mk.assert_called_once()

  @mock.patch.object(pubsub_v1.SubscriberClient, '__enter__', autospec=True)
  @mock.patch.object(pubsub_v1.SubscriberClient, '__exit__', autospec=True)
  @mock.patch.object(
      ack_timeout_monitor.PubSubAckTimeoutMonitor, 'shutdown', autospec=True
  )
  def test_enters_exists_successfully(self, mk_monitor, mk_exit, mk_enter):
    with polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    ) as _:
      pass
    mk_enter.assert_called_once()
    mk_exit.assert_called_once()
    mk_monitor.assert_called_once()

  def test_unset_acknack(self):
    pc = polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    )
    self.assertEqual(pc._message_ack_nack, '')
    self.assertFalse(pc.is_acked())
    self.assertFalse(pc.is_nacked())

  def test_ack(self):
    pc = polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    )
    pc.current_msg = self._create_gcs_message()
    pc.ack()
    self.assertEqual(pc._message_ack_nack, 'ack')
    self.assertTrue(pc.is_acked())
    self.assertFalse(pc.is_nacked())

  def test_nack(self):
    pc = polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    )
    pc.current_msg = self._create_gcs_message()
    pc.nack()
    self.assertEqual(pc._message_ack_nack, 'nack')
    self.assertFalse(pc.is_acked())
    self.assertTrue(pc.is_nacked())

  def test_ack_after_nack(self):
    pc = polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    )
    pc.current_msg = self._create_gcs_message()
    pc.nack()
    pc.ack()
    self.assertEqual(pc._message_ack_nack, 'nack')
    self.assertFalse(pc.is_acked())
    self.assertTrue(pc.is_nacked())

  def test_nack_after_ack(self):
    pc = polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    )
    pc.current_msg = self._create_gcs_message()
    pc.ack()
    pc.nack()
    self.assertEqual(pc._message_ack_nack, 'ack')
    self.assertTrue(pc.is_acked())
    self.assertFalse(pc.is_nacked())

  def test_current_msg_setter(self):
    pc = polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    )
    pc.current_msg = self._create_gcs_message()
    pc.ack()
    with mock.patch('time.time') as mock_method:
      mock_method.return_value = 1635400079.014863
      pc.current_msg = self._create_gcs_message()
    self.assertEqual(pc._message_ack_nack, '')
    self.assertFalse(pc.is_acked())
    self.assertFalse(pc.is_nacked())
    self.assertEqual(pc._current_msg_start_time, 1635400079.014863)
    self.assertIsNotNone(pc.current_msg)

  def test_clear_current_msg(self):
    pc = polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    )
    pc.current_msg = self._create_gcs_message()
    pc._clear_current_msg()
    self.assertIsNone(pc.current_msg)

  @mock.patch.object(
      polling_client.PollingClient,
      '_running',
      side_effect=[True, False],
      autospec=True,
  )
  @mock.patch.object(pubsub_v1.SubscriberClient, 'pull', autospec=True)
  def test_run_ingestion_no_message_succeeds(self, pubsub_mock, run_mk):
    pubsub_mock.return_value = pubsub_v1.types.PullResponse(
        received_messages=[]
    )
    pc = polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    )
    pc.run()
    run_mk.assert_has_calls([] * 2)
    pubsub_mock.assert_called_once()

  @mock.patch.object(
      polling_client.PollingClient,
      '_running',
      side_effect=[True, False],
      autospec=True,
  )
  @mock.patch.object(pubsub_v1.SubscriberClient, 'pull', autospec=True)
  @mock.patch.object(
      ingest_gcs_handler.IngestGcsPubSubHandler,
      'decode_pubsub_msg',
      autospec=True,
  )
  @mock.patch.object(
      ingest_gcs_handler.IngestGcsPubSubHandler,
      'process_message',
      autospec=True,
  )
  def test_run_ingestion_one_message_succeeds(
      self, handler_process_mk, handler_decode_mk, pubsub_mk, run_mk
  ):
    pubsub_msg = pubsub_v1.types.ReceivedMessage(
        ack_id='ack_id',
        message=pubsub_v1.types.PubsubMessage(
            message_id='message_id',
            attributes={'eventType': 'OBJECT_FINALIZE'},
            data=json.dumps({
                'name': 'some_filename',
                'bucket': 'some_bucket',
            }).encode('utf-8'),
        ),
    )
    pubsub_mk.return_value = pubsub_v1.types.PullResponse(
        received_messages=[pubsub_msg]
    )
    handler_decode_mk.return_value = (
        cloud_storage_pubsub_msg.CloudStoragePubSubMsg(pubsub_msg)
    )
    pc = polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    )
    pc.run()
    self.assertIsNone(pc.current_msg)
    run_mk.assert_has_calls([] * 2)
    pubsub_mk.assert_called_once()
    handler_decode_mk.assert_called_once()
    handler_process_mk.assert_called_once()

  @mock.patch.object(
      polling_client.PollingClient,
      '_running',
      side_effect=[True, False],
      autospec=True,
  )
  @mock.patch.object(pubsub_v1.SubscriberClient, 'pull', autospec=True)
  @mock.patch.object(
      ingest_gcs_handler.IngestGcsPubSubHandler,
      'decode_pubsub_msg',
      autospec=True,
  )
  @mock.patch.object(
      ingest_gcs_handler.IngestGcsPubSubHandler,
      'process_message',
      autospec=True,
  )
  def test_run_ingestion_one_message_fails_invalid_data(
      self, handler_process_mk, handler_decode_mk, pubsub_mk, run_mk
  ):
    pubsub_msg = pubsub_v1.types.ReceivedMessage(
        ack_id='ack_id',
        message=pubsub_v1.types.PubsubMessage(
            message_id='message_id',
            attributes={'eventType': 'OBJECT_FINALIZE'},
            data=None,
        ),
    )
    pubsub_mk.return_value = pubsub_v1.types.PullResponse(
        received_messages=[pubsub_msg]
    )
    handler_decode_mk.return_value = (
        cloud_storage_pubsub_msg.CloudStoragePubSubMsg(pubsub_msg)
    )
    pc = polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    )
    pc.run()
    self.assertIsNone(pc.current_msg)
    run_mk.assert_has_calls([] * 2)
    pubsub_mk.assert_called_once()
    handler_decode_mk.assert_called_once()
    handler_process_mk.assert_not_called()


if __name__ == '__main__':
  absltest.main()
