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
from absl.testing import parameterized
from google.cloud import pubsub_v1

from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import ack_timeout_monitor
from transformation_pipeline.ingestion_lib import polling_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_gcs_handler
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client
from transformation_pipeline.ingestion_lib.pubsub_msgs import cloud_storage_pubsub_msg
from transformation_pipeline.ingestion_lib.pubsub_msgs import gcs_file_msg


class PollingClientTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    sub_def = pubsub_v1.types.Subscription()
    sub_def.ack_deadline_seconds = 600
    sub_def.expiration_policy.ttl = pubsub_v1.types.duration_pb2.Duration(
        seconds=0
    )
    self.enter_context(flagsaver.flagsaver(metadata_bucket='test'))
    mock_list = [
        mock.patch.object(
            gcs_file_msg.GCSFileMsg,
            'gcs_file_exists',
            return_value=True,
            autospec=True,
        ),
        mock.patch(
            'google.auth.default',
            autospec=True,
            return_value=('mock_auth_token', 'test_project'),
        ),
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
    for mk in mock_list:
      self.enter_context(mk)
    self._process_message_call_count = 0

  def _mock_process_msg(self, client: polling_client.PollingClient) -> None:
    self._process_message_call_count += 1
    client.ack()

  def _get_process_message_call_count(self) -> int:
    # Returns number of times process message was called on unit test instance
    # of DICOM Handler.
    return self._process_message_call_count

  @flagsaver.flagsaver(
      dicom_guid_prefix=uid_generator.TEST_UID_PREFIX,
      gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.METADATA,
  )
  def _create_dicom_handler(self):
    handler = ingest_gcs_handler.IngestGcsPubSubHandler(
        ingest_succeeded_uri='gs://mybucket/success',
        ingest_failed_uri='gs://mybucket/failed',
        dicom_store_web_path='dicom_weburl',
        ingest_ignore_root_dirs=frozenset(),
        metadata_client=metadata_storage_client.MetadataStorageClient(),
    )
    # Ideally process_message would be targeted with a mock. However, the mocked
    # handler method has a dependency on the polling client which results in a
    # results in the mock leaking an instance of the polling client. In lue of
    # mocking, method call is replaced and method call counting is done directly
    # in the unit test.
    handler.process_message = self._mock_process_msg
    return handler

  @flagsaver.flagsaver(pod_hostname='test_pod')
  def _create_gcs_message(
      self, gcs_file_path='gs://tst_bket/foo.svs'
  ) -> gcs_file_msg.GCSFileMsg:
    return gcs_file_msg.GCSFileMsg(gcs_file_path)

  def test_constructor_missing_project_id(self):
    with self.assertRaisesRegex(ValueError, 'Missing project id'):
      with polling_client.PollingClient(
          project_id='', pubsub_subscription_to_handler={}
      ):
        pass

  def test_constructor_no_subscriptions(self):
    with self.assertRaisesRegex(ValueError, 'Missing ingestion subscriptions'):
      with polling_client.PollingClient(
          project_id='foo', pubsub_subscription_to_handler={}
      ):
        pass

  @flagsaver.flagsaver(gcs_file_to_ingest_list=['gs://bk/foo', 'gs://bk/bar'])
  def test_constructor_multiple_subscriptions_for_gcs_list_ingest(self):
    with self.assertRaisesRegex(
        ValueError, 'Unexpected subscriptions for GCS list ingestion'
    ):
      with polling_client.PollingClient(
          project_id='foo',
          pubsub_subscription_to_handler={
              'bar': self._create_dicom_handler(),
              'baz': self._create_dicom_handler(),
          },
      ):
        pass

  @mock.patch.object(
      ack_timeout_monitor.PubSubAckTimeoutMonitor, 'run', autospec=True
  )
  def test_constructor_ack_monitor_started(self, mk):
    with polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    ):
      pass
    mk.assert_called_once()

  @flagsaver.flagsaver(gcs_file_to_ingest_list=['gs://bk/foo', 'gs://bk/bar'])
  @mock.patch.object(
      ack_timeout_monitor.PubSubAckTimeoutMonitor, 'run', autospec=True
  )
  def test_constructor_ack_monitor_started_for_gcs_list_ingest(self, mk):
    with polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    ):
      pass
    mk.assert_called_once()

  def test_unset_acknack(self):
    with polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    ) as pc:
      self.assertEqual(pc._message_ack_nack, '')
      self.assertFalse(pc.is_acked())
      self.assertFalse(pc.is_nacked())

  def test_ack(self):
    with polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    ) as pc:
      pc.current_msg = self._create_gcs_message()
      pc.ack()
      self.assertEqual(pc._message_ack_nack, 'ack')
      self.assertTrue(pc.is_acked())
      self.assertFalse(pc.is_nacked())

  def test_nack(self):
    with polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    ) as pc:
      pc.current_msg = self._create_gcs_message()
      pc.nack()
      self.assertEqual(pc._message_ack_nack, 'nack')
      self.assertFalse(pc.is_acked())
      self.assertTrue(pc.is_nacked())

  def test_ack_after_nack(self):
    with polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    ) as pc:
      pc.current_msg = self._create_gcs_message()
      pc.nack()
      pc.ack()
      self.assertEqual(pc._message_ack_nack, 'nack')
      self.assertFalse(pc.is_acked())
      self.assertTrue(pc.is_nacked())

  def test_nack_after_ack(self):
    with polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    ) as pc:
      pc.current_msg = self._create_gcs_message()
      pc.ack()
      pc.nack()
      self.assertEqual(pc._message_ack_nack, 'ack')
      self.assertTrue(pc.is_acked())
      self.assertFalse(pc.is_nacked())

  def test_current_msg_setter(self):
    with polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    ) as pc:
      pc.current_msg = self._create_gcs_message()
      pc.ack()
      with mock.patch('time.time', return_value=1635400079.014863):
        pc.current_msg = self._create_gcs_message()
      self.assertEqual(pc._message_ack_nack, '')
      self.assertFalse(pc.is_acked())
      self.assertFalse(pc.is_nacked())
      self.assertEqual(pc._current_msg_start_time, 1635400079.014863)
      self.assertIsNotNone(pc.current_msg)

  def test_clear_current_msg(self):
    with polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    ) as pc:
      pc.current_msg = self._create_gcs_message()
      pc._clear_current_msg()
      self.assertIsNone(pc.current_msg)

  @mock.patch.object(
      polling_client.PollingClient,
      '_running',
      side_effect=[True, False],
  )
  @mock.patch.object(pubsub_v1.SubscriberClient, 'pull', autospec=True)
  def test_run_ingestion_no_message_succeeds(self, pubsub_mock, run_mk):
    pubsub_mock.return_value = pubsub_v1.types.PullResponse(
        received_messages=[]
    )
    with polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    ) as pc:
      pc.run()
    run_mk.assert_has_calls([] * 2)
    pubsub_mock.assert_called_once()

  @mock.patch.object(
      polling_client.PollingClient,
      '_running',
      side_effect=[True, False],
  )
  @mock.patch.object(pubsub_v1.SubscriberClient, 'pull', autospec=True)
  @mock.patch.object(
      ingest_gcs_handler.IngestGcsPubSubHandler,
      'decode_pubsub_msg',
      autospec=True,
  )
  def test_run_ingestion_one_message_succeeds(
      self, handler_decode_mk, pubsub_mk, run_mk
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
    with polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    ) as pc:
      pc.run()
      self.assertIsNone(pc.current_msg)
    run_mk.assert_has_calls([] * 2)
    pubsub_mk.assert_called_once()
    handler_decode_mk.assert_called_once()
    self.assertEqual(self._get_process_message_call_count(), 1)

  @mock.patch.object(
      polling_client.PollingClient,
      '_running',
      side_effect=[True, False],
  )
  @mock.patch.object(pubsub_v1.SubscriberClient, 'pull', autospec=True)
  @mock.patch.object(
      ingest_gcs_handler.IngestGcsPubSubHandler,
      'decode_pubsub_msg',
      autospec=True,
  )
  def test_run_ingestion_one_message_fails_invalid_data(
      self, handler_decode_mk, pubsub_mk, run_mk
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
    with polling_client.PollingClient(
        project_id='foo',
        pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
    ) as pc:
      pc.run()
      self.assertIsNone(pc.current_msg)
    run_mk.assert_has_calls([] * 2)
    pubsub_mk.assert_called_once()
    handler_decode_mk.assert_called_once()
    self.assertEqual(self._get_process_message_call_count(), 0)

  @parameterized.named_parameters(
      dict(
          testcase_name='pubsub_subscription',
          gcs_file_to_ingest_list=None,
          expected='projects/foo/subscriptions/bar',
      ),
      dict(
          testcase_name='gcs_file_to_ingest_list',
          gcs_file_to_ingest_list=['gs://bk/bar'],
          expected='',
      ),
  )
  def test_set_current_subscription(self, gcs_file_to_ingest_list, expected):
    with flagsaver.flagsaver(gcs_file_to_ingest_list=gcs_file_to_ingest_list):
      with polling_client.PollingClient(
          project_id='foo',
          pubsub_subscription_to_handler={'bar': self._create_dicom_handler()},
      ) as pc:
        with mock.patch.object(
            ack_timeout_monitor.PubSubAckTimeoutMonitor,
            'set_subscription',
            autospec=True,
        ) as mock_set_subscription:
          pc._set_current_subscription(pc._subscriptions[0])
          mock_set_subscription.assert_called_once_with(
              pc._ack_monitor, expected
          )


if __name__ == '__main__':
  absltest.main()
