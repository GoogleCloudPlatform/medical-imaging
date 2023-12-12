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
"""Tests for ack_timeout_monitor."""
import json
import time
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
from google.cloud import pubsub_v1

from transformation_pipeline.ingestion_lib import ack_timeout_monitor
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib import redis_client
from transformation_pipeline.ingestion_lib.pubsub_msgs import cloud_storage_pubsub_msg

_PROJECT_ID = '123'


class _MockPubSubMsg(cloud_storage_pubsub_msg.CloudStoragePubSubMsg):

  def __init__(self):
    super().__init__(
        pubsub_v1.types.ReceivedMessage(
            ack_id='ack_id',
            message=pubsub_v1.types.PubsubMessage(
                message_id='message_id',
                data=json.dumps(dict(name='foo', bucket='bar')).encode('utf-8'),
            ),
        )
    )


class AckTimeoutMonitorTest(parameterized.TestCase):

  @parameterized.parameters([None, 'foo'])
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_subscription_path(self, subscription_path, mk_auth):
    ack_mon = ack_timeout_monitor.PubSubAckTimeoutMonitor(subscription_path)
    self.assertEqual(ack_mon._subscription_path, subscription_path)
    mk_auth.assert_called_once()

  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_invalid_ack_extension_interval_raises(self, unused_auth):
    with self.assertRaises(ValueError):
      ack_timeout_monitor.PubSubAckTimeoutMonitor('foo', 0)

  @parameterized.parameters([None, 'bar'])
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_set_subscription_path(self, subscription_path, mk_auth):
    ack_mon = ack_timeout_monitor.PubSubAckTimeoutMonitor('foo')
    ack_mon.set_subscription(subscription_path)
    self.assertEqual(ack_mon._subscription_path, subscription_path)
    mk_auth.assert_called_once()

  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_set_pubsub_msg_on_undefined_subcription_raises(self, mk_auth):
    ack_mon = ack_timeout_monitor.PubSubAckTimeoutMonitor(None)
    with self.assertRaises(
        ack_timeout_monitor.UndefinedPubSubSubscriptionError
    ):
      ack_mon.set_pubsub_msg(_MockPubSubMsg(), time.time())
    mk_auth.assert_called_once()

  @mock.patch.object(
      pubsub_v1.SubscriberClient, 'modify_ack_deadline', autospec=True
  )
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_set_pubsub_msg_thread_start_stop(self, mk_auth, mk_pubsub_client):
    ack_mon = ack_timeout_monitor.PubSubAckTimeoutMonitor('foo', 10)
    self.assertFalse(ack_mon.is_alive())
    ack_mon.start()
    self.assertTrue(ack_mon.is_alive())
    ack_mon.shutdown()
    self.assertFalse(ack_mon.is_alive())
    mk_pubsub_client.assert_not_called()
    mk_auth.assert_called_once()

  @mock.patch.object(
      pubsub_v1.SubscriberClient, 'modify_ack_deadline', autospec=True
  )
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_set_pubsub_msg_not_extended_if_no_pubsub_msg_set(
      self, mk_auth, mk_pubsub_client
  ):
    ack_mon = ack_timeout_monitor.PubSubAckTimeoutMonitor('foo', 5)
    ack_mon.start()
    time.sleep(20)
    self.assertEqual(ack_mon.get_ack_time_extension(), 0)
    ack_mon.shutdown()
    mk_pubsub_client.assert_not_called()
    mk_auth.assert_called_once()

  @mock.patch.object(
      redis_client.RedisClient, 'extend_lock_timeouts', autospec=True
  )
  @mock.patch.object(
      pubsub_v1.SubscriberClient, 'modify_ack_deadline', autospec=True
  )
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_set_pubsub_msg_extended_if_pubsub_msg_set(
      self, mk_auth, mk_pubsub_client, mk_redis_client
  ):
    subscription_path = 'foo'
    wait_time = 10
    ack_extension_time_interval = 2
    ack_mon = ack_timeout_monitor.PubSubAckTimeoutMonitor(
        subscription_path, ack_extension_time_interval
    )
    mock_pubsub_msg = _MockPubSubMsg()
    ack_mon.start()
    start_time = time.time()
    ack_mon.set_pubsub_msg(mock_pubsub_msg, start_time)
    time.sleep(wait_time + (2 * ack_extension_time_interval))
    self.assertGreaterEqual(ack_mon.get_ack_time_extension(), wait_time)
    ack_mon.shutdown()

    mk_pubsub_client.assert_called()
    call_count = mk_pubsub_client.call_count
    pubsub_client_call = mock.call(
        ack_mon._pubsub_subscriber,
        request={
            'subscription': subscription_path,
            'ack_ids': [mock_pubsub_msg.ack_id],
            'ack_deadline_seconds': ingest_const.MESSAGE_TTL_S,
        },
    )
    mk_pubsub_client.assert_has_calls(
        [pubsub_client_call] * call_count, any_order=False
    )
    mk_redis_client.assert_has_calls(
        [
            mock.call(
                redis_client.redis_client(), amount=ingest_const.MESSAGE_TTL_S
            )
        ]
        * call_count,
        any_order=False,
    )
    mk_auth.assert_called_once()

  @mock.patch.object(
      pubsub_v1.SubscriberClient, 'modify_ack_deadline', autospec=True
  )
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_clear_pubsub_msg(self, mk_auth, unused_mk_pubsub_client):
    ack_mon = ack_timeout_monitor.PubSubAckTimeoutMonitor('foo', 10)
    ack_mon.start()
    start_time = time.time()
    ack_mon.set_pubsub_msg(_MockPubSubMsg(), start_time)
    ack_mon.clear_pubsub_msg()
    self.assertEqual(ack_mon.get_ack_time_extension(), 0)
    ack_mon.shutdown()
    mk_auth.assert_called_once()


if __name__ == '__main__':
  absltest.main()
