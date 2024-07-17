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
import contextlib
import json
import time
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
from google.cloud import pubsub_v1

from transformation_pipeline.ingestion_lib import ack_timeout_monitor
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib import mock_redis_client
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

  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_subscription_path(self, mk_auth):
    subscription_path = 'foo'
    ack_mon = ack_timeout_monitor.PubSubAckTimeoutMonitor(subscription_path)
    self.assertEqual(ack_mon._subscription_path, subscription_path)
    mk_auth.assert_called_once()

  @parameterized.parameters([None, ''])
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_subscription_path_none_or_empty(self, subscription_path, mk_auth):
    ack_mon = ack_timeout_monitor.PubSubAckTimeoutMonitor(subscription_path)
    self.assertEqual(ack_mon._subscription_path, subscription_path)
    mk_auth.assert_not_called()

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
      pubsub_v1.SubscriberClient, 'modify_ack_deadline', autospec=True
  )
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_set_pubsub_msg_extended_if_pubsub_msg_set(
      self, mk_auth, mk_pubsub_client
  ):
    with mock_redis_client.MockRedisClient(None):
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
      mk_auth.assert_called_once()

  @mock.patch.object(
      pubsub_v1.SubscriberClient, 'modify_ack_deadline', autospec=True
  )
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_set_pubsub_msg_and_redis_lock_extended_if_pubsub_msg_set(
      self, mk_auth, mk_pubsub_client
  ):
    lock_name = 'foo'
    with mock_redis_client.MockRedisClient('1.2.3.4.5'):
      with contextlib.ExitStack() as lock_context:
        redis_client.redis_client().acquire_non_blocking_lock(
            lock_name, 'abc', 6, lock_context
        )
        lock_time = time.time()
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
        mk_auth.assert_called_once()
        self.assertGreater(
            redis_client.redis_client().client.get_lock_expire_time(lock_name),
            6 + lock_time,
        )

  @mock.patch.object(
      pubsub_v1.SubscriberClient, 'modify_ack_deadline', autospec=True
  )
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  @flagsaver.flagsaver(ops_log_project='mock_project')
  def test_clear_pubsub_msg(self, mk_auth, unused_mk_pubsub_client):
    ack_mon = ack_timeout_monitor.PubSubAckTimeoutMonitor('foo', 10)
    ack_mon.start()
    start_time = time.time()
    ack_mon.set_pubsub_msg(_MockPubSubMsg(), start_time)
    ack_mon.clear_pubsub_msg()
    self.assertEqual(ack_mon.get_ack_time_extension(), 0)
    ack_mon.shutdown()
    mk_auth.assert_called_once()

  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_ack_monitor_subscription_init(self, unused_mk_auth):
    subscription_path = 'foo'
    ack_mon = ack_timeout_monitor.PubSubAckTimeoutMonitor(subscription_path)
    self.assertEqual(ack_mon._subscription_path, subscription_path)
    self.assertIsNotNone(ack_mon._pubsub_subscriber)

  def test_ack_monitor_no_subscription_init(self):
    ack_mon = ack_timeout_monitor.PubSubAckTimeoutMonitor('')
    self.assertEmpty(ack_mon._subscription_path)
    self.assertIsNone(ack_mon._pubsub_subscriber)

  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_ack_monitor_set_subscription(self, unused_mk_auth):
    subscription_path = 'foo'
    ack_mon = ack_timeout_monitor.PubSubAckTimeoutMonitor('')
    ack_mon.set_subscription(subscription_path)
    self.assertEqual(ack_mon._subscription_path, subscription_path)
    self.assertIsNotNone(ack_mon._pubsub_subscriber)

  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_ack_monitor_set_null_subscription_after_client_acquired_keeps_client(
      self, unused_mk_auth
  ):
    ack_mon = ack_timeout_monitor.PubSubAckTimeoutMonitor('')
    ack_mon.set_subscription('foo')
    ack_mon.set_subscription('')
    self.assertEmpty(ack_mon._subscription_path)
    self.assertIsNotNone(ack_mon._pubsub_subscriber)


if __name__ == '__main__':
  absltest.main()
