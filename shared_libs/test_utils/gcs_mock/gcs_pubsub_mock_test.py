# Copyright 2024 Google LLC
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
"""Test for Pub/Sub Mock."""
import datetime
import os
import time
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
import google.cloud.pubsub_v1

from shared_libs.test_utils.gcs_mock import gcs_pubsub_mock


_PROJECT_ID = 'test-project'
_SUBSCRIPTION_NAME = 'test-subscription'
_BUCKET_NAME = 'test-bucket'


class PubsubMockTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.directory_to_monitor = self.create_tempdir()

  def _write_test_file(
      self, filename: str = 'test', content: str = 'test'
  ) -> str:
    path = os.path.join(self.directory_to_monitor, filename)
    with open(path, 'wt') as outfile:
      outfile.write(content)
    return path

  def test_subscription_path(self):
    actual_library_path = (
        google.cloud.pubsub_v1.SubscriberClient.subscription_path(
            _PROJECT_ID, _SUBSCRIPTION_NAME
        )
    )
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        message_queue_delay_sec=0,
    ):
      self.assertEqual(
          google.cloud.pubsub_v1.SubscriberClient.subscription_path(
              _PROJECT_ID, _SUBSCRIPTION_NAME
          ),
          actual_library_path,
      )

  @parameterized.parameters([600, 50])
  def test_get_subscription(self, ack_deadline: int):
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        ack_deadline=ack_deadline,
        message_queue_delay_sec=0,
    ):
      client = google.cloud.pubsub_v1.SubscriberClient()
      subscription_definition = client.get_subscription(
          subscription=client.subscription_path(_PROJECT_ID, _SUBSCRIPTION_NAME)
      )
      self.assertEqual(
          subscription_definition.expiration_policy.ttl, datetime.timedelta(0)
      )
      self.assertEqual(
          subscription_definition.ack_deadline_seconds, ack_deadline
      )

  def test_default_subscription(self):
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        message_queue_delay_sec=0,
    ):
      client = google.cloud.pubsub_v1.SubscriberClient()
      subscription_definition = client.get_subscription(
          request={
              'subscription': client.subscription_path(
                  _PROJECT_ID, _SUBSCRIPTION_NAME
              ),
          }
      )
      self.assertEqual(subscription_definition.ack_deadline_seconds, 600)

  def test_subscription_pull_empty_queue(self):
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        message_queue_delay_sec=0,
    ):
      client = google.cloud.pubsub_v1.SubscriberClient()
      subscription_path = client.subscription_path(
          _PROJECT_ID, _SUBSCRIPTION_NAME
      )
      self.assertEmpty(
          client.pull(
              request={'subscription': subscription_path}, max_messages=1
          ).received_messages,
      )

  def test_subscription_pull_message(self):
    self._write_test_file()
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        message_queue_delay_sec=0,
    ):
      client = google.cloud.pubsub_v1.SubscriberClient()
      subscription_path = client.subscription_path(
          _PROJECT_ID, _SUBSCRIPTION_NAME
      )
      self.assertLen(
          client.pull(
              request={'subscription': subscription_path}, max_messages=1
          ).received_messages,
          1,
      )

  def test_subscription_pull_no_more_messages_message(self):
    self._write_test_file()
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        message_queue_delay_sec=0,
    ):
      subscription_path = (
          google.cloud.pubsub_v1.SubscriberClient.subscription_path(
              _PROJECT_ID, _SUBSCRIPTION_NAME
          )
      )
      client = google.cloud.pubsub_v1.SubscriberClient()
      client.pull(request={'subscription': subscription_path}, max_messages=1)
      message = client.pull(
          request={'subscription': subscription_path}, max_messages=1
      )
      self.assertEmpty(message.received_messages)

  def test_subscription_pull_one_nack(self):
    for count in range(2):
      self._write_test_file(filename=f'test_{count}.txt')
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        message_queue_delay_sec=0,
    ):
      client = google.cloud.pubsub_v1.SubscriberClient()
      subscription_path = client.subscription_path(
          _PROJECT_ID, _SUBSCRIPTION_NAME
      )
      msg1 = client.pull(
          request={
              'subscription': client.subscription_path(
                  _PROJECT_ID, _SUBSCRIPTION_NAME
              ),
          },
          max_messages=1,
      )
      msg2 = client.pull(
          request={
              'subscription': client.subscription_path(
                  _PROJECT_ID, _SUBSCRIPTION_NAME
              ),
          },
          max_messages=1,
      )
      client.modify_ack_deadline(
          request={
              'subscription': subscription_path,
              'ack_ids': [msg1.received_messages[0].ack_id],
              'ack_deadline_seconds': 0,
          }
      )
      self.assertNotEqual(
          msg2.received_messages[0].ack_id, msg1.received_messages[0].ack_id
      )
      msg2 = client.pull(
          request={
              'subscription': client.subscription_path(
                  _PROJECT_ID, _SUBSCRIPTION_NAME
              ),
          },
          max_messages=1,
      )
      self.assertEqual(
          msg2.received_messages[0].ack_id, msg1.received_messages[0].ack_id
      )

  def test_subscription_auto_retry(self):
    self._write_test_file()
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        ack_deadline=1,
        message_queue_delay_sec=0,
    ):
      client = google.cloud.pubsub_v1.SubscriberClient()
      subscription_path = client.subscription_path(
          _PROJECT_ID, _SUBSCRIPTION_NAME
      )
      msg1 = client.pull(
          request={'subscription': subscription_path}, max_messages=1
      )
      self.assertEmpty(
          client.pull(
              request={'subscription': subscription_path}, max_messages=1
          ).received_messages
      )
      time.sleep(1)
      msg2 = client.pull(
          request={'subscription': subscription_path}, max_messages=1
      )
      self.assertEqual(
          msg2.received_messages[0].ack_id, msg1.received_messages[0].ack_id
      )

  def test_subscription_calls_callback_when_all_files_processed(self):
    call_back_called = False

    def _test_callback() -> None:
      nonlocal call_back_called
      call_back_called = True

    self._write_test_file()
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        call_if_no_files=_test_callback,
        message_queue_delay_sec=0,
    ):
      client = google.cloud.pubsub_v1.SubscriberClient()
      subscription_path = client.subscription_path(
          _PROJECT_ID, _SUBSCRIPTION_NAME
      )
      msg1 = client.pull(
          request={'subscription': subscription_path}, max_messages=1
      )
      self.assertEmpty(
          client.pull(
              request={'subscription': subscription_path}, max_messages=1
          ).received_messages
      )
      client.acknowledge(
          subscription=subscription_path,
          ack_ids=[msg1.received_messages[0].ack_id],
      )
      self.assertFalse(call_back_called)
      self.assertEmpty(
          client.pull(
              request={'subscription': subscription_path}, max_messages=1
          ).received_messages
      )
      self.assertTrue(call_back_called)

  def test_subscription_pull_scans_for_new_files(self):
    self._write_test_file()
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        message_queue_delay_sec=0,
    ):
      client = google.cloud.pubsub_v1.SubscriberClient()
      subscription_path = client.subscription_path(
          _PROJECT_ID, _SUBSCRIPTION_NAME
      )
      msg1 = client.pull(
          request={'subscription': subscription_path}, max_messages=1
      )
      self.assertLen(msg1.received_messages, 1)
      client.acknowledge(
          request={
              'subscription': subscription_path,
              'ack_ids': [msg1.received_messages[0].ack_id],
          },
      )
      self._write_test_file(filename='test2.txt')
      self.assertLen(
          client.pull(
              request={'subscription': subscription_path}, max_messages=1
          ).received_messages,
          1,
      )

  def test_subscription_overwriting_file_with_different_value_triggers_msg(
      self,
  ):
    self._write_test_file()
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        message_queue_delay_sec=0,
    ):
      client = google.cloud.pubsub_v1.SubscriberClient()
      subscription_path = client.subscription_path(
          _PROJECT_ID, _SUBSCRIPTION_NAME
      )
      msg1 = client.pull(
          request={'subscription': subscription_path}, max_messages=1
      )
      self.assertLen(msg1.received_messages, 1)
      client.acknowledge(
          request={},
          subscription=subscription_path,
          ack_ids=[msg1.received_messages[0].ack_id],
      )
      self._write_test_file(content='test2')
      self.assertLen(
          client.pull(
              request={'subscription': subscription_path}, max_messages=1
          ).received_messages,
          1,
      )

  def test_stop_generating_pubsub_messages_stops_pull_msgs(
      self,
  ):
    self._write_test_file()
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        message_queue_delay_sec=0,
    ) as mock_ps:
      client = google.cloud.pubsub_v1.SubscriberClient()
      subscription_path = client.subscription_path(
          _PROJECT_ID, _SUBSCRIPTION_NAME
      )
      msg1 = client.pull(
          request={'subscription': subscription_path}, max_messages=1
      )
      self.assertLen(msg1.received_messages, 1)
      client.acknowledge(
          subscription=subscription_path,
          ack_ids=[msg1.received_messages[0].ack_id],
      )
      mock_ps.stop_generating_pubsub_messages()
      self._write_test_file(content='test2')
      self.assertEmpty(
          client.pull(
              request={'subscription': subscription_path}, max_messages=1
          ).received_messages
      )

  def test_pull_returns_empty_if_files_processing(self):
    self._write_test_file()
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        message_queue_delay_sec=0,
    ):
      client = google.cloud.pubsub_v1.SubscriberClient()
      subscription_path = client.subscription_path(
          _PROJECT_ID, _SUBSCRIPTION_NAME
      )
      msg1 = client.pull(
          request={'subscription': subscription_path}, max_messages=1
      )
      self.assertEmpty(
          client.pull(
              subscription=subscription_path, max_messages=1
          ).received_messages
      )
      self.assertEmpty(
          client.pull(
              subscription=subscription_path, max_messages=1
          ).received_messages
      )
      client.acknowledge(
          subscription=subscription_path,
          ack_ids=[msg1.received_messages[0].ack_id],
      )
      self.assertEmpty(
          client.pull(
              subscription=subscription_path, max_messages=1
          ).received_messages
      )

  def test_file_monitor_init_empty(self):
    dir_mon = gcs_pubsub_mock._MockPubSubSubscriptionState(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
    )
    self.assertEmpty(dir_mon._monitor_file_map)

  def test_file_monitor_init_one(self):
    self._write_test_file()
    dir_mon = gcs_pubsub_mock._MockPubSubSubscriptionState(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
    )
    self.assertLen(dir_mon._monitor_file_map, 1)

  def test_file_monitor_track_two(self):
    self._write_test_file()
    dir_mon = gcs_pubsub_mock._MockPubSubSubscriptionState(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
    )
    self._write_test_file(filename='test2.txt')
    dir_mon._update_directory_monitor()
    self.assertLen(dir_mon._monitor_file_map, 2)

  def test_file_monitor_cleans_removes_deleted_files(self):
    path_1 = self._write_test_file()
    dir_mon = gcs_pubsub_mock._MockPubSubSubscriptionState(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
    )
    path_2 = self._write_test_file(filename='test2.txt')
    dir_mon._update_directory_monitor()
    os.remove(path_1)
    dir_mon._update_directory_monitor()
    self.assertLen(dir_mon._monitor_file_map, 1)
    os.remove(path_2)
    dir_mon._update_directory_monitor()
    self.assertEmpty(dir_mon._monitor_file_map)

  @parameterized.named_parameters([
      dict(
          testcase_name='bad_subscription',
          project_id=_PROJECT_ID,
          subscription_name='BAD_NAME',
      ),
      dict(
          testcase_name='bad_project_id',
          project_id='BAD_NAME',
          subscription_name=_SUBSCRIPTION_NAME,
      ),
  ])
  def test_mock_raises_if_called_with_bad_project_or_subscription_name(
      self, project_id, subscription_name
  ):
    self._write_test_file()
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        message_queue_delay_sec=0,
    ):
      client = google.cloud.pubsub_v1.SubscriberClient()
      subscription_path = client.subscription_path(
          project_id, subscription_name
      )
      with self.assertRaises(google.api_core.exceptions.NotFound):
        client.pull(request={'subscription': subscription_path}, max_messages=1)

  def test_mock_raises_if_called_with_ack_bad_ack_id(self):
    self._write_test_file()
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        message_queue_delay_sec=0,
    ):
      client = google.cloud.pubsub_v1.SubscriberClient()
      subscription_path = client.subscription_path(
          _PROJECT_ID, _SUBSCRIPTION_NAME
      )
      with self.assertRaises(google.api_core.exceptions.InvalidArgument):
        client.acknowledge(
            subscription=subscription_path, ack_ids=['BAD_ACK_ID']
        )

  def test_mock_raises_if_called_with_modify_ack_deadline_bad_ack_id(self):
    self._write_test_file()
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        message_queue_delay_sec=0,
    ):
      client = google.cloud.pubsub_v1.SubscriberClient()
      subscription_path = client.subscription_path(
          _PROJECT_ID, _SUBSCRIPTION_NAME
      )
      with self.assertRaises(google.api_core.exceptions.InvalidArgument):
        client.modify_ack_deadline(
            subscription=subscription_path,
            ack_ids=['BAD_ACK_ID'],
            ack_deadline_seconds=10,
        )

  def test_subscription_pull_invalid_max_messages_raises(self):
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        message_queue_delay_sec=0,
    ):
      client = google.cloud.pubsub_v1.SubscriberClient()
      subscription_path = client.subscription_path(
          _PROJECT_ID, _SUBSCRIPTION_NAME
      )
      with self.assertRaises(google.api_core.exceptions.InvalidArgument):
        client.pull(subscription=subscription_path, max_messages=0)

  @parameterized.named_parameters([
      dict(
          testcase_name='load1_pull1',
          num_to_load=1,
          num_to_pull=1,
          expected_len=1,
      ),
      dict(
          testcase_name='load2_pull2',
          num_to_load=2,
          num_to_pull=2,
          expected_len=2,
      ),
      dict(
          testcase_name='load1_pull2',
          num_to_load=1,
          num_to_pull=2,
          expected_len=1,
      ),
      dict(
          testcase_name='load2_pull1',
          num_to_load=2,
          num_to_pull=1,
          expected_len=1,
      ),
      dict(
          testcase_name='load3_pull2',
          num_to_load=3,
          num_to_pull=2,
          expected_len=2,
      ),
  ])
  def test_subscription_returns_expected_number_of_messagses(
      self, num_to_load, num_to_pull, expected_len
  ):
    for index in range(num_to_load):
      self._write_test_file(filename=f'test{index}.txt')
    with gcs_pubsub_mock.MockPubSub(
        _PROJECT_ID,
        _SUBSCRIPTION_NAME,
        _BUCKET_NAME,
        self.directory_to_monitor,
        message_queue_delay_sec=0,
    ):
      client = google.cloud.pubsub_v1.SubscriberClient()
      subscription_path = client.subscription_path(
          _PROJECT_ID, _SUBSCRIPTION_NAME
      )
      returned_ids = {
          msg.ack_id
          for msg in client.pull(
              subscription=subscription_path, max_messages=num_to_pull
          ).received_messages
      }
      self.assertLen(returned_ids, expected_len)

  @mock.patch.object(
      gcs_pubsub_mock, '_MockPubSubSubscriptionState', side_effect=ValueError
  )
  def test_unexpected_error_closes_mock_and_raises(self, unused_mock):
    with self.assertRaises(ValueError):
      with gcs_pubsub_mock.MockPubSub(
          _PROJECT_ID,
          _SUBSCRIPTION_NAME,
          _BUCKET_NAME,
          self.directory_to_monitor,
          message_queue_delay_sec=0,
      ):
        pass


if __name__ == '__main__':
  absltest.main()
