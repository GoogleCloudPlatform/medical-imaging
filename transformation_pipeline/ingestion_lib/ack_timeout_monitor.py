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
"""Thread to auto-extend pub/sub message ack timeout."""
import threading
import time

from google.cloud import pubsub_v1

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib import redis_client
from transformation_pipeline.ingestion_lib.pubsub_msgs import abstract_pubsub_msg


class UndefinedPubSubSubscriptionError(Exception):
  pass


class PubSubAckTimeoutMonitor(threading.Thread):
  """Automatically extends ack timeout for pub/sub message."""

  def __init__(self, subscription_path: str, ack_extension_interval: int = 60):
    super().__init__()
    if ack_extension_interval <= 0:
      raise ValueError('Invalid ack_extension_interval.')
    self._sleep_time_sec = ack_extension_interval
    self.daemon = True  # daemon thread True. can be killed immediately.
    self.name = 'pubsub_ack_timeout_monitor'
    self._current_msg_extended_ack_timeout = 0
    self._should_run = True  # thread is running
    self._monitor_lock = threading.Lock()  # lock for thread
    self._shutdown_condition = threading.Condition(self._monitor_lock)
    self._pubsub_subscriber = None
    self._subscription_path = ''
    self.set_subscription(subscription_path)

  def _clear_current_msg(self) -> None:
    """Clears current message if any.

    Not thread safe, must be called in lock.
    """
    self._current_msg_ack_id = None
    self._current_msg_extended_ack_timeout = 0  # how long in sec msg extended
    self._current_msg_start_time = None
    self._was_message_just_initialized = False

  def set_subscription(self, subscription_path: str) -> None:
    """Clears current pub/sub message if any and sets pub/sub subscription path.

    Args:
      subscription_path: Pub/sub subscription_path to extend timeout.
    """
    with self._monitor_lock:
      self._clear_current_msg()
      self._subscription_path = subscription_path
      if not self._subscription_path:
        return
      if self._pubsub_subscriber is None:
        self._pubsub_subscriber = pubsub_v1.SubscriberClient()

  def clear_pubsub_msg(self) -> float:
    """Clears current pub/sub disabeling acktimeout monitoring.

    Returns:
      Ack timeout extension for message being cleared
    """
    with self._monitor_lock:
      current_msg_extended_ack_timeout = self._current_msg_extended_ack_timeout
      self._clear_current_msg()
      return current_msg_extended_ack_timeout

  def set_pubsub_msg(
      self,
      current_msg: abstract_pubsub_msg.AbstractPubSubMsg,
      current_msg_start_time: float,
  ) -> None:
    """Sets current pub/sub msg for ack timeout extension.

    Args:
      current_msg: Pub/sub msg to extend ack.
      current_msg_start_time: Start time of current msg.
    """
    with self._monitor_lock:
      if not self.is_alive():
        cloud_logging_client.error(
            'PubSubAckTimeoutMonitor thread is not running. '
            'Pub/sub message ack timeout will not be extended.'
        )
        return
      self._current_msg_extended_ack_timeout = 0.0
      self._current_msg_ack_id = current_msg.ack_id
      self._current_msg_start_time = current_msg_start_time
      self._was_message_just_initialized = True  # Set message_init to true to
      self._shutdown_condition.notify()  # first call to notification

  def _extend_msg_ack_deadline(self):
    """Extends the message ack timeout for pub/sub message ack & Redis Key TTL."""
    redis_client.redis_client().extend_lock_timeouts(ingest_const.MESSAGE_TTL_S)

    if self._current_msg_ack_id is None:
      return
    if self._subscription_path:
      self._pubsub_subscriber.modify_ack_deadline(
          request={
              'subscription': self._subscription_path,
              'ack_ids': [self._current_msg_ack_id],
              'ack_deadline_seconds': (
                  ingest_const.MESSAGE_TTL_S
              ),  # ack_deadline_seconds
          }
      )
    elapsed_time = time.time() - self._current_msg_start_time
    self._current_msg_extended_ack_timeout = elapsed_time
    cloud_logging_client.info(
        'Ack deadline extended',
        {ingest_const.LogKeywords.EXTENDING_ACK_DEADLINE_SEC: elapsed_time},
    )

  def get_ack_time_extension(self) -> float:
    with self._monitor_lock:
      return self._current_msg_extended_ack_timeout

  def shutdown(self):
    with self._monitor_lock:
      self._should_run = False
      self._shutdown_condition.notify()
    cloud_logging_client.info('Stopping PubSubAckTimeoutMonitor thread')
    self.join()

  def run(self):
    try:
      cloud_logging_client.info('PubSubAckTimeoutMonitor thread started')
      with self._monitor_lock:
        while self._should_run:
          self._shutdown_condition.wait(self._sleep_time_sec)
          if not self._should_run:
            break
          if self._was_message_just_initialized:
            # thread notified on setting msg to reset log timer.
            # skip first time extension as virtual no time has passed.
            self._was_message_just_initialized = False
          else:
            self._extend_msg_ack_deadline()
    except Exception as exp:
      cloud_logging_client.error(
          'Unexpected error in pub/sub ack extension thread.', exp
      )
      raise
    cloud_logging_client.info('PubSubAckTimeoutMonitor thread stopped')
