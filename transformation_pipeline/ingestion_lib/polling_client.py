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
"""Sync polls pub/sub messages and executes ingest pipeline."""
import dataclasses
import time
from typing import Mapping, Optional

from google.api_core import retry
from google.cloud import pubsub_v1

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import abstract_polling_client
from transformation_pipeline.ingestion_lib import abstract_pubsub_message_handler
from transformation_pipeline.ingestion_lib import ack_timeout_monitor
from transformation_pipeline.ingestion_lib import cloud_storage_client
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.pubsub_msgs import abstract_pubsub_msg
from transformation_pipeline.ingestion_lib.pubsub_msgs import gcs_file_msg


NO_PUBSUB_MSG_LOG_INTERVAL_SECONDS = 600  # 10 min timeout interval for logging


@dataclasses.dataclass(frozen=True)
class _SubscriptionAssets:
  subscription_path: str
  msg_handler: abstract_pubsub_message_handler.AbstractPubSubMsgHandler
  ack_deadline_seconds: int


class PollingClient(abstract_polling_client.AbstractPollingClient):
  """Sync polls pub/sub messages and executes ingest pipeline."""

  def __init__(
      self,
      project_id: str,
      pubsub_subscription_to_handler: Mapping[
          str, abstract_pubsub_message_handler.AbstractPubSubMsgHandler
      ],
  ):
    cloud_logging_client.set_per_thread_log_signatures(False)
    cloud_logging_client.set_log_trace_key(
        ingest_const.LogKeywords.DPAS_INGESTION_TRACE_ID
    )
    cloud_logging_client.info('DPAS ingest pipeline starting')
    if not project_id:
      raise ValueError('Missing project id.')
    if not pubsub_subscription_to_handler:
      raise ValueError('Missing ingestion subscriptions.')
    self._polling_client_running = True
    self._no_pubsub_msg_log_timeout = time.time()
    self._last_pubsub_msg_time = None
    self._current_msg = None
    self._message_ack_nack = ''
    self._project_id = project_id
    self._current_msg_start_time = 0.0
    self._gcs_file_to_ingest_list = ingest_flags.GCS_FILE_INGEST_LIST_FLG.value
    try:
      if self._is_ingesting_from_gcs_file_list():
        self._initialize_polling_client_for_ingestion_list(
            pubsub_subscription_to_handler
        )
        self._ack_monitor = ack_timeout_monitor.PubSubAckTimeoutMonitor(
            subscription_path=''
        )
        self._current_subscription = self._subscriptions[0]
      else:
        self._initialize_polling_client_for_pubsub_subscriptions(
            project_id, pubsub_subscription_to_handler
        )
        self._current_subscription = self._subscriptions[0]
        self._ack_monitor = ack_timeout_monitor.PubSubAckTimeoutMonitor(
            subscription_path=self._current_subscription.subscription_path
        )
      self._clear_current_msg()
      self._ack_monitor.start()
    except ValueError as exp:
      cloud_logging_client.critical('Failed to initialize polling client', exp)
      raise
    except Exception as exp:
      cloud_logging_client.critical(
          'An unexpected exception occurred in the GKE container', exp
      )
      raise

  def _initialize_polling_client_for_ingestion_list(
      self,
      pubsub_subscription_to_handler: Mapping[
          str, abstract_pubsub_message_handler.AbstractPubSubMsgHandler
      ],
  ):
    """Initializes subscription assets for fixed list of GCS files to ingest.

    Args:
      pubsub_subscription_to_handler: Map of Pub/Sub subscription id to message
        handler.

    Raises:
      ValueError if more than one or missing subscription.
    """
    if len(pubsub_subscription_to_handler) != 1:
      raise ValueError(
          'Unexpected subscriptions for GCS list ingestion: '
          f'{pubsub_subscription_to_handler}'
      )
    self._pubsub_subscriber = None
    msg_handler = None
    for _, handler in pubsub_subscription_to_handler.items():
      msg_handler = handler
      break
    subscription = _SubscriptionAssets(  # pytype: disable=wrong-arg-types  # py311-upgrade
        subscription_path='',
        msg_handler=msg_handler,
        ack_deadline_seconds=ingest_const.MESSAGE_TTL_S,
    )
    self._subscriptions = [subscription]
    cloud_logging_client.info(
        'Ingesting list of files stored on GCS.',
        {'list_of_files_to_ingest': self._gcs_file_to_ingest_list},
    )

  def _initialize_polling_client_for_pubsub_subscriptions(
      self,
      project_id: str,
      pubsub_subscription_to_handler: Mapping[
          str, abstract_pubsub_message_handler.AbstractPubSubMsgHandler
      ],
  ):
    """Initializes subscription assets based on subscription to msg handler map.

    Args:
      project_id: Project id to use for subscriptions.
      pubsub_subscription_to_handler: Map of Pub/Sub subscription id to message
        handler.
    """
    self._pubsub_subscriber = pubsub_v1.SubscriberClient()
    self._subscriptions = []
    for pubsub_subscription, handler in pubsub_subscription_to_handler.items():
      # Subscribe to pub/sub topic to poll for messages.
      # The `subscription_path` method creates a fully qualified identifier
      # in the form `projects/{project_id}/subscriptions/{subscription_id}`
      subscription_path = self._pubsub_subscriber.subscription_path(
          project_id, pubsub_subscription
      )
      subscription_def = self._pubsub_subscriber.get_subscription(
          subscription=subscription_path
      )

      ack_deadline_seconds = subscription_def.ack_deadline_seconds
      if ack_deadline_seconds < ingest_const.MESSAGE_TTL_S:
        cloud_logging_client.error(
            'DPAS ingest pub/sub subscription ack deadline for '
            f'{pubsub_subscription} is set to: {ack_deadline_seconds} seconds. '
            'The subscription should have an ack deadline of at least '
            f'{ingest_const.MESSAGE_TTL_S} seconds.'
        )

      subscription_exp = subscription_def.expiration_policy.ttl.seconds
      if subscription_exp != 0:
        cloud_logging_client.error(
            'DPAS ingest pub/sub subscription is set to '
            f'expire after {subscription_exp} seconds. The subscription '
            'should be set to NEVER expire.'
        )

      cloud_logging_client.info(
          f'Running {handler.name} DICOM generator for {pubsub_subscription}',
          {'DICOM_gen': handler.name},
      )

      subscription = _SubscriptionAssets(
          subscription_path, handler, ack_deadline_seconds
      )
      self._subscriptions.append(subscription)

  def _is_ingesting_from_gcs_file_list(self) -> bool:
    return self._gcs_file_to_ingest_list is not None

  def _has_files_in_ingestion_list(self) -> bool:
    return bool(self._gcs_file_to_ingest_list)

  def _is_polling_pubsub(self) -> bool:
    return self._pubsub_subscriber is not None

  def stop_polling_client(self):
    self._polling_client_running = False

  def _running(self) -> bool:
    """Returns True if polling client is running."""
    if not self._polling_client_running:
      return False
    if self._is_polling_pubsub():
      return True
    return self._has_files_in_ingestion_list()

  def _validate_current_msg_processed(self):
    """Validates that all messages received an ack or nack."""
    if self._current_msg is not None and not self._message_ack_nack:
      # Message was not acknowledged (acked or nacked);
      # nack to redeliver the message and log.
      cloud_logging_client.error('Message was not nack or acked')
      self.nack()

  @property
  def current_msg(self) -> Optional[abstract_pubsub_msg.AbstractPubSubMsg]:
    return self._current_msg

  @current_msg.setter
  def current_msg(self, msg: abstract_pubsub_msg.AbstractPubSubMsg):
    self._current_msg = msg
    self._message_ack_nack = ''
    self._current_msg_start_time = time.time()
    if msg is None:
      self._ack_monitor.clear_pubsub_msg()
      cloud_logging_client.clear_log_signature()
    else:
      trace_id = msg.ingestion_trace_id
      cloud_logging_client.set_log_signature({
          ingest_const.LogKeywords.PUBSUB_MESSAGE_ID: msg.message_id,
          ingest_const.LogKeywords.DPAS_INGESTION_TRACE_ID: trace_id,
      })
      self._ack_monitor.set_pubsub_msg(msg, self._current_msg_start_time)

  def _clear_current_msg(self):
    self.current_msg = None

  def _test_log_message_timeout(self, ack_time_extension: float):
    """Tests and logs if time msg ack in less time than pub/sub requirement.

    Args:
      ack_time_extension: number of seconds ack extended
    """
    total_time_sec = time.time() - self._current_msg_start_time
    elapsed_time_sec = total_time_sec - ack_time_extension
    if not self._is_polling_pubsub():
      cloud_logging_client.info(
          'Ingest pipeline processing completed',
          {ingest_const.LogKeywords.TOTAL_TIME_SEC: str(total_time_sec)},
      )
    elif elapsed_time_sec >= self._current_subscription.ack_deadline_seconds:
      cloud_logging_client.error(
          'Ingest pipeline processing exceed subscription ack timeout',
          {
              ingest_const.LogKeywords.ELAPSED_TIME_BEYOND_EXTENSION_SEC: (
                  elapsed_time_sec
              ),
              ingest_const.LogKeywords.ACK_DEADLINE_SEC: (
                  self._current_subscription.ack_deadline_seconds
              ),
              ingest_const.LogKeywords.TOTAL_TIME_SEC: total_time_sec,
          },
      )
    elif (
        elapsed_time_sec
        >= 0.8 * self._current_subscription.ack_deadline_seconds
    ):
      cloud_logging_client.warning(
          'Ingest pipeline processing is approaching subscription ack timeout',
          {
              ingest_const.LogKeywords.ELAPSED_TIME_BEYOND_EXTENSION_SEC: (
                  elapsed_time_sec
              ),
              ingest_const.LogKeywords.ACK_DEADLINE_SEC: (
                  self._current_subscription.ack_deadline_seconds
              ),
              ingest_const.LogKeywords.TOTAL_TIME_SEC: total_time_sec,
          },
      )
    else:
      cloud_logging_client.info(
          'Ingest pipeline processing completed within ack timeout',
          {ingest_const.LogKeywords.TOTAL_TIME_SEC: str(total_time_sec)},
      )

  def __enter__(self):
    """Wraps pubsub_v1.SubscriberClient context managers' entries.

    Returns:
      Self
    """
    if self._pubsub_subscriber:
      self._pubsub_subscriber.__enter__()
    return self

  def __exit__(self, unused_type, unused_value, unused_traceback):
    """Wraps pubsub_v1.SubscriberClient context managers' exits.

    Automatically closes subscribers' gRPC channels when block is done.
    """
    cloud_logging_client.info('DPAS ingest pipeline shutting down')
    if self._pubsub_subscriber:
      self._pubsub_subscriber.__exit__(
          unused_type, unused_value, unused_traceback
      )
      self._validate_current_msg_processed()
    self._ack_monitor.shutdown()

  def is_acked(self) -> bool:
    """Returns true if message was acked."""
    return self._message_ack_nack == 'ack'

  def is_nacked(self) -> bool:
    """Returns true if message was not acked."""
    return self._message_ack_nack == 'nack'

  def ack(self, log_ack: bool = True):
    """Call to indicate Acks message, indicates message was processed.

    https://googleapis.dev/python/pubsub/latest/subscriber/index.html#pulling-a-subscription-synchronously

    Args:
      log_ack: Set to False to disable ack logging.
    """
    if self._message_ack_nack:
      cloud_logging_client.warning(
          f'Ack ignored. Message previously  {self._message_ack_nack}'
      )
    else:
      ack_time_extension = self._ack_monitor.clear_pubsub_msg()
      self._message_ack_nack = 'ack'
      if self._pubsub_subscriber:
        self._pubsub_subscriber.acknowledge(
            request={
                'subscription': self._current_subscription.subscription_path,
                'ack_ids': [self.current_msg.ack_id],
            }
        )
        if log_ack:
          cloud_logging_client.info('Acknowledged msg')
      if log_ack:
        self._test_log_message_timeout(ack_time_extension)

  def nack(self, retry_ttl: int = 0):
    """Call to indicate message was not handled and should be redelivered.

    Args:
      retry_ttl: time in seconds to wait before retrying. 0 == immediate
        https://googleapis.dev/python/pubsub/latest/subscriber/index.html#pulling-a-subscription-synchronously
          0 second deadline time out causes nack behavior and forces pub/sub msg
          reposting.
    """
    if self._message_ack_nack:
      cloud_logging_client.warning(
          f'Nack ignored. Message previously {self._message_ack_nack}'
      )
    else:
      self._ack_monitor.clear_pubsub_msg()
      self._message_ack_nack = 'nack'
      if self._pubsub_subscriber:
        self._pubsub_subscriber.modify_ack_deadline(
            request={
                'subscription': self._current_subscription.subscription_path,
                'ack_ids': [self.current_msg.ack_id],
                'ack_deadline_seconds': retry_ttl,  # ack_deadline_seconds
            }
        )
      cloud_logging_client.warning('Retrying msg')

  def _decode_pubsub_msg(
      self, msg: pubsub_v1.types.ReceivedMessage
  ) -> abstract_pubsub_msg.AbstractPubSubMsg:
    """Pass pubsub msg to decoder described in DICOM Gen.

       Allows decoder to implement decoder specific pubsub msg decoding.

    Args:
      msg: pubsub msg

    Returns:
      implementation of AbstractPubSubMsg
    """
    return self._current_subscription.msg_handler.decode_pubsub_msg(msg)

  def _no_message_received(
      self, response: pubsub_v1.types.PullResponse
  ) -> bool:
    """Test if no message was received.

    Logs no message was received at 10 min intervals (avoid overloading
    cloud ops) to provide a mechanism for pods to indicate that they are
    polling pub/sub for messages.

    Args:
      response: received pub/sub message

    Returns:
      True if no message was received. False if message received.
    """
    current_time = time.time()
    if response.received_messages:
      # Received a message update timers
      self._no_pubsub_msg_log_timeout = current_time
      self._last_pubsub_msg_time = current_time
      return False

    # if nothing is received; not an error
    if (
        current_time - self._no_pubsub_msg_log_timeout
        >= NO_PUBSUB_MSG_LOG_INTERVAL_SECONDS
    ):
      if self._last_pubsub_msg_time is None:
        last_msg_time_str = 'never'
      else:
        last_msg_time_str = str(current_time - self._last_pubsub_msg_time)
      cloud_logging_client.debug(
          'Polling for pub/sub msgs.',
          {'Received_Last_PubMsg(sec)': last_msg_time_str},
      )
      self._no_pubsub_msg_log_timeout = current_time
    return True

  def _set_current_subscription(self, subscription: _SubscriptionAssets):
    """Updates current subscription and any related config.

    Args:
      subscription: Subscription assets to update current subscription to.
    """
    self._current_subscription = subscription
    if self._is_ingesting_from_gcs_file_list():
      self._ack_monitor.set_subscription('')
    else:
      self._ack_monitor.set_subscription(subscription.subscription_path)

  def _sync_pull_one_msg(self) -> bool:
    """Sync pulls one pubsub message from subscription.

    Transformation pipeline converts WSI->DICOM. The container will have
    high storage & CPU requirements for each message. The message processing
    time will be large and the number of messages will be low.
    Processing messages synchronously.

    The subscriber pulls 1 message.

    Returns:
      True if message received.
    """
    if self._is_polling_pubsub():
      response = self._pubsub_subscriber.pull(
          request={
              'subscription': self._current_subscription.subscription_path,
              'max_messages': (
                  1  # Limit the subscriber to handle 1 message at a time.
              ),
          },
          return_immediately=False,
          retry=retry.Retry(deadline=1000),
      )

      if self._no_message_received(response):
        # if nothing is received; not an error; nothing to do.
        return False

      mlen = len(response.received_messages)
      if mlen > 1:
        cloud_logging_client.error(
            'Received multiple messages from pub/sub subscription expected 1.',
            {ingest_const.LogKeywords.MESSAGE_COUNT: str(mlen)},
        )
      msg = self._decode_pubsub_msg(response.received_messages[0])
    else:
      if (
          not self._is_ingesting_from_gcs_file_list()
          or not self._gcs_file_to_ingest_list
      ):
        self._clear_current_msg()
        return False
      gcs_file_path = self._gcs_file_to_ingest_list.pop()
      gcs_msg = gcs_file_msg.GCSFileMsg(gcs_file_path)
      if not gcs_msg.gcs_file_exists():
        cloud_logging_client.info(
            'Specified file does not exist', {'uri': gcs_file_path}
        )
        self.ack()
        self._clear_current_msg()
        return False
      cloud_logging_client.info(
          'Ingesting file specified in file list', {'uri': gcs_file_path}
      )
      msg = self._decode_pubsub_msg(gcs_msg.received_msg)
    self.current_msg = msg
    if msg.ignore:
      cloud_logging_client.debug(
          'Pub/sub msg acked and ignored.',
          {ingest_const.LogKeywords.URI: self.current_msg.uri},
      )
      self.ack(log_ack=False)
      self._clear_current_msg()
      return False
    return True

  def _process_msg_through_ingest_pipeline(self):
    """Runs image transformation pipeline on an AbstractPubSubMsg."""
    cloud_storage_client.reset_storage_client(self._project_id)
    self._current_subscription.msg_handler.process_message(self)
    self._validate_current_msg_processed()
    self._clear_current_msg()

  def run(self):
    """Runs polling client ingestion."""
    try:
      while True:
        for subscription in self._subscriptions:
          self._set_current_subscription(subscription)
          if not self._running():
            return
          if self._sync_pull_one_msg():
            self._process_msg_through_ingest_pipeline()
    except Exception as exp:
      cloud_logging_client.critical(
          'An unexpected exception occurred in the GKE container',
          exp,
          {
              ingest_const.LogKeywords.PUBSUB_SUBSCRIPTION: (
                  self._current_subscription.subscription_path
              )
          },
      )
      self.nack()
      raise
