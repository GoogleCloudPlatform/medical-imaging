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
"""Mock of Pub/Sub to support mocking to GCS style PubSub messages.

Mock is implemented sufficently to execute transform pipline.
"""

from __future__ import annotations

import contextlib
import dataclasses
import fcntl
import functools
import json
import os
import time
from typing import Any, Callable, List, Mapping, MutableSequence, Optional, Sequence, Tuple, Union
from unittest import mock

from absl.testing import absltest
import google.api_core
import google.cloud.pubsub_v1

from shared_libs.logging_lib import cloud_logging_client


_DEFAULT_VALUE = google.api_core.gapic_v1.method._MethodDefault._DEFAULT_VALUE  # pylint: disable=protected-access
_MESSAGE_ACK_DEADLINE = 600
_DEFAULT_MESSAGE_DELAY_SEC = 5.0


@dataclasses.dataclass
class _QueuedFile:
  path: str
  timeout: float


class _MockPubSubSubscriptionState:
  """State for an individual mock pub/sub subscription."""

  def __init__(
      self,
      project_id: str,
      subscription_name: str,
      bucket_name: str,
      directory_to_monitor: Union[str, absltest._TempDir],
      call_if_no_files: Optional[Callable[[], None]] = None,
      ack_deadline: float = _MESSAGE_ACK_DEADLINE,
      message_queue_delay_sec: float = _DEFAULT_MESSAGE_DELAY_SEC,
  ):
    if isinstance(directory_to_monitor, absltest._TempDir):
      directory_to_monitor = directory_to_monitor.full_path
    self._ack_id = 0
    self._project_id = project_id
    self._subscription_name = subscription_name
    self._bucket_name = bucket_name
    self._ack_deadline = ack_deadline
    self._pubsub_ack_dict = {}
    self._undelivered_file_dict_ackid = {}
    self._smallest_queued_file_timeout = 0
    self._monitor_directory = directory_to_monitor
    self._monitor_file_map = {}
    self._call_if_no_files = call_if_no_files
    # set message queue delay to avoid processing files if being written to.
    self._message_queue_delay_sec = message_queue_delay_sec
    cloud_logging_client.info(
        f'GCS pubs/sub mock monitoring: {self._monitor_directory}'
    )
    self._update_directory_monitor()

  def _update_directory_monitor(self):
    """Updates pub/sub files list."""
    removed_files = set(self._monitor_file_map.keys())
    files_to_add = set()
    for root, _, filenames in os.walk(self._monitor_directory):
      for filename in filenames:
        path = os.path.join(root, filename)
        file_state = tuple([val for val in os.stat(path)])
        existing_state = self._monitor_file_map.get(path)
        if existing_state is None:
          self._monitor_file_map[path] = file_state
          files_to_add.add(path)
        elif existing_state != file_state:
          self._monitor_file_map[path] = file_state
          files_to_add.add(path)
          removed_files.remove(path)
        else:
          removed_files.remove(path)
    schedualed_time = time.time() + self._message_queue_delay_sec
    new_files = set()
    for fname in files_to_add:
      previously_schedualed_id = self._undelivered_file_dict_ackid.get(fname)
      if previously_schedualed_id is not None:
        del self._pubsub_ack_dict[previously_schedualed_id]
      else:
        new_files.add(fname)
      self._ack_id += 1
      ack_str = str(self._ack_id)
      self._pubsub_ack_dict[ack_str] = _QueuedFile(fname, schedualed_time)
      self._undelivered_file_dict_ackid[fname] = ack_str
      self._smallest_queued_file_timeout = min(
          schedualed_time, self._smallest_queued_file_timeout
      )
    if new_files:
      cloud_logging_client.info(
          f'GCS pubs/sub mock dir: {self._monitor_directory} has changed'
          f' files: {new_files}'
      )
    for file_path in removed_files:
      del self._monitor_file_map[file_path]

  @property
  def path(self) -> str:
    return (
        f'projects/{self._project_id}/subscriptions/{self._subscription_name}'
    )

  @property
  def ack_deadline(self) -> float:
    return self._ack_deadline

  def acknowledge(self, ack_id: str):
    try:
      del self._pubsub_ack_dict[ack_id]
    except KeyError as exp:
      raise google.api_core.exceptions.InvalidArgument(
          'You have passed an invalid ack ID to the service'
      ) from exp

  def modify_ack_deadline(self, ack_id: str, timeout: int):
    new_time = timeout + time.time()
    try:
      self._pubsub_ack_dict[ack_id].timeout = new_time
    except KeyError as exp:
      raise google.api_core.exceptions.InvalidArgument(
          'You have passed an invalid ack ID to the service'
      ) from exp
    self._smallest_queued_file_timeout = min(
        new_time, self._smallest_queued_file_timeout
    )

  def _get_next_msg_ack_id(self) -> Optional[str]:
    """Returns the ack_id of the next message ti processs."""
    current_time = time.time()
    if self._smallest_queued_file_timeout > current_time:
      # Next message to process schedualed in the future.
      return None
    found_ack_id = None
    found_file_path = ''
    found_time = current_time + 1
    # interate over all messges find message with next timeout.
    for ack_id, queued_file in self._pubsub_ack_dict.items():
      if found_ack_id is None or queued_file.timeout < found_time:
        found_time = queued_file.timeout
        found_file_path = queued_file.path
        found_ack_id = ack_id
    if found_ack_id is None:
      self._smallest_queued_file_timeout = 0
      return None
    self._smallest_queued_file_timeout = found_time
    # test file is not open by something else.
    try:
      fd = os.open(found_file_path, os.O_RDONLY | os.O_NONBLOCK)
      try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fd, fcntl.LOCK_UN)
      finally:
        os.close(fd)
    except BlockingIOError:
      return None
    except FileNotFoundError:
      # Valid that file could be removed after having been added to queue.
      pass
    except OSError:
      return None
    if found_time > current_time:
      return None
    if found_file_path in self._undelivered_file_dict_ackid:
      del self._undelivered_file_dict_ackid[found_file_path]
    return found_ack_id

  def pull(
      self, pubsub_id: str
  ) -> Optional[google.cloud.pubsub_v1.types.ReceivedMessage]:
    """Pull one pubsub message."""
    self._update_directory_monitor()
    ack_id = self._get_next_msg_ack_id()
    if ack_id is None:
      if not self._pubsub_ack_dict and self._call_if_no_files is not None:
        self._call_if_no_files()
      return None
    file_path = self._pubsub_ack_dict[ack_id].path
    if file_path.startswith(self._monitor_directory):
      file_path = file_path[len(self._monitor_directory) :]
    file_path = file_path.lstrip('/')
    self._pubsub_ack_dict[ack_id].timeout = time.time() + self.ack_deadline
    return google.cloud.pubsub_v1.types.ReceivedMessage(
        ack_id=ack_id,
        message=google.cloud.pubsub_v1.types.PubsubMessage(
            message_id=pubsub_id,
            attributes={'eventType': 'OBJECT_FINALIZE'},
            data=json.dumps({
                'name': file_path,
                'bucket': self._bucket_name,
            }).encode('utf-8'),
        ),
    )


class _MockPubSubState:
  """General state for pub/sub mock."""

  def __init__(self, subscriptions: List[_MockPubSubSubscriptionState]):
    self._subscription = {sub.path: sub for sub in subscriptions}
    self._generate_pubsub_messages = True
    self._pubsub_message_id = 0

  def _get_subscription(self, name: str) -> _MockPubSubSubscriptionState:
    try:
      return self._subscription[name]
    except KeyError as exp:
      raise google.api_core.exceptions.NotFound(
          '404 Resource not found'
      ) from exp

  def get_subscription_ack_deadline(self, subscription: str) -> float:
    return self._get_subscription(subscription).ack_deadline

  def acknowledge(self, subscription: str, ack_id: str):
    self._get_subscription(subscription).acknowledge(ack_id)

  def modify_ack_deadline(self, subscription: str, ack_id: str, timeout: int):
    self._get_subscription(subscription).modify_ack_deadline(ack_id, timeout)

  def pull(
      self, subscription: str
  ) -> Optional[google.cloud.pubsub_v1.types.ReceivedMessage]:
    if not self._generate_pubsub_messages:
      return None
    self._pubsub_message_id += 1
    return self._get_subscription(subscription).pull(
        f'MockPubSubID_{time.time()}_{self._pubsub_message_id}'
    )

  def stop_generating_pubsub_messages(self) -> None:
    self._generate_pubsub_messages = False


class _MockSubscriberClient(contextlib.ExitStack):
  """Mocks pubsub subscriber client calls used by Transformation pipeline."""

  def __init__(self, mock_state: _MockPubSubState):
    super().__init__()
    self._mock_state = mock_state

  @classmethod
  def subscription_path(cls, project_id: str, subscription: str) -> str:
    """Returns subscription path for the pub-sub description."""
    return f'projects/{project_id}/subscriptions/{subscription}'

  def get_subscription(
      self,
      request: Optional[
          Union[
              google.cloud.pubsub_v1.types.GetSubscriptionRequest,
              Mapping[Any, Any],
          ]
      ] = None,
      *,
      subscription: Optional[str] = None,
      retry: Union[
          google.api_core.retry.Retry,
          google.api_core.gapic_v1.method._MethodDefault,
      ] = _DEFAULT_VALUE,
      timeout: Union[float, object] = _DEFAULT_VALUE,
      metadata: Sequence[Tuple[str, str]] = (),
  ) -> google.cloud.pubsub_v1.types.Subscription:
    """Returns mock description of the pub-sub description."""
    del retry, timeout, metadata
    if subscription is None and request is not None:
      subscription = request.get('subscription')
    sub_def = google.cloud.pubsub_v1.types.Subscription()
    # Ack deadline
    sub_def.ack_deadline_seconds = (
        self._mock_state.get_subscription_ack_deadline(subscription)
    )
    # Messages never expire.
    sub_def.expiration_policy.ttl = (
        google.cloud.pubsub_v1.types.duration_pb2.Duration(seconds=0)
    )
    return sub_def

  def acknowledge(
      self,
      request: Optional[
          Union[
              google.cloud.pubsub_v1.types.AcknowledgeRequest, Mapping[Any, Any]
          ]
      ] = None,
      *,
      subscription: Optional[str] = None,
      ack_ids: Optional[MutableSequence[str]] = None,
      retry: Union[
          google.api_core.retry.Retry,
          google.api_core.gapic_v1.method._MethodDefault,
      ] = _DEFAULT_VALUE,
      timeout: Union[float, object] = _DEFAULT_VALUE,
      metadata: Sequence[Tuple[str, str]] = (),
  ) -> None:
    """Acknowledge pubsub message."""
    del retry, timeout, metadata
    if subscription is None and request is not None:
      subscription = request.get('subscription')
    if ack_ids is None and request is not None:
      ack_ids = request.get('ack_ids')
    for ack_id in ack_ids:
      self._mock_state.acknowledge(subscription, ack_id)

  def modify_ack_deadline(
      self,
      request: Optional[
          Union[
              google.cloud.pubsub_v1.types.ModifyAckDeadlineRequest,
              Mapping[Any, Any],
          ]
      ] = None,
      *,
      subscription: Optional[str] = None,
      ack_ids: Optional[MutableSequence[str]] = None,
      ack_deadline_seconds: Optional[int] = None,
      retry: Union[
          google.api_core.retry.Retry,
          google.api_core.gapic_v1.method._MethodDefault,
      ] = _DEFAULT_VALUE,
      timeout: Union[float, object] = _DEFAULT_VALUE,
      metadata: Sequence[Tuple[str, str]] = (),
  ) -> None:
    """Modify the deadline of pubsub message."""
    del retry, timeout, metadata
    if subscription is None and request is not None:
      subscription = request.get('subscription')
    if ack_ids is None and request is not None:
      ack_ids = request.get('ack_ids')
    if ack_deadline_seconds is None and request is not None:
      ack_deadline_seconds = request.get('ack_deadline_seconds')
    for ack_id in ack_ids:
      self._mock_state.modify_ack_deadline(
          subscription, ack_id, ack_deadline_seconds
      )

  def pull(
      self,
      request: Optional[
          Union[google.cloud.pubsub_v1.types.PullRequest, Mapping[Any, Any]]
      ] = None,
      *,
      subscription: Optional[str] = None,
      return_immediately: Optional[bool] = None,
      max_messages: Optional[int] = None,
      retry: Union[
          google.api_core.retry.Retry,
          google.api_core.gapic_v1.method._MethodDefault,
      ] = _DEFAULT_VALUE,
      timeout: Union[float, object] = _DEFAULT_VALUE,
      metadata: Sequence[Tuple[str, str]] = (),
  ) -> google.cloud.pubsub_v1.types.PullResponse:
    """Pull one pubsub message."""
    del (
        return_immediately,
        retry,
        timeout,
        metadata,
    )
    if max_messages is None and request is not None:
      max_messages = request.get('max_messages')
    if max_messages is None or max_messages <= 0:
      raise google.api_core.exceptions.InvalidArgument(
          'You have passed an invalid argument to the service'
          ' (argument=max_messages)'
      )
    if subscription is None and request is not None:
      subscription = request.get('subscription', '')
    received_messages = []
    for _ in range(max_messages):
      pubsub_msg = self._mock_state.pull(subscription)
      if pubsub_msg is None:
        break
      received_messages.append(pubsub_msg)
    return google.cloud.pubsub_v1.types.PullResponse(
        received_messages=received_messages
    )


class MockPubSub(contextlib.ExitStack):
  """General Public mock for transform pipleine pub/sub."""

  def __init__(
      self,
      project_id: str,
      subscription_name: str,
      bucket_name: str,
      directory_to_monitor: Union[str, absltest._TempDir],
      call_if_no_files: Optional[Callable[[], None]] = None,
      ack_deadline: float = _MESSAGE_ACK_DEADLINE,
      message_queue_delay_sec: float = _DEFAULT_MESSAGE_DELAY_SEC,
  ):
    super().__init__()
    if isinstance(directory_to_monitor, absltest._TempDir):
      directory_to_monitor = directory_to_monitor.full_path
    self._project_id = project_id
    self._subscription_name = subscription_name
    self._bucket_name = bucket_name
    self._directory_to_monitor = directory_to_monitor
    self._call_if_no_files = call_if_no_files
    self._ack_deadline = ack_deadline
    self._message_queue_delay_sec = message_queue_delay_sec

  def __enter__(self) -> MockPubSub:
    # Context manager entry point.
    super().__enter__()
    try:
      self._mock_state = _MockPubSubState([
          _MockPubSubSubscriptionState(
              self._project_id,
              self._subscription_name,
              self._bucket_name,
              self._directory_to_monitor,
              self._call_if_no_files,
              self._ack_deadline,
              self._message_queue_delay_sec,
          )
      ])
      self.enter_context(
          mock.patch(
              'google.cloud.pubsub_v1.SubscriberClient',
              autospec=True,
              side_effect=functools.partial(
                  _MockSubscriberClient, self._mock_state
              ),
          )
      )
      self.enter_context(
          mock.patch.object(
              google.cloud.pubsub_v1.SubscriberClient,
              'subscription_path',
              side_effect=_MockSubscriberClient.subscription_path,
          )
      )
      return self
    except:
      # Exception occurred during context manager entry. Close any opened
      # context managers attached to this class.
      self.close()
      raise

  def stop_generating_pubsub_messages(self):
    self._mock_state.stop_generating_pubsub_messages()
