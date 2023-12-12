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
"""Structure to hold generic pub/sub message."""
import abc
from typing import Optional

from google.api_core import datetime_helpers
from google.cloud import pubsub_v1


class AbstractPubSubMsg(metaclass=abc.ABCMeta):
  """Decodes and stores received cloud pub/sub msg."""

  def __init__(self, msg: pubsub_v1.types.ReceivedMessage):
    self._ignore_msg = False
    self._received_msg = msg

  @property
  def received_msg(self) -> pubsub_v1.types.ReceivedMessage:
    return self._received_msg

  @property
  def event_type(self):
    return self._received_msg.message.attributes.get('eventType', '')

  @property
  def publish_time(self) -> datetime_helpers.DatetimeWithNanoseconds:
    return self._received_msg.message.publish_time

  @property
  def ack_id(self) -> str:
    """Returns pub/sub acknowledgment id."""
    return self._received_msg.ack_id

  @property
  def message_id(self) -> str:
    """Returns pub/sub message_id."""
    return self._received_msg.message.message_id

  @property
  @abc.abstractmethod
  def ingestion_trace_id(self) -> str:
    """Returns ID used to trace ingestion logs e2e."""

  @property
  @abc.abstractmethod
  def uri(self) -> str:
    """Returns URI to referenced resource."""

  @property
  @abc.abstractmethod
  def filename(self) -> Optional[str]:
    """Returns name of image file to upload."""

  def __str__(self) -> str:
    """Returns string to identify msg in logs."""
    return f'pubsub_message_id: {self.message_id}'

  @property
  def ignore(self) -> bool:
    """Returns true if received msg should be acked and ignored."""
    return self._ignore_msg

  @ignore.setter
  def ignore(self, val: bool):
    self._ignore_msg = val
