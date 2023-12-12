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
"""Abstract PollingClient interface definition."""
import abc
from typing import Optional

from transformation_pipeline.ingestion_lib.pubsub_msgs import abstract_pubsub_msg


class AbstractPollingClient(metaclass=abc.ABCMeta):
  """Abstract PollingClient interface definition."""

  @property
  @abc.abstractmethod
  def current_msg(self) -> Optional[abstract_pubsub_msg.AbstractPubSubMsg]:
    """Returns the current message, or None if no message is available."""

  @abc.abstractmethod
  def ack(self):
    """Call to indicate Acks message, indicates message was processed."""

  @abc.abstractmethod
  def nack(self, retry_ttl: int = 0):
    """Call to indicate message was not handed and should be redelivered.

    Args:
      retry_ttl: time in seconds to wait before retrying.  0 == immediate
    """

  @abc.abstractmethod
  def is_acked(self) -> bool:
    """Returns true if message was acked."""
