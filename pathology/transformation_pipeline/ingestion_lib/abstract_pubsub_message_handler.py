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
"""Base class for pub/sub msg handler."""
import abc

from google.cloud import pubsub_v1

from transformation_pipeline.ingestion_lib import abstract_polling_client
from transformation_pipeline.ingestion_lib.pubsub_msgs import abstract_pubsub_msg


class AbstractPubSubMsgHandler(metaclass=abc.ABCMeta):
  """Base class for receiving pub/sub msgs."""

  @property
  def name(self) -> str:
    """Name of conversion class."""
    return self.__class__.__name__

  @abc.abstractmethod
  def process_message(
      self, polling_client: abstract_polling_client.AbstractPollingClient
  ):
    """Called to process received pub/sub msg.

    Args:
      polling_client: instance of polling client receiving msg.
    """

  @abc.abstractmethod
  def decode_pubsub_msg(
      self, msg: pubsub_v1.types.ReceivedMessage
  ) -> abstract_pubsub_msg.AbstractPubSubMsg:
    """Pass pubsub msg to decoder described in DICOM Gen.

       Allows decoder to implement decoder specific pubsub msg decoding.

    Args:
      msg: pubsub msg

    Returns:
      implementation of AbstractPubSubMsg
    """
