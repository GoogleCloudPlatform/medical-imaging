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
"""Structure to hold pub/sub cloud storage message."""
import json

from google.cloud import pubsub_v1

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib.pubsub_msgs import abstract_pubsub_msg


class CloudStoragePubSubMsg(abstract_pubsub_msg.AbstractPubSubMsg):
  """Decodes and stores received cloud storage pub/sub msg."""

  def __init__(self, msg: pubsub_v1.types.ReceivedMessage):
    super().__init__(msg)
    # decode received pub/sub message data field.
    try:
      pubsub_msg_data_dict = json.loads(
          self._received_msg.message.data.decode('utf-8')
      )
      self._filename = pubsub_msg_data_dict['name']
      self._bucket_name = pubsub_msg_data_dict['bucket']
    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exp:
      cloud_logging_client.error('Error decoding pub/sub msg.', exp)
      self._filename = ''
      self._bucket_name = ''
      self.ignore = True

  @property
  def filename(self) -> str:
    """Returns file name as string."""
    return self._filename

  @property
  def ingestion_trace_id(self) -> str:
    return self.message_id

  @property
  def bucket_name(self) -> str:
    """Returns bucket name as string."""
    return self._bucket_name

  @property
  def uri(self) -> str:
    """Returns URI to referenced file."""
    return f'gs://{self.bucket_name}/{self.filename}'
