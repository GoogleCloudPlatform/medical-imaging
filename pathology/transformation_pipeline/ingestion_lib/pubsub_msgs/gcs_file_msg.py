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
"""Represents files known to exist which are sitting on GCS."""

import json
from typing import Optional

import google.api_core
from google.cloud import pubsub_v1
from google.cloud import storage

from transformation_pipeline.ingestion_lib import cloud_storage_client as cloud_storage
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.pubsub_msgs import abstract_pubsub_msg


class GCSFileMsg(abstract_pubsub_msg.AbstractPubSubMsg):
  """Represents files known to exist which are sitting on GCS."""

  def __init__(self, gcs_file_path: str):
    self._received_msg = None
    self._ignore_msg = False
    self._ack_id = '0'
    self._messageid = uid_generator.generate_uid()

    # raises ValueError if uri is invalid format
    path_parts = cloud_storage.get_gsuri_bucket_and_path(gcs_file_path)
    self._bucket_name = path_parts.bucket_name
    self._filename = path_parts.file_path
    if not self.gcs_file_exists():
      raise ValueError(f'Blob does not exist {gcs_file_path}')

  @property
  def received_msg(self) -> pubsub_v1.types.ReceivedMessage:
    data = json.dumps({'name': self._filename, 'bucket': self._bucket_name})
    message = pubsub_v1.types.PubsubMessage(
        attributes={'eventType': 'OBJECT_FINALIZE'},
        data=data.encode('utf-8'),
        message_id=self._messageid,
        publish_time=google.api_core.datetime_helpers.DatetimeWithNanoseconds.now(),
    )
    return pubsub_v1.types.ReceivedMessage(
        ack_id=self._ack_id, message=message, delivery_attempt=1
    )

  @property
  def ack_id(self) -> str:
    """Returns pub/sub acknolgement id."""
    return self._ack_id

  @property
  def message_id(self) -> str:
    """Returns pub/sub message_id."""
    return self._messageid

  @property
  def ingestion_trace_id(self) -> str:
    return self.message_id

  @property
  def uri(self) -> str:
    """Returns URI to referenced file."""
    return f'gs://{self.bucket_name}/{self.filename}'

  @property
  def bucket_name(self) -> Optional[str]:
    """Return bucket referenced by msg."""
    return self._bucket_name

  @property
  def filename(self) -> Optional[str]:
    """Return file referenced by msg in bucket."""
    return self._filename

  def __str__(self) -> str:
    """Returns string to identify msg in logs."""
    return f'gcs_file_message_id: {self.message_id}'

  def gcs_file_exists(self) -> bool:
    """Returns true if file referenced in message exists.

    Returns:
      True if file exists.
    """
    try:
      storage_client = storage.Client()
      bucket = storage_client.get_bucket(self._bucket_name)
      blob = bucket.blob(self._filename)
      return blob.exists()
    except google.api_core.exceptions.NotFound:
      # Bucket not found
      return False
