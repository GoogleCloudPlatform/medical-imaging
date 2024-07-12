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
"""Structure to hold DICOM store pub/sub message."""
from google.cloud import pubsub_v1
from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.pubsub_msgs import abstract_pubsub_msg


class DicomStorePubSubMsg(abstract_pubsub_msg.AbstractPubSubMsg):
  """Decodes and stores received DICOM store pub/sub msg."""

  def __init__(self, msg: pubsub_v1.types.ReceivedMessage):
    super().__init__(msg)
    self._dicom_instance = self._received_msg.message.data.decode('utf-8')
    if not self._dicom_instance:
      cloud_logging_client.error(
          'Error decoding DICOM store pub/sub msg. Missing DICOM instance data.'
      )
      self.ignore = True

  @property
  def filename(self) -> str:
    """Returns name of image file to upload."""
    return self._dicom_instance

  @property
  def ingestion_trace_id(self) -> str:
    return self.message_id

  @property
  def uri(self) -> str:
    """Returns URI to referenced DICOM store resource."""
    return f'{ingest_const.HEALTHCARE_API}/{self._dicom_instance}'
