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
"""Structure to hold inference pipeline pub/sub message."""
import json
import re
from typing import Any, List, Mapping

from google.cloud import pubsub_v1

from shared_libs.logging_lib import cloud_logging_client
from shared_libs.ml.inference_pipeline import inference_pubsub_message
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_secondary_capture
from transformation_pipeline.ingestion_lib.pubsub_msgs import abstract_pubsub_msg


class _PubSubMsgParsingError(Exception):
  pass


class InferencePubSubMsg(abstract_pubsub_msg.AbstractPubSubMsg):
  """Decodes and stores pub/sub msg received from inference pipeline."""

  def __init__(
      self, msg: pubsub_v1.types.ReceivedMessage, parse_legacy_pipeline_msg=True
  ):
    super().__init__(msg)
    try:
      if parse_legacy_pipeline_msg:
        self._parse_legacy_pipeline_msg()
      else:
        self._parse_inference_pipeline_msg()
    except _PubSubMsgParsingError as exp:
      cloud_logging_client.error('Error decoding pub/sub msg.', exp)
      self._dicomstore_path = ''
      self._study_uid = ''
      self._series_uid = ''
      self._instances = []
      self._whole_slide_score = ''
      self._model_name = ''
      self._model_version = ''
      self._image_paths = []
      self._additional_params = {}
      self._filename = ''
      self._uri = ''
      self.ignore = True

  def _parse_inference_pipeline_msg(self):
    try:
      inference_msg = inference_pubsub_message.OutputPubSubMessage.from_json(
          self._received_msg.message.data.decode('utf-8')
      )
    except json.decoder.JSONDecodeError as exp:
      raise _PubSubMsgParsingError(
          f'Error decoding pub/sub msg: {self._received_msg.message.data}'
      ) from exp
    if (
        not inference_msg.dicom_store_path
        or not inference_msg.heatmap_image_path
    ):
      raise _PubSubMsgParsingError(
          f'Inference pub/sub msg missing fields: {inference_msg}'
      )
    self._dicomstore_path = inference_msg.dicom_store_path
    self._study_uid = inference_msg.study_instance_uid
    self._series_uid = inference_msg.series_instance_uid
    self._instances = inference_msg.sop_instance_uids
    self._whole_slide_score = inference_msg.whole_slide_score
    self._model_name = inference_msg.model_name
    self._model_version = inference_msg.model_version
    try:
      self._additional_params = json.loads(inference_msg.additional_payload)
    except json.decoder.JSONDecodeError as exp:
      raise _PubSubMsgParsingError(
          'Error decoding additional payload:'
          f' {inference_msg.additional_payload}'
      ) from exp
    self._filename = re.sub('^gs://.*?/', '', inference_msg.heatmap_image_path)
    self._image_paths = [self._filename]
    self._uri = inference_msg.heatmap_image_path

  def _parse_legacy_pipeline_msg(self):
    # decode received pub/sub message data field.
    try:
      pubsub_msg_data_dict = json.loads(
          self._received_msg.message.data.decode('utf-8')
      )
      cloud_logging_client.info('Received pub/sub msg.', pubsub_msg_data_dict)

      self._dicomstore_path = pubsub_msg_data_dict['dicomstore_path']
      slide_uid = pubsub_msg_data_dict['slide_uid']
      self._study_uid = slide_uid.split(':')[0]
      self._series_uid = slide_uid.split(':')[1]
      self._instances = pubsub_msg_data_dict['instances']
      self._whole_slide_score = pubsub_msg_data_dict['whole_slide_score']
      self._model_name = pubsub_msg_data_dict['model_name']
      self._model_version = pubsub_msg_data_dict['model_version']
      self._image_paths = pubsub_msg_data_dict['uploaded_heatmaps']
      self._additional_params = pubsub_msg_data_dict['additional_params']
      # For MVP, there is only one image path to the argmax heatmap
      self._filename = self._image_paths[0]
      output_bucket = pubsub_msg_data_dict['output_bucket']
      self._uri = f'gs://{output_bucket}/{self._filename}'
      if not output_bucket or not self._filename or not self._dicomstore_path:
        raise _PubSubMsgParsingError(
            f'OOF pub/sub msg missing fields: {pubsub_msg_data_dict}'
        )
    except (
        ValueError,
        TypeError,
        KeyError,
        IndexError,
        json.JSONDecodeError,
    ) as exp:
      raise _PubSubMsgParsingError(
          f'Error decoding pub/sub msg: {self._received_msg.message.data}'
      ) from exp

  @property
  def dicomstore_path(self) -> str:
    """Returns dicomstore path name as string."""
    return self._dicomstore_path

  @property
  def slide_uid(self) -> str:
    """Returns slide uid as a string."""
    return f'{self._study_uid}:{self._series_uid}'

  @property
  def ingestion_trace_id(self) -> str:
    return self.additional_params.get(
        ingest_const.OofPassThroughKeywords.DPAS_INGESTION_TRACE_ID,
        ingest_const.MISSING_INGESTION_TRACE_ID,
    )

  @property
  def study_uid(self) -> str:
    """Returns study uid as a string."""
    return self._study_uid

  @property
  def series_uid(self) -> str:
    """Returns series uid as a string."""
    return self._series_uid

  @property
  def whole_slide_score(self) -> str:
    """Returns out of focus score as a string."""
    return self._whole_slide_score

  @property
  def model_name(self) -> str:
    """Returns the model name as a string."""
    return self._model_name

  @property
  def model_version(self) -> str:
    """Returns model version as a string."""
    return self._model_version

  @property
  def instances(self) -> List[dicom_secondary_capture.DicomReferencedInstance]:
    """Returns list of WSI reference instances used to create heatmap."""
    # Make into list of referenced instances
    return [
        dicom_secondary_capture.DicomReferencedInstance(
            ingest_const.WSI_IMPLEMENTATION_CLASS_UID, i
        )
        for i in self._instances
    ]

  @property
  def image_paths(self) -> List[str]:
    """Returns list of all heatmap image paths to ingest."""
    return self._image_paths

  @property
  def filename(self) -> str:
    """Returns name of image file to upload."""
    return self._filename

  @property
  def uri(self) -> str:
    """Returns URI to referenced resource."""
    return self._uri

  @property
  def additional_params(self) -> Mapping[str, Any]:
    return self._additional_params
