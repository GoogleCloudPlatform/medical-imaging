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
"""Tests for inference_pubsub_msg."""

import json
from absl.testing import absltest
from google.cloud import pubsub_v1
from shared_libs.ml.inference_pipeline import inference_pubsub_message
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_secondary_capture
from transformation_pipeline.ingestion_lib.pubsub_msgs import inference_pubsub_msg


_DICOM_STORE_PATH = 'dicom_store_path'
_STUDY_INSTANCE_UID = '1.22.301'
_SERIES_INSTANCE_UID = '1.22.401'
_SLIDE_UID = f'{_STUDY_INSTANCE_UID}:{_SERIES_INSTANCE_UID}'
_SOP_INSTANCE_UIDS = ['1.2.501', '1.2.601']
_DICOM_REFERENCE_INSTANCES = [
    dicom_secondary_capture.DicomReferencedInstance(
        ingest_const.WSI_IMPLEMENTATION_CLASS_UID, '1.2.501'
    ),
    dicom_secondary_capture.DicomReferencedInstance(
        ingest_const.WSI_IMPLEMENTATION_CLASS_UID, '1.2.601'
    ),
]
_MODEL_NAME = 'oof-model-20X'
_MODEL_VERSION = 'v0'
_HEATMAP_BUCKET = 'fake-bucket'
_HEATMAP_SUBPATH = 'path/to/image.png'
_HEATMAP_FULL_PATH = f'gs://{_HEATMAP_BUCKET}/{_HEATMAP_SUBPATH}'
_WHOLE_SLIDE_SCORE = 0.95
_ADDITIONAL_PAYLOAD = {'additional-value': 42}


class InferencePubsubMsgTest(absltest.TestCase):

  def test_legacy_oof_msg_missing_data(self):
    pubsub_msg = pubsub_v1.types.ReceivedMessage(
        ack_id='ack_id',
        message=pubsub_v1.types.PubsubMessage(message_id='message_id'),
    )
    parsed_msg = inference_pubsub_msg.InferencePubSubMsg(
        pubsub_msg, parse_legacy_pipeline_msg=True
    )
    self.assertTrue(parsed_msg.ignore)

  def test_legacy_oof_msg_valid(self):
    oof_msg_dict = {
        'dicomstore_path': _DICOM_STORE_PATH,
        'slide_uid': _SLIDE_UID,
        'instances': _SOP_INSTANCE_UIDS,
        'uploaded_heatmaps': [_HEATMAP_SUBPATH],
        'output_bucket': _HEATMAP_BUCKET,
        'uploaded_tf_records': [],
        'whole_slide_score': _WHOLE_SLIDE_SCORE,
        'model_name': _MODEL_NAME,
        'model_version': _MODEL_VERSION,
        'additional_params': _ADDITIONAL_PAYLOAD,
    }
    pubsub_msg = pubsub_v1.types.ReceivedMessage(
        ack_id='ack_id',
        message=pubsub_v1.types.PubsubMessage(
            message_id='message_id',
            data=json.dumps(oof_msg_dict).encode('utf-8'),
        ),
    )
    parsed_msg = inference_pubsub_msg.InferencePubSubMsg(
        pubsub_msg, parse_legacy_pipeline_msg=True
    )
    self.assertFalse(parsed_msg.ignore)
    self.assertEqual(parsed_msg.dicomstore_path, _DICOM_STORE_PATH)
    self.assertEqual(parsed_msg.slide_uid, _SLIDE_UID)
    self.assertEqual(parsed_msg.study_uid, _STUDY_INSTANCE_UID)
    self.assertEqual(parsed_msg.series_uid, _SERIES_INSTANCE_UID)
    self.assertEqual(parsed_msg.whole_slide_score, _WHOLE_SLIDE_SCORE)
    self.assertEqual(parsed_msg.model_name, _MODEL_NAME)
    self.assertEqual(parsed_msg.model_version, _MODEL_VERSION)
    self.assertEqual(parsed_msg.instances, _DICOM_REFERENCE_INSTANCES)
    self.assertEqual(parsed_msg.additional_params, _ADDITIONAL_PAYLOAD)
    self.assertEqual(
        parsed_msg.ingestion_trace_id, ingest_const.MISSING_INGESTION_TRACE_ID
    )
    self.assertEqual(parsed_msg.filename, _HEATMAP_SUBPATH)
    self.assertEqual(parsed_msg.uri, _HEATMAP_FULL_PATH)
    self.assertEqual(parsed_msg.image_paths, [_HEATMAP_SUBPATH])

  def test_inference_msg_missing_data(self):
    pubsub_msg = pubsub_v1.types.ReceivedMessage(
        ack_id='ack_id',
        message=pubsub_v1.types.PubsubMessage(message_id='message_id'),
    )
    parsed_msg = inference_pubsub_msg.InferencePubSubMsg(
        pubsub_msg, parse_legacy_pipeline_msg=False
    )
    self.assertTrue(parsed_msg.ignore)

  def test_inference_msg_valid(self):
    inference_msg = inference_pubsub_message.OutputPubSubMessage(
        dicom_store_path=_DICOM_STORE_PATH,
        study_instance_uid=_STUDY_INSTANCE_UID,
        series_instance_uid=_SERIES_INSTANCE_UID,
        sop_instance_uids=_SOP_INSTANCE_UIDS,
        model_name=_MODEL_NAME,
        model_version=_MODEL_VERSION,
        pipeline_start_time=1.0,
        pipeline_runtime=3.0,
        heatmap_image_path=_HEATMAP_FULL_PATH,
        predictions_path=None,
        whole_slide_score=_WHOLE_SLIDE_SCORE,
        additional_payload=json.dumps(_ADDITIONAL_PAYLOAD),
    )
    pubsub_msg = pubsub_v1.types.ReceivedMessage(
        ack_id='ack_id',
        message=pubsub_v1.types.PubsubMessage(
            message_id='message_id',
            data=inference_msg.to_json().encode('utf-8'),
        ),
    )
    parsed_msg = inference_pubsub_msg.InferencePubSubMsg(
        pubsub_msg, parse_legacy_pipeline_msg=False
    )
    self.assertFalse(parsed_msg.ignore)
    self.assertEqual(parsed_msg.dicomstore_path, _DICOM_STORE_PATH)
    self.assertEqual(parsed_msg.slide_uid, _SLIDE_UID)
    self.assertEqual(parsed_msg.study_uid, _STUDY_INSTANCE_UID)
    self.assertEqual(parsed_msg.series_uid, _SERIES_INSTANCE_UID)
    self.assertEqual(parsed_msg.whole_slide_score, _WHOLE_SLIDE_SCORE)
    self.assertEqual(parsed_msg.model_name, _MODEL_NAME)
    self.assertEqual(parsed_msg.model_version, _MODEL_VERSION)
    self.assertEqual(parsed_msg.instances, _DICOM_REFERENCE_INSTANCES)
    self.assertEqual(parsed_msg.additional_params, _ADDITIONAL_PAYLOAD)
    self.assertEqual(
        parsed_msg.ingestion_trace_id, ingest_const.MISSING_INGESTION_TRACE_ID
    )
    self.assertEqual(parsed_msg.filename, _HEATMAP_SUBPATH)
    self.assertEqual(parsed_msg.uri, _HEATMAP_FULL_PATH)
    self.assertEqual(parsed_msg.image_paths, [_HEATMAP_SUBPATH])


if __name__ == '__main__':
  absltest.main()
