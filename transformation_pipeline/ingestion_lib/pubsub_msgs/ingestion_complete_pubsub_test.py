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
"""Tests for ingestion_complete_pubsub."""

import json
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized

from ml.inference_pipeline.test_utils import inference_task_test_utils
from shared_libs.ml.inference_pipeline import inference_pubsub_message
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_util import dicom_test_util
from transformation_pipeline.ingestion_lib.pubsub_msgs import ingestion_complete_pubsub


_STUDY_UID = '1.2.3'
_SERIES_UID = '4.5.6'
_HASH = 'ABCDE'
_DICOM_STORE_WEB_PATH = 'fake-dicom-web-path'
_PUBSUB_TOPIC_NAME = 'fake-inference-pubsub-topic-name'
_PIPELINE_PASSTHROUGH_PARAMS = {'custom-param': 'value', 'other-param': 42}

_DICOM_FILE_REF_0 = dicom_test_util.create_mock_dpas_generated_dicom_fref(
    metadata_changes={
        ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: _STUDY_UID,
        ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: _SERIES_UID,
        ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: _HASH,
        ingest_const.DICOMTagKeywords.IMAGED_VOLUME_WIDTH: '50',
        ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_COLUMNS: '100',
    }
)
_DICOM_FILE_REF_1 = dicom_test_util.create_mock_dpas_generated_dicom_fref(
    metadata_changes={
        ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: _STUDY_UID,
        ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: _SERIES_UID,
        ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: _HASH,
        ingest_const.DICOMTagKeywords.IMAGED_VOLUME_WIDTH: '25',
        ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_COLUMNS: '100',
    }
)
_DICOM_FILE_REF_MODEL_PIXEL_SPACING = (
    dicom_test_util.create_mock_dpas_generated_dicom_fref(
        metadata_changes={
            ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: _STUDY_UID,
            ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: _SERIES_UID,
            ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: _HASH,
            ingest_const.DICOMTagKeywords.IMAGED_VOLUME_WIDTH: '51',
            ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_COLUMNS: '100000',
        }
    )
)
_DICOM_FILE_REF_TISSUE_MASK_PIXEL_SPACING = (
    dicom_test_util.create_mock_dpas_generated_dicom_fref(
        metadata_changes={
            ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: _STUDY_UID,
            ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: _SERIES_UID,
            ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: _HASH,
            ingest_const.DICOMTagKeywords.IMAGED_VOLUME_WIDTH: '799',
            ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_COLUMNS: '100000',
        }
    )
)
_PIXEL_SPACING_0 = '0.5'
_PIXEL_SPACING_1 = '0.25'

_INFERENCE_PUBSUB_MESSAGE_CONFIG = (
    inference_task_test_utils.create_inference_config()
)


class IngestionCompletePubsubTest(parameterized.TestCase):
  """Tests ingestion_complete_pubsub."""

  def test_read_inference_pipeline_config_from_json_failed_to_parse(self):
    json_file = self.create_tempfile(content='invalid config')
    with self.assertRaises(ValueError):
      _ = ingestion_complete_pubsub.read_inference_pipeline_config_from_json(
          json_file.full_path
      )

  def test_read_inference_pipeline_config_from_json_invalid_config(self):
    with mock.patch.object(
        inference_pubsub_message.ModelConfig,
        '__post_init__',
        autospec=True,
        return_value=None,
    ):
      invalid_config = inference_task_test_utils.create_inference_config(
          model_config=inference_task_test_utils.create_model_config(
              gcs_path=''
          )
      )
      json_file = self.create_tempfile(content=invalid_config.to_json())
    with self.assertRaises(ValueError):
      ingestion_complete_pubsub.read_inference_pipeline_config_from_json(
          json_file.full_path
      )

  def test_read_inference_pipeline_config_from_json_succeeds(self):
    json_file = self.create_tempfile(
        content=_INFERENCE_PUBSUB_MESSAGE_CONFIG.to_json()
    )
    config = ingestion_complete_pubsub.read_inference_pipeline_config_from_json(
        json_file.full_path
    )
    self.assertEqual(config, _INFERENCE_PUBSUB_MESSAGE_CONFIG)

  @parameterized.parameters(
      ('50', '100', '0.5'),
      ('50', '0', None),
      (None, '100', None),
      ('50', None, None),
      ('0', '100', '0.0'),
  )
  def test_get_pixel_spacing(self, volume, pixel, spacing):
    test = {}
    if volume:
      test['volume'] = volume
    if pixel:
      test['pixel'] = pixel
    ingestion_complete_pubsub._get_pixel_spacing(
        test, 'volume', 'pixel', 'spacing'
    )
    expected = {}
    if volume:
      expected['volume'] = volume
    if pixel:
      expected['pixel'] = pixel
    if spacing:
      expected['spacing'] = spacing
    self.assertDictEqual(test, expected)

  def test_add_derived_pixel_spacing(self):
    imaged_volume_width = ingest_const.DICOMTagKeywords.IMAGED_VOLUME_WIDTH
    total_pixel_matrix_columns = (
        ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_COLUMNS
    )
    pixel_spacing_width = ingest_const.PubSubKeywords.PIXEL_SPACING_WIDTH

    imaged_volume_height = ingest_const.DICOMTagKeywords.IMAGED_VOLUME_HEIGHT
    total_pixel_matrix_rows = (
        ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_ROWS
    )
    pixel_spacing_height = ingest_const.PubSubKeywords.PIXEL_SPACING_HEIGHT

    ingest_dicoms = [{
        imaged_volume_width: '50',
        total_pixel_matrix_columns: '100',
        imaged_volume_height: '40',
        total_pixel_matrix_rows: '200',
    }]
    ingestion_complete_pubsub._add_derived_pixel_spacing(ingest_dicoms)
    self.assertDictEqual(
        ingest_dicoms[0],
        {
            imaged_volume_width: '50',
            total_pixel_matrix_columns: '100',
            pixel_spacing_width: '0.5',
            imaged_volume_height: '40',
            total_pixel_matrix_rows: '200',
            pixel_spacing_height: '0.2',
        },
    )

  def test_create_ingest_complete_pubsub_msg_for_oof_pipeline_no_dicoms(self):
    pubsub_msg = ingestion_complete_pubsub.create_ingest_complete_pubsub_msg(
        dicomstore_web_path=_DICOM_STORE_WEB_PATH,
        topic_name=_PUBSUB_TOPIC_NAME,
        ingest_dicoms=[],
        duplicate_dicoms=[],
        pipeline_passthrough_params=_PIPELINE_PASSTHROUGH_PARAMS,
    )

    self.assertEqual(pubsub_msg.topic_name, _PUBSUB_TOPIC_NAME)
    self.assertEqual(
        pubsub_msg.log,
        {
            'StudyInstanceUID': 'StudyInstanceUID is uninitialized',
            'SeriesInstanceUID': 'SeriesInstanceUID is uninitialized',
            'hash': 'HashValue is uninitialized',
            'pubsub_topic_name': _PUBSUB_TOPIC_NAME,
        },
    )
    oof_pubsub_json = json.loads(pubsub_msg.message.decode('utf-8'))
    self.assertEqual(
        oof_pubsub_json,
        {
            'ingest': 'success',
            'dicom_store': _DICOM_STORE_WEB_PATH,
            'ingest_dicoms': [],
            'duplicate_dicoms': [],
            'pipeline_passthrough_params': _PIPELINE_PASSTHROUGH_PARAMS,
        },
    )

  @parameterized.named_parameters(
      (
          'ingest_and_duplicate_dicoms',
          [_DICOM_FILE_REF_0],
          [_DICOM_FILE_REF_1],
          [_PIXEL_SPACING_0],
          [_PIXEL_SPACING_1],
      ),
      (
          'ingest_dicoms_only',
          [_DICOM_FILE_REF_0, _DICOM_FILE_REF_1],
          [],
          [_PIXEL_SPACING_0, _PIXEL_SPACING_1],
          [],
      ),
      (
          'duplicate_dicoms_only',
          [],
          [_DICOM_FILE_REF_0, _DICOM_FILE_REF_1],
          [],
          [_PIXEL_SPACING_0, _PIXEL_SPACING_1],
      ),
  )
  def test_create_ingest_complete_pubsub_msg_for_oof_pipeline(
      self,
      ingest_dicoms,
      duplicate_dicoms,
      expected_ingest_dicoms_pixel_spacing,
      expected_duplicate_dicoms_pixel_spacing,
  ):
    pubsub_msg = ingestion_complete_pubsub.create_ingest_complete_pubsub_msg(
        dicomstore_web_path=_DICOM_STORE_WEB_PATH,
        topic_name=_PUBSUB_TOPIC_NAME,
        ingest_dicoms=ingest_dicoms,
        duplicate_dicoms=duplicate_dicoms,
        pipeline_passthrough_params=_PIPELINE_PASSTHROUGH_PARAMS,
    )

    self.assertEqual(pubsub_msg.topic_name, _PUBSUB_TOPIC_NAME)
    self.assertEqual(
        pubsub_msg.log,
        {
            'StudyInstanceUID': _STUDY_UID,
            'SeriesInstanceUID': _SERIES_UID,
            'hash': _HASH,
            'pubsub_topic_name': _PUBSUB_TOPIC_NAME,
        },
    )
    oof_pubsub_json = json.loads(pubsub_msg.message.decode('utf-8'))
    self.assertGreaterEqual(
        oof_pubsub_json.items(),
        {
            'ingest': 'success',
            'dicom_store': _DICOM_STORE_WEB_PATH,
            'pipeline_passthrough_params': _PIPELINE_PASSTHROUGH_PARAMS,
        }.items(),
    )
    self.assertLen(oof_pubsub_json['ingest_dicoms'], len(ingest_dicoms))
    for dcm_json, pixel_spacing in zip(
        oof_pubsub_json['ingest_dicoms'],
        expected_ingest_dicoms_pixel_spacing,
    ):
      self.assertGreaterEqual(
          dcm_json.items(),
          {
              ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: _STUDY_UID,
              ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: _SERIES_UID,
              ingest_const.PubSubKeywords.PIXEL_SPACING_WIDTH: pixel_spacing,
          }.items(),
      )
    self.assertLen(oof_pubsub_json['duplicate_dicoms'], len(duplicate_dicoms))
    for dcm_json, pixel_spacing in zip(
        oof_pubsub_json['duplicate_dicoms'],
        expected_duplicate_dicoms_pixel_spacing,
    ):
      self.assertGreaterEqual(
          dcm_json.items(),
          {
              ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: _STUDY_UID,
              ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: _SERIES_UID,
              ingest_const.PubSubKeywords.PIXEL_SPACING_WIDTH: pixel_spacing,
          }.items(),
      )

  @parameterized.named_parameters(
      ('no_dicoms', [], []),
      (
          'ingest_dicoms_missing_model_pixel_spacing',
          [_DICOM_FILE_REF_0, _DICOM_FILE_REF_TISSUE_MASK_PIXEL_SPACING],
          [],
      ),
      (
          'duplicate_dicoms_missing_model_pixel_spacing',
          [],
          [_DICOM_FILE_REF_0, _DICOM_FILE_REF_TISSUE_MASK_PIXEL_SPACING],
      ),
      (
          'dicoms_missing_tissue_mask_pixel_spacing',
          [_DICOM_FILE_REF_MODEL_PIXEL_SPACING, _DICOM_FILE_REF_1],
          [],
      ),
  )
  def test_create_ingest_complete_pubsub_msg_for_inference_pipeline_no_dicoms(
      self, ingest_dicoms, duplicate_dicoms
  ):
    with mock.patch.object(
        inference_pubsub_message.InferencePubSubMessage,
        '__post_init__',
        autospec=True,
        return_value=None,
    ):
      pubsub_msg = ingestion_complete_pubsub.create_ingest_complete_pubsub_msg(
          dicomstore_web_path=_DICOM_STORE_WEB_PATH,
          topic_name=_PUBSUB_TOPIC_NAME,
          ingest_dicoms=ingest_dicoms,
          duplicate_dicoms=duplicate_dicoms,
          pipeline_passthrough_params=_PIPELINE_PASSTHROUGH_PARAMS,
          create_legacy_pipeline_msg=False,
          inference_config=_INFERENCE_PUBSUB_MESSAGE_CONFIG,
      )
    with self.assertRaises(inference_pubsub_message.PubSubValidationError):
      inference_pubsub_message.InferencePubSubMessage.from_json(
          pubsub_msg.message.decode('utf-8')
      )

  @parameterized.named_parameters(
      (
          'ingest_and_duplicate_dicoms',
          [_DICOM_FILE_REF_MODEL_PIXEL_SPACING],
          [_DICOM_FILE_REF_TISSUE_MASK_PIXEL_SPACING],
      ),
      (
          'ingest_dicoms_only',
          [
              _DICOM_FILE_REF_MODEL_PIXEL_SPACING,
              _DICOM_FILE_REF_TISSUE_MASK_PIXEL_SPACING,
          ],
          [],
      ),
      (
          'duplicate_dicoms_only',
          [],
          [
              _DICOM_FILE_REF_MODEL_PIXEL_SPACING,
              _DICOM_FILE_REF_TISSUE_MASK_PIXEL_SPACING,
          ],
      ),
  )
  def test_create_ingest_complete_pubsub_msg_for_inference_pipeline(
      self, ingest_dicoms, duplicate_dicoms
  ):
    pubsub_msg = ingestion_complete_pubsub.create_ingest_complete_pubsub_msg(
        dicomstore_web_path=_DICOM_STORE_WEB_PATH,
        topic_name=_PUBSUB_TOPIC_NAME,
        ingest_dicoms=ingest_dicoms,
        duplicate_dicoms=duplicate_dicoms,
        pipeline_passthrough_params=_PIPELINE_PASSTHROUGH_PARAMS,
        create_legacy_pipeline_msg=False,
        inference_config=_INFERENCE_PUBSUB_MESSAGE_CONFIG,
    )

    self.assertEqual(pubsub_msg.topic_name, _PUBSUB_TOPIC_NAME)
    self.assertEqual(
        pubsub_msg.log,
        {
            'StudyInstanceUID': _STUDY_UID,
            'SeriesInstanceUID': _SERIES_UID,
            'hash': _HASH,
            'pubsub_topic_name': _PUBSUB_TOPIC_NAME,
        },
    )
    inference_pubsub = (
        inference_pubsub_message.InferencePubSubMessage.from_json(
            pubsub_msg.message.decode('utf-8')
        )
    )
    self.assertEqual(
        inference_pubsub.image_refs,
        [
            inference_pubsub_message.DicomImageRef(
                dicomweb_path=_DICOM_STORE_WEB_PATH,
                study_instance_uid=_STUDY_UID,
                series_instance_uid=_SERIES_UID,
            )
        ],
    )
    self.assertEqual(
        json.loads(inference_pubsub.additional_payload),
        _PIPELINE_PASSTHROUGH_PARAMS,
    )


if __name__ == '__main__':
  absltest.main()
