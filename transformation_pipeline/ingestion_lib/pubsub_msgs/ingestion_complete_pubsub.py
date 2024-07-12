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
"""Utilities for generation of message sent at SVS -> DICOM completion."""
import collections
import dataclasses
import json
import typing
from typing import Any, List, Mapping, MutableMapping, NewType, Optional, Set, Tuple

import google.api_core
from google.cloud import pubsub_v1

from shared_libs.logging_lib import cloud_logging_client
from shared_libs.ml.inference_pipeline import inference_pubsub_message
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import wsi_dicom_file_ref
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import types

PIXEL_SPACING_WIDTH = ingest_const.PubSubKeywords.PIXEL_SPACING_WIDTH
STUDY_UID = ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID
SERIES_UID = ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID

# OOF model was trained at a pixel spacing of 0.000499mm per pixel, which
# represents canonical 20X magnification. We only run inference on slides for
# which a Level is present at approximately this pixel spacing. The pipeline
# also expects a Level at 16X downsampled of this spacing for computing a
# 'tissue mask' to identify non-whitespace regions for running inference on.
_MIN_PIXEL_WIDTH_INFERENCE_MODEL = 0.0005 * 0.8
_MAX_PIXEL_WIDTH_INFERENCE_MODEL = 0.0005 * 1.2
_MIN_PIXEL_WIDTH_TISSUE_MASK = _MIN_PIXEL_WIDTH_INFERENCE_MODEL * 16
_MAX_PIXEL_WIDTH_TISSUE_MASK = _MAX_PIXEL_WIDTH_INFERENCE_MODEL * 16


class CreatePubSubMessageError(Exception):
  """Error occurred during the creation of the Pub/Sub message."""


def read_inference_pipeline_config_from_json(
    config_path: str,
) -> inference_pubsub_message.InferenceConfig:
  """Returns inference config.

  Args:
    config_path: Path to JSON config to read from.

  Raises:
    ValueError if config is invalid.
  """
  with open(config_path, 'r') as f:
    config_str = f.read()
  try:
    config = inference_pubsub_message.InferenceConfig.from_json(config_str)
  except (
      json.decoder.JSONDecodeError,
      inference_pubsub_message.PubSubValidationError,
  ) as exp:
    raise ValueError('Invalid inference config.') from exp
  return config


def validate_message(pubsub: types.IngestCompletePubSub):
  """Validate Pub/Sub messages coming from Ingest pipeline."""
  for dicom_ref in pubsub.ingest_dicoms:
    for req_key in [STUDY_UID, SERIES_UID]:
      if req_key not in dicom_ref:
        raise ValueError(
            f'IngestPubSub dicom missing required field: {req_key} '
            f'in Pub/Sub {pubsub}'
        )
    if PIXEL_SPACING_WIDTH in dicom_ref:
      try:
        float(dicom_ref[PIXEL_SPACING_WIDTH])
      except Exception as exp:
        raise ValueError(
            'PIXEL_SPACING_WIDTH expected to be numeric, got: %s in Pub/Sub %s'
            % (dicom_ref[PIXEL_SPACING_WIDTH], pubsub)
        ) from exp


def _add_derived_pixel_spacing(
    dicomref_dict_lst: List[MutableMapping[str, str]]
):
  """Derives pixel spacing from image dimensions and adds to dict.

  Args:
    dicomref_dict_lst: List of dicom_file_ref derived dicts.
  """
  for dicom_ref_dict in dicomref_dict_lst:
    _get_pixel_spacing(
        dicom_ref_dict,
        ingest_const.DICOMTagKeywords.IMAGED_VOLUME_WIDTH,
        ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_COLUMNS,
        PIXEL_SPACING_WIDTH,
    )
    _get_pixel_spacing(
        dicom_ref_dict,
        ingest_const.DICOMTagKeywords.IMAGED_VOLUME_HEIGHT,
        ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_ROWS,
        ingest_const.PubSubKeywords.PIXEL_SPACING_HEIGHT,
    )


def _get_pixel_spacing(
    dicom_ref_dict: MutableMapping[str, str],
    volume_dim: str,
    total_pixel_matrix: str,
    pixel_spacing: str,
):
  """Computes pixelspacing for single dimension.

  Adds result to dict.

  Args:
    dicom_ref_dict: dicom_file_ref derived dict
    volume_dim: string keyword for volume dimension in mm
    total_pixel_matrix: string key word for pixel dimension
    pixel_spacing: string key word for pixel spacing
  """
  try:
    world_width = dicom_ref_dict[volume_dim]
    image_columns = dicom_ref_dict[total_pixel_matrix]
    pixel_spacing_width = float(world_width) / float(image_columns)
    dicom_ref_dict[pixel_spacing] = str(pixel_spacing_width)
  except (ValueError, ZeroDivisionError, KeyError) as _:
    pass


@dataclasses.dataclass(frozen=True)
class PubSubMsg:
  """Container holds pub/sub message and topic to send to."""

  topic_name: str
  message: bytes
  log: Optional[Mapping[str, str]]


# Dictionary for WSIDicomFileRef object.
_WSIDicomFileRefDict = NewType('_WSIDicomFileRefDict', MutableMapping[str, Any])
# Tuple of (StudyInstanceUID, SeriesInstanceUID).
_SlideId = NewType('_SlideId', Tuple[str, str])


def _create_oof_pipeline_pubsub_msg(
    dicomstore_web_path: str,
    topic_name: str,
    ingest_dicoms: List[_WSIDicomFileRefDict],
    duplicate_dicoms: List[_WSIDicomFileRefDict],
    pipeline_passthrough_params: Mapping[str, Any],
    log_struct: Optional[Mapping[str, str]],
) -> PubSubMsg:
  """Creates pub/sub msg signaling successful completion of ingest pipeline.

  Args:
    dicomstore_web_path: DICOM store web path instances written to.
    topic_name: Pub/Sub topic to publish msg to.
    ingest_dicoms: List of DICOM instances ingested into DICOM store
    duplicate_dicoms: List of DICOM instances that were already in store
    pipeline_passthrough_params: Params to pass OOF and ML Ingest.
    log_struct: optional struct for logging.

  Returns:
    PubSubMsg for OOF pipeline.
  """
  # Pass keys dictionary across in deterministic order.
  dct = collections.OrderedDict()
  for key in sorted(pipeline_passthrough_params):
    dct[key] = pipeline_passthrough_params[key]
  return PubSubMsg(
      topic_name,
      bytes(
          json.dumps(
              dataclasses.asdict(
                  types.IngestCompletePubSub(
                      ingest='success',
                      dicom_store=dicomstore_web_path,
                      ingest_dicoms=ingest_dicoms,
                      duplicate_dicoms=duplicate_dicoms,
                      pipeline_passthrough_params=dct,
                  )
              )
          ),
          'utf-8',
      ),
      log_struct,
  )


def _filter_slides(
    dicoms: List[_WSIDicomFileRefDict],
    min_pixel_spacing: float,
    max_pixel_spacing: float,
) -> Set[_SlideId]:
  """Returns slide ids between min and max pixel spacing (inclusive)."""
  filtered_slides = set()
  for dcm in dicoms:
    if (
        STUDY_UID in dcm
        and SERIES_UID in dcm
        and PIXEL_SPACING_WIDTH in dcm
        and float(dcm[PIXEL_SPACING_WIDTH]) >= min_pixel_spacing
        and float(dcm[PIXEL_SPACING_WIDTH]) <= max_pixel_spacing
    ):
      filtered_slides.add(_SlideId((dcm[STUDY_UID], dcm[SERIES_UID])))
  return filtered_slides


def _filter_slides_for_oof(
    dicoms: List[_WSIDicomFileRefDict],
) -> Set[_SlideId]:
  """Returns slide ids that contain pixel spacings required for running OOF."""
  oof_model_slides = _filter_slides(
      dicoms, _MIN_PIXEL_WIDTH_INFERENCE_MODEL, _MAX_PIXEL_WIDTH_INFERENCE_MODEL
  )
  oof_tissue_mask_slides = _filter_slides(
      dicoms, _MIN_PIXEL_WIDTH_TISSUE_MASK, _MAX_PIXEL_WIDTH_TISSUE_MASK
  )
  return oof_model_slides.intersection(oof_tissue_mask_slides)


def _create_inference_pipeline_pubsub_msg(
    inference_config: inference_pubsub_message.InferenceConfig,
    dicomstore_web_path: str,
    topic_name: str,
    ingest_dicoms: List[_WSIDicomFileRefDict],
    duplicate_dicoms: List[_WSIDicomFileRefDict],
    pipeline_passthrough_params: Mapping[str, Any],
    log_struct: Optional[Mapping[str, str]],
) -> PubSubMsg:
  """Creates inference pipeline pub/sub msg.

  Pub/sub message signals successful completion of transformation pipeline and
  triggers inference on 20X slides. Inference is run only on slides with both
  20X and 1.25X magnification levels.

  Args:
    inference_config: inference config to use in Pub/Sub message.
    dicomstore_web_path: DICOM store web path instances written to.
    topic_name: Pub/Sub topic to publish msg to.
    ingest_dicoms: List of DICOM instances ingested into DICOM store
    duplicate_dicoms: List of DICOM instances that were already in store
    pipeline_passthrough_params: Params to pass through inference pipeline.
    log_struct: optional struct for logging.

  Returns:
    PubSubMsg for inference pipeline.

  Raises:
    CreatePubSubMessageError: Error occurred creating inference PubSub Message.
  """
  oof_slides = _filter_slides_for_oof(ingest_dicoms + duplicate_dicoms)
  pubsub_image_refs = []
  for study, series in oof_slides:
    pubsub_image_refs.append(
        inference_pubsub_message.DicomImageRef(
            dicomweb_path=dicomstore_web_path,
            study_instance_uid=study,
            series_instance_uid=series,
        )
    )
  pubsub_additional_payload = json.dumps(pipeline_passthrough_params)
  try:
    inference_pubsub_msg = inference_pubsub_message.InferencePubSubMessage(
        image_refs=pubsub_image_refs,
        config=inference_config,
        additional_payload=pubsub_additional_payload,
    )
  except inference_pubsub_message.PubSubValidationError as exp:
    raise CreatePubSubMessageError(
        'Error occurred creating InferencePubSubMessage'
    ) from exp
  return PubSubMsg(
      topic_name, inference_pubsub_msg.to_json().encode('utf-8'), log_struct
  )


def create_ingest_complete_pubsub_msg(
    dicomstore_web_path: str,
    topic_name: str,
    ingest_dicoms: List[wsi_dicom_file_ref.WSIDicomFileRef],
    duplicate_dicoms: List[wsi_dicom_file_ref.WSIDicomFileRef],
    pipeline_passthrough_params: Mapping[str, Any],
    create_legacy_pipeline_msg: bool = True,
    inference_config: Optional[inference_pubsub_message.InferenceConfig] = None,
) -> PubSubMsg:
  """Creates pub/sub msg signaling successful completion of ingest pipeline.

  Args:
    dicomstore_web_path: DICOM store web path instances written to.
    topic_name: Pub/Sub topic to publish msg to.
    ingest_dicoms: List of DICOM instances ingested into DICOM store
    duplicate_dicoms: List of DICOM instances that were already in store
    pipeline_passthrough_params: Params to pass OOF and MLIngest.
    create_legacy_pipeline_msg: Whether to create legacy (OOF) pipeline pub/sub
      message. If false, generates pub/sub message for new inference pipeline.
    inference_config: config to use in Pub/Sub messages to inference pipeline.

  Returns:
    PubSubMsg

  Raises:
    RuntimeError: if inference config is missing for new inference pipeline.
    CreatePubSubMessageError: Error occurred creating inference PubSub Message.
  """
  # All files have same hash, StudyInstanceUID and SeriesInstanceUID.
  dicom_ref = None
  if ingest_dicoms:
    dicom_ref = ingest_dicoms[0]
  elif duplicate_dicoms:
    dicom_ref = duplicate_dicoms[0]
  if dicom_ref is not None:
    hash_val = dicom_ref.hash
    study_uid = dicom_ref.study_instance_uid
    series_uid = dicom_ref.series_instance_uid
  else:
    hash_val = 'HashValue is uninitialized'
    study_uid = 'StudyInstanceUID is uninitialized'
    series_uid = 'SeriesInstanceUID is uninitialized'

  # Convert list of DICOM_file_refs to list of dicts.
  ingest_dicoms = [
      typing.cast(_WSIDicomFileRefDict, dcm.dict()) for dcm in ingest_dicoms
  ]
  duplicate_dicoms = [
      typing.cast(_WSIDicomFileRefDict, dcm.dict()) for dcm in duplicate_dicoms
  ]

  # Add struct to cloud op logs.
  log_struct = {
      ingest_const.LogKeywords.PUBSUB_TOPIC_NAME: topic_name,
      ingest_const.LogKeywords.HASH: hash_val,
      ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: series_uid,
      ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: study_uid,
  }

  # Add pixel spacing to pub/sub msg.
  if ingest_dicoms:
    _add_derived_pixel_spacing(ingest_dicoms)
  if duplicate_dicoms:
    _add_derived_pixel_spacing(duplicate_dicoms)

  if create_legacy_pipeline_msg:
    return _create_oof_pipeline_pubsub_msg(
        dicomstore_web_path,
        topic_name,
        ingest_dicoms,
        duplicate_dicoms,
        pipeline_passthrough_params,
        log_struct,
    )
  if not inference_config:
    raise RuntimeError(
        'Missing OOF inference config. To use legacy pipeline, set '
        '--oof_legacy_inference_pipeline=true'
    )
  return _create_inference_pipeline_pubsub_msg(
      inference_config,
      dicomstore_web_path,
      topic_name,
      ingest_dicoms,
      duplicate_dicoms,
      pipeline_passthrough_params,
      log_struct,
  )


def publish_pubsubmsg(msg: PubSubMsg):
  """Publish Pub/Sub to topic.

  Args:
    msg: message to publish
  """
  if not msg.topic_name:
    return
  log = msg.log
  if log is None:
    log = {}
  with pubsub_v1.PublisherClient() as publisher:
    msg_elements_log = {
        'pubsub_topic_name': msg.topic_name,
        'pubsub_message': msg.message,
    }
    try:
      future = publisher.publish(msg.topic_name, msg.message)
      msg_id = future.result()
      cloud_logging_client.info(
          f'DICOM ingest complete pub/sub msg {msg_id} published.',
          log,
          msg_elements_log,
      )
    except google.api_core.exceptions.NotFound as exp:
      cloud_logging_client.critical(
          'Could not publish DICOM ingest complete pub/sub msg',
          log,
          msg_elements_log,
          exp,
      )
