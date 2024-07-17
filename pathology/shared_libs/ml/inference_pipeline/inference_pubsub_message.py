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
"""Pub/Sub message to be processed by inference pipeline."""
import dataclasses
import enum
import math
from typing import List, Optional, Union

import dataclasses_json


class PubSubValidationError(Exception):
  pass


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class DicomImageRef:
  dicomweb_path: str
  study_instance_uid: str
  series_instance_uid: str

  def __post_init__(self):
    if not self.dicomweb_path:
      raise PubSubValidationError('Missing DICOMweb path in image ref.')
    if not self.study_instance_uid:
      raise PubSubValidationError('Missing study instance UID in image ref.')
    if not self.series_instance_uid:
      raise PubSubValidationError('Missing series instance UID in image ref.')


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class InferenceConfig:
  """Inference configuration."""


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class InferencePubSubMessage:
  image_refs: List[DicomImageRef]
  config: InferenceConfig
  # Additional payload to pass through, not used in inference pipeline.
  additional_payload: Optional[str] = None

  def __post_init__(self):
    if not self.image_refs:
      raise PubSubValidationError('Missing image refs.')


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class OutputPubSubMessage:
  """Pub/Sub message output by inference pipeline, if configured."""

  # Metadata for input DICOM(s) used to run inference on.
  dicom_store_path: str
  study_instance_uid: str
  series_instance_uid: str
  sop_instance_uids: List[str]

  # Inference model metadata.
  model_name: str
  model_version: str

  # Pipeline runtime metadata.
  pipeline_start_time: float
  pipeline_runtime: float

  # Inference results.
  heatmap_image_path: Optional[str]
  predictions_path: Optional[str]
  whole_slide_score: Optional[float]

  # Additional pass-through payload from input inference pub/sub message.
  additional_payload: Optional[str]
