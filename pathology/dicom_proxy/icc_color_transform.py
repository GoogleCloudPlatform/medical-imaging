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
"""Dataclass that hold ICC_Color Transformation."""
import dataclasses
from typing import Optional

from PIL import ImageCms

from pathology.dicom_proxy import enum_types


@dataclasses.dataclass(frozen=True)
class IccColorTransform:
  config: enum_types.ICCProfile
  color_transform: Optional[ImageCms.ImageCmsTransform]
  rendered_icc_profile: bytes
