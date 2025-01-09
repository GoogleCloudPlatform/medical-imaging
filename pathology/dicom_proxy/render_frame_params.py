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
"""RenderFrameParameters Dataclass."""
from __future__ import annotations

import dataclasses

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import proxy_const

# Types
_Compression = enum_types.Compression
_Interpolation = enum_types.Interpolation


@dataclasses.dataclass
class RenderFrameParams:
  """Parameters for downsampling functions."""

  downsample: float = 2.0  # Factor downsample image by 1.0 = No change
  interpolation: _Interpolation = _Interpolation.AREA  # Downsample algorithm
  compression: _Compression = _Compression.JPEG  # Output compression format.
  quality: int = 75  # _Compression quality effects JPEG and WEBP output
  # Color correction to perfrom
  icc_profile: enum_types.ICCProfile = proxy_const.ICCProfile.NO
  enable_caching: cache_enabled_type.CachingEnabled = (
      cache_enabled_type.CachingEnabled(True)
  )  # debug flag to disable caching.
  embed_iccprofile: bool = True  # Disable embedding ICCProfile in images.

  def copy(self) -> RenderFrameParams:
    return RenderFrameParams(**dataclasses.asdict(self))
