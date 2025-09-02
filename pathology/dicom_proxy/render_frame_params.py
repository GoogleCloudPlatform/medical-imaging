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
from typing import Optional, Sequence

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import proxy_const

# Types
_Compression = enum_types.Compression
_Interpolation = enum_types.Interpolation


@dataclasses.dataclass(frozen=True)
class Viewport:
  """Viewport for rendering.

  Data structure storing request parameters for implementing:
  https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_8.3.5.html#sect_8.3.5.1.3
  """

  _parameter: Sequence[str]

  def vw(self) -> int:
    return abs(int(self._parameter[0]))

  def vh(self) -> int:
    return abs(int(self._parameter[1]))

  def sx(self) -> int:
    try:
      return abs(int(self._parameter[2]))
    except (ValueError, IndexError) as _:
      return 0

  def sy(self) -> int:
    try:
      return abs(int(self._parameter[3]))
    except (ValueError, IndexError) as _:
      return 0

  def sw(self, default: int) -> int:
    try:
      return int(self._parameter[4])
    except (ValueError, IndexError) as _:
      return default

  def sh(self, default: int) -> int:
    try:
      return int(self._parameter[5])
    except (ValueError, IndexError) as _:
      return default

  def is_defined(self) -> bool:
    """Returns True if the viewport is defined."""
    if not self._parameter:
      return False
    try:
      _ = self.vw()
      _ = self.vh()
      return True
    except (ValueError, IndexError) as _:
      return False


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
  viewport: Optional[Viewport] = None  # Viewport for rendering.

  def is_viewport_defined(self) -> bool:
    """Returns True if the viewport is defined."""
    return self.viewport is not None and self.viewport.is_defined()

  def copy(self) -> RenderFrameParams:
    base_dict = dataclasses.asdict(self)
    base_dict['enable_caching'] = self.enable_caching
    if self.viewport is not None:
      base_dict['viewport'] = Viewport(**dataclasses.asdict(self.viewport))
    return RenderFrameParams(**base_dict)
