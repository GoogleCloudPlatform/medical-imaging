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
"""DICOM Proxy enum types."""
import enum
from typing import NewType


class Interpolation(enum.Enum):
  """Interpolation algorithm to use to downsample frames."""

  NEAREST = 0
  LINEAR = 1
  CUBIC = 2
  AREA = 3
  LANCZOS4 = 4


class Compression(enum.Enum):
  """Compression format to return imaging."""
  JPEG = 0
  PNG = 1
  WEBP = 2
  GIF = 3
  RAW = 4
  NUMPY = 5  # Used internally for downsampling instance optimization
  JPEG2000 = 6
  JPEGXL = 7
  JPEG_TRANSCODED_TO_JPEGXL = 8


ICCProfile = NewType('ICCProfile', str)
