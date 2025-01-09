# Copyright 2024 Google LLC
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
"""Utility functions to aid porting pydicom between V2 and V3 API."""
import io
from typing import Iterator, Sequence

import pydicom
import pydicom.encaps


def set_little_endian_explicit_vr(dcm: pydicom.FileDataset) -> None:
  dcm.is_little_endian = True
  dcm.is_implicit_VR = False


def save_as_validated_dicom(dcm: pydicom.Dataset, filename: str) -> None:
  # pylint: disable=unexpected-keyword-arg
  # pytype: disable=wrong-keyword-args
  dcm.save_as(filename, write_like_original=False)
  # pylint: enable=unexpected-keyword-arg
  # pytype: enable=wrong-keyword-args


def get_frame_offset_table(dcm: pydicom.Dataset) -> Sequence[int]:
  file_like = pydicom.filebase.DicomFileLike(io.BytesIO(dcm.PixelData))
  # pytype: disable=module-attr
  file_like.is_little_endian = True
  _, offset_table = pydicom.encaps.get_frame_offsets(file_like)
  # pytype: enable=module-attr
  return offset_table


def has_basic_offset_table(dcm: pydicom.Dataset) -> bool:
  file_like = pydicom.filebase.DicomFileLike(io.BytesIO(dcm.PixelData))
  # pytype: disable=module-attr
  file_like.is_little_endian = True
  has_offset_table, _ = pydicom.encaps.get_frame_offsets(file_like)
  # pytype: enable=module-attr
  return has_offset_table


def generate_frames(buffer: bytes, number_of_frames: int) -> Iterator[bytes]:
  # pytype: disable=module-attr
  return pydicom.encaps.generate_pixel_data_frame(buffer, int(number_of_frames))
  # pytype: enable=module-attr
