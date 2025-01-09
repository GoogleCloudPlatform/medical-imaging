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
from typing import Iterator, Sequence

import pydicom
import pydicom.encaps


def set_little_endian_explicit_vr(dcm: pydicom.FileDataset) -> None:
  """NOP in pydicom V3."""
  del dcm
  return


def save_as_validated_dicom(dcm: pydicom.Dataset, filename: str) -> None:
  # pylint: disable=unexpected-keyword-arg
  # pytype: disable=wrong-keyword-args
  dcm.save_as(
      filename,
      enforce_file_format=True,
      little_endian=dcm.file_meta.TransferSyntaxUID != '1.2.840.10008.1.2.2',
      implicit_vr=dcm.file_meta.TransferSyntaxUID == '1.2.840.10008.1.2',
  )
  # pytype: enable=wrong-keyword-args
  # pylint: enable=unexpected-keyword-arg


def get_frame_offset_table(dcm: pydicom.Dataset) -> Sequence[int]:
  # pytype: disable=module-attr
  table = pydicom.encaps.parse_basic_offsets(dcm.PixelData)
  # This logic replicates pydicom V2 behavior. If the basic offset table is
  # undefined, V2 returns [0] this what what would be defined for the first
  # element regardless. For safty, we are replicating that behavior here.
  # When the migration code is deprecated replicating this behavior is not
  # necessary if the frame offset table is only used when it is actually
  # defined.
  if table:
    return table
  return [0]
  # pytype: enable=module-attr


def has_basic_offset_table(dcm: pydicom.Dataset) -> bool:
  # pytype: disable=module-attr
  return bool(pydicom.encaps.parse_basic_offsets(dcm.PixelData))
  # pytype: enable=module-attr


def generate_frames(buffer: bytes, number_of_frames: int) -> Iterator[bytes]:
  # pytype: disable=module-attr
  return pydicom.encaps.generate_frames(
      buffer, number_of_frames=int(number_of_frames)
  )
  # pytype: enable=module-attr
