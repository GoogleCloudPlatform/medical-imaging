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

_PYDICOM_MAJOR_VERSION = int((pydicom.__version__).split('.')[0])

if _PYDICOM_MAJOR_VERSION <= 2:
  from pathology.shared_libs.pydicom_version_util import pydicom_v2_version_util as pydicom_version_interface
else:
  from pathology.shared_libs.pydicom_version_util import pydicom_v3_version_util as pydicom_version_interface


def set_little_endian_explicit_vr(dcm: pydicom.FileDataset) -> None:
  pydicom_version_interface.set_little_endian_explicit_vr(dcm)


def save_as_validated_dicom(dcm: pydicom.Dataset, filename: str) -> None:
  pydicom_version_interface.save_as_validated_dicom(dcm, filename)


def get_frame_offset_table(dcm: pydicom.Dataset) -> Sequence[int]:
  return pydicom_version_interface.get_frame_offset_table(dcm)


def has_basic_offset_table(dcm: pydicom.Dataset) -> bool:
  return pydicom_version_interface.has_basic_offset_table(dcm)


def generate_frames(buffer: bytes, number_of_frames: int) -> Iterator[bytes]:
  return pydicom_version_interface.generate_frames(buffer, number_of_frames)
