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
"""Single instance local pydicom read cache.

Cache is used to speed repeated thread DICOM instance reads.
"""

import dataclasses
from typing import NewType, Optional, Union

import pydicom

from pathology.dicom_proxy import metadata_util
from pathology.shared_libs.pydicom_version_util import pydicom_version_util


PyDicomFilePath = NewType('PyDicomFilePath', str)

# DICOM Tag Keywords
_ICCPROFILE = 'ICCProfile'
_OPTICAL_PATH_SEQUENCE = 'OpticalPathSequence'


@dataclasses.dataclass(frozen=True)
class DicomICCProfile:
  dicom_tag_path: str
  icc_profile: Optional[bytes]
  color_space: str


def _has_icc_profile_tag(ds: pydicom.Dataset) -> bool:
  return _ICCPROFILE in ds and ds.ICCProfile is not None and bool(ds.ICCProfile)


def _get_color_space(ds: pydicom.Dataset) -> str:
  try:
    return str(ds.ColorSpace)
  except AttributeError:
    return ''


def _get_iccprofile_from_pydicom_dataset(
    ds: pydicom.Dataset,
) -> DicomICCProfile:
  """Returns ICC Profile embedded in pydicom dataset.

  Args:
    ds: PyDICOM dataset to search for ICCProfile.

  Returns:
    ICCProfile (bytes) or None if not found.
  """
  if _OPTICAL_PATH_SEQUENCE in ds:
    for index, opt in enumerate(ds.OpticalPathSequence):
      if _has_icc_profile_tag(opt):
        return DicomICCProfile(
            f'{_OPTICAL_PATH_SEQUENCE}/{index}/{_ICCPROFILE}',
            opt.ICCProfile,
            _get_color_space(opt),
        )
  if _has_icc_profile_tag(ds):
    return DicomICCProfile(_ICCPROFILE, ds.ICCProfile, _get_color_space(ds))
  return DicomICCProfile('', None, '')


def _get_iccprofile_from_local_dataset(
    path: Union[str, pydicom.Dataset],
) -> DicomICCProfile:
  """Returns ICC Profile (bytes) stored in file or in loaded pydicom dataset.

  Args:
    path: path to dicom instance or loaded pydicom dataset.

  Returns:
    ICC_Profile (bytes) or None if ICC Profile not defined in DICOM instance.
  """
  if isinstance(path, str):
    with pydicom.dcmread(path, defer_size='512 KB') as ds:
      return _get_iccprofile_from_pydicom_dataset(ds)
  return _get_iccprofile_from_pydicom_dataset(path)


class PyDicomSingleInstanceCache:
  """PyDicom single instance read cache."""

  def __init__(self, path: PyDicomFilePath):
    self._filepath = path
    with pydicom.dcmread(self._filepath) as ds:
      if metadata_util.is_transfer_syntax_encapsulated(
          ds.file_meta.TransferSyntaxUID
      ):
        self._encapsulated_frames = list(
            pydicom_version_util.generate_frames(
                ds.PixelData, ds.NumberOfFrames
            )
        )
      else:
        frame_size = int(len(ds.PixelData) / ds.NumberOfFrames)
        self._encapsulated_frames = []
        for byte_offset in range(0, ds.NumberOfFrames * frame_size, frame_size):
          self._encapsulated_frames.append(
              ds.PixelData[byte_offset : byte_offset + frame_size]
          )
      self._metadata = metadata_util.get_instance_metadata_from_local_instance(
          ds
      )
      self._dicom_icc_profile = _get_iccprofile_from_local_dataset(ds)

  @property
  def path(self) -> PyDicomFilePath:
    return self._filepath

  @property
  def metadata(self) -> metadata_util.DicomInstanceMetadata:
    """Returns DicomInstanceMetadata."""
    return self._metadata

  @property
  def icc_profile_path(self) -> str:
    """Returns ICC Profile path."""
    return self._dicom_icc_profile.dicom_tag_path

  @property
  def icc_profile(self) -> Optional[bytes]:
    """Returns ICC Profile."""
    return self._dicom_icc_profile.icc_profile

  @property
  def color_space(self) -> str:
    """Returns ICC Profile."""
    return self._dicom_icc_profile.color_space

  def get_encapsulated_frame(self, frame_number: int) -> bytes:
    """Returns encapsulated frame blob from DICOM file.

    Args:
      frame_number: Encapsulated frame number.

    Returns:
      Frame bytes.
    """
    return self._encapsulated_frames[frame_number]
