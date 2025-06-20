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
from typing import NewType, Optional, Sequence, Union

import pydicom

from pathology.dicom_proxy import metadata_util


PyDicomFilePath = NewType('PyDicomFilePath', str)

# DICOM Tag Keywords
_ICCPROFILE = 'ICCProfile'
_OPTICAL_PATH_SEQUENCE = 'OpticalPathSequence'
_PYDICOM_MAJOR_VERSION = int((pydicom.__version__).split('.')[0])


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


class _InstanceFrameAccessor(Sequence[bytes]):
  """Class to access frames of a DICOM instance."""

  def __init__(self, ds: pydicom.FileDataset):
    self._unencapsulated_step = 0
    self._byte_order = (
        'little'
        if ds.file_meta.TransferSyntaxUID != '1.2.840.10008.1.​2.​2'
        else 'big'
    )
    if 'PixelData' not in ds or not ds.PixelData:
      self._offset_table = []
      self._pixel_data = b''
      self._number_of_frames = 0
      self._basic_offset_table_offset = 0
      return
    self._pixel_data = ds.PixelData
    try:
      self._number_of_frames = int(ds.NumberOfFrames)
    except (TypeError, ValueError, AttributeError) as _:
      self._number_of_frames = 1
    if not metadata_util.is_transfer_syntax_encapsulated(
        ds.file_meta.TransferSyntaxUID
    ):
      self._basic_offset_table_offset = 0
      try:
        self._unencapsulated_step = int(
            ds.Columns
            * ds.Rows
            * ds.SamplesPerPixel
            * int(ds.BitsAllocated / 8)
        )
      except (TypeError, ValueError, AttributeError) as _:
        self._unencapsulated_step = int(
            len(ds.PixelData) / self._number_of_frames
        )
      return
    # encapsulated pixel data always starts with 4 byte tag
    # length of basic offset table if it present.
    if len(self._pixel_data) <= 8:
      self._offset_table = []
      self._pixel_data = b''
      self._number_of_frames = 0
      self._basic_offset_table_offset = 0
      return
    self._basic_offset_table_offset = (
        int.from_bytes(
            self._pixel_data[4:8], byteorder=self._byte_order, signed=False
        )
        + 8
    )
    if 'ExtendedOffsetTable' in ds:
      # if dicom has extended offset table, use it.
      table = ds.ExtendedOffsetTable
      self._offset_table = [
          int.from_bytes(
              table[i : i + 8],
              byteorder=self._byte_order,
              signed=False,
          )
          for i in range(0, len(table), 8)
      ]
      if len(self._offset_table) == self._number_of_frames:
        return
    if self._basic_offset_table_offset > 8:
      # if dicom has basic offset table, use it.
      # pytype: disable=module-attr
      if _PYDICOM_MAJOR_VERSION <= 2:
        fp = pydicom.filebase.DicomBytesIO(self._pixel_data)
        fp.is_little_endian = ds.is_little_endian
        _, self._offset_table = pydicom.encaps.get_frame_offsets(fp)
      else:
        self._offset_table = pydicom.encaps.parse_basic_offsets(
            self._pixel_data,
            endianness='<' if self._byte_order == 'little' else '>',
        )
      # pytype: enable=module-attr
      if len(self._offset_table) == self._number_of_frames:
        return
    # derive encapsulated data offset table from pixel data.
    offset = self._basic_offset_table_offset
    # Data encoded [basic offset table (tag 4 bytes, length 4 bytes),
    # (tag (4 bytes), length (4 bytes), data) * number of frames]
    pixel_data_length = len(self._pixel_data)
    self._offset_table = []
    for _ in range(self._number_of_frames):
      self._offset_table.append(offset - self._basic_offset_table_offset)
      length = int.from_bytes(
          self._pixel_data[offset + 4 : offset + 8],
          byteorder=self._byte_order,
          signed=False,
      )
      offset += 8 + length
      if offset > pixel_data_length:
        raise ValueError('Invalid pixel data')

  @property
  def size_of_pixel_data(self) -> int:
    """Return size in bytes of DICOM pixel data."""
    return len(self._pixel_data)

  def __len__(self) -> int:
    """Return the number of frames defined in the pixel data."""
    return self._number_of_frames

  def __getitem__(self, frame_list_index: int) -> bytes:
    """Return the frame bytes encoded in DICOM pixel data."""
    if self._unencapsulated_step != 0:
      # if unencapsulated, offset is a multiple of frame size.
      start = frame_list_index * self._unencapsulated_step
      end = start + self._unencapsulated_step
      if start < 0 or end > len(self._pixel_data):
        raise IndexError('Accessing pixel data number out of range.')
    else:
      # if encapsulated then the offset to the actual start of the encapsulated
      # frame data =  the size of the basic offset table
      # plus the offset from the table to the start of the frame.
      # plus 4 bytes for the tag indicating the start of the frame.
      # and 4 bytes defining the length of the frame.
      start = (
          self._basic_offset_table_offset
          + self._offset_table[frame_list_index]
          + 8
      )
      # get length of frame data
      frame_length = int.from_bytes(
          self._pixel_data[start - 4 : start],
          byteorder=self._byte_order,
          signed=False,
      )
      # compute index of end frame data byte.
      end = start + frame_length
      if start < 4 or end > len(self._pixel_data):
        raise IndexError('Accessing pixel data number out of range.')
    return self._pixel_data[start:end]


class PyDicomSingleInstanceCache:
  """PyDicom single instance read cache."""

  def __init__(self, path: PyDicomFilePath):
    self._filepath = path
    with pydicom.dcmread(self._filepath) as ds:
      self._metadata = metadata_util.get_instance_metadata_from_local_instance(
          ds
      )
      self._dicom_icc_profile = _get_iccprofile_from_local_dataset(ds)
      self._frames = _InstanceFrameAccessor(ds)

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

  def get_frame(self, frame_number: int) -> bytes:
    """Returns frame blob from DICOM file.

    Args:
      frame_number: frame number.

    Returns:
      Frame bytes.
    """
    return self._frames[frame_number]
