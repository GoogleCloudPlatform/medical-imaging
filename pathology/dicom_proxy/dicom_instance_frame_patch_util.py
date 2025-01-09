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
"""DicomInstanceFramePatch utility.

Takes downsampled frame coordinates, downsampling factor, source metadata,
source imaging frames and computes downsampled image.
"""
import dataclasses
import itertools
import math
from typing import List, Mapping, NewType

import numpy as np

from pathology.dicom_proxy import dicom_image_coordinate_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import image_util
from pathology.dicom_proxy import metadata_util

_DicomInstanceMetadata = metadata_util.DicomInstanceMetadata
_FramePixelCoordinates = dicom_image_coordinate_util.FramePixelCoordinates
_Interpolation = enum_types.Interpolation

DownsampledFrameCoordScaleFactor = NewType(
    'DownsampledFrameCoordScaleFactor', float
)


@dataclasses.dataclass
class _PaddedFramePixelCoordinates:
  coordinates: _FramePixelCoordinates
  is_padded: bool


@dataclasses.dataclass
class _PixelCoordFrames:
  frames: list[int]  # List of frame numbers, frames describe rectangular patch.
  frames_per_column: int  # Number of frames per-column.


def _get_padded_frame_coordinates(
    frame_pixel_coord: _FramePixelCoordinates,
    metadata: _DicomInstanceMetadata,
    downsample: float,
    interpolation: _Interpolation,
    img_filter: image_util.GaussianImageFilter,
) -> _PaddedFramePixelCoordinates:
  """Returns padding required to downsample source imaging using interpolation.

  Args:
    frame_pixel_coord: Pixel coordinates to pad (referenced to instance).
    metadata: DICOM instance metadata to downsample.
    downsample: Downsampling factor to downsample image described in
      frame_pixel_coord. (1.0 = no downsampling)
    interpolation: Interpolation algorithm to use to perfrom downsampling.
    img_filter: Gaussian image filter to apply prior to downsampling.

  Returns:
    _PaddedFramePixelCoordinates
  """
  padding = img_filter.image_padding + image_util.get_cv2_interpolation_padding(
      interpolation
  )
  if padding <= 0:
    # if no padding
    return _PaddedFramePixelCoordinates(frame_pixel_coord, False)

  # round up padding if not multiple of downsampling.
  multiple = math.ceil(padding / downsample)
  padding = int(math.ceil(multiple * downsample))

  padded_frame_pixel_coord = frame_pixel_coord.pad(padding)

  padded_frame_pixel_coord.crop_to_instance(metadata)
  return _PaddedFramePixelCoordinates(padded_frame_pixel_coord, True)


def _get_pixel_coordinate_frames(
    frame_coords: _FramePixelCoordinates, metadata: _DicomInstanceMetadata
) -> _PixelCoordFrames:
  """Returns frame nums and frames-per-row for instance that describes coords.

  Args:
    frame_coords: Coordinates relative to DICOM instance.
    metadata: Metadata for DICOM instance.

  Returns:
    _PixelCoordFrames: list of frame numbers and number of frames per column.
  """
  upper_left = frame_coords.upper_left.get_frame_coordinate(metadata)
  lower_right = frame_coords.lower_right.get_frame_coordinate(metadata)
  frames_per_row = metadata.frames_per_row
  first_row_offset = upper_left.frame_row * frames_per_row
  rows = [
      np.arange(
          upper_left.frame_column + first_row_offset,
          lower_right.frame_column + 1 + first_row_offset,
      )
  ]
  for _ in range(upper_left.frame_row + 1, lower_right.frame_row + 1):
    rows.append(rows[-1] + frames_per_row)
  frame_list = list(itertools.chain(*rows))
  return _PixelCoordFrames(frame_list, len(rows))


def get_downsampled_frame_coord_scale_factor(
    source_metadata: _DicomInstanceMetadata,
    downsample_metadata: _DicomInstanceMetadata,
) -> DownsampledFrameCoordScaleFactor:
  """Returns scaling factor which transforms between source and dest img dims.

  Args:
    source_metadata: Source imaging metadata
    downsample_metadata: Destination (downsampled) image metadata.

  Returns:
    precise scale factor which transforms between source and downsampled
    image dimensions. Used to back transform destination pixel cooordintes
    to source pixel coordinates.
  """
  return DownsampledFrameCoordScaleFactor(
      min(
          source_metadata.total_pixel_matrix_columns
          / downsample_metadata.total_pixel_matrix_columns,
          source_metadata.total_pixel_matrix_rows
          / downsample_metadata.total_pixel_matrix_rows,
      )
  )


class DicomInstanceFramePatch:
  """Computes source frames and generates downsampled image."""

  def __init__(
      self,
      downsampled_frame_coords: _FramePixelCoordinates,
      downsample: float,
      downsampled_frame_coord_scale_factor: DownsampledFrameCoordScaleFactor,
      source_metadata: _DicomInstanceMetadata,
      interpolation: _Interpolation,
      img_filter: image_util.GaussianImageFilter,
  ):
    """DicomInstanceFramePatch constructor.

    Args:
      downsampled_frame_coords: Frame pixel coordinates in downsampled image.
      downsample: Downsampling factor.
      downsampled_frame_coord_scale_factor: Precise scale factor which
        transforms between downsampled dimensions and source dimensions !=
        downsampling factor if source and destination dimensions are not
        multiples. Ensures full area of source is correctly sampled.
      source_metadata: DICOM metadata for source image.
      interpolation: Image interpolation algorithm to use to downsample.
      img_filter: Gaussian image filter to apply prior to downsampling.
    """
    source_pixel_coords = downsampled_frame_coords.scale(
        downsampled_frame_coord_scale_factor
    )
    source_pixel_coords.crop_to_instance(source_metadata)
    padded_coords = _get_padded_frame_coordinates(
        source_pixel_coords,
        source_metadata,
        downsample,
        interpolation,
        img_filter,
    )

    pcs_frames = _get_pixel_coordinate_frames(
        padded_coords.coordinates, source_metadata
    )
    self._are_source_pixel_coordinates_padded = padded_coords.is_padded
    self._source_pixel_coordinates = padded_coords.coordinates
    self._frame_indexes = pcs_frames.frames
    self._frames_per_column = pcs_frames.frames_per_column
    self._downsampled_pixel_coordinates = downsampled_frame_coords
    self._source_metadata = source_metadata
    self._downsample = downsample
    self._interpolation = interpolation
    self._img_filter = img_filter

  @property
  def frame_indexes(self) -> List[int]:
    """Frame indexs in source instance required to generate downsampled img."""
    return self._frame_indexes

  @property
  def _upper_left_most_frame_index(self) -> int:
    """Returns frame index of the upper left most frame in patch."""
    return self._frame_indexes[0]

  @property
  def _frames_per_row(self) -> int:
    """Returns number of frames per row in patch."""
    if not self._frame_indexes:
      return 0
    return int(len(self._frame_indexes) / self._frames_per_column)

  def _get_padded_image(
      self, frame_images: Mapping[int, np.ndarray]
  ) -> np.ndarray:
    """Returns image described by padded image bounds.

    Args:
      frame_images: Mapping of frames (numbers: decoded raw bytes).

    Returns:
      Padded image (np.ndarray)
    """
    first_image = frame_images[self._upper_left_most_frame_index]
    frame_height, frame_width, samples = first_image.shape
    patch_x = 0
    patch_y = 0
    patch_y_end = frame_height
    patch_width = frame_width * self._frames_per_row
    patch_memory = np.zeros(
        (frame_height * self._frames_per_column, patch_width, samples),
        dtype=first_image.dtype,
    )
    for frame_number in self._frame_indexes:
      frame_image = frame_images[frame_number]

      patch_x_end = patch_x + frame_width
      patch_memory[patch_y:patch_y_end, patch_x:patch_x_end, ...] = frame_image[
          ...
      ]
      patch_x = patch_x_end
      if patch_x >= patch_width:
        patch_x = 0
        patch_y = patch_y_end
        patch_y_end += frame_height

    first_frame_coord = (
        dicom_image_coordinate_util.get_instance_frame_pixel_coordinates(
            self._source_metadata, self._upper_left_most_frame_index
        )
    )
    py = (
        self._source_pixel_coordinates.upper_left.row_px
        - first_frame_coord.upper_left.row_px
    )
    px = (
        self._source_pixel_coordinates.upper_left.column_px
        - first_frame_coord.upper_left.column_px
    )
    return patch_memory[
        py : py + self._source_pixel_coordinates.height,
        px : px + self._source_pixel_coordinates.width,
    ]

  def _clip_image_padding(self, img: np.ndarray) -> np.ndarray:
    """Removes padding from downsampled image.

    Args:
      img: Padded image.

    Returns:
      Image with padding clipped.
    """
    if not self._are_source_pixel_coordinates_padded:
      return img
    padded_ul = self._source_pixel_coordinates.upper_left
    downsampled_ul = self._downsampled_pixel_coordinates.upper_left
    column_pad = downsampled_ul.column_px - int(
        padded_ul.column_px / self._downsample
    )
    row_pad = downsampled_ul.row_px - int(padded_ul.row_px / self._downsample)
    return img[
        row_pad : row_pad + self._downsampled_pixel_coordinates.height,
        column_pad : column_pad + self._downsampled_pixel_coordinates.width,
    ]

  def get_downsampled_image(
      self, frame_images: Mapping[int, np.ndarray]
  ) -> np.ndarray:
    """Returns downsampled image described by patch.

    Args:
      frame_images: Map of source DICOM instance frame number to raw bytes.

    Returns:
      Raw frame bytes for downsampled image.
    """
    img = self._get_padded_image(frame_images)
    img = self._img_filter.filter(img)
    img = image_util.downsample_image(
        img,
        self._downsample,
        self._downsampled_pixel_coordinates.width,
        self._downsampled_pixel_coordinates.height,
        self._interpolation,
    )
    img = self._clip_image_padding(img)
    return self._source_metadata.pad_image_to_frame(img)
