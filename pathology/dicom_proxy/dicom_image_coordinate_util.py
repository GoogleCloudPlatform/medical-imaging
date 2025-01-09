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
"""DICOM Image Coordinate Utilities."""
from __future__ import annotations

import dataclasses
from typing import Tuple

from pathology.dicom_proxy import metadata_util

_DicomInstanceMetadata = metadata_util.DicomInstanceMetadata


@dataclasses.dataclass
class DicomFrameCoordinate:
  """DICOM Instance Frame Coordinates.

  Assumes instance is fully tiled and all tiles are contained in a single
  instance with a single set of frames for each pixel.
  """

  frame_column: int
  frame_row: int

  def get_frame_pixel_coordinates(
      self, metadata: _DicomInstanceMetadata
  ) -> FramePixelCoordinates:
    """Returns the pixel coordinates of referenced DICOM frame.

    Args:
        metadata: Instance metadata to project frame coordinates into.

    Returns:
      FramePixelCoordinates
    """
    column_pixel_dim = metadata.columns
    row_pixel_dim = metadata.rows
    ul = PixelCoordinate(
        self.frame_column * column_pixel_dim, self.frame_row * row_pixel_dim
    )
    return FramePixelCoordinates(
        ul, ul.offset(column_pixel_dim - 1, row_pixel_dim - 1)
    )


def get_instance_frame_coordinate(
    metadata: _DicomInstanceMetadata, frame_index: int
) -> DicomFrameCoordinate:
  """Returns DicomFrameCoordinate for instance metadata and a frame index.

     Assumes instance is fully tiled and all tiles are contained in a single
     instance with a single set of frames for each pixel.

  Args:
    metadata: Instance metadata to generate frame coordinates.
    frame_index: Zero base, index in metadata frames.

  Returns:
    DicomFrameCoordinate
  """
  frm_per_row = metadata.frames_per_row
  return DicomFrameCoordinate(
      frame_index % frm_per_row, int(frame_index / frm_per_row)
  )


@dataclasses.dataclass
class PixelCoordinate:
  """Pixel coordinates in image instance.

  Assumes instance is fully tiled and all tiles are contained in a single
  instance with a single set of frames for each pixel.
  """

  column_px: int
  row_px: int

  def get_frame_coordinate(
      self, metadata: _DicomInstanceMetadata
  ) -> DicomFrameCoordinate:
    """Converts pixel coordinates into metadata frame coordinates.

    Args:
      metadata: DICOM instance metadata to project pixel coordinates into.

    Returns:
      DicomFrameCoordinate
    """
    return DicomFrameCoordinate(
        int(self.column_px / metadata.columns), int(self.row_px / metadata.rows)
    )

  def scale(self, factor: float) -> PixelCoordinate:
    """Returns pixel coordinate scaled by factor.

    Args:
      factor: Coordinate scaling factor.

    Returns:
      Scaled Pixel coordinate.
    """
    return PixelCoordinate(
        int(self.column_px * factor), int(self.row_px * factor)
    )

  def offset(self, column: int, row: int) -> PixelCoordinate:
    """Returns pixel coordinates translated by column and row.

    Args:
      column: Column offset to add to pixel coordinates.
      row: Row offset to add to pixel coordinates.

    Returns:
      PixelCoordinate
    """
    return PixelCoordinate(self.column_px + column, self.row_px + row)

  def crop_to_instance(self, metadata: _DicomInstanceMetadata) -> None:
    """Crops dimensions of image to the pixel dimensions of DICOM instance.

    Args:
      metadata: DICOM instance metadata crop pixel dimensions to.

    Returns:
      None
    """
    self.column_px = max(
        0, min(metadata.total_pixel_matrix_columns - 1, self.column_px)
    )
    self.row_px = max(0, min(metadata.total_pixel_matrix_rows - 1, self.row_px))


@dataclasses.dataclass
class FramePixelCoordinates:
  """Pixel coordinates of a frame within instance.

  Coordinates are inclusive.

  Assumes instance is fully tiled and all tiles are contained in a single
  instance with a single set of frames for each pixel.
  """

  upper_left: PixelCoordinate
  lower_right: PixelCoordinate

  def scale(self, factor: float) -> FramePixelCoordinates:
    """Returns scaled pixel coordinates.

    Args:
      factor: Factor to scale frame pixel coordinates by (float).

    Returns:
      FramePixelCoordinates
    """
    ul = self.upper_left.scale(factor)
    lr = PixelCoordinate(
        int((self.lower_right.column_px + 1) * factor - 1),
        int((self.lower_right.row_px + 1) * factor - 1),
    )
    return FramePixelCoordinates(ul, lr)

  def pad(self, pixel_dim: int) -> FramePixelCoordinates:
    """Pads frame pixel coordinates on all dimensions.

    Args:
      pixel_dim: Factor to scale frame pixel coordinates by (float).

    Returns:
      Padded FramePixelCoordinates
    """
    ul = self.upper_left.offset(-pixel_dim, -pixel_dim)
    lr = self.lower_right.offset(pixel_dim, pixel_dim)
    return FramePixelCoordinates(ul, lr)

  def crop_to_instance(self, metadata: _DicomInstanceMetadata) -> None:
    """Crops frame pixel coordinates to instance dimensions.

    Args:
      metadata: DICOM instance metadata crop pixel dimensions to.

    Returns:
      None
    """
    self.upper_left.crop_to_instance(metadata)
    self.lower_right.crop_to_instance(metadata)

  @property
  def to_tuple(self) -> Tuple[int, int, int, int]:
    """Returns a tuple representing frame pixel coordinates."""
    return (
        self.upper_left.column_px,
        self.upper_left.row_px,
        self.lower_right.column_px,
        self.lower_right.row_px,
    )

  @property
  def width(self) -> int:
    """Returns width in pixels of framed coordinates."""
    return self.lower_right.column_px - self.upper_left.column_px + 1

  @property
  def height(self) -> int:
    """Returns height in pixels of framed coordinates."""
    return self.lower_right.row_px - self.upper_left.row_px + 1


def get_instance_frame_pixel_coordinates(
    metadata: _DicomInstanceMetadata, frame_index: int
) -> FramePixelCoordinates:
  """Returns FramePixelCoordinates for instance metadata and a frame index.

     Assumes instance is fully tiled and all tiles are contained in a single
     instance with a single set of frames for each pixel.

  Args:
    metadata: Instance metadata to generate frame pixel coordinates.
    frame_index: Zero base, index in metadata frames.

  Returns:
    FramePixelCoordinates
  """
  coord = get_instance_frame_coordinate(
      metadata, frame_index
  ).get_frame_pixel_coordinates(metadata)
  coord.crop_to_instance(metadata)
  return coord
