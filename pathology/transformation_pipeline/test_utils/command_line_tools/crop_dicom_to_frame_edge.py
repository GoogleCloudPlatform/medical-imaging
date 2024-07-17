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
"""Command Line tool to crop DICOM to a region starting at frame boundary."""
import logging

from absl import flags
import pydicom


SOURCE_DICOM_INSTANCE_FLG = flags.DEFINE_string(
    'source_dicom_instance', None, 'File path to DICOM instance to read.'
)
ORIGIN_X_PIXEL_FLG = flags.DEFINE_integer(
    'origin_x_pixel',
    0,
    'Approximate origin column position (pixels) to crop DICOM instance from.',
)
ORIGIN_Y_PIXEL_FLG = flags.DEFINE_integer(
    'origin_y_pixel',
    0,
    'Approximate origin row position (pixels) to crop DICOM instance from.',
)
WIDTH_FLG = flags.DEFINE_integer(
    'width', None, 'Approximate width (pixels) of cropped DICOM image.'
)
HEIGHT_FLG = flags.DEFINE_integer(
    'height', None, 'Approximate height (pixels) of cropped DICOM image.'
)
OUTPUT_DICOM_INSTANCE_FLG = flags.DEFINE_string(
    'output_dicom_instance',
    None,
    'Path to DICOM instance file to write cropped image to.',
)


def _is_pixel_coordinate_frame_in_cropped_region(
    position: int, frame_dim: int, pixel_origin: int, output_dim: int
) -> bool:
  """Returns true if frame at pixel position has pixels in croppped region.

  Args:
    position: Position in pixels (in source image) along a dimension
      (Row/column).
    frame_dim: Dimension of DICOM frame along pixel Dimension in source image.
    pixel_origin: Pixel coordinate in source image to define as origin in
      cropped image.
    output_dim: Output image dimension.

  Returns:
    True if frame in source image has pixels which falls within cropped region.
  """
  return (
      position + frame_dim - 1 >= pixel_origin
      and position < pixel_origin + output_dim
  )


def _crop_dicom_to_frame_edge(
    input_dcm_filename: str,
    new_orig_x: int,
    new_orig_y: int,
    new_width: int,
    new_height: int,
    output_dcm_filename: str,
) -> None:
  """Generates a PNG of wsi-dicom image and saves to disk in working dir.

  Args:
    input_dcm_filename: DICOM file path.
    new_orig_x: Coordinate of upper left column start (will be expanded to align
      to start of a frame boundary).
    new_orig_y: Coordinate of upper left row start (will be expanded to align to
      start of a frame boundary).
    new_width: Approximate width to crop to (expanded to align to frame edge.).
    new_height: Approximate height to crop to (expanded to align to frame edge).
    output_dcm_filename: File name to write cropped DICOM to.
  """
  if not input_dcm_filename.lower().endswith('.dcm'):
    return
  new_orig_x = max(0, new_orig_x)
  new_orig_y = max(0, new_orig_y)
  with pydicom.dcmread(input_dcm_filename) as ds:
    if (
        new_width > ds.TotalPixelMatrixColumns
        or new_height > ds.TotalPixelMatrixRows
    ):
      return
    wsi_width = ds.TotalPixelMatrixColumns
    wsi_height = ds.TotalPixelMatrixRows
    frame_width = ds.Columns
    frame_height = ds.Rows
    if len(ds.pixel_array.shape) == 3:
      num_frames = 1
    else:
      num_frames = ds.NumberOfFrames

    # DICOM doesn't provide a way to set origin of an image to a location of
    # other than a the upper left coordinate of a frames (0,0).
    # To simplify image cropping and avoid having to re-layout (uncompress/
    # recompress the image frames) the user specified frame cropping in pixels
    # is adjusted to the nearest frame boundary.

    # Compute offset from user defined coordinates to frame boundary
    height_offset = new_orig_y % frame_height
    width_offset = new_orig_x % frame_width
    # Expand the image as needed, move origin to nearest frame boundary which
    # occures at or before the user specified origin.
    new_orig_x -= width_offset
    new_orig_y -= height_offset
    # Expand the size of the returned image to include the adjustment.
    new_width += width_offset
    new_height += height_offset
    if new_height + new_orig_y > wsi_height:
      new_height = wsi_height - new_orig_y
    if new_width + new_orig_x > wsi_width:
      new_width = wsi_width - new_orig_x
    if new_width <= 0 or new_height <= 0:
      return
    if new_height == wsi_height and new_width == wsi_width:
      return
    new_frame_list = []
    px = 0
    py = 0

    valid_row = _is_pixel_coordinate_frame_in_cropped_region(
        py, frame_height, new_orig_y, new_height
    )
    logging.info(
        'Cropping DICOM Origin(%d, %d) Dim(%d, %d)',
        new_orig_x,
        new_orig_y,
        new_width,
        new_height,
    )
    # Iterate over frames in DICOM, assume full tiling.
    for dicom_frame in pydicom.encaps.generate_pixel_data_frame(
        ds.PixelData, num_frames
    ):
      # Add frame to cropped dicom if frame at pixel coordinate px (column),
      # py (row), falls in cropped region.
      if valid_row and _is_pixel_coordinate_frame_in_cropped_region(
          px, frame_width, new_orig_x, new_width
      ):
        new_frame_list.append(dicom_frame)
      px += frame_width  # Increment pixel coordinate by frame width
      if px >= wsi_width:  # If at or past edge of source image move to start
        px = 0  # of next row.
        py += frame_height
        if py >= new_orig_y + new_height:
          break
        valid_row = _is_pixel_coordinate_frame_in_cropped_region(
            py, frame_height, new_orig_y, new_height
        )

    ds.ImagedVolumeWidth *= float(new_width) / float(wsi_width)
    ds.ImagedVolumeHeight *= float(new_height) / float(wsi_height)
    ds.TotalPixelMatrixColumns = new_width
    ds.TotalPixelMatrixRows = new_height
    ds.NumberOfFrames = len(new_frame_list)
    ds.PixelData = pydicom.encaps.encapsulate(new_frame_list)
    logging.info('Writing: %s', output_dcm_filename)
    ds.save_as(output_dcm_filename)


if __name__ == '__main__':
  _crop_dicom_to_frame_edge(
      SOURCE_DICOM_INSTANCE_FLG.value,
      ORIGIN_X_PIXEL_FLG.value,
      ORIGIN_Y_PIXEL_FLG.value,
      WIDTH_FLG.value,
      HEIGHT_FLG.value,
      OUTPUT_DICOM_INSTANCE_FLG.value,
  )
