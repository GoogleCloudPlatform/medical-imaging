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
"""Tests for dicom image coordinate util."""
from absl.testing import absltest
from absl.testing import parameterized

from pathology.dicom_proxy import dicom_image_coordinate_util
from pathology.dicom_proxy import shared_test_util


class DicomImageCoordinateUtilTest(parameterized.TestCase):

  def _frame_coordinate_assert(  # pylint: disable=g-unreachable-test-method
      self,
      fc: dicom_image_coordinate_util.DicomFrameCoordinate,
      column_px: int,
      row_px: int,
  ) -> None:
    self.assertEqual((fc.frame_column, fc.frame_row), (column_px, row_px))

  def _pixel_coordinate_assert(  # pylint: disable=g-unreachable-test-method
      self,
      pc: dicom_image_coordinate_util.PixelCoordinate,
      column_px: int,
      row_px: int,
  ) -> None:
    self.assertEqual((pc.column_px, pc.row_px), (column_px, row_px))

  def _frame_pixel_coordinate_assert(  # pylint: disable=g-unreachable-test-method
      self,
      fpc: dicom_image_coordinate_util.FramePixelCoordinates,
      upper_left: dicom_image_coordinate_util.PixelCoordinate,
      lower_right: dicom_image_coordinate_util.PixelCoordinate,
  ) -> None:
    self.assertEqual(
        (
            (fpc.upper_left.column_px, fpc.upper_left.row_px),
            (fpc.lower_right.column_px, fpc.lower_right.row_px),
        ),
        (
            (upper_left.column_px, upper_left.row_px),
            (lower_right.column_px, lower_right.row_px),
        ),
    )

  def test_dicom_frame_coordinate_dataclass(self):
    frame_x = 1
    frame_y = 2
    fc = dicom_image_coordinate_util.DicomFrameCoordinate(frame_x, frame_y)

    self._frame_coordinate_assert(fc, frame_x, frame_y)

  def test_dicom_frame_coordinate_get_pixel_coordinates(self):
    frame_x = 1
    frame_y = 2
    fc = dicom_image_coordinate_util.DicomFrameCoordinate(frame_x, frame_y)
    metadata = shared_test_util.mock_multi_frame_test_metadata()

    fpc = fc.get_frame_pixel_coordinates(metadata)

    self._frame_coordinate_assert(fc, frame_x, frame_y)
    self._frame_pixel_coordinate_assert(
        fpc,
        dicom_image_coordinate_util.PixelCoordinate(
            metadata.columns * frame_x, metadata.rows * frame_y
        ),
        dicom_image_coordinate_util.PixelCoordinate(
            metadata.columns * (frame_x + 1) - 1,
            metadata.rows * (frame_y + 1) - 1,
        ),
    )
    self.assertEqual((fpc.width, fpc.height), (metadata.columns, metadata.rows))

  def test_pixel_coordinate_dataclass(self):
    column_px = 1
    row_px = 2
    px = dicom_image_coordinate_util.PixelCoordinate(column_px, row_px)
    self._pixel_coordinate_assert(px, column_px, row_px)

  def test_pixel_coordinate_get_frame_coordinate(self):
    metadata = shared_test_util.mock_multi_frame_test_metadata()
    column_px = 5
    row_px = 9
    px = dicom_image_coordinate_util.PixelCoordinate(column_px, row_px)
    fc = px.get_frame_coordinate(metadata)

    self._pixel_coordinate_assert(px, column_px, row_px)
    self.assertEqual(
        (fc.frame_column, fc.frame_row),
        (int(column_px / metadata.columns), int(row_px / metadata.rows)),
    )

  def test_pixel_coordinate_offset(self):
    column_px = 1
    row_px = 2
    translate_column = 5
    translate_row = 10
    px = dicom_image_coordinate_util.PixelCoordinate(column_px, row_px)
    translated_px = px.offset(translate_column, translate_row)

    self._pixel_coordinate_assert(px, column_px, row_px)
    self._pixel_coordinate_assert(
        translated_px, column_px + translate_column, row_px + translate_row
    )

  def test_pixel_coordinate_scale(self):
    column_px = 1
    row_px = 2
    factor = 3.0
    px = dicom_image_coordinate_util.PixelCoordinate(column_px, row_px)

    scaled_px = px.scale(factor)

    self._pixel_coordinate_assert(px, column_px, row_px)
    self._pixel_coordinate_assert(
        scaled_px, int(column_px * factor), int(row_px * factor)
    )

  def test_frame_pixel_coordinate_dataclass(self):
    upper_left = dicom_image_coordinate_util.PixelCoordinate(0, 1)
    lower_right = dicom_image_coordinate_util.PixelCoordinate(5, 6)

    fpc = dicom_image_coordinate_util.FramePixelCoordinates(
        upper_left, lower_right
    )

    self._frame_pixel_coordinate_assert(fpc, upper_left, lower_right)

  def test_frame_pixel_coordinate_scale(self):
    upper_left = dicom_image_coordinate_util.PixelCoordinate(1, 2)
    lower_right = dicom_image_coordinate_util.PixelCoordinate(5, 6)
    fpc = dicom_image_coordinate_util.FramePixelCoordinates(
        upper_left, lower_right
    )

    scale_factor = 3.0
    scaled_fpc = fpc.scale(scale_factor)

    self._frame_pixel_coordinate_assert(fpc, upper_left, lower_right)
    self._frame_pixel_coordinate_assert(
        scaled_fpc,
        upper_left.scale(scale_factor),
        lower_right.offset(1, 1).scale(scale_factor).offset(-1, -1),
    )

  def test_frame_pixel_coordinate_pad(self):
    upper_left = dicom_image_coordinate_util.PixelCoordinate(0, 1)
    lower_right = dicom_image_coordinate_util.PixelCoordinate(5, 6)
    fpc = dicom_image_coordinate_util.FramePixelCoordinates(
        upper_left, lower_right
    )
    pad_dim = 5

    padded_fpc = fpc.pad(pad_dim)

    self._frame_pixel_coordinate_assert(fpc, upper_left, lower_right)
    self._frame_pixel_coordinate_assert(
        padded_fpc,
        upper_left.offset(-pad_dim, -pad_dim),
        lower_right.offset(pad_dim, pad_dim),
    )

  def test_frame_pixel_coordinate_crop_to_instance(self):
    upper_left = dicom_image_coordinate_util.PixelCoordinate(-10, -10)
    lower_right = dicom_image_coordinate_util.PixelCoordinate(100, 100)
    fc_pc = dicom_image_coordinate_util.FramePixelCoordinates(
        upper_left, lower_right
    )
    md = shared_test_util.mock_multi_frame_test_metadata()

    fc_pc.crop_to_instance(md)

    self._frame_pixel_coordinate_assert(
        fc_pc,
        dicom_image_coordinate_util.PixelCoordinate(0, 0),
        dicom_image_coordinate_util.PixelCoordinate(
            md.total_pixel_matrix_columns - 1, md.total_pixel_matrix_rows - 1
        ),
    )

  def test_frame_pixel_coordinate_width(self):
    upper_left = dicom_image_coordinate_util.PixelCoordinate(0, 1)
    lower_right = dicom_image_coordinate_util.PixelCoordinate(5, 6)
    fc_pc = dicom_image_coordinate_util.FramePixelCoordinates(
        upper_left, lower_right
    )

    self.assertEqual(
        fc_pc.width, (lower_right.column_px - upper_left.column_px + 1)
    )

  def test_frame_pixel_coordinate_height(self):
    upper_left = dicom_image_coordinate_util.PixelCoordinate(0, 1)
    lower_right = dicom_image_coordinate_util.PixelCoordinate(5, 6)
    fc_pc = dicom_image_coordinate_util.FramePixelCoordinates(
        upper_left, lower_right
    )

    self.assertEqual(fc_pc.height, (lower_right.row_px - upper_left.row_px + 1))

  def test_frame_pixel_to_tuple(self):
    ul_x = 0
    ul_y = 1
    lr_x = 5
    lr_y = 6
    upper_left = dicom_image_coordinate_util.PixelCoordinate(ul_x, ul_y)
    lower_right = dicom_image_coordinate_util.PixelCoordinate(lr_x, lr_y)
    fc_pc = dicom_image_coordinate_util.FramePixelCoordinates(
        upper_left, lower_right
    )

    self.assertEqual(fc_pc.to_tuple, (ul_x, ul_y, lr_x, lr_y))

  def test_get_instance_frame_pixel_coordinates_uncropped(self):
    column_px = 15
    row_px = 5
    metadata = shared_test_util.mock_multi_frame_test_metadata()
    upper_left = dicom_image_coordinate_util.PixelCoordinate(column_px, row_px)
    lower_right = dicom_image_coordinate_util.PixelCoordinate(
        column_px + metadata.columns - 1, row_px + metadata.rows - 1
    )
    frame_index = int(
        ((row_px / metadata.rows) * metadata.frames_per_row)
        + (column_px / metadata.columns)
    )

    fpc = dicom_image_coordinate_util.get_instance_frame_pixel_coordinates(
        metadata, frame_index
    )
    self._frame_pixel_coordinate_assert(fpc, upper_left, lower_right)

  def test_get_instance_frame_pixel_coordinates_crop(self):
    metadata = shared_test_util.mock_multi_frame_test_metadata().downsample(2)
    row_px = (metadata.frames_per_column - 1) * metadata.rows
    column_px = (metadata.frames_per_row - 1) * metadata.columns

    upper_left = dicom_image_coordinate_util.PixelCoordinate(column_px, row_px)
    lower_right = dicom_image_coordinate_util.PixelCoordinate(
        metadata.total_pixel_matrix_columns - 1,
        metadata.total_pixel_matrix_rows - 1,
    )
    frame_index = int(
        ((row_px / metadata.rows) * metadata.frames_per_row)
        + (column_px / metadata.columns)
    )

    fpc = dicom_image_coordinate_util.get_instance_frame_pixel_coordinates(
        metadata, frame_index
    )
    self._frame_pixel_coordinate_assert(fpc, upper_left, lower_right)

  @parameterized.parameters([
      (0, (0, 0)),
      (1, (1, 0)),
      (10, (10, 0)),
      (13, (0, 1)),
      (14, (1, 1)),
      (25, (12, 1)),
      (26, (0, 2)),
  ])
  def test_get_instance_frame_coordinate(self, index, expected):
    metadata = shared_test_util.mock_multi_frame_test_metadata()

    fc = dicom_image_coordinate_util.get_instance_frame_coordinate(
        metadata, index
    )

    self.assertEqual((fc.frame_column, fc.frame_row), expected)

  @parameterized.parameters([
      ((-10, -10), (0, 0)),
      ((5, -10), (5, 0)),
      ((-10, 5), (0, 5)),
      ((10, 5), (10, 5)),
      ((65, 5), (64, 5)),
      ((5, 55), (5, 54)),
      ((100, 100), (64, 54)),
  ])
  def test_pixel_coordinate_crop_to_instance(self, pixel_coord, expected):
    fc = dicom_image_coordinate_util.PixelCoordinate(
        pixel_coord[0], pixel_coord[1]
    )
    self.assertIsNone(
        fc.crop_to_instance(shared_test_util.mock_multi_frame_test_metadata())
    )
    self.assertEqual((fc.column_px, fc.row_px), expected)


if __name__ == '__main__':
  absltest.main()
