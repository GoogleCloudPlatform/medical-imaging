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
"""Tests for dicom instance frame patch util."""
from absl.testing import absltest
from absl.testing import parameterized

from pathology.dicom_proxy import dicom_image_coordinate_util
from pathology.dicom_proxy import dicom_instance_frame_patch_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import image_util
from pathology.dicom_proxy import shared_test_util

# Types
_Interpolation = enum_types.Interpolation
_PixelCoordinate = dicom_image_coordinate_util.PixelCoordinate
_FramePixelCoordinates = dicom_image_coordinate_util.FramePixelCoordinates


class DicomInstanceFramePatchUtilTest(parameterized.TestCase):

  def test_get_downsampled_frame_coord_scale_factor(self):
    cache = shared_test_util.jpeg_encoded_pydicom_instance_cache()
    scale_factor = dicom_instance_frame_patch_util.get_downsampled_frame_coord_scale_factor(
        cache.metadata, cache.metadata.downsample(15.0)
    )
    self.assertEqual(int(scale_factor * 1000.0) / 1000.0, 15.157)

  def test_get_padded_frame_coordinates_no_padding(self):
    x = 5
    y = 5
    ul = _PixelCoordinate(x, y)
    metadata = shared_test_util.mock_multi_frame_test_metadata()
    lr = ul.offset(metadata.columns - 1, metadata.rows - 1)
    frame_pixel_coord = _FramePixelCoordinates(ul, lr)
    metadata = shared_test_util.mock_multi_frame_test_metadata()
    downsampling = 2
    interpolation = _Interpolation.AREA
    img_filter = image_util.GaussianImageFilter(downsampling, interpolation)

    coords = dicom_instance_frame_patch_util._get_padded_frame_coordinates(
        frame_pixel_coord, metadata, downsampling, interpolation, img_filter
    )

    self.assertFalse(coords.is_padded)
    self.assertEqual(coords.coordinates.to_tuple, frame_pixel_coord.to_tuple)

  def test_get_padded_frame_coordinates_padding(self):
    x = 10
    y = 15
    ul = _PixelCoordinate(x, y)
    metadata = shared_test_util.mock_multi_frame_test_metadata()
    lr = ul.offset(metadata.columns - 1, metadata.rows - 1)
    frame_pixel_coord = _FramePixelCoordinates(ul, lr)
    downsampling = 2
    interpolation = _Interpolation.CUBIC
    img_filter = image_util.GaussianImageFilter(downsampling, interpolation)

    coords = dicom_instance_frame_patch_util._get_padded_frame_coordinates(
        frame_pixel_coord, metadata, downsampling, interpolation, img_filter
    )

    expected_padded_frame = frame_pixel_coord.pad(10)
    self.assertTrue(coords.is_padded)
    self.assertEqual(
        coords.coordinates.to_tuple, expected_padded_frame.to_tuple
    )

  def test_get_padded_frame_coordinates_padding_and_crop(self):
    x = 0
    y = 0
    ul = _PixelCoordinate(x, y)
    metadata = shared_test_util.mock_multi_frame_test_metadata()
    lr = ul.offset(metadata.columns - 1, metadata.rows - 1)
    frame_pixel_coord = _FramePixelCoordinates(ul, lr)
    downsampling = 2
    interpolation = _Interpolation.CUBIC
    img_filter = image_util.GaussianImageFilter(downsampling, interpolation)

    coords = dicom_instance_frame_patch_util._get_padded_frame_coordinates(
        frame_pixel_coord, metadata, downsampling, interpolation, img_filter
    )

    expected_padded_frame = frame_pixel_coord.pad(10)
    expected_padded_frame.crop_to_instance(metadata)
    self.assertTrue(coords.is_padded)
    self.assertEqual(
        coords.coordinates.to_tuple, expected_padded_frame.to_tuple
    )

  @parameterized.parameters([
      (_PixelCoordinate(0, 0), _Interpolation.AREA, [0, 1, 5, 6]),
      (_PixelCoordinate(256, 0), _Interpolation.AREA, [2, 3, 7, 8]),
      (_PixelCoordinate(0, 256), _Interpolation.AREA, [10, 11]),
      (_PixelCoordinate(256, 256), _Interpolation.AREA, [12, 13]),
      (
          _PixelCoordinate(0, 0),
          _Interpolation.CUBIC,
          [0, 1, 2, 5, 6, 7, 10, 11, 12],
      ),
      (
          _PixelCoordinate(256, 0),
          _Interpolation.CUBIC,
          [1, 2, 3, 4, 6, 7, 8, 9, 11, 12, 13, 14],
      ),
      (_PixelCoordinate(0, 256), _Interpolation.CUBIC, [5, 6, 7, 10, 11, 12]),
      (
          _PixelCoordinate(256, 256),
          _Interpolation.CUBIC,
          [6, 7, 8, 9, 11, 12, 13, 14],
      ),
  ])
  def test_frame_indexes(self, ul, interpolation, expected_frame_list):
    downsampling = 2.0
    metadata = shared_test_util.jpeg_encoded_pydicom_instance_cache().metadata
    img_filter = image_util.GaussianImageFilter(downsampling, interpolation)
    lr = ul.offset(metadata.columns - 1, metadata.rows - 1)
    scale_factor = dicom_instance_frame_patch_util.get_downsampled_frame_coord_scale_factor(
        metadata, metadata.downsample(downsampling)
    )

    patch = dicom_instance_frame_patch_util.DicomInstanceFramePatch(
        _FramePixelCoordinates(ul, lr),
        downsampling,
        scale_factor,
        metadata,
        interpolation,
        img_filter,
    )

    self.assertEqual(patch.frame_indexes, expected_frame_list)

  @parameterized.parameters([
      (_PixelCoordinate(0, 0), _Interpolation.AREA, 0),
      (_PixelCoordinate(256, 0), _Interpolation.AREA, 2),
      (_PixelCoordinate(0, 256), _Interpolation.AREA, 10),
      (_PixelCoordinate(256, 256), _Interpolation.AREA, 12),
      (_PixelCoordinate(0, 0), _Interpolation.CUBIC, 0),
      (_PixelCoordinate(256, 0), _Interpolation.CUBIC, 1),
      (_PixelCoordinate(0, 256), _Interpolation.CUBIC, 5),
      (_PixelCoordinate(256, 256), _Interpolation.CUBIC, 6),
  ])
  def test_frame_upper_left_index(self, ul, interpolation, expected_index):
    downsampling = 2.0
    metadata = shared_test_util.jpeg_encoded_pydicom_instance_cache().metadata
    img_filter = image_util.GaussianImageFilter(downsampling, interpolation)
    lr = ul.offset(metadata.columns - 1, metadata.rows - 1)
    scale_factor = dicom_instance_frame_patch_util.get_downsampled_frame_coord_scale_factor(
        metadata, metadata.downsample(downsampling)
    )

    patch = dicom_instance_frame_patch_util.DicomInstanceFramePatch(
        _FramePixelCoordinates(ul, lr),
        downsampling,
        scale_factor,
        metadata,
        interpolation,
        img_filter,
    )

    self.assertEqual(patch._upper_left_most_frame_index, expected_index)

  @parameterized.parameters([
      (_PixelCoordinate(0, 0), _Interpolation.AREA, 2),
      (_PixelCoordinate(256, 0), _Interpolation.AREA, 2),
      (_PixelCoordinate(0, 256), _Interpolation.AREA, 2),
      (_PixelCoordinate(256, 256), _Interpolation.AREA, 2),
      (_PixelCoordinate(0, 0), _Interpolation.CUBIC, 3),
      (_PixelCoordinate(256, 0), _Interpolation.CUBIC, 4),
      (_PixelCoordinate(0, 256), _Interpolation.CUBIC, 3),
      (_PixelCoordinate(256, 256), _Interpolation.CUBIC, 4),
  ])
  def test_frame_per_row(self, ul, interpolation, expected_index):
    downsampling = 2.0
    metadata = shared_test_util.jpeg_encoded_pydicom_instance_cache().metadata
    img_filter = image_util.GaussianImageFilter(downsampling, interpolation)
    lr = ul.offset(metadata.columns - 1, metadata.rows - 1)
    scale_factor = dicom_instance_frame_patch_util.get_downsampled_frame_coord_scale_factor(
        metadata, metadata.downsample(downsampling)
    )

    patch = dicom_instance_frame_patch_util.DicomInstanceFramePatch(
        _FramePixelCoordinates(ul, lr),
        downsampling,
        scale_factor,
        metadata,
        interpolation,
        img_filter,
    )

    self.assertEqual(patch._frames_per_row, expected_index)

  @parameterized.parameters([
      (_PixelCoordinate(0, 0), 2, _Interpolation.AREA, '2xarea_0.png'),
      (_PixelCoordinate(256, 0), 2, _Interpolation.AREA, '2xarea_1.png'),
      (_PixelCoordinate(0, 256), 2, _Interpolation.AREA, '2xarea_2.png'),
      (_PixelCoordinate(256, 256), 2, _Interpolation.AREA, '2xarea_4.png'),
      (_PixelCoordinate(0, 0), 2, _Interpolation.CUBIC, '2xcubic_0.png'),
      (_PixelCoordinate(256, 0), 2, _Interpolation.CUBIC, '2xcubic_1.png'),
      (_PixelCoordinate(0, 256), 2, _Interpolation.CUBIC, '2xcubic_2.png'),
      (
          _PixelCoordinate(256, 256),
          2,
          _Interpolation.CUBIC,
          '2xcubic_4.png',
      ),
      (_PixelCoordinate(0, 0), 3.9, _Interpolation.AREA, '3.9xarea_0.png'),
      (
          _PixelCoordinate(0, 0),
          3.9,
          _Interpolation.CUBIC,
          '3.9xcubic_0.png',
      ),
      (_PixelCoordinate(0, 0), 8, _Interpolation.AREA, '8xarea_0.png'),
      (_PixelCoordinate(0, 0), 8, _Interpolation.CUBIC, '8xcubic_0.png'),
      (_PixelCoordinate(0, 0), 256, _Interpolation.AREA, '256xarea_0.png'),
      (
          _PixelCoordinate(0, 0),
          256,
          _Interpolation.CUBIC,
          '256xcubic_0.png',
      ),
  ])
  def test_frame_get_downsampled_image(
      self, ul, downsample, interpolation, filename
  ):
    path = shared_test_util.get_testdir_path(
        'dicom_instance_frame_patch_util', filename
    )
    with open(path, 'rb') as infile:
      expected_img_bytes = infile.read()
    cache = shared_test_util.jpeg_encoded_pydicom_instance_cache()
    frames = {
        fn: image_util.decode_image_bytes(cache.get_frame(fn), cache.metadata)
        for fn in range(cache.metadata.number_of_frames)
    }
    metadata = cache.metadata

    downsample = min(
        downsample,
        float(
            max(
                metadata.total_pixel_matrix_columns,
                metadata.total_pixel_matrix_rows,
            )
        ),
    )
    img_filter = image_util.GaussianImageFilter(downsample, interpolation)
    lr = ul.offset(metadata.columns - 1, metadata.rows - 1)
    fc = _FramePixelCoordinates(ul, lr)
    downsampled_metadata = metadata.downsample(downsample)
    fc.crop_to_instance(downsampled_metadata)
    scale_factor = dicom_instance_frame_patch_util.get_downsampled_frame_coord_scale_factor(
        metadata, downsampled_metadata
    )

    patch = dicom_instance_frame_patch_util.DicomInstanceFramePatch(
        fc, downsample, scale_factor, metadata, interpolation, img_filter
    )

    img = patch.get_downsampled_image(frames)
    encoded_img = image_util.encode_image(
        img, enum_types.Compression.PNG, None, None
    )
    self.assertEqual(img.shape, (metadata.rows, metadata.columns, 3))
    self.assertTrue(
        shared_test_util.rgb_image_almost_equal(encoded_img, expected_img_bytes)
    )


if __name__ == '__main__':
  absltest.main()
