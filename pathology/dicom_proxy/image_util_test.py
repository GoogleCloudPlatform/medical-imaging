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
"""Tests for image util."""

import hashlib
import io
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import cv2
import numpy as np
import PIL.Image

from pathology.dicom_proxy import color_conversion_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import image_util
from pathology.dicom_proxy import render_frame_params
from pathology.dicom_proxy import shared_test_util

# Types
_Interpolation = enum_types.Interpolation
_Compression = enum_types.Compression


def _img_bytes_hash(decoded_img: np.ndarray) -> str:
  return hashlib.md5(decoded_img.tobytes()).hexdigest()


def _test_opencv_image() -> np.ndarray:
  return cv2.imread(shared_test_util.get_testdir_path('image.jpeg'))


def _test_pil_image() -> image_util.PILImage:
  img = PIL.Image.open(shared_test_util.get_testdir_path('image.jpeg'))
  return image_util.PILImage(img)


def _image_util_test_path(filename: str) -> str:
  return shared_test_util.get_testdir_path('image_util', filename)


class ImageUtilTest(parameterized.TestCase):

  def test_pil_image_class(self):
    with PIL.Image.open(shared_test_util.get_testdir_path('image.jpeg')) as img:
      test = image_util.PILImage(img)
      self.assertIs(test.image, img)

  def test_gaussian_image_filter_sigma(self):
    self.assertEqual(
        image_util.GaussianImageFilter(4.0, _Interpolation.CUBIC)._sigma, 1.5
    )

  def test_gaussian_image_filter_image_padding(self):
    self.assertEqual(
        image_util.GaussianImageFilter(2.0, _Interpolation.CUBIC).image_padding,
        2.0,
    )

  def test_gaussian_image_filter_image_padding_area_disables(self):
    self.assertEqual(
        image_util.GaussianImageFilter(2.0, _Interpolation.AREA).image_padding,
        0.0,
    )

  def test_gaussian_image_filter_nop(self):
    test = np.zeros((2, 2))
    self.assertIs(
        image_util.GaussianImageFilter(1.0, _Interpolation.CUBIC).filter(test),
        test,
    )

  def test_gaussian_image_filter(self):
    smoothed = image_util.GaussianImageFilter(2.0, _Interpolation.CUBIC).filter(
        _test_opencv_image()
    )
    with open(_image_util_test_path('blur_opencv2.png'), 'rb') as infile:
      expected_bytes = infile.read()

    img_bytes = image_util.encode_image(smoothed, _Compression.PNG, 75, None)

    self.assertEqual(img_bytes, expected_bytes)

  @parameterized.parameters([
      (_Interpolation.CUBIC, 8),
      (_Interpolation.LANCZOS4, 8),
      (_Interpolation.LINEAR, 0),
      (_Interpolation.AREA, 0),
      (_Interpolation.NEAREST, 0),
      (None, 0),
  ])
  def test_get_cv2_interpolation_padding(self, interpolation, padding):
    self.assertEqual(
        image_util.get_cv2_interpolation_padding(interpolation), padding
    )

  @parameterized.parameters([
      (_Interpolation.CUBIC, cv2.INTER_CUBIC),
      (_Interpolation.LANCZOS4, cv2.INTER_LANCZOS4),
      (_Interpolation.LINEAR, cv2.INTER_LINEAR),
      (_Interpolation.AREA, cv2.INTER_AREA),
      (_Interpolation.NEAREST, cv2.INTER_NEAREST),
      (None, cv2.INTER_AREA),
  ])
  def test_get_cv2_interpolation(self, interpolation, cv2_interpolation):
    self.assertEqual(
        image_util._get_cv2_interpolation(interpolation), cv2_interpolation
    )

  @parameterized.parameters([
      _Interpolation.CUBIC,
      _Interpolation.LANCZOS4,
      _Interpolation.LINEAR,
      _Interpolation.AREA,
      _Interpolation.NEAREST,
      None,
  ])
  def test_downsample_image(self, interpolation):
    img = _test_opencv_image()
    height, width, channels = img.shape
    downsampling_factor = 2.0
    downsample_width = 50
    downsample_height = 33
    result = image_util.downsample_image(
        img,
        downsampling_factor,
        downsample_width,
        downsample_height,
        interpolation,
    )
    self.assertEqual(
        result.shape,
        (
            int(height / downsampling_factor),
            int(width / downsampling_factor),
            channels,
        ),
    )

    encoded_img = image_util.encode_image(result, _Compression.PNG, 75, None)
    iterm = {
        _Interpolation.CUBIC: 'cubic',
        _Interpolation.LANCZOS4: 'lanczos4',
        _Interpolation.LINEAR: 'linear',
        _Interpolation.AREA: 'area',
        _Interpolation.NEAREST: 'nearest',
        None: 'none',
    }
    method = iterm[interpolation]
    with open(
        _image_util_test_path(f'downsample_{method}.png'), 'rb'
    ) as infile:
      self.assertEqual(infile.read(), encoded_img)

  @parameterized.parameters([
      ('image.jpeg', '70916545848f2db1f5d33a1b6482ff57'),
      ('image.png', 'd59ab1d413fd24c724c73bc9c151acb8'),
      ('image.webp', 'e31cb9203ad6bbde544e614ee6992308'),
  ])
  def test_image_decoder(self, path, expected):
    with open(shared_test_util.get_testdir_path(path), 'rb') as f:
      imgbytes = f.read()
    # Expected image dimensions
    height = 67
    width = 100
    channels = 3

    # Decode using opencv decoder, claim image is a JPEG.
    decoded_img = image_util.decode_image_bytes(
        imgbytes, '1.2.840.10008.1.2.4.50'
    )

    self.assertEqual(decoded_img.shape, (height, width, channels))
    self.assertEqual(_img_bytes_hash(decoded_img), expected)

  @parameterized.parameters(['CV2', 'PIL'])
  def test_jpeg_encoder(self, jpeg_encoder):
    img = _test_opencv_image()
    filename = f'img_compression_jpg_{jpeg_encoder.lower()}_encoder.jpeg'

    with flagsaver.flagsaver(jpeg_encoder_flg=jpeg_encoder):
      encoded_img = image_util._encode_jpeg(img, quality=95, icc_profile=None)

    with PIL.Image.open(io.BytesIO(encoded_img)) as im:
      self.assertEqual(im.size, (100, 67))
      self.assertEqual(im.format, 'JPEG')
      self.assertEqual(im.mode, 'RGB')
      self.assertIsNone(im.info.get('icc_profile'))
    with open(_image_util_test_path(filename), 'rb') as infile:
      self.assertEqual(infile.read(), encoded_img)

  def test_encode_pil_image_raw(self):
    img = _test_pil_image()
    encoded_img = image_util.encode_image(img, _Compression.RAW, 75, None)
    self.assertEqual(encoded_img, img.image.tobytes())

  def test_encode_pil_image_numpy(self):
    img = _test_pil_image()
    encoded_img = image_util.encode_image(img, _Compression.NUMPY, 75, None)
    self.assertTrue(np.array_equal(encoded_img, _test_opencv_image()))

  def test_encode_pil_image_png(self):
    img = _test_pil_image()
    encoded_img = image_util.encode_image(img, _Compression.PNG, 75, None)
    # test image shape, encoding, and color
    with PIL.Image.open(io.BytesIO(encoded_img)) as im:
      expected_mode = 'RGB'
      im_bytes = im.tobytes()
      self.assertEqual(im.size, (100, 67))
      self.assertEqual(im.format, 'PNG')
      self.assertEqual(im.mode, expected_mode)
      self.assertIsNone(im.info.get('icc_profile'))

    with PIL.Image.open(
        _image_util_test_path('img_compression_pil.png')
    ) as expected:
      self.assertEqual(im_bytes, expected.tobytes())

  def test_encode_pil_image_gif(self):
    img = _test_pil_image()
    encoded_img = image_util.encode_image(img, _Compression.GIF, 75, None)
    # test image shape, encoding, and color
    with PIL.Image.open(io.BytesIO(encoded_img)) as im:
      expected_mode = 'P'
      self.assertEqual(im.size, (100, 67))
      self.assertEqual(im.format, 'GIF')
      self.assertEqual(im.mode, expected_mode)
      self.assertIsNone(im.info.get('icc_profile'))
    # test image bytes match
    with open(_image_util_test_path('img_compression_pil.gif'), 'rb') as infile:
      expected_bytes = infile.read()
      self.assertEqual(expected_bytes, encoded_img)

  @parameterized.parameters(
      [(_Compression.JPEG, 'JPEG'), (_Compression.WEBP, 'WEBP')]
  )
  def test_encode_pil_image(self, compression, expected_format):
    img = _test_pil_image()
    encoded_img = image_util.encode_image(img, compression, 75, None)
    # test image shape, encoding, and color
    with PIL.Image.open(io.BytesIO(encoded_img)) as im:
      expected_mode = 'RGB'
      self.assertEqual(im.size, (100, 67))
      self.assertEqual(im.format, expected_format)
      self.assertEqual(im.mode, expected_mode)
      self.assertIsNone(im.info.get('icc_profile'))

    # test image bytes match
    ext = expected_format.lower()
    with open(
        _image_util_test_path(f'img_compression_pil.{ext}'), 'rb'
    ) as infile:
      expected_bytes = infile.read()
      self.assertEqual(expected_bytes, encoded_img)

  @parameterized.parameters([
      (_Compression.JPEG, 'JPEG'),
      (_Compression.WEBP, 'WEBP'),
      (_Compression.PNG, 'PNG'),
      (_Compression.GIF, 'GIF'),
      (_Compression.RAW, ''),
      (_Compression.NUMPY, ''),
  ])
  def test_encode_opencv_image(self, compression, expected_format):
    img = _test_opencv_image()
    encoded_img = image_util.encode_image(img, compression, 75, None)
    if compression == _Compression.RAW:
      # Byte ordering should be RGB
      self.assertEqual(encoded_img, _test_pil_image().image.tobytes())
      return
    if compression == _Compression.NUMPY:
      self.assertIs(encoded_img, img)
      return

    # test image shape, encoding, and color
    with PIL.Image.open(io.BytesIO(encoded_img)) as im:
      if compression == _Compression.GIF:
        expected_mode = 'P'
      else:
        expected_mode = 'RGB'
      self.assertEqual(im.size, (100, 67))
      self.assertEqual(im.format, expected_format)
      self.assertEqual(im.mode, expected_mode)
      self.assertIsNone(im.info.get('icc_profile'))

    # test image bytes match
    ext = expected_format.lower()
    with open(_image_util_test_path(f'img_compression.{ext}'), 'rb') as infile:
      self.assertEqual(infile.read(), encoded_img)

  @parameterized.parameters(
      [(1, 0), (2, 5), (3, 7), (4, 11), (5, 13), (100, 299)]
  )
  def test_gaussian_image_filter_kernel_size(self, downsampling, expected):
    self.assertEqual(
        image_util.GaussianImageFilter(
            downsampling, _Interpolation.CUBIC
        )._kernel_size,
        expected,
    )

  def test_bgr2rgb_color_in_place(self):
    input_image = np.array([[[1, 2, 3]]], dtype=np.uint8)
    expected_output = np.array([[[3, 2, 1]]], dtype=np.uint8)
    output = image_util.bgr2rgb(input_image)
    self.assertIs(input_image, output)
    self.assertTrue(np.array_equal(output, expected_output))

  def test_bgr2rgb_color_copy(self):
    input_image = np.array([[[1, 2, 3]]], dtype=np.uint8)
    input_copy = input_image.copy()
    expected_output = np.array([[[3, 2, 1]]], dtype=np.uint8)
    output = image_util.bgr2rgb(input_image, copy=True)
    self.assertIsNot(input_image, output)
    self.assertTrue(np.array_equal(input_image, input_copy))
    self.assertTrue(np.array_equal(output, expected_output))

  @parameterized.parameters([([[1, 2, 3]],), ([[[1], [2], [3]]],)])
  def test_bgr2rgb_bw_copy(self, bw_img):
    input_image = np.array(bw_img, dtype=np.uint8)
    expected_output = np.array(bw_img, dtype=np.uint8)
    output = image_util.bgr2rgb(input_image, copy=True)
    self.assertIsNot(input_image, output)
    self.assertTrue(np.array_equal(input_image, expected_output))
    self.assertTrue(np.array_equal(output, expected_output))

  @parameterized.parameters([([[1, 2, 3]],), ([[[1], [2], [3]]],)])
  def test_bgr2rgb_bw_in_place(self, bw_img):
    input_image = np.array(bw_img, dtype=np.uint8)
    expected_output = np.array(bw_img, dtype=np.uint8)
    output = image_util.bgr2rgb(input_image, copy=False)
    self.assertIs(input_image, output)
    self.assertTrue(np.array_equal(input_image, expected_output))
    self.assertTrue(np.array_equal(output, expected_output))

  def test_opencv_to_pil_image_color(self):
    cv_image = np.array([[[1, 2, 3]]], dtype=np.uint8)
    expected = np.array([[[3, 2, 1]]], dtype=np.uint8)
    input_img = cv_image.copy()
    pil_image = image_util._opencv_to_pil_image(cv_image)
    self.assertTrue(np.array_equal(input_img, cv_image))
    self.assertEqual(pil_image.image.tobytes(), expected.tobytes())
    self.assertIsNone(pil_image.image.info.get('icc_profile'))

  @parameterized.parameters([([[1, 2, 3]],), ([[[1], [2], [3]]],)])
  def test_opencv_to_pil_image_bw(self, bw_img):
    cv_image = np.array(bw_img, dtype=np.uint8)
    pil_image = image_util._opencv_to_pil_image(cv_image)
    self.assertTrue(
        np.array_equal(pil_image.image.tobytes(), cv_image.tobytes())
    )
    self.assertIsNone(pil_image.image.info.get('icc_profile'))

  @parameterized.parameters([
      enum_types.Compression.PNG,
      enum_types.Compression.JPEG,
      enum_types.Compression.WEBP,
  ])
  def test_save_embedd_icc_profile_in_np_image(self, compression):
    cv_image = np.array([[[1, 2, 3], [4, 5, 6], [7, 8, 9]]], dtype=np.uint8)
    icc_profile_bytes = color_conversion_util._get_srgb_iccprofile()
    encoded_bytes = image_util.encode_image(
        cv_image, compression, quality=75, icc_profile=icc_profile_bytes
    )
    with PIL.Image.open(io.BytesIO(encoded_bytes)) as im:
      self.assertEqual(im.info.get('icc_profile'), icc_profile_bytes)

  @parameterized.parameters([
      enum_types.Compression.PNG,
      enum_types.Compression.JPEG,
      enum_types.Compression.WEBP,
  ])
  def test_save_embedd_icc_profile_in_pil_image(self, compression):
    cv_image = np.array([[[1, 2, 3], [4, 5, 6], [7, 8, 9]]], dtype=np.uint8)
    icc_profile_bytes = color_conversion_util._get_srgb_iccprofile()
    encoded_bytes = image_util.encode_image(
        image_util._opencv_to_pil_image(cv_image),
        compression,
        quality=75,
        icc_profile=icc_profile_bytes,
    )
    with PIL.Image.open(io.BytesIO(encoded_bytes)) as im:
      self.assertEqual(im.info.get('icc_profile'), icc_profile_bytes)

  def test_save_embedd_icc_profile_in_gif_image_throws(self):
    cv_image = np.array([[[1, 2, 3], [4, 5, 6], [7, 8, 9]]], dtype=np.uint8)
    icc_profile_bytes = color_conversion_util._get_srgb_iccprofile()
    with self.assertRaises(
        image_util.ImageEncodingDoesNotSupportEmbeddingICCProfileError
    ):
      image_util.encode_image(
          image_util._opencv_to_pil_image(cv_image),
          enum_types.Compression.GIF,
          quality=75,
          icc_profile=icc_profile_bytes,
      )

  @mock.patch.object(PIL.Image, 'open', autospec=True)
  def test_cv2_loaded_bw_image(self, mock_pil_open):
    with open(shared_test_util.get_testdir_path('bw.jpeg'), 'rb') as infile:
      decoded_img = image_util.decode_image_bytes(
          infile.read(), '1.2.840.10008.1.2.4.50'
      )
    np.testing.assert_array_equal(
        decoded_img, np.full((4, 4, 3), 255, dtype=np.uint8)
    )
    mock_pil_open.assert_not_called()

  @mock.patch.object(cv2, 'imdecode', autospec=True)
  def test_pil_loaded_bw_image(self, mock_cv2_imdecode):
    with open(shared_test_util.get_testdir_path('bw.jpeg'), 'rb') as infile:
      decoded_img = image_util.decode_image_bytes(
          infile.read(), '1.2.840.10008.1.2.4.90'
      )
    np.testing.assert_array_equal(
        decoded_img, np.full((4, 4, 3), 255, dtype=np.uint8)
    )
    mock_cv2_imdecode.assert_not_called()

  @parameterized.parameters([
      None,
      render_frame_params.Viewport([]),
  ])
  def test_transform_cv2_image_viewport_nop(self, undefined_viewport):
    test_img = np.zeros((100, 100, 3), dtype=np.uint8)
    self.assertIs(
        image_util.transform_image_viewport(test_img, undefined_viewport),
        test_img,
    )

  @parameterized.parameters([
      None,
      render_frame_params.Viewport([]),
  ])
  def test_transform_pil_image_viewport_nop(self, undefined_viewport):
    image = image_util.PILImage(
        PIL.Image.fromarray(np.zeros((100, 100, 3), dtype=np.uint8))
    )
    self.assertIs(
        image_util.transform_image_viewport(image, undefined_viewport).image,
        image.image,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='equal_size',
          viewport=render_frame_params.Viewport(['10', '100']),
          expected_dim=(10, 100),
      ),
      dict(
          testcase_name='grow_height',
          viewport=render_frame_params.Viewport(['20', '150']),
          expected_dim=(15, 150),
      ),
      dict(
          testcase_name='grow_width',
          viewport=render_frame_params.Viewport(['15', '200']),
          expected_dim=(15, 150),
      ),
      dict(
          testcase_name='grow',
          viewport=render_frame_params.Viewport(['20', '200']),
          expected_dim=(20, 200),
      ),
      dict(
          testcase_name='shrink_width',
          viewport=render_frame_params.Viewport(['5', '75']),
          expected_dim=(5, 50),
      ),
      dict(
          testcase_name='shrink_height',
          viewport=render_frame_params.Viewport(['8', '50']),
          expected_dim=(5, 50),
      ),
      dict(
          testcase_name='shrink',
          viewport=render_frame_params.Viewport(['5', '50']),
          expected_dim=(5, 50),
      ),
      dict(
          testcase_name='non_uniform_width',
          viewport=render_frame_params.Viewport(['5', '200']),
          expected_dim=(5, 50),
      ),
      dict(
          testcase_name='non_uniform_height',
          viewport=render_frame_params.Viewport(['20', '50']),
          expected_dim=(5, 50),
      ),
  ])
  def test_transform_image_viewport_non_uniform_resize(
      self, viewport, expected_dim
  ):
    img = np.zeros((100, 10, 3), dtype=np.uint8)
    self.assertEqual(
        image_util.transform_image_viewport(img, viewport).image.size,
        expected_dim,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='flip_both',
          viewport=render_frame_params.Viewport(['2', '2', '', '', '-2', '-2']),
          expected=np.asarray([[4, 3], [2, 1]], dtype=np.uint8),
      ),
      dict(
          testcase_name='flip_left_right',
          viewport=render_frame_params.Viewport(['2', '2', '', '', '-2', '2']),
          expected=np.asarray([[2, 1], [4, 3]], dtype=np.uint8),
      ),
      dict(
          testcase_name='flip_top_bottom',
          viewport=render_frame_params.Viewport(
              ['2', '2', '0', '0', '2', '-2']
          ),
          expected=np.asarray([[3, 4], [1, 2]], dtype=np.uint8),
      ),
      dict(
          testcase_name='unchanged_1',
          viewport=render_frame_params.Viewport(['2', '2', '0', '0', '2', '2']),
          expected=np.asarray([[1, 2], [3, 4]], dtype=np.uint8),
      ),
      dict(
          testcase_name='unchanged_2',
          viewport=render_frame_params.Viewport(['2', '2', '0', '0']),
          expected=np.asarray([[1, 2], [3, 4]], dtype=np.uint8),
      ),
      dict(
          testcase_name='unchanged_3',
          viewport=render_frame_params.Viewport(['2', '2']),
          expected=np.asarray([[1, 2], [3, 4]], dtype=np.uint8),
      ),
      dict(
          testcase_name='crop_1',
          viewport=render_frame_params.Viewport(['1', '1', '1', '1', '1', '1']),
          expected=np.asarray([[4]], dtype=np.uint8),
      ),
      dict(
          testcase_name='crop_2',
          viewport=render_frame_params.Viewport(['1', '1', '0', '0', '1', '1']),
          expected=np.asarray([[1]], dtype=np.uint8),
      ),
      dict(
          testcase_name='crop_3',
          viewport=render_frame_params.Viewport(['1', '1', '1', '0', '1', '1']),
          expected=np.asarray([[2]], dtype=np.uint8),
      ),
      dict(
          testcase_name='crop_4',
          viewport=render_frame_params.Viewport(['1', '1', '0', '1', '1', '1']),
          expected=np.asarray([[3]], dtype=np.uint8),
      ),
      dict(
          testcase_name='crop_scale',
          viewport=render_frame_params.Viewport(['2', '2', '1', '1', '1', '1']),
          expected=np.asarray([[4, 4], [4, 4]], dtype=np.uint8),
      ),
  ])
  def test_transform_image_viewport(self, viewport, expected):
    img = np.asarray([[1, 2], [3, 4]], dtype=np.uint8)
    np.testing.assert_array_equal(
        np.asarray(image_util.transform_image_viewport(img, viewport).image),
        expected,
    )


if __name__ == '__main__':
  absltest.main()
