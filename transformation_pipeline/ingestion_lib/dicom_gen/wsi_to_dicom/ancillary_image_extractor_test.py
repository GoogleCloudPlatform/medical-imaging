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
"""Tests for ancillary_image_extractor."""

import filecmp
import os
from typing import Optional
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
import cv2
import numpy as np
import openslide
import PIL.Image
import pydicom
import tifffile

from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_image_extractor
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingested_dicom_file_ref


class AncillaryImageExtractorTest(parameterized.TestCase):

  def test_read_ancillary_jpeg_dicom_no_frames(self):
    extract_img_path = os.path.join(self.create_tempdir(), 'extracted.jpg')
    test_dicom_path = os.path.join(self.create_tempdir(), 'test_dicom.dcm')
    ds = pydicom.dcmread(gen_test_util.test_file_path('test_jpeg_dicom.dcm'))
    del ds['NumberOfFrames']
    ds.save_as(test_dicom_path)
    dicom_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        test_dicom_path
    )

    ancillary_image_extractor._extract_ancillary_dicom_image(
        dicom_ref, extract_img_path
    )

    test_img = np.array(cv2.imread(gen_test_util.test_file_path('google.jpg')))
    extracted_image = np.array(cv2.imread(extract_img_path))
    difference = np.abs(
        test_img.astype(np.int32) - extracted_image.astype(np.int32)
    )
    difference[difference <= 110] = 0  # threshold difference.
    self.assertEqual(np.sum(difference), 0)

  def test_read_ancillary_jpeg_dicom_one_frame(self):
    extract_img_path = os.path.join(self.create_tempdir(), 'extracted.jpg')
    test_dicom_path = os.path.join(self.create_tempdir(), 'test_dicom.dcm')
    ds = pydicom.dcmread(gen_test_util.test_file_path('test_jpeg_dicom.dcm'))
    ds.NumberOfFrames = 1
    ds.save_as(test_dicom_path)
    dicom_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        test_dicom_path
    )

    ancillary_image_extractor._extract_ancillary_dicom_image(
        dicom_ref, extract_img_path
    )

    test_img = np.array(cv2.imread(gen_test_util.test_file_path('google.jpg')))
    extracted_image = np.array(cv2.imread(extract_img_path))
    difference = np.abs(
        test_img.astype(np.int32) - extracted_image.astype(np.int32)
    )
    difference[difference <= 110] = 0  # threshold difference.
    self.assertEqual(np.sum(difference), 0)

  def test_read_ancillary_raw(self):
    extract_img_path = os.path.join(self.create_tempdir(), 'extracted.jpg')
    dicom_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        gen_test_util.test_file_path('test_raw_dicom.dcm')
    )

    ancillary_image_extractor._extract_ancillary_dicom_image(
        dicom_ref, extract_img_path
    )

    test_img = np.array(cv2.imread(gen_test_util.test_file_path('google.jpg')))
    extracted_image = np.array(cv2.imread(extract_img_path))
    difference = np.abs(
        test_img.astype(np.int32) - extracted_image.astype(np.int32)
    )
    difference[difference <= 65] = 0  # threshold difference.
    self.assertEqual(np.sum(difference), 0)

  def test_read_ancillary_jpeg_raises_if_extracting_to_non_jpeg(self):
    extract_img_path = os.path.join(self.create_tempdir(), 'extracted.foo')
    dicom_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        gen_test_util.test_file_path('test_jpeg_dicom.dcm')
    )
    with self.assertRaises(ValueError):
      ancillary_image_extractor._extract_ancillary_dicom_image(
          dicom_ref, extract_img_path
      )

  @parameterized.named_parameters([
      dict(testcase_name='no_frames', number_of_frames=0),
      dict(testcase_name='multiple_frames', number_of_frames=2),
  ])
  def test_get_first_encapsulated_frame_raises_invalid_number_of_frames(
      self, number_of_frames
  ):
    mock_frames = pydicom.encaps.encapsulate([b'123'] * number_of_frames)
    with self.assertRaises(ValueError):
      ancillary_image_extractor._get_first_encapsulated_frame(
          pydicom.encaps.generate_pixel_data_frame(
              mock_frames, number_of_frames
          ),
          'image_type',
      )

  @mock.patch.object(
      openslide.OpenSlide, 'associated_images', new_callable=mock.PropertyMock
  )
  def test_extract_ancillary_image_with_openslide_return_false_invalid_img_mode(
      self, openslide_mock_poperty
  ):
    mock_pil_image = mock.create_autospec(PIL.Image, instance=True)
    mock_pil_image.mode = 'invalid_image_mode'
    mock_pil_image.size = (5, 5)
    openslide_mock_poperty.return_value = {'macro': mock_pil_image}
    output_path = os.path.join(self.create_tempdir(), 'temp.jpg')
    image = ancillary_image_extractor.AncillaryImage(path=output_path)
    self.assertFalse(
        ancillary_image_extractor._extract_ancillary_image_with_openslide(
            gen_test_util.test_file_path('ndpi_test.ndpi'),
            image,
            'macro',
        )
    )

  @parameterized.named_parameters([
      dict(testcase_name='rgb', filename='google.jpg'),
      dict(testcase_name='rgba', filename='logo_transparent.png'),
      dict(testcase_name='l', filename='logo_grayscale.jpg'),
  ])
  @mock.patch.object(
      openslide.OpenSlide, 'associated_images', new_callable=mock.PropertyMock
  )
  def test_extract_ancillary_image_with_openslide_valid_img_mode_succeed(
      self, openslide_mock_poperty, filename
  ):
    test_file_path = gen_test_util.test_file_path(filename)
    with PIL.Image.open(test_file_path) as pil_image:
      openslide_mock_poperty.return_value = {'macro': pil_image}
      output_path = os.path.join(self.create_tempdir(), 'temp.jpg')
      image = ancillary_image_extractor.AncillaryImage(path=output_path)
      self.assertTrue(
          ancillary_image_extractor._extract_ancillary_image_with_openslide(
              gen_test_util.test_file_path('ndpi_test.ndpi'),
              image,
              'macro',
          )
      )
      expected_image = cv2.cvtColor(
          cv2.imread(test_file_path)[..., :3], cv2.COLOR_BGR2RGB
      )
      with PIL.Image.open(output_path) as gen_image:
        self.assertEqual(gen_image.mode, 'RGB')
        # Test generated image is close to origional.
        generated_image = np.asarray(gen_image)
        img_diff = np.abs(
            generated_image.astype(np.int32) - expected_image.astype(np.int32)
        )
        self.assertLess(np.mean(img_diff), 2)

  def test_extract_ancillary_image_with_openslide_invalid_file_type(self):
    output_path = os.path.join(self.create_tempdir(), 'temp.jpg')
    image = ancillary_image_extractor.AncillaryImage(path=output_path)
    self.assertFalse(
        ancillary_image_extractor._extract_ancillary_image_with_openslide(
            gen_test_util.test_file_path('google.png'),
            image,
            'macro',
        )
    )

  def test_extract_ancillary_image_with_openslide_invalid_openslide_key(self):
    output_path = os.path.join(self.create_tempdir(), 'temp.jpg')
    image = ancillary_image_extractor.AncillaryImage(path=output_path)
    self.assertFalse(
        ancillary_image_extractor._extract_ancillary_image_with_openslide(
            gen_test_util.test_file_path('ndpi_test.ndpi'),
            image,
            'thumbnail',
        )
    )

  def test_extract_ancillary_image_with_openslide(self):
    expected_img_path = gen_test_util.test_file_path('ndpi_macro.jpg')
    output_path = os.path.join(self.create_tempdir(), 'temp.jpg')
    image = ancillary_image_extractor.AncillaryImage(path=output_path)
    with open(expected_img_path, 'rb') as expected_bytes:
      data_bytes = expected_bytes.read()
    self.assertTrue(
        ancillary_image_extractor._extract_ancillary_image_with_openslide(
            gen_test_util.test_file_path('ndpi_test.ndpi'),
            image,
            openslide_key='macro',
        )
    )
    self.assertEqual(os.path.basename(image.path), 'temp.jpg')
    self.assertEqual(image.photometric_interpretation, 'YBR_FULL_422')
    self.assertEqual(image.extracted_without_decompression, False)
    with open(image.path, 'rb') as output_bytes:
      self.assertEqual(output_bytes.read(), data_bytes)

  def test_macro_image_succeeds(self):
    expected_img_path = gen_test_util.test_file_path(
        'ndpi_jpeg_pixel_equal_extraction_macro.jpg'
    )
    output_path = os.path.join(self.create_tempdir(), 'temp.jpg')
    image = ancillary_image_extractor.AncillaryImage(path=output_path)
    with open(expected_img_path, 'rb') as expected_bytes:
      data_bytes = expected_bytes.read()
    self.assertTrue(
        ancillary_image_extractor.macro_image(
            gen_test_util.test_file_path('ndpi_test.ndpi'), image
        )
    )
    self.assertEqual(os.path.basename(image.path), 'temp.jpg')
    self.assertEqual(image.photometric_interpretation, 'YBR_FULL_422')
    self.assertEqual(image.extracted_without_decompression, True)
    with open(image.path, 'rb') as output_bytes:
      self.assertEqual(output_bytes.read(), data_bytes)

  def test_get_ancillary_images_from_dicom_no_valid_image(self):
    dicom_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        gen_test_util.test_file_path('test_jpeg_dicom.dcm')
    )
    dicom_ref.set_tag_value('ImageType', 'ORIGINAL\\PRIMARY\\VOLUME\\NONE')

    result = ancillary_image_extractor.get_ancillary_images_from_dicom(
        [dicom_ref], self.create_tempdir().full_path
    )

    self.assertEmpty(result)

  @parameterized.parameters([
      'ORIGINAL\\PRIMARY\\LABEL\\NONE',
      'ORIGINAL\\PRIMARY\\THUMBNAIL\\NONE',
      'ORIGINAL\\PRIMARY\\OVERVIEW\\NONE',
  ])
  def test_get_ancillary_images_from_dicom_by_imagetype(self, image_type: str):
    dicom_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        gen_test_util.test_file_path('test_jpeg_dicom.dcm')
    )
    dicom_ref.set_tag_value('ImageType', image_type)

    result = ancillary_image_extractor.get_ancillary_images_from_dicom(
        [dicom_ref], self.create_tempdir().full_path
    )

    self.assertLen(result, 1)

  @parameterized.parameters([
      'ORIGINAL\\PRIMARY\\LABEL\\NONE',
      'ORIGINAL\\PRIMARY\\THUMBNAIL\\NONE',
      'ORIGINAL\\PRIMARY\\OVERVIEW\\NONE',
  ])
  def test_get_ancillary_images_from_dicom_by_frametype(self, image_type: str):
    dicom_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        gen_test_util.test_file_path('test_jpeg_dicom.dcm')
    )
    dicom_ref.set_tag_value('FrameType', image_type)
    dicom_ref.set_tag_value('ImageType', '')

    result = ancillary_image_extractor.get_ancillary_images_from_dicom(
        [dicom_ref], self.create_tempdir().full_path
    )

    self.assertLen(result, 1)

  def test_extract_jpeg_using_tifffile_empty_series_bytes_returns_false(self):
    def mk_as_array() -> Optional[np.ndarray]:
      return None

    series = mock.create_autospec(tifffile.TiffPageSeries, instance=True)
    series.asarray = mk_as_array
    self.assertFalse(
        ancillary_image_extractor._extract_jpeg_using_tifffile(
            series, ancillary_image_extractor.AncillaryImage(path='foo'), True
        )
    )

  @mock.patch.object(
      PIL.Image, 'fromarray', autospec=True, side_effect=ValueError
  )
  def test_extract_jpeg_using_tifffile_jpg_2000_save_failure_returns_false(
      self, _
  ):
    def mk_as_array() -> Optional[np.ndarray]:
      return np.asarray([[[255, 255, 255]]], dtype=np.uint8)

    series = mock.create_autospec(tifffile.TiffPageSeries, instance=True)
    series.asarray = mk_as_array
    self.assertFalse(
        ancillary_image_extractor._extract_jpeg_using_tifffile(
            series, ancillary_image_extractor.AncillaryImage(path='foo'), True
        )
    )

  @mock.patch.object(cv2, 'imwrite', autospec=True, return_value=False)
  def test_extract_jpeg_using_tifffile_jpg_save_failure_returns_false(self, _):
    def mk_as_array() -> Optional[np.ndarray]:
      return np.asarray([[[255, 255, 255]]], dtype=np.uint8)

    series = mock.create_autospec(tifffile.TiffPageSeries, instance=False)
    series.asarray = mk_as_array
    self.assertFalse(
        ancillary_image_extractor._extract_jpeg_using_tifffile(
            series, ancillary_image_extractor.AncillaryImage(path='foo'), False
        )
    )

  @parameterized.parameters(['jpg', 'jpeg'])
  def test_extract_jpg_image(self, extension):
    output_image = f'google_logo_extracted_from_tiff_file.{extension}'
    temp_dir = self.create_tempdir()
    output_path = os.path.join(temp_dir, output_image)
    self.assertTrue(
        ancillary_image_extractor.extract_jpg_image(
            gen_test_util.test_file_path('google.tif'),
            ancillary_image_extractor.AncillaryImage(path=output_path),
        )
    )
    test_filename = 'google_logo_extracted_from_tiff_file.jpg'
    test_path = os.path.join(temp_dir, test_filename)
    os.rename(output_path, test_path)
    self.assertTrue(
        filecmp.cmp(test_path, gen_test_util.test_file_path(test_filename))
    )

  def test_extract_jpeg_2000_image(self):
    test_tmpdir = self.create_tempdir()
    output_path = os.path.join(test_tmpdir, 'google.jpg')
    modified_output_path = os.path.join(test_tmpdir, 'google.jp2')
    self.assertTrue(
        ancillary_image_extractor.extract_jpg_image(
            gen_test_util.test_file_path('google.tif'),
            ancillary_image_extractor.AncillaryImage(path=output_path),
            convert_to_jpeg_2000=True,
        )
    )
    self.assertTrue(
        filecmp.cmp(
            modified_output_path, gen_test_util.test_file_path('google.jp2')
        )
    )

  def test_extract_jpg_image_raises_if_not_jpeg(self):
    output_image = 'google_logo_extracted_from_tiff_file.foo'
    output_path = os.path.join(self.create_tempdir(), output_image)
    with self.assertRaises(
        ancillary_image_extractor.ExtractingAncillaryImageToNonJpegImageError
    ):
      ancillary_image_extractor.extract_jpg_image(
          gen_test_util.test_file_path('google.tif'),
          ancillary_image_extractor.AncillaryImage(path=output_path),
      )


if __name__ == '__main__':
  absltest.main()
