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

from absl.testing import absltest
from absl.testing import parameterized
import cv2
import numpy as np

from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_image_extractor
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingested_dicom_file_ref


class AncillaryImageExtractorTest(parameterized.TestCase):

  def test_read_ancillary_jpeg(self):
    extract_img_path = os.path.join(self.create_tempdir(), 'extracted.jpg')
    dicom_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        gen_test_util.test_file_path('test_jpeg_dicom.dcm')
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
    extracted_image = cv2.imread(extract_img_path)
    extracted_image = np.array(extracted_image)
    difference = np.abs(
        test_img.astype(np.int32) - extracted_image.astype(np.int32)
    )
    difference[difference <= 65] = 0  # threshold difference.
    self.assertEqual(np.sum(difference), 0)

  def test_extract_ancillary_image_with_openslide_invalid_file_type(self):
    output_path = os.path.join(self.create_tempdir(), 'temp.jpg')
    image = ancillary_image_extractor.AncillaryImage(path=output_path)
    self.assertFalse(
        ancillary_image_extractor._extract_ancillary_image_with_openslide(
            gen_test_util.test_file_path('google.png'),
            image,
            flip_red_blue=True,
            openslide_key='macro',
        )
    )

  def test_extract_ancillary_image_with_openslide_invalid_openslide_key(self):
    output_path = os.path.join(self.create_tempdir(), 'temp.jpg')
    image = ancillary_image_extractor.AncillaryImage(path=output_path)
    self.assertFalse(
        ancillary_image_extractor._extract_ancillary_image_with_openslide(
            gen_test_util.test_file_path('ndpi_test.ndpi'),
            image,
            flip_red_blue=True,
            openslide_key='thumbnail',
        )
    )

  @parameterized.parameters(
      [(True, 'ndpi_macro_flip_rb.jpg'), (False, 'ndpi_macro.jpg')]
  )
  def test_extract_ancillary_image_with_openslide(
      self, flip_red_blue, expected_img_filename
  ):
    expected_img_path = gen_test_util.test_file_path(expected_img_filename)
    output_path = os.path.join(self.create_tempdir(), 'temp.jpg')
    image = ancillary_image_extractor.AncillaryImage(path=output_path)
    with open(expected_img_path, 'rb') as expected_bytes:
      data_bytes = expected_bytes.read()
    self.assertTrue(
        ancillary_image_extractor._extract_ancillary_image_with_openslide(
            gen_test_util.test_file_path('ndpi_test.ndpi'),
            image,
            flip_red_blue=flip_red_blue,
            openslide_key='macro',
        )
    )
    self.assertEqual(os.path.basename(image.path), 'temp.jpg')
    self.assertEqual(image.photometric_interpretation, 'YBR_FULL_422')
    self.assertEqual(image.extracted_without_decompression, False)
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

  @parameterized.parameters([
      (False, 'google_flipped_red_blue.jpg'),
      (True, 'google_not_flipped_red_blue.jpg'),
  ])
  def test_extract_jpg_image(self, flip_red_blue, output_image):
    output_path = os.path.join(self.create_tempdir(), output_image)
    self.assertTrue(
        ancillary_image_extractor.extract_jpg_image(
            gen_test_util.test_file_path('google.tif'),
            ancillary_image_extractor.AncillaryImage(path=output_path),
            flip_red_blue,
        )
    )
    self.assertTrue(
        filecmp.cmp(output_path, gen_test_util.test_file_path(output_image))
    )

  def test_extract_jpeg_2000_image(self):
    test_tmpdir = self.create_tempdir()
    output_path = os.path.join(test_tmpdir, 'google.jpg')
    modified_output_path = os.path.join(test_tmpdir, 'google.jp2')
    self.assertTrue(
        ancillary_image_extractor.extract_jpg_image(
            gen_test_util.test_file_path('google.tif'),
            ancillary_image_extractor.AncillaryImage(path=output_path),
            flip_red_blue=False,
            convert_to_jpeg_2000=True,
        )
    )
    self.assertTrue(
        filecmp.cmp(
            modified_output_path, gen_test_util.test_file_path('google.jp2')
        )
    )


if __name__ == '__main__':
  absltest.main()
