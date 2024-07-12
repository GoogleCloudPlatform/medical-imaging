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
"""Tests for VL Slide-Coordinates Microscopic Image DICOM builder."""

import datetime
import itertools
import os
import shutil
from typing import Any, Mapping
from unittest import mock

from absl import logging
from absl.testing import absltest
from absl.testing import parameterized
import PIL
import pydicom

from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_slide_coordinates_microscopic_image
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_image_extractor
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util


_STUDY_UID = '1.2.3.4.5'
_SERIES_UID = '1.2.3.4.5.6'
_INSTANCE_UID = '1.2.3.4.5.6.7'

DicomImageTransferSyntax = ingest_const.DicomImageTransferSyntax


def _test_dicom_json(patient_name: str = '') -> Mapping[str, Any]:
  ds = pydicom.Dataset()
  ds.StudyInstanceUID = _STUDY_UID
  ds.SeriesInstanceUID = _SERIES_UID
  if patient_name:
    ds.PatientName = patient_name
  return ds.to_json_dict()


class DicomSlideCoordinatesMicroscopicImageTest(parameterized.TestCase):
  """Tests for VL Slide-Coordinates Microscopic Image DICOM builder."""

  def test_create_encapsulated_flat_image_dicom_missing_image_fails(self):
    image_path = 'invalid path/img.jpg'
    with self.assertRaisesRegex(
        dicom_slide_coordinates_microscopic_image.InvalidFlatImageError,
        'Failed to open',
    ):
      _ = dicom_slide_coordinates_microscopic_image.create_encapsulated_flat_image_dicom(
          image_path,
          _INSTANCE_UID,
          ingest_const.DicomSopClasses.SLIDE_COORDINATES_IMAGE,
          _test_dicom_json(),
      )

  def test_create_encapsulated_flat_image_dicom_invalid_image_mode_fails(self):
    image_path = gen_test_util.test_file_path('logo_cmyk.jpg')
    with self.assertRaisesRegex(
        dicom_slide_coordinates_microscopic_image.InvalidFlatImageError,
        'Unexpected image mode',
    ):
      _ = dicom_slide_coordinates_microscopic_image.create_encapsulated_flat_image_dicom(
          image_path,
          _INSTANCE_UID,
          ingest_const.DicomSopClasses.SLIDE_COORDINATES_IMAGE,
          _test_dicom_json(),
      )

  def test_create_encapsulated_flat_image_dicom_unsupported_format_fails(self):
    image_path = gen_test_util.test_file_path('logo.gif')
    with self.assertRaisesRegex(
        dicom_slide_coordinates_microscopic_image.InvalidFlatImageError,
        'Unexpected image format',
    ):
      _ = dicom_slide_coordinates_microscopic_image.create_encapsulated_flat_image_dicom(
          image_path,
          _INSTANCE_UID,
          ingest_const.DicomSopClasses.SLIDE_COORDINATES_IMAGE,
          _test_dicom_json(),
      )

  @mock.patch.object(
      ancillary_image_extractor,
      'extract_jpg_image',
      return_value=False,
      autospec=True,
  )
  def test_create_encapsulated_flat_image_dicom_tif_to_jpg_conversion_fails(
      self, unused_mock
  ):
    image_path = gen_test_util.test_file_path('logo.tif')
    with self.assertRaisesRegex(
        dicom_slide_coordinates_microscopic_image.InvalidFlatImageError,
        'Failed to convert TIF to JPG',
    ):
      _ = dicom_slide_coordinates_microscopic_image.create_encapsulated_flat_image_dicom(
          image_path,
          _INSTANCE_UID,
          ingest_const.DicomSopClasses.SLIDE_COORDINATES_IMAGE,
          _test_dicom_json(),
      )

  @parameterized.parameters(
      itertools.product(
          [
              (
                  'logo.jpg',
                  'logo.jpg',
                  ['ORIGINAL', 'PRIMARY'],
                  'YBR_FULL_422',
                  DicomImageTransferSyntax.JPEG_LOSSY,
              ),
              (
                  'logo.png',
                  'logo_png_converted.jp2',
                  ['DERIVED', 'PRIMARY'],
                  'RGB',
                  DicomImageTransferSyntax.JPEG_2000,
              ),
              (
                  'logo_transparent.png',
                  'logo_png_transparent_converted-pil940.jp2'
                  if PIL.__version__ == '9.4.0'
                  else 'logo_png_transparent_converted.jp2',
                  ['DERIVED', 'PRIMARY'],
                  'RGB',
                  DicomImageTransferSyntax.JPEG_2000,
              ),
              (
                  'logo_jpg.tif',
                  'logo_tif_converted.jpg',
                  ['ORIGINAL', 'PRIMARY'],
                  'YBR_FULL_422',
                  DicomImageTransferSyntax.JPEG_LOSSY,
              ),
              (
                  'logo.tif',
                  'logo_tif_converted.jp2',
                  ['DERIVED', 'PRIMARY'],
                  'RGB',
                  DicomImageTransferSyntax.JPEG_2000,
              ),
          ],
          [
              ingest_const.DicomSopClasses.MICROSCOPIC_IMAGE,
              ingest_const.DicomSopClasses.SLIDE_COORDINATES_IMAGE,
          ],
      )
  )
  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  def test_create_encapsulated_flat_image_dicom_succeeds(
      self, image_params, sop_class, unused_mock
  ):
    (
        input_image,
        output_image,
        expected_image_type,
        expected_photometric_interpretation,
        expected_transfer_syntax,
    ) = image_params
    temp_dir = self.create_tempdir()
    src_image_path = gen_test_util.test_file_path(input_image)
    image_path = os.path.join(temp_dir, input_image)
    shutil.copyfile(src_image_path, image_path)
    dcm = dicom_slide_coordinates_microscopic_image.create_encapsulated_flat_image_dicom(
        image_path, _INSTANCE_UID, sop_class, _test_dicom_json()
    )

    # Saves DICOM file locally and read it.
    dcm_filename = os.path.splitext(input_image)[0]
    dcm_tmp_filepath = os.path.join(temp_dir, dcm_filename)
    dcm.save_as(dcm_tmp_filepath, write_like_original=False)
    saved_dcm = pydicom.dcmread(dcm_tmp_filepath)
    logging.info(saved_dcm)
    self.assertEqual(saved_dcm.NumberOfFrames, 1)
    self.assertEqual(saved_dcm.StudyInstanceUID, _STUDY_UID)
    self.assertEqual(saved_dcm.SeriesInstanceUID, _SERIES_UID)
    self.assertEqual(saved_dcm.SOPInstanceUID, _INSTANCE_UID)
    self.assertEqual(saved_dcm.SOPClassUID, sop_class.uid)
    self.assertEqual(
        saved_dcm.PhotometricInterpretation, expected_photometric_interpretation
    )
    self.assertEqual(saved_dcm.SamplesPerPixel, 3)
    self.assertIn('OpticalPathSequence', saved_dcm)
    self.assertEqual(saved_dcm.ImageType, expected_image_type)
    self.assertEqual(
        saved_dcm.file_meta.TransferSyntaxUID, expected_transfer_syntax
    )

    output_image_path = gen_test_util.test_file_path(output_image)
    with open(output_image_path, 'rb') as img:
      img_bytes = img.read()
    self.assertEqual(
        saved_dcm.PixelData, pydicom.encaps.encapsulate([img_bytes])
    )

  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  def test_create_encapsulated_flat_image_dicom_additional_tags_succeeds(
      self, unused_mock
  ):
    image_path = gen_test_util.test_file_path('logo.jpg')
    dcm = dicom_slide_coordinates_microscopic_image.create_encapsulated_flat_image_dicom(
        image_path,
        _INSTANCE_UID,
        ingest_const.DicomSopClasses.SLIDE_COORDINATES_IMAGE,
        _test_dicom_json(patient_name='John^Doe'),
    )

    # Saves DICOM file locally and read it.
    dcm_tmp_filepath = os.path.join(self.create_tempdir(), 'test_jpg_tags.dcm')
    dcm.save_as(dcm_tmp_filepath, write_like_original=False)
    saved_dcm = pydicom.dcmread(dcm_tmp_filepath)
    logging.info(saved_dcm)

    self.assertEqual(saved_dcm.StudyInstanceUID, _STUDY_UID)
    self.assertEqual(saved_dcm.SeriesInstanceUID, _SERIES_UID)
    self.assertEqual(saved_dcm.SOPInstanceUID, _INSTANCE_UID)
    self.assertEqual(
        saved_dcm.SOPClassUID,
        ingest_const.DicomSopClasses.SLIDE_COORDINATES_IMAGE.uid,
    )
    self.assertEqual(saved_dcm.PhotometricInterpretation, 'YBR_FULL_422')
    self.assertEqual(saved_dcm.SamplesPerPixel, 3)
    self.assertEqual(saved_dcm.PatientName, 'John^Doe')
    self.assertIn('OpticalPathSequence', saved_dcm)

    with open(image_path, 'rb') as img:
      img_bytes = img.read()
    self.assertEqual(
        saved_dcm.PixelData, pydicom.encaps.encapsulate([img_bytes])
    )

  def test_create_encapsulated_flat_image_dicom_grayscale_succeeds(self):
    image_path = gen_test_util.test_file_path('logo_grayscale.jpg')
    dcm = dicom_slide_coordinates_microscopic_image.create_encapsulated_flat_image_dicom(
        image_path,
        _INSTANCE_UID,
        ingest_const.DicomSopClasses.SLIDE_COORDINATES_IMAGE,
        _test_dicom_json(),
    )

    # Saves DICOM file locally and read it.
    dcm_tmp_filepath = os.path.join(self.create_tempdir(), 'test_jpg_gray.dcm')
    dcm.save_as(dcm_tmp_filepath, write_like_original=False)
    saved_dcm = pydicom.dcmread(dcm_tmp_filepath)
    logging.info(saved_dcm)

    self.assertEqual(saved_dcm.StudyInstanceUID, _STUDY_UID)
    self.assertEqual(saved_dcm.SeriesInstanceUID, _SERIES_UID)
    self.assertEqual(saved_dcm.SOPInstanceUID, _INSTANCE_UID)
    self.assertEqual(
        saved_dcm.SOPClassUID,
        ingest_const.DicomSopClasses.SLIDE_COORDINATES_IMAGE.uid,
    )
    self.assertEqual(saved_dcm.PhotometricInterpretation, 'MONOCHROME2')
    self.assertEqual(saved_dcm.SamplesPerPixel, 1)
    self.assertIn('OpticalPathSequence', saved_dcm)

    with open(image_path, 'rb') as img:
      img_bytes = img.read()
    self.assertEqual(
        saved_dcm.PixelData, pydicom.encaps.encapsulate([img_bytes])
    )

  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  def test_create_encapsulated_flat_image_dicom_includes_tif_tags(
      self, unused_mock
  ):
    temp_dir = self.create_tempdir()
    src_image_path = gen_test_util.test_file_path('logo_tags.tif')
    image_path = os.path.join(temp_dir, 'logo_tags.tif')
    shutil.copyfile(src_image_path, image_path)
    dcm = dicom_slide_coordinates_microscopic_image.create_encapsulated_flat_image_dicom(
        image_path,
        _INSTANCE_UID,
        ingest_const.DicomSopClasses.SLIDE_COORDINATES_IMAGE,
        _test_dicom_json(),
    )

    # Saves DICOM file locally and read it.
    dcm_tmp_filepath = os.path.join(temp_dir, 'logo_tags.dcm')
    dcm.save_as(dcm_tmp_filepath, write_like_original=False)
    saved_dcm = pydicom.dcmread(dcm_tmp_filepath)
    logging.info(saved_dcm)

    self.assertEqual(saved_dcm.StudyInstanceUID, _STUDY_UID)
    self.assertEqual(saved_dcm.SeriesInstanceUID, _SERIES_UID)
    self.assertEqual(saved_dcm.SOPInstanceUID, _INSTANCE_UID)
    self.assertEqual(
        saved_dcm.SOPClassUID,
        ingest_const.DicomSopClasses.SLIDE_COORDINATES_IMAGE.uid,
    )
    self.assertEqual(saved_dcm.PhotometricInterpretation, 'RGB')
    self.assertEqual(saved_dcm.SamplesPerPixel, 3)
    self.assertEqual(saved_dcm.AcquisitionDateTime, '20220831131415.000000')
    self.assertEqual(saved_dcm.PixelSpacing, [0.254, 0.635])
    self.assertIn('OpticalPathSequence', saved_dcm)

    output_image_path = gen_test_util.test_file_path(
        'logo_tags_tif_converted.jp2'
    )
    with open(output_image_path, 'rb') as img:
      img_bytes = img.read()
    self.assertEqual(
        saved_dcm.PixelData, pydicom.encaps.encapsulate([img_bytes])
    )

  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  @mock.patch.object(datetime, 'datetime', autospec=True)
  def test_create_encapsulated_flat_image_dicom_invalid_time_tif_tag(
      self,
      mock_datetime,
      unused_mock,
  ):
    mock_datetime.strptime.side_effect = ValueError()
    temp_dir = self.create_tempdir()
    src_image_path = gen_test_util.test_file_path('logo_tags.tif')
    image_path = os.path.join(temp_dir, 'logo_tags.tif')
    shutil.copyfile(src_image_path, image_path)
    dcm = dicom_slide_coordinates_microscopic_image.create_encapsulated_flat_image_dicom(
        image_path,
        _INSTANCE_UID,
        ingest_const.DicomSopClasses.SLIDE_COORDINATES_IMAGE,
        _test_dicom_json(),
    )

    # Saves DICOM file locally and read it.
    dcm_tmp_filepath = os.path.join(temp_dir, 'logo_tags.dcm')
    dcm.save_as(dcm_tmp_filepath, write_like_original=False)
    saved_dcm = pydicom.dcmread(dcm_tmp_filepath)
    logging.info(saved_dcm)

    self.assertEqual(saved_dcm.StudyInstanceUID, _STUDY_UID)
    self.assertEqual(saved_dcm.SeriesInstanceUID, _SERIES_UID)
    self.assertEqual(saved_dcm.SOPInstanceUID, _INSTANCE_UID)
    self.assertEqual(
        saved_dcm.SOPClassUID,
        ingest_const.DicomSopClasses.SLIDE_COORDINATES_IMAGE.uid,
    )
    self.assertEqual(saved_dcm.PhotometricInterpretation, 'RGB')
    self.assertEqual(saved_dcm.SamplesPerPixel, 3)
    self.assertNotIn('AcquisitionDateTime', saved_dcm)
    self.assertEqual(saved_dcm.PixelSpacing, [0.254, 0.635])
    self.assertIn('OpticalPathSequence', saved_dcm)

    output_image_path = gen_test_util.test_file_path(
        'logo_tags_tif_converted.jp2'
    )
    with open(output_image_path, 'rb') as img:
      img_bytes = img.read()
    self.assertEqual(
        saved_dcm.PixelData, pydicom.encaps.encapsulate([img_bytes])
    )


if __name__ == '__main__':
  absltest.main()
