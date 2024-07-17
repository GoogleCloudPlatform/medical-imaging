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
"""Test for Secondary Capture DICOM Builder."""
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
import numpy as np
import PIL
import pydicom

from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_private_tag_generator
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_secondary_capture


_WSI_INSTANCE_UID = '1.2.3.4'
_STUDY_UID = '1.2.3.4.5'
_SERIES_UID = '1.2.3.4.5.6'
_INSTANCE_UID = '1.2.3.4.5.6.7'
_PRIVATE_TAG_ADDRESS = 0x02
_PRIVATE_TAG_VALUE = 'private_tag_value'
_MOCK_TIME = 1704302201.2582734


class DicomSecondaryCaptureTest(parameterized.TestCase):
  """Test for Secondary Capture DICOM Builder."""

  @parameterized.named_parameters([
      dict(testcase_name='rgb_jpeg', filename='logo.jpg', samples_per_pixel=3),
      dict(
          testcase_name='bw_jpeg',
          filename='logo_grayscale.jpg',
          samples_per_pixel=1,
      ),
  ])
  def test_create_encapsulated_jpeg_secondary_capture(
      self, filename, samples_per_pixel
  ):
    metadata = pydicom.Dataset()
    metadata.SecondaryCaptureDeviceManufacturer = 'test'
    # Open jpeg to get dimensions and encapsulate
    imgfile = gen_test_util.test_file_path(filename)
    with PIL.Image.open(imgfile) as img:
      width, height = img.size
    with open(imgfile, 'rb') as infile:
      expected = infile.read()

    with mock.patch('time.time', autospec=True, return_value=_MOCK_TIME):
      ds = dicom_secondary_capture.create_encapsulated_jpeg_dicom_secondary_capture(
          imgfile,
          _STUDY_UID,
          _SERIES_UID,
          _INSTANCE_UID,
          instance_number=5,
          dcm_metadata=metadata,
          reference_instances=None,
          private_tags=None,
      )

    self.assertEqual(ds.PixelData, pydicom.encaps.encapsulate([expected]))
    # Verify metadata tags
    self.assertEqual(ds.SecondaryCaptureDeviceManufacturer, 'test')
    self.assertEqual(ds.StudyInstanceUID, _STUDY_UID)
    self.assertEqual(ds.SeriesInstanceUID, _SERIES_UID)
    self.assertEqual(ds.SOPInstanceUID, _INSTANCE_UID)
    self.assertEqual(ds.BitsAllocated, 8)
    self.assertEqual(ds.BitsStored, 8)
    self.assertEqual(ds.HighBit, 7)

    self.assertEqual(ds.file_meta.MediaStorageSOPInstanceUID, _INSTANCE_UID)
    self.assertEqual(
        ds.file_meta.MediaStorageSOPClassUID,
        ingest_const.DicomSopClasses.SECONDARY_CAPTURE_IMAGE.uid,
    )
    self.assertEqual(ds.PlanarConfiguration, 0)
    self.assertEqual(ds.PixelRepresentation, 0)
    self.assertEqual(ds.NumberOfFrames, 1)
    self.assertEqual(
        ds.SOPClassUID, ingest_const.DicomSopClasses.SECONDARY_CAPTURE_IMAGE.uid
    )
    self.assertEqual(ds.Columns, width)
    self.assertEqual(ds.Rows, height)
    self.assertEqual(ds.TotalPixelMatrixColumns, width)
    self.assertEqual(ds.TotalPixelMatrixRows, height)
    self.assertEqual(len(ds.PixelData) % 2, 0)
    self.assertEqual(ds.SamplesPerPixel, samples_per_pixel)
    self.assertEqual(ds.PhotometricInterpretation, 'YBR_FULL_422')
    self.assertEqual(ds.InstanceNumber, '5')
    self.assertEqual(ds.ContentTime, '171641.258273')
    self.assertEqual(ds.ContentDate, '20240103')

  @parameterized.named_parameters([
      dict(testcase_name='rgb_jpeg', filename='logo.jpg', samples_per_pixel=3),
      dict(testcase_name='rgb_png', filename='logo.png', samples_per_pixel=3),
      dict(
          testcase_name='rgb_transp_png',
          filename='logo_transparent.png',
          samples_per_pixel=3,
      ),
      dict(
          testcase_name='bw_jpeg',
          filename='logo_grayscale.jpg',
          samples_per_pixel=1,
      ),
  ])
  def test_create_raw_secondary_capture(self, filename, samples_per_pixel):
    imgfile = gen_test_util.test_file_path(filename)
    with PIL.Image.open(imgfile) as img:
      width, height = img.size
    metadata = pydicom.Dataset()
    metadata.SecondaryCaptureDeviceManufacturer = 'test'
    private_tags = [
        dicom_private_tag_generator.DicomPrivateTag(
            ingest_const.DICOMTagKeywords.OOF_SCORE_PRIVATE_TAG,
            'LO',
            _PRIVATE_TAG_VALUE,
        )
    ]
    instances = [
        dicom_secondary_capture.DicomReferencedInstance(
            ingest_const.WSI_IMPLEMENTATION_CLASS_UID, _WSI_INSTANCE_UID
        )
    ]

    with mock.patch('time.time', autospec=True, return_value=_MOCK_TIME):
      ds = dicom_secondary_capture.create_raw_dicom_secondary_capture_from_img(
          imgfile,
          _STUDY_UID,
          _SERIES_UID,
          _INSTANCE_UID,
          instance_number=4,
          dcm_metadata=metadata,
          reference_instances=instances,
          private_tags=private_tags,
      )

    # testing jpg rgb equals, rgb values decode from encapsulated DICOM JPEG
    with PIL.Image.open(imgfile) as img:
      img_bytes = np.asarray(img)
      if len(img_bytes.shape) == 3:
        img_bytes = img_bytes[..., :3]
    self.assertTrue(np.all(np.equal(img_bytes, ds.pixel_array)))
    self.assertEqual(img_bytes.tobytes(), ds.PixelData)
    self.assertEqual(ds.SecondaryCaptureDeviceManufacturer, 'test')
    # verify private tags
    self.assertEqual(
        ds.get_private_item(
            int(ingest_const.DICOMTagKeywords.GROUP_ADDRESS, 16),
            _PRIVATE_TAG_ADDRESS,
            ingest_const.PRIVATE_TAG_CREATOR,
        ).value.strip(),
        _PRIVATE_TAG_VALUE,
    )
    # verify referenced instances
    ref_series = ds.ReferencedSeriesSequence[0]
    ref_instance = ref_series.ReferencedInstanceSequence[0]
    self.assertEqual(ref_series.SeriesInstanceUID, _SERIES_UID)
    self.assertEqual(
        ref_instance.ReferencedSOPClassUID,
        ingest_const.WSI_IMPLEMENTATION_CLASS_UID,
    )
    self.assertEqual(ref_instance.ReferencedSOPInstanceUID, _WSI_INSTANCE_UID)
    self.assertEqual(ds.file_meta.MediaStorageSOPInstanceUID, _INSTANCE_UID)
    self.assertEqual(
        ds.file_meta.MediaStorageSOPClassUID,
        ingest_const.DicomSopClasses.SECONDARY_CAPTURE_IMAGE.uid,
    )
    self.assertEqual(ds.PlanarConfiguration, 0)
    self.assertEqual(ds.PixelRepresentation, 0)
    self.assertEqual(ds.NumberOfFrames, 1)
    self.assertEqual(
        ds.SOPClassUID, ingest_const.DicomSopClasses.SECONDARY_CAPTURE_IMAGE.uid
    )
    self.assertEqual(ds.Columns, width)
    self.assertEqual(ds.Rows, height)
    self.assertEqual(ds.TotalPixelMatrixColumns, width)
    self.assertEqual(ds.TotalPixelMatrixRows, height)
    self.assertEqual(len(ds.PixelData) % 2, 0)
    self.assertEqual(ds.SamplesPerPixel, samples_per_pixel)
    self.assertEqual(ds.PhotometricInterpretation, 'RGB')
    self.assertEqual(ds.InstanceNumber, '4')
    self.assertEqual(ds.ContentTime, '171641.258273')
    self.assertEqual(ds.ContentDate, '20240103')

  def test_create_encapsulated_jpeg_secondary_capture_raises_not_jpeg(self):
    with self.assertRaises(
        dicom_secondary_capture.UnsupportedDICOMSecondaryCaptureImageError
    ):
      dicom_secondary_capture.create_encapsulated_jpeg_dicom_secondary_capture(
          gen_test_util.test_file_path('logo.png'),
          _STUDY_UID,
          _SERIES_UID,
          _INSTANCE_UID,
          dcm_metadata=None,
          reference_instances=None,
          private_tags=None,
      )

  def test_pad_pixel_data_to_even_length(self):
    self.assertLen(
        dicom_secondary_capture._build_dicom_instance(
            b'0',
            (1, 1),
            1,
            None,
            _STUDY_UID,
            _SERIES_UID,
            _INSTANCE_UID,
            'RGB',
            None,
            None,
            None,
        ).PixelData,
        2,
    )


if __name__ == '__main__':
  absltest.main()
