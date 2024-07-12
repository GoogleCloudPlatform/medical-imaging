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
"""Unit tests for ancillary_dicom_gen."""

import datetime
import os
import shutil
import typing
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import pydicom

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_general_equipment
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_dicom_gen
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_image_extractor
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util
from transformation_pipeline.ingestion_lib.dicom_util import dicom_test_util


class AncillaryDicomGenTest(parameterized.TestCase):
  """Tests ancillary_dicom_gen."""

  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_generate_ancillary_dicom_succeeds(self, unused_mock):
    out_dir = self.create_tempdir()
    dcm = typing.cast(
        pydicom.FileDataset,
        dicom_test_util.create_test_dicom_instance(
            dcm_json=dicom_test_util.create_metadata_dict()
        ),
    )
    dcm.ImagedVolumeWidth = 500
    dcm.ImagedVolumeHeight = 500
    dcm.PatientName = 'test'
    dcm.SharedFunctionalGroupsSequence = []
    dcm_file_path = os.path.join(out_dir, 'test.dcm')
    dcm.save_as(dcm_file_path, write_like_original=False)
    test_img_path = gen_test_util.test_file_path('barcode_cant_read.jpg')
    test_img = os.path.join(out_dir, 'barcode_cant_read.jpg')
    shutil.copyfile(test_img_path, test_img)
    svs_path = ''
    dcm_gen = abstract_dicom_generation.GeneratedDicomFiles(svs_path, None)
    dcm_gen.generated_dicom_files = [dcm_file_path]
    ds = pydicom.Dataset()
    ds.PatientName = 'Bob'
    ds.StudyInstanceUID = '1.2.3'
    ds.SeriesInstanceUID = '1.2.3.4'
    metadata = ds.to_json_dict()
    img = ancillary_image_extractor.AncillaryImage(test_img)
    img.photometric_interpretation = 'RGB'
    img.extracted_without_decompression = True
    svs_metadata = {}
    scan_datetime = datetime.datetime.now(datetime.timezone.utc)
    ac_date = datetime.datetime.strftime(scan_datetime, '%Y%m%d')
    svs_metadata['AcquisitionDate'] = pydicom.DataElement(
        '00080022', 'DA', ac_date
    )
    additional_metadata = pydicom.Dataset()
    additional_metadata.FrameOfReferenceUID = '2.3.4'
    additional_metadata.PositionReferenceIndicator = 'Edge'

    sec_capture_list = ancillary_dicom_gen.generate_ancillary_dicom(
        dcm_gen,
        [img],
        metadata,
        [],
        additional_metadata,
        svs_metadata,
    )

    self.assertLen(sec_capture_list, 1)
    dcm = pydicom.dcmread(sec_capture_list[0])
    self.assertEqual(dcm.PatientName, 'Bob')
    self.assertEqual(dcm.StudyInstanceUID, '1.2.3')
    self.assertEqual(dcm.SeriesInstanceUID, '1.2.3.4')
    self.assertEqual(dcm.InstanceNumber, 2)
    self.assertEqual(dcm.PhotometricInterpretation, 'RGB')
    self.assertEqual(
        dcm.DerivationDescription,
        (
            'Image bytes extracted from '
            'SVS file without decompression and ecapsulated in DICOM;'
            ' image unchanged.'
        ),
    )
    self.assertEqual(dcm.AcquisitionDate, ac_date)
    self.assertEqual(dcm.FrameOfReferenceUID, '2.3.4')
    self.assertEqual(dcm.PositionReferenceIndicator, 'Edge')
    self.assertListEqual(
        list(dcm.ImageType),
        ['ORIGINAL', 'PRIMARY', 'BARCODE_CANT_READ', 'NONE'],
    )

  @parameterized.parameters(
      ('label', 'LABEL'),
      ('thumbnail', 'THUMBNAIL'),
      ('macro', 'OVERVIEW'),
      ('other', 'OTHER'),
  )
  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_gen_ancillary_dicom_instance_succeeds(
      self, label, imagetype, unused_mock
  ):
    out_dir = self.create_tempdir()
    test_img_path = gen_test_util.test_file_path('barcode_datamatrix.jpeg')
    test_img = os.path.join(out_dir, 'barcode_datamatrix.jpeg')
    shutil.copyfile(test_img_path, test_img)

    ds = pydicom.dataset.Dataset()
    ds.file_meta = pydicom.dataset.Dataset()
    ds.file_meta.MediaStorageSOPClassUID = '1.2.840.10008.5.1.4.1.1.77.1.6'
    ds.file_meta.TransferSyntaxUID = '1.2.840.10008.1.2.4.50'
    ds.file_meta.MediaStorageSOPInstanceUID = '4.5.6'
    ds.PatientName = 'test'
    ds.StudyInstanceUID = '1.2.3'
    ds.SeriesInstanceUID = '4.5.6'
    ds.DimensionOrganizationType = 'TILED_FULL'

    img = ancillary_image_extractor.AncillaryImage(test_img)
    img.photometric_interpretation = 'YBR_FULL_422'
    img.extracted_without_decompression = False
    additional_metadata = pydicom.Dataset()
    additional_metadata.FrameOfReferenceUID = '2.3.5'

    tst_dcm_path = ancillary_dicom_gen._gen_ancillary_dicom_instance(
        test_img,
        img,
        ds,
        label,
        '9',
        {},
        [],
        additional_metadata,
    )
    dcm = pydicom.dcmread(tst_dcm_path)
    self.assertEqual(dcm.PatientName, 'test')
    self.assertEqual(dcm.StudyInstanceUID, '1.2.3')
    self.assertEqual(dcm.SeriesInstanceUID, '4.5.6')
    self.assertEqual(dcm.InstanceNumber, 9)
    self.assertEqual(dcm.Manufacturer, 'GOOGLE')
    self.assertEqual(dcm.ManufacturerModelName, 'DPAS_transformation_pipeline')
    self.assertEqual(
        dcm.SoftwareVersions,
        cloud_logging_client.get_build_version(
            dicom_general_equipment._MAX_STRING_LENGTH_DICOM_SOFTWARE_VERSION_TAG
        ),
    )
    self.assertListEqual(
        list(dcm.ImageType), ['ORIGINAL', 'PRIMARY', imagetype, 'NONE']
    )
    self.assertEqual(dcm.PhotometricInterpretation, 'YBR_FULL_422')
    self.assertEqual(
        dcm.DerivationDescription,
        (
            'Image uncompressed from SVS file and saved '
            'using lossy jpeg compression (quality: 95%).'
        ),
    )
    self.assertEqual(dcm.FrameOfReferenceUID, '2.3.5')
    self.assertEmpty(dcm.PositionReferenceIndicator)


if __name__ == '__main__':
  absltest.main()
