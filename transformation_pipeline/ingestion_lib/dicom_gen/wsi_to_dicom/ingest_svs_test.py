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
"""Unit tests for IngestSVS."""
import datetime
import os
import typing
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
import pydicom

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_svs
from transformation_pipeline.ingestion_lib.dicom_util import dicom_test_util

_TEST_JSON_METADATA = {
    '00100010': {'Value': [{'Alphabetic': 'test'}], 'vr': 'PN'}
}


class IngestSVSTest(absltest.TestCase):
  """Tests IngestSVS."""

  def test_add_burned_in_annotation_and_spec_label_already_defined(self):
    dcm = pydicom.Dataset()
    dcm.BurnedInAnnotation = 'YES'
    dcm.SpecimenLabelInImage = 'YES'
    (
        ingest_svs._add_burned_in_annotation_and_specimen_label_in_image_if_not_def(
            dcm
        )
    )
    self.assertEqual(dcm.BurnedInAnnotation, 'YES')
    self.assertEqual(dcm.SpecimenLabelInImage, 'YES')

  def test_add_burned_in_annotation_and_spec_label_not_defined(self):
    dcm = pydicom.Dataset()
    (
        ingest_svs._add_burned_in_annotation_and_specimen_label_in_image_if_not_def(
            dcm
        )
    )
    self.assertEqual(dcm.BurnedInAnnotation, 'NO')
    self.assertEqual(dcm.SpecimenLabelInImage, 'NO')

  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_add_metadata_to_wsi_dicom_files_no_files(self):
    dcm_gen = abstract_dicom_generation.GeneratedDicomFiles('', None)
    dcm_gen.generated_dicom_files = []
    svs_metadata = {}
    scan_datetime = datetime.datetime.now(datetime.timezone.utc)
    ac_date = datetime.datetime.strftime(scan_datetime, '%Y%m%d')
    svs_metadata['AcquisitionDate'] = pydicom.DataElement(
        '00080022', 'DA', ac_date
    )

    self.assertFalse(
        ingest_svs.add_metadata_to_wsi_dicom_files(
            dcm_gen,
            [],
            dicom_util.DicomFrameOfReferenceModuleMetadata('1.2.3', 'Side'),
            svs_metadata,
            _TEST_JSON_METADATA,
        )
    )

  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_add_metadata_to_wsi_dicom_files(self):
    out_dir = self.create_tempdir()
    dcm = typing.cast(
        pydicom.FileDataset,
        dicom_test_util.create_test_dicom_instance(
            dcm_json=dicom_test_util.create_metadata_dict()
        ),
    )
    dcm_file_path = os.path.join(out_dir, 'test.dcm')
    dcm.ImageType = 'test_dicom'
    dcm.save_as(dcm_file_path, write_like_original=False)

    dcm_gen = abstract_dicom_generation.GeneratedDicomFiles('', None)
    dcm_gen.generated_dicom_files = [dcm_file_path]
    svs_metadata = {}
    scan_datetime = datetime.datetime.now(datetime.timezone.utc)
    ac_date = datetime.datetime.strftime(scan_datetime, '%Y%m%d')
    svs_metadata['AcquisitionDate'] = pydicom.DataElement(
        '00080022', 'DA', ac_date
    )
    dt = datetime.datetime(
        2022, 5, 3, 3, 34, 24, 884733, tzinfo=datetime.timezone.utc
    )
    save_func = datetime.datetime.strftime
    with mock.patch('datetime.datetime') as mk:
      mk.now = mock.Mock(return_value=dt)
      mk.strftime = save_func
      ingest_svs.add_metadata_to_wsi_dicom_files(
          dcm_gen,
          [],
          dicom_util.DicomFrameOfReferenceModuleMetadata('1.2.3', 'Side'),
          svs_metadata,
          _TEST_JSON_METADATA,
      )
    dcm = pydicom.dcmread(dcm_file_path)
    self.assertTrue(
        dcm.SOPInstanceUID.startswith(uid_generator.TEST_UID_PREFIX)
    )
    self.assertEqual(dcm.ContentDate, '20220503')
    # content time component is derived current time in dicom_util.
    # Dicom_util converts time to UTC formatted DICOM string.
    self.assertEqual(dcm.ContentTime, '033424.884733')
    self.assertEqual(dcm.PatientName, 'test')
    self.assertEqual(dcm.Manufacturer, 'GOOGLE')
    self.assertEqual(dcm.ManufacturerModelName, 'DPAS_transformation_pipeline')
    self.assertEqual(
        dcm.SoftwareVersions, cloud_logging_client.logger().build_version[:64]
    )
    self.assertEqual(dcm.FrameOfReferenceUID, '1.2.3')
    self.assertEqual(dcm.PositionReferenceIndicator, 'Side')
    self.assertEqual(dcm.AcquisitionDate, ac_date)
    self.assertEqual(dcm.BurnedInAnnotation, 'NO')
    self.assertEqual(dcm.SpecimenLabelInImage, 'NO')


if __name__ == '__main__':
  absltest.main()
