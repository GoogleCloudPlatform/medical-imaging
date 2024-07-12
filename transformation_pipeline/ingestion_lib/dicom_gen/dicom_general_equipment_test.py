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
"""Tests for dicom_general_equipment."""

from absl.testing import absltest
import pydicom
from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_general_equipment


class DicomGeneralEquipmentTest(absltest.TestCase):

  def test_general_equipment(self):
    ds = pydicom.Dataset()
    dicom_general_equipment.add_ingest_general_equipment(ds)
    self.assertEqual(ds.Manufacturer, 'GOOGLE')
    self.assertEqual(ds.ManufacturerModelName, 'DPAS_transformation_pipeline')
    self.assertEqual(
        ds.SoftwareVersions,
        cloud_logging_client.get_build_version(
            dicom_general_equipment._MAX_STRING_LENGTH_DICOM_SOFTWARE_VERSION_TAG
        ),
    )

  def test_general_equipment_doesnt_overwrite(self):
    ds = pydicom.Dataset()
    ds.Manufacturer = 'Foo'
    ds.ManufacturerModelName = 'Bar'
    ds.SoftwareVersions = '1.2.3'
    dicom_general_equipment.add_ingest_general_equipment(ds)
    self.assertEqual(ds.Manufacturer, 'Foo')
    self.assertEqual(ds.ManufacturerModelName, 'Bar')
    self.assertEqual(ds.SoftwareVersions, '1.2.3')


if __name__ == '__main__':
  absltest.main()
