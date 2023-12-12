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
"""Tests for dicom_test_util."""

from absl.testing import absltest
import pydicom

from transformation_pipeline.ingestion_lib.dicom_util import dicom_test_util


class DicomTestUtilTest(absltest.TestCase):

  def test_create_test_dicom_instance(self):
    dcm = dicom_test_util.create_test_dicom_instance()
    self.assertIsNotNone(dcm)

  def test_create_test_dicom_instance_with_uid_triple(self):
    study_uid = '3.2.1'
    series_uid = '4.3.2.1'
    sop_instance_uid = '5.4.3.2.1'
    dcm = dicom_test_util.create_test_dicom_instance(
        study=study_uid, series=series_uid, sop_instance_uid=sop_instance_uid
    )
    self.assertIsNotNone(dcm)
    self.assertIsInstance(dcm, pydicom.FileDataset)
    self.assertEqual(dcm.StudyInstanceUID, study_uid)
    self.assertEqual(dcm.SeriesInstanceUID, series_uid)
    self.assertEqual(dcm.SOPInstanceUID, sop_instance_uid)

  def test_create_test_dicom_instance_write_to_temp_dir(self):
    dcm_path = dicom_test_util.create_test_dicom_instance(
        self.create_tempdir().full_path
    )
    dcm = pydicom.dcmread(dcm_path)
    self.assertIsNotNone(dcm)


if __name__ == '__main__':
  absltest.main()
