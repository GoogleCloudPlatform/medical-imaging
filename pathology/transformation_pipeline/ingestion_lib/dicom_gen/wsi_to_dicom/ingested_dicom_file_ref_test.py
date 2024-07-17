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
"""Tests for ingested_dicom_file_ref."""
import os

from absl.testing import absltest
import pydicom

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingested_dicom_file_ref


class IngestedDicomFileRefTest(absltest.TestCase):
  """Unit tests for IngestDicomFileRef."""

  def test_raise_error_on_uninitialized_transfer_syntax_access(self):
    ingest_ref = ingested_dicom_file_ref.IngestDicomFileRef()
    with self.assertRaises(ingested_dicom_file_ref.DicomIngestError):
      cloud_logging_client.error(str(ingest_ref.transfer_syntax))

  def test_raise_error_double_initialization_of_transfer_syntax(self):
    ingest_ref = ingested_dicom_file_ref.IngestDicomFileRef()
    ingest_ref.set_transfer_syntax('abc')
    with self.assertRaises(ingested_dicom_file_ref.DicomIngestError):
      ingest_ref.set_transfer_syntax('def')

  def test_set_once_transfer_syntax_and_read(self):
    ingest_ref = ingested_dicom_file_ref.IngestDicomFileRef()
    ingest_ref.set_transfer_syntax('abc')
    self.assertEqual(ingest_ref._transfer_syntax, 'abc')

  def test_load_ingest_dicom_fileref_invalid_file(self):
    temp_dir = self.create_tempdir()
    bad_file_path = os.path.join(temp_dir, 'bad_file.txt')
    with open(bad_file_path, 'wt') as outfile:
      outfile.write('bad dicom file.')

    with self.assertRaises(pydicom.errors.InvalidDicomError):
      ingested_dicom_file_ref.load_ingest_dicom_fileref(bad_file_path)

  def test_load_ingest_dicom_fileref_valid_file(self):
    wikipedia_dicom_path = gen_test_util.test_file_path('test_wikipedia.dcm')

    dcm_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        wikipedia_dicom_path
    )

    self.assertEqual(dcm_ref.source, wikipedia_dicom_path)

  def test_load_ingest_dicom_fileref_raises_if_cannot_read_transfer_syntax(
      self,
  ):
    path = os.path.join(self.create_tempdir(), 'test_dicom.dcm')
    file_meta = pydicom.dataset.FileMetaDataset()
    ds = pydicom.dataset.FileDataset(
        '', pydicom.Dataset(), file_meta=file_meta, preamble=b'\0' * 128
    )
    ds.save_as(path)
    with self.assertRaises(ingested_dicom_file_ref.DicomIngestError):
      ingested_dicom_file_ref.load_ingest_dicom_fileref(path)


if __name__ == '__main__':
  absltest.main()
