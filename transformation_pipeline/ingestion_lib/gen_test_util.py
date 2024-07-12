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
"""Helper functions for tests."""
import os

from absl import flags
import pydicom

from transformation_pipeline.ingestion_lib import ingest_const


def test_file_path(*path: str) -> str:
  """Returns path to file in unit test."""
  return os.path.join(
      flags.FLAGS.test_srcdir,
      'transformation_pipeline/testdata',
      *path
  )


def create_mock_wsi_dicom_dataset(barcode_value: str = '') -> pydicom.Dataset:
  """Returns pydicom Dataset with mock wsi DICOM metadata."""
  ds = pydicom.Dataset()
  if barcode_value:
    ds.BarcodeValue = barcode_value
  ds.StudyInstanceUID = '1.2.3'
  ds.SeriesInstanceUID = '1.2.3.4'
  ds.SOPInstanceUID = '1.2.3.4.5'
  ds.SOPClassUID = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
  ds.Modality = 'SM'
  ds.BitsAllocated = 8
  ds.BitsStored = 8
  ds.HighBit = 7
  ds.SamplesPerPixel = 1
  ds.NumberOfFrames = 1
  ds.Rows = 1
  ds.Columns = 1
  ds.TotalPixelMatrixColumns = 1
  ds.TotalPixelMatrixRows = 1
  ds.SpecimenLabelInImage = 'NO'
  ds.BurnedInAnnotation = 'NO'
  ds.ImageType = '\\'.join(
      [ingest_const.ORIGINAL, ingest_const.PRIMARY, ingest_const.VOLUME]
  )
  return ds


def write_test_dicom(path: str, base_ds: pydicom.Dataset):
  file_meta = pydicom.dataset.FileMetaDataset()
  file_meta.TransferSyntaxUID = (
      ingest_const.DicomImageTransferSyntax.EXPLICIT_VR_LITTLE_ENDIAN
  )
  ds = pydicom.dataset.FileDataset(
      '', base_ds, file_meta=file_meta, preamble=b'\0' * 128
  )
  ds.is_little_endian = True
  ds.is_implicit_VR = False
  ds.save_as(path)
