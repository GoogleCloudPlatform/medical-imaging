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
"""Holds representation of DICOM files ingested for pyramid generation."""
import pydicom

from transformation_pipeline.ingestion_lib.dicom_gen import dicom_file_ref
from transformation_pipeline.ingestion_lib.dicom_gen import wsi_dicom_file_ref


class DicomIngestError(Exception):
  pass


class IngestDicomFileRef(wsi_dicom_file_ref.WSIDicomFileRef):
  """Holds representation of DICOM files ingested for pyramid generation."""

  def __init__(self):
    super().__init__()
    self._transfer_syntax = None

  def set_transfer_syntax(self, value: str):
    if self._transfer_syntax is not None:
      raise DicomIngestError('transfer_syntax_set_twice')
    self._transfer_syntax = value.strip()

  @property
  def transfer_syntax(self) -> str:
    if self._transfer_syntax is None:
      raise DicomIngestError('transfer_not_initialized')
    return self._transfer_syntax


def load_ingest_dicom_fileref(path: str) -> IngestDicomFileRef:
  """Reads DICOM instance and returns initialized IngestDicomFileRef.

  Args:
    path: Path to DICOM instance.

  Returns:
    IngestDicomFileRef

  Raises:
    pydicom.errors.InvalidDicomError: Invalid DICOM file.
    DicomIngestError: Unaable to determine transfer syntax.
  """
  with pydicom.dcmread(path, defer_size='512 KB', force=False) as dcm:
    dcm_ref = dicom_file_ref.init_from_loaded_file(
        path, dcm, IngestDicomFileRef()
    )
    try:
      dcm_ref.set_transfer_syntax(dcm.file_meta.TransferSyntaxUID)
    except AttributeError as exp:
      raise DicomIngestError(
          'unable_to_determine_dicom_transfer_syntrax'
      ) from exp
    return dcm_ref
