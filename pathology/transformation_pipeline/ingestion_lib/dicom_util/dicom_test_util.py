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
"""DICOM test utils."""

import json
import os
import re
from typing import Any, Mapping, MutableMapping, Optional, Union

import pydicom

from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import wsi_dicom_file_ref
from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard

STUDY_UID = '1.2.3'
SERIES_UID = '1.2.3.4'
INSTANCE_UID = '1.2.3.4.5'
CLASS_UID = '1.2.3.4.5.6'


def _convert_to_hex_address(keyword: str) -> str:
  """Converts a string value to its dicom tag hex address.

  Args:
    keyword: DICOM tag keyword or hex tag address.

  Returns:
    str of hex address

  Raises:
    ValueError: Cannot find DICOM tag address for keyword.
  """
  address = dicom_standard.dicom_standard_util().get_keyword_address(
      keyword, striphex=True
  )
  if address is not None:
    return address
  address = keyword.strip().lower()
  if address.startswith('0x'):
    address = address[2:]
  if re.fullmatch('[a-f,0-9]{8,8}', address) is None:
    raise ValueError(f'Can not find DICOM tag address for keyword {keyword}')
  return address


def _convert_to_dicom_format(value: str, vr: str) -> MutableMapping[str, Any]:
  """Converts a string value to dicom format value.

  Args:
    value: Value to be stored.
    vr: DICOM vr type.

  Returns:
    dict for value and value representation.
  """
  return {'Value': [value], 'vr': vr}


def add_dicom_tag(
    dicom_meta: MutableMapping[str, MutableMapping[str, Any]],
    dicom_tag_keyword: str,
    value: Union[str, int, float],
    vr: Optional[str] = None,
) -> None:
  """Adds DICOM tag metadata to DICOM JSON Mapping in place.

  Args:
    dicom_meta: DICOM metadata JSON Dict.
    dicom_tag_keyword: DICOM tag keyword.
    value: Tag value (str, int, float).
    vr: Optional tag vr type

  Raises:
    ValueError: Specified VR type is not a valid vr type for tag or
                Cannot find vr type of specified DICOM tag.
  """
  tag_hex = _convert_to_hex_address(dicom_tag_keyword)
  tag_vr_types = dicom_standard.dicom_standard_util().get_tag_vr(tag_hex)
  if vr is not None and vr:
    if tag_vr_types is not None and vr not in tag_vr_types:
      raise ValueError('Sepecified VR not defined VR type for tag.')
  else:
    if tag_vr_types is None:
      raise ValueError(
          f'Cannot find vr type of specified DICOM tag {dicom_tag_keyword}'
      )
    vr = tag_vr_types.pop()
  vr_type_set = set(vr)
  if dicom_standard.dicom_standard_util().is_vr_float_type(vr_type_set):
    value = float(value)
  elif dicom_standard.dicom_standard_util().is_vr_int_type(vr_type_set):
    value = int(value)
  else:
    value = str(value)
  dicom_meta[tag_hex] = _convert_to_dicom_format(value, vr)


def create_metadata_dict() -> MutableMapping[str, MutableMapping[str, Any]]:
  """Creates a dict of dicom metadata.

  Returns:
    dict with str dicom tags as key.
  """
  metadata_ds = pydicom.Dataset()
  metadata_ds.StudyInstanceUID = STUDY_UID
  metadata_ds.SeriesInstanceUID = SERIES_UID
  metadata_ds.SOPClassUID = CLASS_UID
  metadata_ds.SOPInstanceUID = INSTANCE_UID
  return json.loads(metadata_ds.to_json())


def _create_json_wsi_metadata(
    metadata_changes: Optional[Mapping[str, Union[str, int, float]]] = None,
) -> MutableMapping[str, MutableMapping[str, Union[str, int, float]]]:
  """Returns JSON formatted DICOM test metadata."""
  metadata_dict = create_metadata_dict()
  if metadata_changes is not None and metadata_changes:
    for key, value in metadata_changes.items():
      if key == ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG:
        add_dicom_tag(
            metadata_dict,
            ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG,
            value,
            'LT',
        )
      else:
        add_dicom_tag(metadata_dict, key, value)
  return metadata_dict


def create_mock_non_dpas_generated_wsi_fref(
    metadata_changes: Optional[Mapping[str, Union[str, int, float]]] = None,
) -> wsi_dicom_file_ref.WSIDicomFileRef:
  """Returns mocked WSIDicomFileRef initialized from mock metadata."""
  return wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_json(
      _create_json_wsi_metadata(metadata_changes)
  )


def create_mock_dpas_generated_dicom_fref(
    metadata_changes: Optional[Mapping[str, Union[str, int, float]]] = None,
) -> wsi_dicom_file_ref.WSIDicomFileRef:
  """Returns WSIDicomFileRef with default initialized mocked metadata."""
  metadata_updates = {
      ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: (
          f'{ingest_const.DPAS_UID_PREFIX}.1.2.3'
      ),
      ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: (
          f'{ingest_const.DPAS_UID_PREFIX}.1.2.3.4'
      ),
      ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
          f'{ingest_const.DPAS_UID_PREFIX}.1.2.3.4.5'
      ),
      ingest_const.DICOMTagKeywords.IMAGED_VOLUME_WIDTH: '12.12',
      ingest_const.DICOMTagKeywords.IMAGED_VOLUME_HEIGHT: '13.13',
      ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_COLUMNS: 40000,
      ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_ROWS: 50000,
      ingest_const.DICOMTagKeywords.MODALITY: 'SM',
      ingest_const.DICOMTagKeywords.COLUMNS: '256',
      ingest_const.DICOMTagKeywords.ROWS: '512',
      ingest_const.DICOMTagKeywords.CONCATENATION_UID: (
          f'{ingest_const.DPAS_UID_PREFIX}.1.2.3.4.6'
      ),
      ingest_const.DICOMTagKeywords.CONCATENATION_FRAME_OFFSET_NUMBER: '1',
      ingest_const.DICOMTagKeywords.IN_CONCATENATION_NUMBER: '2',
      ingest_const.DICOMTagKeywords.IMAGE_TYPE: 'ORIGINAL\\PRIMARY\\VOLUME',
      ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: 'ABCDEFG',
  }
  if metadata_changes is not None:
    metadata_updates.update(metadata_changes)
  metadata = _create_json_wsi_metadata(metadata_updates)
  return wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_json(metadata)


def create_test_dicom_instance(
    temp_dir: Optional[str] = None,
    filepath: str = '',
    study: str = '1.2.3',
    series: str = '1.2.3.4',
    sop_instance_uid: str = '1.2.3.4.5',
    sop_class_uid: str = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid,
    dcm_json: Optional[Mapping[str, Any]] = None,
) -> Union[str, pydicom.FileDataset]:
  """Creates minimal valid DICOM.

  Args:
    temp_dir: Directory to write DICOM instance to. If set, DICOM is written to
      this directory. If unset, pydicom.Dataset is returned instead.
    filepath: Filename to write to if set.
    study: DICOM StudyInstanceUID.
    series: DICOM SeriesInstanceUID.
    sop_instance_uid: DICOM SOPInstanceUID.
    sop_class_uid: DICOM SOPClassUID for instance.
    dcm_json: Optional DICOM metadata to embed in the DICOM instance.

  Returns:
    Path to file written.
  """
  if temp_dir:
    filename = os.path.join(
        temp_dir, f'{study}.{series}.{sop_instance_uid}.dcm'
    )
  else:
    filename = filepath
  base_ds = pydicom.Dataset.from_json(json.dumps(dcm_json or {}))
  dcm = pydicom.FileDataset(filename, dataset=base_ds, preamble=b'\0' * 128)
  dcm.file_meta = pydicom.dataset.FileMetaDataset()
  dcm.file_meta.MediaStorageSOPClassUID = sop_class_uid
  dcm.file_meta.TransferSyntaxUID = (
      ingest_const.DicomImageTransferSyntax.EXPLICIT_VR_LITTLE_ENDIAN
  )
  if sop_class_uid == ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid:
    dcm.file_meta.MediaStorageSOPInstanceUID = sop_instance_uid
    dcm.file_meta.ImplementationClassUID = (
        ingest_const.WSI_IMPLEMENTATION_CLASS_UID
    )
  dcm.StudyInstanceUID = study
  dcm.SeriesInstanceUID = series
  dcm.SOPInstanceUID = sop_instance_uid
  if 'SOPClassUID' not in dcm:
    dcm.SOPClassUID = sop_class_uid
  dcm.is_implicit_VR = False
  dcm.is_little_endian = True
  if temp_dir or filepath:
    dcm.save_as(filename, write_like_original=False)
    return filename
  return dcm
