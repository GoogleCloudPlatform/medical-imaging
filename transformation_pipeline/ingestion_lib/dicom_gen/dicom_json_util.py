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
"""Utility function to merge json formatted dicom with pydicom dataset."""
from typing import Any, Dict, Mapping, MutableMapping, Optional

import pydicom

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import ingest_const

UID_TRIPLE_TAGS = frozenset([
    pydicom.tag.Tag(ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID),
    pydicom.tag.Tag(ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID),
    pydicom.tag.Tag(ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID),
])


class MissingAccessionNumberMetadataError(Exception):

  def __init__(self):
    super().__init__(ingest_const.ErrorMsgs.MISSING_ACCESSION_NUMBER)


class MissingSeriesUIDInMetadataError(Exception):

  def __init__(self):
    super().__init__(ingest_const.ErrorMsgs.MISSING_SERIES_UID)


class MissingStudyUIDInMetadataError(Exception):

  def __init__(self):
    super().__init__(ingest_const.ErrorMsgs.MISSING_STUDY_UID)


class MissingPatientIDMetadataError(Exception):

  def __init__(self):
    super().__init__(ingest_const.ErrorMsgs.MISSING_PATIENT_ID)


def merge_json_metadata_with_pydicom_ds(
    dcm_file: pydicom.dataset.Dataset,
    dcm_json: Optional[Dict[str, Any]] = None,
    tags_to_skip: Optional[Any] = None,
) -> pydicom.dataset.Dataset:
  """Merge JSON formatted dataset with existing pydicom Dataset.

  Args:
    dcm_file: pydicom Dataset.
    dcm_json: JSON formatted DICOM.
    tags_to_skip: Tags to be skipped from JSON when merging.

  Returns:
    Merged pydicom Dataset.
  """
  if not dcm_json:
    return dcm_file
  if not tags_to_skip:
    tags_to_skip = set()

  json_ds = pydicom.dataset.Dataset.from_json(dcm_json)
  for key, value in json_ds.items():
    if key in tags_to_skip:
      continue
    dcm_file[key] = value
  return dcm_file


def _get_dicom_tag_key_val(
    dcm_json: Mapping[str, Any], address: str
) -> Optional[str]:
  """Return tag value from DICOM JSON.

  Args:
    dcm_json: DICOM JSON.
    address: Address to return value of.

  Returns:
    Value stored at address.
  """
  tag = dcm_json.get(address)
  if tag is None:
    return None
  value = tag.get(ingest_const.VALUE)
  if value is None or not isinstance(value, list) or not value:
    return None
  return value[0]


def missing_patient_id(dcm_json: Mapping[str, Any]) -> bool:
  patient_id = _get_dicom_tag_key_val(
      dcm_json, ingest_const.DICOMTagAddress.PATIENT_ID
  )
  return patient_id is None or not patient_id


def get_patient_id(dcm_json: Mapping[str, Any]) -> str:
  """Returns DICOM patient id stored in metadata."""
  patient_id = _get_dicom_tag_key_val(
      dcm_json, ingest_const.DICOMTagAddress.PATIENT_ID
  )
  if patient_id is None or not patient_id:
    cloud_logging_client.logger().info('PatientID not found in metadata.')
    raise MissingPatientIDMetadataError()
  return patient_id


def get_accession_number(dcm_json: Mapping[str, Any]) -> str:
  """Returns DICOM accession number stored in metadata."""
  accession_number = _get_dicom_tag_key_val(
      dcm_json, ingest_const.DICOMTagAddress.ACCESSION_NUMBER
  )
  if accession_number is None or not accession_number:
    cloud_logging_client.logger().warning(
        'AccessionNumber not found in metadata.'
    )
    raise MissingAccessionNumberMetadataError()
  return accession_number


def get_study_instance_uid(
    dcm_json: Mapping[str, Any], log_error: bool = True
) -> str:
  """Returns DICOM Study Instance UID stored in metadata."""
  study_uid = _get_dicom_tag_key_val(
      dcm_json, ingest_const.DICOMTagAddress.STUDY_INSTANCE_UID
  )
  if study_uid is None or not study_uid:
    if log_error:
      cloud_logging_client.logger().warning(
          'StudyInstanceUID not found in metadata.'
      )
    raise MissingStudyUIDInMetadataError()
  return study_uid


def set_study_instance_uid_in_metadata(
    dcm_json: MutableMapping[str, Any], study_uid: str
):
  """Set DICOM Study Instance UID in DICOM formated JSON metadata."""
  try:
    old_uid = get_study_instance_uid(dcm_json, log_error=False)
  except MissingStudyUIDInMetadataError:
    old_uid = None
  dcm_json[ingest_const.DICOMTagAddress.STUDY_INSTANCE_UID] = {
      ingest_const.VR: ingest_const.DICOMVRCodes.UI,
      ingest_const.VALUE: [study_uid],
  }
  if old_uid is None:
    cloud_logging_client.logger().info(
        'Setting DICOM Study Instance UID defined in metadata.',
        {ingest_const.LogKeywords.study_instance_uid: study_uid},
        dcm_json,
    )
  else:
    cloud_logging_client.logger().info(
        'Overriding DICOM Study Instance UID defined in metadata.',
        {
            ingest_const.LogKeywords.PREVIOUS_STUDY_INSTANCE_UID: old_uid,
            ingest_const.LogKeywords.study_instance_uid: study_uid,
        },
        dcm_json,
    )


def get_series_instance_uid(dcm_json: Mapping[str, Any]) -> str:
  series_uid = _get_dicom_tag_key_val(
      dcm_json, ingest_const.DICOMTagAddress.SERIES_INSTANCE_UID
  )
  if series_uid is None or not series_uid:
    cloud_logging_client.logger().warning(
        'SeriesInstanceUID not found in metadata.'
    )
    raise MissingSeriesUIDInMetadataError()
  return series_uid
