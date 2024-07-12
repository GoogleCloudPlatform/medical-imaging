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
"""Validate ingested DICOM."""
import dataclasses
import math
from typing import List, Optional

import pydicom

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingested_dicom_file_ref

# string type const
_LABEL = ingest_const.LABEL
_OVERVIEW = ingest_const.OVERVIEW
_THUMBNAIL = ingest_const.THUMBNAIL
_RESAMPLED = ingest_const.RESAMPLED
_ORIGINAL_PRIMARY_VOLUME = ingest_const.ORIGINAL_PRIMARY_VOLUME
_DERIVED_PRIMARY_VOLUME = ingest_const.DERIVED_PRIMARY_VOLUME
_YES = 'YES'
_NO = 'NO'
_SM = 'SM'

# https://www.dicomlibrary.com/dicom/transfer-syntax/
SUPPORTED_DICOM_TRANSFER_SYNTAX = (
    '1.2.840.10008.1.2',
    '1.2.840.10008.1.2.1',
    ingest_const.DicomImageTransferSyntax.JPEG_LOSSY,
    '1.2.840.10008.1.2.4.51',
    '1.2.840.10008.1.2.4.90',
    '1.2.840.10008.1.2.4.91',
)


@dataclasses.dataclass
class DicomFileInfo:
  """Container holds ingested DICOM instance descriptions."""

  study_uid: str
  series_uid: str
  barcode_value: Optional[str]
  original_image: Optional[ingested_dicom_file_ref.IngestDicomFileRef]
  wsi_image_filerefs: List[
      ingested_dicom_file_ref.IngestDicomFileRef
  ]  # WSI IOD DICOM
  other_dicom_filerefs: List[
      ingested_dicom_file_ref.IngestDicomFileRef
  ]  # All other DICOM


def _dicom_ref_num(dicom_ref_val: str) -> int:
  """Helper for _validate_wsi_dicom_instance converts string to int.

  Args:
    dicom_ref_val: string value to return as integer

  Returns:
    int value or -1 if conversion error occurred.
  """
  try:
    return int(dicom_ref_val)
  except ValueError:
    return -1


def _invalid_total_pixel_matrix_focal_planes(
    dcm_ref: ingested_dicom_file_ref.IngestDicomFileRef,
) -> bool:
  """Tests if total_pixel_matrix_focal_planes is not 1 in wsi instance."""
  if not dcm_ref.total_pixel_matrix_focal_planes:
    # value undefined, pipeline will assume 1
    return False
  try:
    total_pixel_matrix_focal_planes = int(
        dcm_ref.total_pixel_matrix_focal_planes
    )
  except ValueError:
    return True
  return total_pixel_matrix_focal_planes != 1


def _validate_uid(
    uid: str, dcm_ref: Optional[ingested_dicom_file_ref.IngestDicomFileRef]
):
  """Validate UID is formatted correctly.

  Args:
    uid: uid string to test.
    dcm_ref: ingested_dicom_file_ref.IngestDicomFileRef containing UID

  Raises:
    ingested_dicom_file_ref.DicomIngestError: UID incorrectly formatted.
  """
  if not dcm_ref:
    ref_dict = {}
  else:
    ref_dict = dcm_ref.dict()

  if not uid:
    cloud_logging_client.error('Empty UID', ref_dict)
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.DICOM_UID_INCORRECTLY_FORMATTED
    )
  if len(uid) > uid_generator.MAX_LENGTH_OF_DICOM_UID:
    cloud_logging_client.error(
        'UID exceeds max length', ref_dict, {'ERRORING_UID': uid}
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.DICOM_UID_INCORRECTLY_FORMATTED
    )
  for num_str in uid.split('.'):
    if not uid_generator.is_uid_block_correctly_formatted(num_str):
      cloud_logging_client.error(
          'UID block contains invalid char.', ref_dict, {'ERRORING_UID': uid}
      )
      raise ingested_dicom_file_ref.DicomIngestError(
          ingest_const.ErrorMsgs.DICOM_UID_INCORRECTLY_FORMATTED
      )


def _validate_wsi_dicom_instance(
    dcm_ref: ingested_dicom_file_ref.IngestDicomFileRef,
):
  """Validates that wsi dicom is formatted as expected.

  Args:
    dcm_ref: ingested_dicom_file_ref.IngestDicomFileRef to check

  Raises:
    ingested_dicom_file_ref.DicomIngestError: if file not formatted as expected.
  """

  # https://www.dicomlibrary.com/dicom/transfer-syntax/
  if dcm_ref.transfer_syntax not in SUPPORTED_DICOM_TRANSFER_SYNTAX:
    cloud_logging_client.error(
        'DICOM instance encoded with unsupported transfer syntax.',
        {'supported_transfer_syntax': str(SUPPORTED_DICOM_TRANSFER_SYNTAX)},
        dcm_ref.dict(),
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.DICOM_INSTANCE_ENCODED_WITH_UNSUPPORTED_TRANSFER_SYNTAX
    )
  if _dicom_ref_num(dcm_ref.bits_allocated) != 8:
    cloud_logging_client.error(
        'DICOM instance pixel not allocated with 8 bits per pixel.',
        dcm_ref.dict(),
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.WSI_DICOM_INSTANCE_PIXEL_NOT_ALLOCATED_WITH_8_BITS_PER_PIXEL
    )
  if _dicom_ref_num(dcm_ref.bits_stored) != 8:
    cloud_logging_client.error(
        'DICOM instance pixel not stored with 8 bits per pixel.', dcm_ref.dict()
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.WSI_DICOM_INSTANCE_PIXEL_NOT_STORED_WITH_8_BITS_PER_PIXEL
    )
  if _dicom_ref_num(dcm_ref.high_bit) != 7:
    cloud_logging_client.error(
        'DICOM instance pixel high bit != 7.', dcm_ref.dict()
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.WSI_DICOM_INSTANCE_ENCODED_WITH_INVALID_HIGH_PIXEL_BIT
    )
  if _dicom_ref_num(dcm_ref.samples_per_pixel) not in (1, 3):
    cloud_logging_client.error(
        'DICOM instance encoded with samples per pixel != 1 or 3.',
        dcm_ref.dict(),
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.WSI_DICOM_INSTANCE_ENCODED_WITH_INVALID_SAMPLES_PER_PIXEL
    )
  if _dicom_ref_num(dcm_ref.number_of_frames) <= 0:
    cloud_logging_client.error(
        'WSI DICOM instance has 0 frames.', dcm_ref.dict()
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.WSI_DICOM_INSTANCE_HAS_0_FRAMES
    )
  if _dicom_ref_num(dcm_ref.rows) <= 0:
    cloud_logging_client.error('WSI DICOM instance has 0 rows.', dcm_ref.dict())
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.WSI_DICOM_INSTANCE_HAS_0_ROWS
    )
  if _dicom_ref_num(dcm_ref.columns) <= 0:
    cloud_logging_client.error(
        'WSI DICOM instance has 0 columns.', dcm_ref.dict()
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.WSI_DICOM_INSTANCE_HAS_0_COLUMNS
    )
  if _dicom_ref_num(dcm_ref.total_pixel_matrix_columns) <= 0:
    cloud_logging_client.error(
        'WSI DICOM instance has 0 total_pixel_matrix_columns.', dcm_ref.dict()
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        'wsi_dicom_instance_has_0_total_pixel_matrix_columns'
    )
  if _dicom_ref_num(dcm_ref.total_pixel_matrix_rows) <= 0:
    cloud_logging_client.error(
        'WSI DICOM instance has 0 total_pixel_matrix_rows.', dcm_ref.dict()
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.WSI_DICOM_INSTANCE_HAS_0_TOTAL_PIXEL_MATRIX_ROWS
    )
  if dcm_ref.specimen_label_in_image not in (_YES, _NO):
    cloud_logging_client.error(
        'WSI DICOM SpecimenLabelInImage != "YES" or "NO"', dcm_ref.dict()
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.WSI_DICOM_SPECIMEN_LABEL_IN_IMAGE_NOT_YES_OR_NO
    )
  if dcm_ref.burned_in_annotation not in (_YES, _NO):
    cloud_logging_client.error(
        'WSI DICOM BurnedInAnnotation != "YES" or "NO"', dcm_ref.dict()
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.WSI_DICOM_BURNED_IN_ANNOTATION_IN_IMAGE_NOT_YES_OR_NO
    )
  if _invalid_total_pixel_matrix_focal_planes(dcm_ref):
    cloud_logging_client.error(
        'Transforming DICOM instance encoded with total pixel matrix focal'
        ' planes != 1 is unsupported.',
        dcm_ref.dict(),
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.DICOM_INSTANCE_HAS_UNSUPPORTED_TOTAL_PIXEL_MATRIX_FOCAL_PLANE_VALUE
    )
  # validate frame count is as expected
  total_rows = _dicom_ref_num(dcm_ref.total_pixel_matrix_rows)
  total_columns = _dicom_ref_num(dcm_ref.total_pixel_matrix_columns)
  rows = _dicom_ref_num(dcm_ref.rows)
  columns = _dicom_ref_num(dcm_ref.columns)
  row_frames = int(math.ceil(float(total_rows) / float(rows)))
  column_frames = int(math.ceil(float(total_columns) / float(columns)))
  total_frames = row_frames * column_frames
  if _dicom_ref_num(dcm_ref.number_of_frames) != total_frames:
    cloud_logging_client.error(
        'WSI DICOM instance does not have the expected number of frames.',
        {'expected_frame_count': total_frames},
        dcm_ref.dict(),
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.WSI_DICOM_INSTANCE_DOES_NOT_HAVE_EXPECTED_FRAME_COUNT
    )

  if (
      total_frames > 1
      and dcm_ref.dimension_organization_type != ingest_const.TILED_FULL
  ):
    cloud_logging_client.error(
        (
            'WSI DICOM instance has invalid dimensional organization. Only '
            'TILED_FULL is supported.'
        ),
        dcm_ref.dict(),
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.WSI_DICOM_INSTANCE_HAS_INVALID_DIMENSIONAL_ORGANIZATION_TYPE
    )

  ancillary_image_indicator = 0
  if (
      _LABEL in dcm_ref.image_type.upper()
      or _LABEL in dcm_ref.frame_type.upper()
  ):
    ancillary_image_indicator += 1
  if (
      _OVERVIEW in dcm_ref.image_type.upper()
      or _OVERVIEW in dcm_ref.frame_type.upper()
  ):
    ancillary_image_indicator += 1
  if (
      _THUMBNAIL in dcm_ref.image_type.upper()
      or _THUMBNAIL in dcm_ref.frame_type.upper()
  ):
    ancillary_image_indicator += 1
  if ancillary_image_indicator > 1:
    cloud_logging_client.error(
        'DICOM instance image_type/frame_type indeterminate.', dcm_ref.dict()
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.WSI_DICOM_ANCILLARY_INSTANCE_TYPE_INDETERMINATE
    )
  if (
      ancillary_image_indicator == 1
      and _dicom_ref_num(dcm_ref.number_of_frames) > 1
  ):
    cloud_logging_client.error(
        'DICOM ancillary wsi instance image has more than one frame.',
        dcm_ref.dict(),
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.WSI_DICOM_ANCILLARY_INSTANCE_HAS_MORE_THAN_ONE_FRAME
    )


def validate_dicom_files(
    dicom_file_list: List[ingested_dicom_file_ref.IngestDicomFileRef],
) -> DicomFileInfo:
  """Validate ingested DICOM.

  Args:
    dicom_file_list: list of file refs to dicom files ingested

  Returns:
    DicomFileInfo object describing ingested imaging.

  Raises:
    ingested_dicom_file_ref.DicomIngestError: If imaging violates expectation.
  """
  if not dicom_file_list:
    cloud_logging_client.error('No dicom instances found in payload.')
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.DICOM_INSTANCE_NOT_FOUND
    )
  study_uid = None
  series_uid = None
  barcode_value = None
  instance_uid_set = set()
  modality_found = set()
  accession_number_found = set()
  patient_name_found = set()
  patient_id_found = set()
  wsi_image_filerefs = []
  other_dicom_filerefs = []
  original_image_count_found = 0
  original_image = None
  label_count = 0
  overview_count = 0
  thumbnail_count = 0
  for dcm_ref in dicom_file_list:
    # Validate DICOM has StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID
    #                    and SOPClassUID defined.
    if not dcm_ref.study_instance_uid:
      cloud_logging_client.error(
          'DICOM instance missing StudyInstanceUID.', dcm_ref.dict()
      )
      raise ingested_dicom_file_ref.DicomIngestError(
          ingest_const.ErrorMsgs.DICOM_INSTANCE_MISSING_STUDY_INSTANCE_UID
      )
    if not dcm_ref.series_instance_uid:
      cloud_logging_client.error(
          'DICOM instance missing SeriesInstanceUID.', dcm_ref.dict()
      )
      raise ingested_dicom_file_ref.DicomIngestError(
          ingest_const.ErrorMsgs.DICOM_INSTANCE_MISSING_SERIES_INSTANCE_UID
      )
    if not dcm_ref.sop_instance_uid:
      cloud_logging_client.error(
          'DICOM instance missing SOPInstanceUID.', dcm_ref.dict()
      )
      raise ingested_dicom_file_ref.DicomIngestError(
          ingest_const.ErrorMsgs.DICOM_INSTANCE_MISSING_SOP_INSTANCE_UID
      )
    if not dcm_ref.sop_class_uid:
      cloud_logging_client.error(
          'DICOM instance missing SOPClassUID.', dcm_ref.dict()
      )
      raise ingested_dicom_file_ref.DicomIngestError(
          ingest_const.ErrorMsgs.DICOM_INSTANCE_MISSING_SOP_CLASS_UID
      )

    # Make sure all DICOM have same StudyInstanceUID, SeriesInstanceUID
    if not study_uid:
      study_uid = dcm_ref.study_instance_uid
      _validate_uid(study_uid, dcm_ref)
    if study_uid != dcm_ref.study_instance_uid:
      cloud_logging_client.error(
          'DICOM StudyInstanceUID are not the same across dicom instances.',
          {'previously_found_StudyInstanceUID': study_uid},
          dcm_ref.dict(),
      )
      raise ingested_dicom_file_ref.DicomIngestError(
          ingest_const.ErrorMsgs.DICOM_INSTANCES_STUDY_INSTANCE_UID_DO_NOT_MATCH
      )
    if not series_uid:
      series_uid = dcm_ref.series_instance_uid
      _validate_uid(series_uid, dcm_ref)
    if series_uid != dcm_ref.series_instance_uid:
      cloud_logging_client.error(
          'DICOM SeriesInstanceUID are not the same across dicom instances.',
          {'previously_found_SeriesInstanceUID': series_uid},
          dcm_ref.dict(),
      )
      raise ingested_dicom_file_ref.DicomIngestError(
          ingest_const.ErrorMsgs.DICOM_INSTANCES_SERIES_INSTANCE_UID_DO_NOT_MATCH
      )

    # Make sure all DICOM have same Unique SOPInstanceUID
    if dcm_ref.sop_instance_uid in instance_uid_set:
      cloud_logging_client.error(
          'Duplicate DICOM SOPInstanceUID.',
          {'previously_found_SOPInstanceUID': str(instance_uid_set)},
          dcm_ref.dict(),
      )
      raise ingested_dicom_file_ref.DicomIngestError(
          ingest_const.ErrorMsgs.DICOM_INSTANCES_HAVE_DUPLICATE_SOP_INSTANCE_UID
      )
    _validate_uid(dcm_ref.sop_instance_uid, dcm_ref)
    instance_uid_set.add(dcm_ref.sop_instance_uid)

    # Make sure all DICOM that have a defined barcode value
    # have it defined to the same value
    if dcm_ref.barcode_value:
      if not barcode_value:
        barcode_value = dcm_ref.barcode_value
      if barcode_value != dcm_ref.barcode_value:
        cloud_logging_client.error(
            'DICOM instance barcode value do not match.',
            {'previously_found_barcodevalue': barcode_value},
            dcm_ref.dict(),
        )
        raise ingested_dicom_file_ref.DicomIngestError(
            ingest_const.ErrorMsgs.DICOM_INSTANCES_BARCODES_DO_NOT_MATCH
        )

    if dcm_ref.modality:
      modality_found.add(dcm_ref.modality)
    if dcm_ref.accession_number:
      accession_number_found.add(dcm_ref.accession_number)
    with pydicom.dcmread(dcm_ref.source, defer_size='512 KB') as dcm:
      patient_name = dcm.get(ingest_const.DICOMTagKeywords.PATIENT_NAME)
      if patient_name is not None:
        patient_name_found.add(patient_name)
      patient_id = dcm.get(ingest_const.DICOMTagKeywords.PATIENT_ID)
      if patient_id is not None:
        patient_id_found.add(patient_id)
    # if dicom is whole slide imaging. Validate imaging tags.
    if (
        dcm_ref.sop_class_uid
        != ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
        or dcm_ref.modality != _SM
    ):
      other_dicom_filerefs.append(dcm_ref)
    else:
      _validate_wsi_dicom_instance(dcm_ref)
      wsi_image_filerefs.append(dcm_ref)
      image_type = dcm_ref.image_type.upper()
      if _RESAMPLED not in image_type and (
          _ORIGINAL_PRIMARY_VOLUME in image_type
          or _DERIVED_PRIMARY_VOLUME in image_type
      ):
        original_image_count_found += 1
        original_image = dcm_ref
        if original_image_count_found > 1:
          cloud_logging_client.error(
              (
                  'Ingested DICOM contains multiple instances with an ImageType'
                  ' that defines original imaging, e.g. '
                  'ORIGINAL\\PRIMARY\\VOLUME or DERIVED\\PRIMARY\\VOLUME.'
              ),
              dcm_ref.dict(),
          )
          raise ingested_dicom_file_ref.DicomIngestError(
              ingest_const.ErrorMsgs.DICOM_INSTANCES_HAVE_MULTIPLE_PRIMARY_VOLUME_IMAGES
          )
      elif _LABEL in image_type:
        label_count += 1
        if label_count > 1:
          cloud_logging_client.error(
              'DICOM instances contain multiple label images.', dcm_ref.dict()
          )
          raise ingested_dicom_file_ref.DicomIngestError(
              ingest_const.ErrorMsgs.DICOM_INSTANCES_HAVE_MULTIPLE_LABEL_IMAGES
          )
      elif _OVERVIEW in image_type:
        overview_count += 1
        if overview_count > 1:
          cloud_logging_client.error(
              'DICOM instances contain multiple overview images.',
              dcm_ref.dict(),
          )
          raise ingested_dicom_file_ref.DicomIngestError(
              ingest_const.ErrorMsgs.DICOM_INSTANCES_HAVE_MULTIPLE_OVERVIEW_IMAGES
          )
      elif _THUMBNAIL in image_type:
        thumbnail_count += 1
        if thumbnail_count > 1:
          cloud_logging_client.error(
              'DICOM instances contain multiple thumbnail images.',
              dcm_ref.dict(),
          )
          raise ingested_dicom_file_ref.DicomIngestError(
              ingest_const.ErrorMsgs.DICOM_INSTANCES_HAVE_MULTIPLE_THUMBNAIL_IMAGES
          )
  if len(accession_number_found) > 1:
    cloud_logging_client.error(
        'DICOM instances describe instances with multiple accession numbers.',
        {'accession_numbers_found': accession_number_found},
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.DICOM_INSTANCES_HAVE_MULTIPLE_ACCESSION_NUMBERS
    )
  if len(patient_name_found) > 1:
    cloud_logging_client.error(
        'DICOM instances describe instances with multiple patient names.',
        {'patient_names_found': patient_name_found},
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.DICOM_INSTANCES_HAVE_MULTIPLE_PATIENT_NAMES
    )
  if len(patient_id_found) > 1:
    cloud_logging_client.error(
        'DICOM instances describe instances with multiple patient ids.',
        {'patient_ids_found': patient_id_found},
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.DICOM_INSTANCES_HAVE_MULTIPLE_PATIENT_IDS
    )
  if len(modality_found) > 1:
    cloud_logging_client.error(
        'DICOM instances describe instances from multiple modalties.',
        {'modalities_found': modality_found},
    )
    raise ingested_dicom_file_ref.DicomIngestError(
        ingest_const.ErrorMsgs.DICOM_INSTANCES_DESCRIBE_MULTIPLE_MODALITIES
    )

  return DicomFileInfo(
      study_uid,
      series_uid,
      barcode_value,
      original_image,
      wsi_image_filerefs,
      other_dicom_filerefs,
  )
