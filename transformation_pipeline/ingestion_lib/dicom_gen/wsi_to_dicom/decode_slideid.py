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
"""Decodes identifies (filename, image, string) and validates slide id."""

import enum
import os
import re
from typing import List, Mapping, Optional, Tuple

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_image_extractor
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import barcode_reader
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client


class _SlideIdErrorLevel(enum.Enum):
  BASE_ERROR_LEVEL = 0
  LOW_ERROR_LEVEL = 1
  MIDDLE_ERROR_LEVEL = 2
  HIGH_ERROR_LEVEL = 3
  HIGHEST_ERROR_LEVEL = 4


# Slide id not found at all errors.
_CANDIDATE_SLIDE_ID_MISSING_ERROR_LEVEL = _SlideIdErrorLevel.BASE_ERROR_LEVEL
_FILENAME_MISSING_SLIDE_METADATA_PRIMARY_KEY_ERROR_LEVEL = (
    _SlideIdErrorLevel.BASE_ERROR_LEVEL
)
_BARCODE_IMAGE_MISSING_ERROR_LEVEL = _SlideIdErrorLevel.BASE_ERROR_LEVEL
_BARCODE_MISSING_FROM_IMAGES_ERROR_LEVEL = _SlideIdErrorLevel.BASE_ERROR_LEVEL
_SLIDE_ID_MISSING_FROM_BQ_METADATA_ERROR_LEVEL = (
    _SlideIdErrorLevel.BASE_ERROR_LEVEL
)

# Slide id found but not in metadata.
_PRIMARY_KEY_MISSING_FROM_METADATA_ERROR_LEVEL = (
    _SlideIdErrorLevel.MIDDLE_ERROR_LEVEL
)

# Slide id found in metadata more than once.
_SLIDE_ID_DEFINED_ON_MULTIPLE_ROWS_ERROR_LEVEL = (
    _SlideIdErrorLevel.HIGH_ERROR_LEVEL
)

# Otherwise valid slide id has invalid length.
_INVALID_SLIDE_ID_LENGTH_ERROR_LEVEL = _SlideIdErrorLevel.HIGHEST_ERROR_LEVEL


class BigQueryMetadataTableEnvFormatError(Exception):

  def __init__(self):
    super().__init__(ingest_const.ErrorMsgs.BQ_METADATA_TABLE_ENV_FORMAT)


class SlideIdIdentificationError(Exception):
  """Exceptions raised identifying slide id in metadata."""

  def __init__(self, msg: str, error_level: _SlideIdErrorLevel):
    super().__init__(msg)
    self._error_level = error_level

  @property
  def error_level(self) -> _SlideIdErrorLevel:
    return self._error_level


def highest_error_level_exception(
    current_exception: Optional[SlideIdIdentificationError],
    exception: SlideIdIdentificationError,
) -> SlideIdIdentificationError:
  if (
      current_exception is None
      or current_exception.error_level.value < exception.error_level.value
  ):
    return exception
  return current_exception


def _get_whole_filename_to_test(
    dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
) -> str:
  """Returns string to test as whole filename slideid.

  Args:
    dicom_gen: File payload to convert into DICOM.

  Returns:
    Whole file name slide id (string).
  """
  path = abstract_dicom_generation.get_ingest_triggering_file_path(
      dicom_gen,
      ingest_flags.INCLUDE_UPLOAD_BUCKET_PATH_IN_WHOLE_FILENAME_SLIDEID_FLG.value,
  )
  return os.path.splitext(path)[0]


def _get_candidate_slide_ids_from_filename(
    dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
) -> List[str]:
  """Breaks filename into candidate slide id and ignored slide id parts.

  Args:
    dicom_gen: File payload to convert into DICOM.

  Returns:
    FilenameSlideId

  Raises:
    SlideIdIdentificationError: Failed to identify metadata primary key
      candidate in filename.
  """
  _, filename = os.path.split(dicom_gen.localfile)
  log = {
      ingest_const.LogKeywords.FILENAME: filename,
      ingest_const.LogKeywords.FILE_NAME_PART_SPLIT_STRING: (
          ingest_flags.FILENAME_SLIDEID_SPLIT_STR_FLG.value
      ),
      ingest_const.LogKeywords.FILE_NAME_PART_REGEX: (
          ingest_flags.FILENAME_SLIDEID_REGEX_FLG.value
      ),
  }
  filename, _ = os.path.splitext(filename)  # remove file extension
  if not filename:
    cloud_logging_client.warning(
        f'Could not find a candidate slide metadata primary key in {filename}.',
        log,
    )
    raise SlideIdIdentificationError(
        ingest_const.ErrorMsgs.FILENAME_MISSING_SLIDE_METADATA_PRIMARY_KEY,
        _FILENAME_MISSING_SLIDE_METADATA_PRIMARY_KEY_ERROR_LEVEL,
    )
  if ingest_flags.FILENAME_SLIDEID_SPLIT_STR_FLG.value:
    candidate_lst = filename.split(
        ingest_flags.FILENAME_SLIDEID_SPLIT_STR_FLG.value
    )
  else:
    candidate_lst = [filename]
  slide_id_regex = re.compile(
      ingest_flags.FILENAME_SLIDEID_REGEX_FLG.value.strip()
  )
  candidate_slide_ids = (
      [_get_whole_filename_to_test(dicom_gen)]
      if ingest_flags.TEST_WHOLE_FILENAME_AS_SLIDEID_FLG.value
      else []
  )
  for candidate_slide_id in candidate_lst:
    if not candidate_slide_id or candidate_slide_id in candidate_slide_ids:
      continue
    if slide_id_regex.fullmatch(candidate_slide_id):
      candidate_slide_ids.append(candidate_slide_id)
  if not candidate_slide_ids:
    cloud_logging_client.warning(
        f'Could not find a candidate slide metadata primary key in {filename}.',
        log,
    )
    raise SlideIdIdentificationError(
        ingest_const.ErrorMsgs.FILENAME_MISSING_SLIDE_METADATA_PRIMARY_KEY,
        _FILENAME_MISSING_SLIDE_METADATA_PRIMARY_KEY_ERROR_LEVEL,
    )
  log['candidate_slide_metadata_primary_keys'] = candidate_slide_ids
  cloud_logging_client.info(
      f'Identified candidate slide metadata primary key(s) in {filename}.', log
  )
  return candidate_slide_ids


def decode_bq_metadata_table_env() -> Tuple[str, str, str]:
  """Decodes BQ metadata table enviromental variable.

  Returns:
    Tuple(ProjectID, DatasetID, TableName)

  Raises:
    BigQueryMetadataTableEnvFormatError: Error decoding environmental variable.
  """
  bq_table_id = ingest_flags.BIG_QUERY_METADATA_TABLE_FLG.value.split('.')
  try:
    project_id, dataset_id, table_name = bq_table_id
    return project_id.strip(), dataset_id.strip(), table_name.strip()
  except ValueError as exp:
    cloud_logging_client.critical(
        'BIG_QUERY_METADATA_TABLE enviromental variable is incorrectly'
        ' formatted. Expected: {ProjectID}.{DatasetID}.{TableName}',
        {'BIG_QUERY_METADATA_TABLE': bq_table_id},
        exp,
    )
    raise BigQueryMetadataTableEnvFormatError() from exp


def find_slide_id_in_metadata(
    candidate_slide_id: Optional[str],
    metadata: metadata_storage_client.MetadataStorageClient,
    log: Optional[Mapping[str, str]] = None,
) -> str:
  """Attempts to find valid Slide Id in metadata.

  Args:
    candidate_slide_id: String slide id to test.
    metadata: To validate barcode in.
    log: optional [str, str] to include in logs.

  Returns:
    Slide Id as string

  Raises:
    SlideIdIdentificationError: Error occured trying to resolve slide id in
      metadata.
  """
  if candidate_slide_id is None or not candidate_slide_id:
    raise SlideIdIdentificationError(
        ingest_const.ErrorMsgs.CANDIDATE_SLIDE_ID_MISSING,
        _CANDIDATE_SLIDE_ID_MISSING_ERROR_LEVEL,
    )
  if log is None:
    log = {}
  bq_metadata_source = bool(
      ingest_flags.BIG_QUERY_METADATA_TABLE_FLG.value.strip()
  )
  try:
    if bq_metadata_source:
      project_id, dataset_id, table_name = decode_bq_metadata_table_env()
      metadata.get_slide_metadata_from_bigquery(
          project_id, dataset_id, table_name, candidate_slide_id
      )
    else:
      metadata.get_slide_metadata_from_csv(candidate_slide_id)
    return candidate_slide_id
  except BigQueryMetadataTableEnvFormatError as exp:
    raise SlideIdIdentificationError(
        ingest_const.ErrorMsgs.SLIDE_ID_MISSING_FROM_BQ_METADATA,
        _SLIDE_ID_MISSING_FROM_BQ_METADATA_ERROR_LEVEL,
    ) from exp
  except metadata_storage_client.MetadataNotFoundExceptionError as exp:
    msg = (
        ingest_const.ErrorMsgs.SLIDE_ID_MISSING_FROM_BQ_METADATA
        if bq_metadata_source
        else ingest_const.ErrorMsgs.SLIDE_ID_MISSING_FROM_CSV_METADATA
    )
    error_level = _PRIMARY_KEY_MISSING_FROM_METADATA_ERROR_LEVEL
    raise SlideIdIdentificationError(msg, error_level) from exp
  except metadata_storage_client.MetadataDefinedOnMultipleRowError as exp:
    cloud_logging_client.warning(
        'Candidate slide primary key ignored; defined on multiple rows.',
        {
            ingest_const.LogKeywords.SLIDE_ID: candidate_slide_id,
            ingest_const.LogKeywords.METADATA_SOURCE: exp.source,
        },
        log,
        exp,
    )
    raise SlideIdIdentificationError(
        ingest_const.ErrorMsgs.SLIDE_ID_DEFINED_ON_MULTIPLE_ROWS,
        _SLIDE_ID_DEFINED_ON_MULTIPLE_ROWS_ERROR_LEVEL,
    ) from exp


def get_slide_id_from_filename(
    dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
    metadata: metadata_storage_client.MetadataStorageClient,
) -> str:
  """Searches filename for valid Slide Id.

  Args:
    dicom_gen: File payload to convert into DICOM.
    metadata: To validate barcode in.

  Returns:
    Slide Id

  Raises:
    SlideIdIdentificationError: Unable to determine slide id from filename.
  """
  candidate_slide_ids = _get_candidate_slide_ids_from_filename(dicom_gen)
  current_exception = None
  for candidate_slide_id in candidate_slide_ids:
    try:
      return find_slide_id_in_metadata(candidate_slide_id, metadata)
    except SlideIdIdentificationError as exp:
      current_exception = highest_error_level_exception(current_exception, exp)

  cloud_logging_client.info(
      'None of the filename generated slide id candidates were found in'
      ' metadata primary key.',
      {'tested_slide_id_candidates': candidate_slide_ids},
  )
  raise current_exception


def get_slide_id_from_ancillary_images(
    ancillary_images: List[ancillary_image_extractor.AncillaryImage],
    metadata: metadata_storage_client.MetadataStorageClient,
    current_exception: Optional[SlideIdIdentificationError],
) -> str:
  """Attempts to find valid Slide Id in barcode images.

  Args:
    ancillary_images: Images to search for barcode.
    metadata: To validate barcode in.
    current_exception: Exception with the highest error level encountered
      determining the slide id.

  Returns:
    Decoded Barcode (slide id)

  Raises:
    SlideIdIdentificationError: Unable to determine Barcode from images.
  """
  if not ancillary_images:
    cloud_logging_client.warning('No barcode containing images found.')
    raise highest_error_level_exception(
        current_exception,
        SlideIdIdentificationError(
            ingest_const.ErrorMsgs.BARCODE_IMAGE_MISSING,
            _BARCODE_IMAGE_MISSING_ERROR_LEVEL,
        ),
    )
  barcodevalue_dict = barcode_reader.read_barcode_in_files(
      [image.path for image in ancillary_images]
  )
  if '' in barcodevalue_dict:
    del barcodevalue_dict['']
  if not barcodevalue_dict:
    cloud_logging_client.warning(
        'Could not find and decode barcodes in any images.'
    )
    raise highest_error_level_exception(
        current_exception,
        SlideIdIdentificationError(
            ingest_const.ErrorMsgs.BARCODE_MISSING_FROM_IMAGES,
            _BARCODE_MISSING_FROM_IMAGES_ERROR_LEVEL,
        ),
    )
  if len(barcodevalue_dict) > 1:
    cloud_logging_client.warning(
        'More than one barcode found in images',
        {ingest_const.LogKeywords.BARCODE: str(barcodevalue_dict)},
    )
  for barcodevalue, files_foundin in barcodevalue_dict.items():
    log = {'file_list': str(files_foundin)}
    try:
      slide_id = find_slide_id_in_metadata(barcodevalue, metadata, log)
      return slide_id
    except SlideIdIdentificationError as exp:
      current_exception = highest_error_level_exception(current_exception, exp)
  raise current_exception


def get_metadata_free_slide_id(
    dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
) -> str:
  """Returns slide id when metadata validation is not provided.

  Args:
   dicom_gen: File payload to convert into DICOM.

  Returns:
   Slide id.

  Raises:
    SlideIdIdentificationError: Errors encountered identifying slide id.
  """
  slide_id = _get_whole_filename_to_test(dicom_gen)
  cloud_logging_client.info(
      'Metadata free transform.',
      {ingest_const.LogKeywords.SLIDE_ID: slide_id},
  )
  maximum_id_length = 64
  if slide_id and len(slide_id) <= maximum_id_length:
    return slide_id
  if not slide_id:
    error_msg = 'slide id is empty.'
    cloud_logging_client.error(
        f'Metadata free transform; {error_msg}',
        {ingest_const.LogKeywords.SLIDE_ID: slide_id},
    )
    raise SlideIdIdentificationError(
        ingest_const.ErrorMsgs.INVALID_SLIDE_ID_LENGTH,
        _INVALID_SLIDE_ID_LENGTH_ERROR_LEVEL,
    )
  length_validation = ingest_flags.METADATA_TAG_LENGTH_VALIDATION_FLG.value
  if length_validation == ingest_flags.MetadataTagLengthValidation.NONE:
    return slide_id
  if length_validation in (
      ingest_flags.MetadataTagLengthValidation.LOG_WARNING,
      ingest_flags.MetadataTagLengthValidation.LOG_WARNING_AND_CLIP,
  ):
    error_msg = (
        f'Filename exceeds {maximum_id_length} characters; the length limit for'
        ' LO VR tags. PatentID and Container ID will contain values which'
        ' exceed the DICOM Standard length limits. Tag cropping is not'
        ' supported for metadata free ingestion.'
    )
    cloud_logging_client.warning(
        f'Metadata free transform; {error_msg}',
        {ingest_const.LogKeywords.SLIDE_ID: slide_id},
    )
    return slide_id
  error_msg = (
      f'Filename exceeds {maximum_id_length} characters; the length limit for'
      ' LO VR tags.'
  )
  cloud_logging_client.error(
      f'Metadata free transform; {error_msg}',
      {ingest_const.LogKeywords.SLIDE_ID: slide_id},
  )
  raise SlideIdIdentificationError(
      ingest_const.ErrorMsgs.INVALID_SLIDE_ID_LENGTH,
      _INVALID_SLIDE_ID_LENGTH_ERROR_LEVEL,
  )
