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
import copy
import dataclasses
import enum
import os
import re
from typing import List, Mapping, Optional, Tuple
from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import ingest_const
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
_SLIDE_ID_MISSING_FROM_FILE_NAME_ERROR_LEVEL = (
    _SlideIdErrorLevel.BASE_ERROR_LEVEL
)
_BARCODE_IMAGE_MISSING_ERROR_LEVEL = _SlideIdErrorLevel.BASE_ERROR_LEVEL
_BARCODE_MISSING_FROM_IMAGES_ERROR_LEVEL = _SlideIdErrorLevel.BASE_ERROR_LEVEL
_SLIDE_ID_MISSING_FROM_BQ_METADATA_ERROR_LEVEL = (
    _SlideIdErrorLevel.BASE_ERROR_LEVEL
)
_FILE_NAME_CONTAINS_MULTIPLE_SLIDE_ID_CANDIDATES_ERROR_LEVEL = (
    _SlideIdErrorLevel.BASE_ERROR_LEVEL
)
_SLIDE_CONTAINS_MULTIPLE_BARCODES_WITH_SLIDE_ID_CANDIDATES_ERROR_LEVEL = (
    _SlideIdErrorLevel.BASE_ERROR_LEVEL
)

# No slide Slide Id dedetected in filename.
_FILE_NAME_MISSING_SLIDE_ID_CANDIDATES_ERROR_LEVEL = (
    _SlideIdErrorLevel.LOW_ERROR_LEVEL
)

# Slide id found but not in metadata.
_SLIDE_ID_MISSING_FROM_CSV_METADATA_ERROR_LEVEL = (
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


@dataclasses.dataclass
class FilenameSlideId:
  """Describes candidate slide id components identified from filename."""

  filename: str
  candidate_filename_slide_id_parts: List[str]
  test_filename_as_slide_id: bool
  ignored_text: List[str]
  candidate_slide_ids: List[str] = dataclasses.field(init=False)

  def __post_init__(self) -> None:
    filename_parts = copy.copy(self.candidate_filename_slide_id_parts)
    if (
        self.test_filename_as_slide_id
        and self.filename
        and self.filename not in filename_parts
    ):
      filename_parts.append(self.filename)
    self.candidate_slide_ids = filename_parts


def _highest_error_level_exception(
    current_exception: Optional[SlideIdIdentificationError],
    exception: SlideIdIdentificationError,
) -> SlideIdIdentificationError:
  if (
      current_exception is None
      or current_exception.error_level.value < exception.error_level.value
  ):
    return exception
  return current_exception


def get_candidate_slide_ids_from_filename(filename: str) -> FilenameSlideId:
  """Breaks filename into candidate slide id and ignored slide id parts.

  Args:
    filename: filename to split.

  Returns:
    FilenameSlideId
  """
  ignored_filename_parts = []
  candidate_slide_ids = []
  filename, _ = os.path.splitext(filename)  # remove file extension
  if ingest_flags.FILENAME_SLIDEID_SPLIT_STR_FLG.value:
    candidate_lst = filename.split(
        ingest_flags.FILENAME_SLIDEID_SPLIT_STR_FLG.value
    )
  else:
    candidate_lst = [filename]
  slide_id_regex = re.compile(
      ingest_flags.FILENAME_SLIDEID_REGEX_FLG.value.strip()
  )
  for candidate_slide_id in candidate_lst:
    if (
        not slide_id_regex.fullmatch(candidate_slide_id)
        or not candidate_slide_id
    ):
      ignored_filename_parts.append(candidate_slide_id)
    else:
      candidate_slide_ids.append(candidate_slide_id)
  return FilenameSlideId(
      filename,
      candidate_slide_ids,
      ingest_flags.TEST_WHOLE_FILENAME_AS_SLIDEID_FLG.value and filename,
      ignored_filename_parts,
  )


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
    cloud_logging_client.logger().critical(
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
    cloud_logging_client.logger().info(
        'Candidate slide id found in metadata.',
        {ingest_const.LogKeywords.SLIDE_ID: candidate_slide_id},
        log,
    )
    return candidate_slide_id
  except BigQueryMetadataTableEnvFormatError as exp:
    raise SlideIdIdentificationError(
        ingest_const.ErrorMsgs.SLIDE_ID_MISSING_FROM_BQ_METADATA,
        _SLIDE_ID_MISSING_FROM_BQ_METADATA_ERROR_LEVEL,
    ) from exp
  except metadata_storage_client.MetadataNotFoundExceptionError as exp:
    cloud_logging_client.logger().warning(
        'Candidate slide id not found in metadata.',
        {ingest_const.LogKeywords.SLIDE_ID: candidate_slide_id},
        log,
        exp,
    )
    if bq_metadata_source:
      msg = ingest_const.ErrorMsgs.SLIDE_ID_MISSING_FROM_BQ_METADATA
      error_level = _SLIDE_ID_MISSING_FROM_BQ_METADATA_ERROR_LEVEL
    else:
      msg = ingest_const.ErrorMsgs.SLIDE_ID_MISSING_FROM_CSV_METADATA
      error_level = _SLIDE_ID_MISSING_FROM_CSV_METADATA_ERROR_LEVEL
    raise SlideIdIdentificationError(msg, error_level) from exp
  except metadata_storage_client.MetadataDefinedOnMultipleRowError as exp:
    cloud_logging_client.logger().warning(
        'Candidate slide id ignored; defined on multiple rows.',
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
    local_file_path: str,
    metadata: metadata_storage_client.MetadataStorageClient,
    current_exception: Optional[SlideIdIdentificationError] = None,
) -> str:
  """Searches filename for valid Slide Id.

  Args:
    local_file_path:  To search for barcode.
    metadata: To validate barcode in.
    current_exception: Exception with the highest error level encountered
      determining the slide id.

  Returns:
    Slide Id

  Raises:
    SlideIdIdentificationError: Unable to determine slide id from filename.
  """
  if not local_file_path:
    cloud_logging_client.logger().warning('File name is empty.')
    raise _highest_error_level_exception(
        current_exception,
        SlideIdIdentificationError(
            ingest_const.ErrorMsgs.SLIDE_ID_MISSING_FROM_FILE_NAME,
            _SLIDE_ID_MISSING_FROM_FILE_NAME_ERROR_LEVEL,
        ),
    )
  _, filename = os.path.split(local_file_path)
  cloud_logging_client.logger().info(
      f'Testing {filename} for barcode value prefix.'
  )
  file_slide_id_parts = get_candidate_slide_ids_from_filename(filename)
  log = {
      ingest_const.LogKeywords.filename: filename,
      ingest_const.LogKeywords.FILE_NAME_PART_SPLIT_STRING: (
          ingest_flags.FILENAME_SLIDEID_SPLIT_STR_FLG.value
      ),
      ingest_const.LogKeywords.FILE_NAME_PART_REGEX: (
          ingest_flags.FILENAME_SLIDEID_REGEX_FLG.value
      ),
  }
  if not file_slide_id_parts.candidate_slide_ids:
    cloud_logging_client.logger().warning(
        'File name does not contain slide id candidates.', log
    )
    raise _highest_error_level_exception(
        current_exception,
        SlideIdIdentificationError(
            ingest_const.ErrorMsgs.FILE_NAME_MISSING_SLIDE_ID_CANDIDATES,
            _FILE_NAME_MISSING_SLIDE_ID_CANDIDATES_ERROR_LEVEL,
        ),
    )
  for candidate_slide_id in file_slide_id_parts.candidate_slide_ids:
    try:
      return find_slide_id_in_metadata(candidate_slide_id, metadata, log)
    except SlideIdIdentificationError as exp:
      current_exception = _highest_error_level_exception(current_exception, exp)

  if file_slide_id_parts.ignored_text:
    cloud_logging_client.logger().info(
        (
            'Parts of filename not tested for slide id because they do not '
            'match the slide id regx.'
        ),
        log,
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
    cloud_logging_client.logger().warning('No barcode containing images found.')
    raise _highest_error_level_exception(
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
    cloud_logging_client.logger().warning(
        'Could not find and decode barcodes in any images.'
    )
    raise _highest_error_level_exception(
        current_exception,
        SlideIdIdentificationError(
            ingest_const.ErrorMsgs.BARCODE_MISSING_FROM_IMAGES,
            _BARCODE_MISSING_FROM_IMAGES_ERROR_LEVEL,
        ),
    )
  if len(barcodevalue_dict) > 1:
    cloud_logging_client.logger().warning(
        'More than one barcode found in images',
        {ingest_const.LogKeywords.BARCODE: str(barcodevalue_dict)},
    )
  for barcodevalue, files_foundin in barcodevalue_dict.items():
    log = {'file_list': str(files_foundin)}
    try:
      slide_id = find_slide_id_in_metadata(barcodevalue, metadata, log)
      return slide_id
    except SlideIdIdentificationError as exp:
      current_exception = _highest_error_level_exception(current_exception, exp)

  cloud_logging_client.logger().error('Missing metadata')
  raise current_exception


def _get_metadata_free_slide_id(
    filename: str,
    ancillary_images: List[ancillary_image_extractor.AncillaryImage],
    dicom_barcode_tag_value: str = '',
) -> str:
  """Returns slide id when metadata validation is not provided.

  Tests DICOM Tag first, then filename, then barcode value.
  Requires single candidate values if either filename parts are used or
  segmented barcode. If either define multiple candidates, e.g. file name
  is broken into multiple fragments which match the slide id regular
  expresion or multiple barcode values are identified on the ingested imaging
  then these methods of identfication are indeterminate and will result in
  an exception being raised.

  Args:
   filename: File name to decode slide id from.
   ancillary_images: List of ancillary images to decode barcodes in.
   dicom_barcode_tag_value: Barcode value DICOM tag value.

  Returns:
   Found slide id or empty string.

  Raises:
    SlideIdIdentificationError: Errors encountered identifying slide id.
  """
  if dicom_barcode_tag_value:
    cloud_logging_client.logger().info(
        'Metadata free transform; identified slide id in DICOM barcode tag.',
        {ingest_const.LogKeywords.SLIDE_ID: dicom_barcode_tag_value},
    )
    return dicom_barcode_tag_value
  candidate_filename_slide_ids = get_candidate_slide_ids_from_filename(filename)
  log = {
      ingest_const.LogKeywords.filename: candidate_filename_slide_ids.filename,
      ingest_const.LogKeywords.FILE_NAME_PART_SPLIT_STRING: (
          ingest_flags.FILENAME_SLIDEID_SPLIT_STR_FLG.value
      ),
      ingest_const.LogKeywords.FILE_NAME_PART_REGEX: (
          ingest_flags.FILENAME_SLIDEID_REGEX_FLG.value
      ),
  }
  if len(candidate_filename_slide_ids.candidate_filename_slide_id_parts) == 1:
    slide_id = candidate_filename_slide_ids.candidate_filename_slide_id_parts[0]
    log[ingest_const.LogKeywords.SLIDE_ID] = slide_id
    cloud_logging_client.logger().info(
        'Metadata free transform; identified slide id in file name part.', log
    )
    return slide_id
  if candidate_filename_slide_ids.candidate_filename_slide_id_parts:
    log[ingest_const.LogKeywords.SLIDE_ID] = (
        candidate_filename_slide_ids.candidate_filename_slide_id_parts
    )
    cloud_logging_client.logger().error(
        'Metadata free transform; Can not determine slide id, file name '
        'contains multiple candidate slide ids.',
        log,
    )
    raise SlideIdIdentificationError(
        ingest_const.ErrorMsgs.FILE_NAME_CONTAINS_MULTIPLE_SLIDE_ID_CANDIDATES,
        _FILE_NAME_CONTAINS_MULTIPLE_SLIDE_ID_CANDIDATES_ERROR_LEVEL,
    )
  if (
      candidate_filename_slide_ids.test_filename_as_slide_id
      and candidate_filename_slide_ids.filename
  ):
    log[ingest_const.LogKeywords.SLIDE_ID] = (
        candidate_filename_slide_ids.filename
    )
    cloud_logging_client.logger().info(
        'Metadata free transform; using filename as slide id.',
        log,
    )
    return candidate_filename_slide_ids.filename
  cloud_logging_client.logger().warning(
      'Metadata free transform; file name does not contain candidate slide id.',
      log,
  )
  barcodevalue_dict = barcode_reader.read_barcode_in_files(
      [image.path for image in ancillary_images]
  )
  if '' in barcodevalue_dict:
    del barcodevalue_dict['']
  candidate_barcode_value_slide_ids = list(barcodevalue_dict)
  if len(candidate_barcode_value_slide_ids) == 1:
    log[ingest_const.LogKeywords.SLIDE_ID] = candidate_barcode_value_slide_ids[
        0
    ]
    cloud_logging_client.logger().info(
        'Metadata free transform; identified slide id in barcode.',
        log,
    )
    return candidate_barcode_value_slide_ids[0]
  if len(candidate_barcode_value_slide_ids) > 1:
    cloud_logging_client.logger().error(
        'Metadata free transform; Can not determine slide id, multiple barcode '
        'values decoded.',
        log,
        {ingest_const.LogKeywords.BARCODE: candidate_barcode_value_slide_ids},
    )
    raise SlideIdIdentificationError(
        ingest_const.ErrorMsgs.MULTIPLE_BARCODES_SLIDE_ID_CANDIDATES,
        _SLIDE_CONTAINS_MULTIPLE_BARCODES_WITH_SLIDE_ID_CANDIDATES_ERROR_LEVEL,
    )
  cloud_logging_client.logger().error(
      'Metadata free transform; Could not identify slide id.',
      log,
  )
  raise SlideIdIdentificationError(
      ingest_const.ErrorMsgs.SLIDE_ID_MISSING,
      _CANDIDATE_SLIDE_ID_MISSING_ERROR_LEVEL,
  )


def get_metadata_free_slide_id(
    filename: str,
    ancillary_images: List[ancillary_image_extractor.AncillaryImage],
    dicom_barcode_tag_value: str = '',
    maximum_id_length: int = 64,
) -> str:
  """Returns slide id when metadata validation is not provided.

  Args:
   filename: File name to decode slide id from.
   ancillary_images: List of ancillary images to decode barcodes in.
   dicom_barcode_tag_value: Barcode value DICOM tag value.
   maximum_id_length: Maximum length of Slide ID use to enforce constraints on
     slide_id value conforming to DICOM VR code size limits.

  Returns:
   Slide id.

  Raises:
    SlideIdIdentificationError: Errors encountered identifying slide id.
  """
  slide_id = _get_metadata_free_slide_id(
      filename, ancillary_images, dicom_barcode_tag_value
  )
  if slide_id and len(slide_id) <= maximum_id_length:
    return slide_id
  error_msg = (
      'slide id is empty.'
      if not slide_id
      else f'length exceeds {maximum_id_length} characters.'
  )
  cloud_logging_client.logger().error(
      f'Metadata free transform; {error_msg}',
      {ingest_const.LogKeywords.SLIDE_ID: slide_id},
  )
  raise SlideIdIdentificationError(
      ingest_const.ErrorMsgs.INVALID_SLIDE_ID_LENGTH,
      _INVALID_SLIDE_ID_LENGTH_ERROR_LEVEL,
  )
