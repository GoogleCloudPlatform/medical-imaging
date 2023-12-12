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
"""Constants used in transformation pipeline."""

import dataclasses
from typing import NewType

# ******************************************************************************
# File is imported outside of ingestion (OOF). Do not import internal ingestion
# dependencies.
# ******************************************************************************

HEALTHCARE_API = 'https://healthcare.googleapis.com/v1'

# Private creator tag def.
PRIVATE_TAG_CREATOR = 'GOOGLE'
SC_MANUFACTURER_NAME = 'GOOGLE'

# DICOM UIDs used in ingest pipeline.
SC_IMPLEMENTATION_CLASS_UID = '1.3.6.1.4.1.11129.5.4.1'
WSI_IMPLEMENTATION_CLASS_UID = '1.3.6.1.4.1.11129.5.4.1'
SCMI_IMPLEMENTATION_CLASS_UID = '1.3.6.1.4.1.11129.5.4.1'

DPAS_UID_PREFIX = '1.3.6.1.4.1.11129.5.7'
# components should subprefix.
# NDSAIngest = 1.3.6.1.4.1.11129.5.7.1
# MLIngest   = 1.3.6.1.4.1.11129.5.7.2

# DICOM default. Private tags not compatible with transfer syntax.
# And are not returned by store on query.
# DO NOT USE implicit_vr_endian_transfer_syntax = '1.2.840.10008.1.2'
# See b/207162516

EXPLICIT_VR_LITTLE_ENDIAN = '1.2.840.10008.1.2.1'

MESSAGE_TTL_S = int(600)

# wsi image and frame type keywords
OVERVIEW = 'OVERVIEW'
THUMBNAIL = 'THUMBNAIL'
LABEL = 'LABEL'
DERIVED = 'DERIVED'
SECONDARY = 'SECONDARY'
ORIGINAL = 'ORIGINAL'
PRIMARY = 'PRIMARY'
VOLUME = 'VOLUME'
NONE = 'NONE'
RESAMPLED = 'RESAMPLED'

TILED_FULL = 'TILED_FULL'
MISSING_INGESTION_TRACE_ID = 'MISSING_INGESTION_TRACE_ID'

VALUE = 'Value'
VR = 'vr'


class DICOMVRCodes:
  CS = 'CS'
  LO = 'LO'
  LT = 'LT'
  PN = 'PN'
  SH = 'SH'
  SQ = 'SQ'
  UI = 'UI'


class DICOMTagAddress:
  """HEX address of DICOM tags."""
  ACCESSION_NUMBER = '00080050'
  DICOM_GOOGLE_PRIVATE_CREATOR_BLOCK_TAG = '30210010'
  HASH_PRIVATE_TAG = '30211001'
  INGEST_FILENAME_TAG = '30211004'
  OOF_SCORE_PRIVATE_TAG = '30211002'
  PATIENT_ID = '00100020'
  PATIENT_NAME = '00100010'
  PUBSUB_MESSAGE_ID_TAG = '30211003'
  SERIES_DESCRIPTION = '0008103E'
  SERIES_INSTANCE_UID = '0020000E'
  SOP_CLASS_UID = '00080016'
  SOP_INSTANCE_UID = '00080018'
  STUDY_INSTANCE_UID = '0020000D'


class DICOMTagKeywords:
  """DICOM Keywords used in ingest project."""

  # Used in dicom_file_ref
  STUDY_INSTANCE_UID = 'StudyInstanceUID'
  SERIES_INSTANCE_UID = 'SeriesInstanceUID'
  SOP_CLASS_UID = 'SOPClassUID'
  SOP_INSTANCE_UID = 'SOPInstanceUID'

  # Used in dicom_store_client
  IMAGE_TYPE = 'ImageType'
  FRAME_TYPE = 'FrameType'
  BITS_ALLOCATED = 'BitsAllocated'
  BITS_STORED = 'BitsStored'
  HIGH_BIT = 'HighBit'
  PIXEL_REPRESENTATION = 'PixelRepresentation'
  SAMPLES_PER_PIXEL = 'SamplesPerPixel'
  PLANAR_CONFIGURATION = 'PlanarConfiguration'
  ACCESSION_NUMBER = 'AccessionNumber'
  SERIES_NUMBER = 'SeriesNumber'
  BARCODE_VALUE = 'BarcodeValue'
  INSTANCE_NUMBER = 'InstanceNumber'
  PHOTOMETRIC_INTERPRETATION = 'PhotometricInterpretation'
  IMAGED_VOLUME_WIDTH = 'ImagedVolumeWidth'
  IMAGED_VOLUME_HEIGHT = 'ImagedVolumeHeight'
  TOTAL_PIXEL_MATRIX_COLUMNS = 'TotalPixelMatrixColumns'
  TOTAL_PIXEL_MATRIX_ROWS = 'TotalPixelMatrixRows'
  MANUFACTURER = 'Manufacturer'
  MANUFACTURER_MODEL_NAME = 'ManufacturerModelName'
  NUMBER_OF_FRAMES = 'NumberOfFrames'
  COLUMNS = 'Columns'
  ROWS = 'Rows'
  DIMENSION_ORGANIZATION_TYPE = 'DimensionOrganizationType'
  MODALITY = 'Modality'
  SPECIMEN_LABEL_IN_IMAGE = 'SpecimenLabelInImage'
  BURNED_IN_ANNOTATION = 'BurnedInAnnotation'
  CONCATENATION_UID = 'ConcatenationUID'
  CONCATENATION_FRAME_OFFSET_NUMBER = 'ConcatenationFrameOffsetNumber'
  IN_CONCATENATION_NUMBER = 'InConcatenationNumber'
  FRAME_OF_REFERENCE_UID = 'FrameOfReferenceUID'
  POSITION_REFERENCE_INDICATOR = 'PositionReferenceIndicator'
  SHARED_FUNCTIONAL_GROUPS_SEQUENCE = 'SharedFunctionalGroupsSequence'
  SOFTWARE_VERSIONS = 'SoftwareVersions'

  GROUP_ADDRESS = '3021'
  # Extends DicomFileRef to add a private tag representing the MD5 of source.
  DICOM_GOOGLE_PRIVATE_CREATOR_BLOCK_TAG = (
      DICOMTagAddress.DICOM_GOOGLE_PRIVATE_CREATOR_BLOCK_TAG
  )
  HASH_PRIVATE_TAG = DICOMTagAddress.HASH_PRIVATE_TAG
  OOF_SCORE_PRIVATE_TAG = DICOMTagAddress.OOF_SCORE_PRIVATE_TAG
  PUBSUB_MESSAGE_ID_TAG = DICOMTagAddress.PUBSUB_MESSAGE_ID_TAG
  INGEST_FILENAME_TAG = DICOMTagAddress.INGEST_FILENAME_TAG

  ICC_PROFILE = 'ICCProfile'
  OPTICAL_PATH_SEQUENCE = 'OpticalPathSequence'
  COLOR_SPACE = 'ColorSpace'

  TRANSFER_SYNTAX_UID = 'TransferSyntaxUID'


SOPClassUID = NewType('SOPClassUID', str)
SOPClassName = NewType('SOPClassName', str)


@dataclasses.dataclass(frozen=True)
class DicomSopClass:
  name: SOPClassName
  uid: SOPClassUID


class DicomSopClasses:
  """DICOM SOP Class values used in ingest project."""

  MICROSCOPIC_IMAGE = DicomSopClass(
      SOPClassName('VL Microscopic Image Storage'),
      SOPClassUID('1.2.840.10008.5.1.4.1.1.77.1.2'),
  )
  SLIDE_COORDINATES_IMAGE = DicomSopClass(
      SOPClassName('VL Slide-Coordinates Microscopic Image Storage'),
      SOPClassUID('1.2.840.10008.5.1.4.1.1.77.1.3'),
  )
  WHOLE_SLIDE_IMAGE = DicomSopClass(
      SOPClassName('VL Whole Slide Microscopy Image Storage'),
      SOPClassUID('1.2.840.10008.5.1.4.1.1.77.1.6'),
  )
  MICROSCOPY_ANNOTATION = DicomSopClass(
      SOPClassName('Microscopy Bulk Simple Annotations Storage'),
      SOPClassUID('1.2.840.10008.5.1.4.1.1.91.1'),
  )


class DicomImageTransferSyntax:
  JPEG_LOSSY = '1.2.840.10008.1.2.4.50'
  JPEG_2000 = '1.2.840.10008.1.2.4.90'


class DicomImageCompressionMethod:
  JPEG_LOSSY = 'ISO_10918_1'


class PubSubKeywords:
  PIXEL_SPACING_WIDTH = 'PixelSpacingWidth'
  PIXEL_SPACING_HEIGHT = 'PixelSpacingHeight'


class LogKeywords:
  """Keywords used in structured logs.

  Define all new constants in uppercase.  Refactoring constants to uppercase to
  match global style.
  """

  ack_deadline_sec = 'ack_deadline_sec'
  ACCESSION_NUMBER = 'accession_number'
  BARCODE = 'barcode'
  BIGQUERY_TABLE = 'bigquery_table'
  BIGQUERY_TABLE_COLUMN_NAMES = 'bigquery_table_column_names'
  bucket_name = 'bucket_name'
  dest_file = 'dest_file'
  dest_uri = 'dest_URI'
  dicomweb_path = 'dicomweb_path'
  dpas_ingestion_trace_id = 'dpas_ingestion_trace_id'
  DICOM_TAGS = 'dicom_tags'
  elapsed_time_beyond_extension_sec = 'elapsed_time_beyond_extension_sec'
  exception = 'exception'
  extending_ack_deadline_sec = 'extending_ack_deadline_sec'
  file_extension = 'file_extension'
  FILE_NAME_PART_SPLIT_STRING = 'file_name_slide_id_part_split_string'
  FILE_NAME_PART_REGEX = 'file_name_slide_id_part_regex'
  file_size = 'file_size(bytes)'
  filename = 'filename'
  hash = 'hash'
  METADATA_SOURCE = 'metadata_source'
  main_dicom_store = 'main_dicom_store'
  message_count = 'message_count'
  METADATA = 'metadata'
  METADATA_PRIMARY_KEY = 'metadata_primary_key'
  METADATA_PRIMARY_KEY_COLUMN_NAME = 'metadata_primary_key_column_name'
  new_series_instance_uid = 'new_series_instance_uid'
  old_series_instance_uid = 'old_series_instance_uid'
  oof_dicom_store = 'oof_dicom_store'
  pubsub_message_id = 'pubsub_message_id'
  pubsub_subscription = 'pubsub_subscription'
  pubsub_topic_name = 'pubsub_topic_name'
  received_event_type = 'received_event_type'
  return_code = 'return-code'
  SERIES_INSTANCE_UID = 'series_instance_uid'
  SLIDE_ID = 'slide_id'
  sop_instance_uid = 'sop_instance_uid'
  source_uri = 'source_URI'
  stderr = 'stderr'
  stdout = 'stdout'
  study_instance_uid = 'study_instance_uid'
  STUDY_INSTANCE_UIDS_FOUND = 'study_instance_uids_found'
  PATIENT_ID = 'patient_id'
  PREVIOUS_STUDY_INSTANCE_UID = 'previous_study_instance_uid`'
  tag_number = 'private_tag_number'
  total_time_sec = 'total_time_sec'
  uri = 'URI'


class EnvVarNames:
  INGEST_COMPLETE_OOF_TRIGGER_PUBSUB_TOPIC = (
      'INGEST_COMPLETE_OOF_TRIGGER_PUBSUB_TOPIC'
  )
  DICOMWEB_URL = 'DICOMWEB_URL'
  DICOM_STORE_TO_CLEAN = 'DICOM_STORE_TO_CLEAN'
  OOF_DICOMWEB_BASE_URL = 'OOF_DICOMWEB_BASE_URL'
  OOF_INFERENCE_CONFIG_PATH = 'OOF_INFERENCE_CONFIG_PATH'
  GCS_UPLOAD_IGNORE_FILE_EXT = 'GCS_UPLOAD_IGNORE_FILE_EXT'


class OofPassThroughKeywords:
  DISABLE_TFEXAMPLE_WRITE = 'DISABLE_TFEXAMPLE_WRITE'
  DPAS_INGESTION_TRACE_ID = 'DICOM_INGESTION_TRACE_ID'
  SOURCE_DICOM_IN_MAIN_STORE = 'SOURCE_DICOM_IN_MAIN_STORE'


class ErrorMsgs:
  """Simplified error messages.

  GCS ingestion may use this simplified error message for GCS failure bucket
  paths, which are passed as an additional args element in exceptions.
  """

  BQ_METADATA_NOT_FOUND = 'bigquery_metadata_not_found'
  BQ_METADATA_TABLE_ENV_FORMAT = (
      'big_query_metadata_table_env_incorrectly_formatted'
  )
  INVALID_DICOM = 'invalid_dicom'
  INVALID_DICOM_STANDARD_REPRESENTATION = (
      'invalid_dicom_standard_representation'
  )
  INVALID_ICC_PROFILE = 'invalid_icc_profile'
  INVALID_METADATA_SCHEMA = 'invalid_metadata_schema'
  MISSING_PIXEL_SPACING = 'missing_pixel_spacing'
  MISSING_SERIES_UID = 'missing_series_uid_in_metadata'
  MISSING_STUDY_UID = 'missing_study_uid_in_metadata'
  MISSING_ACCESSION_NUMBER = 'missing_accession_number_metadata'
  MISSING_PATIENT_ID = 'missing_patient_id_metadata'
  MISSING_TIFF_SERIES = 'missing_tiff_series'
  UNEXPECTED_EXCEPTION = 'unexpected_exception'
  WSI_DICOM_PAYLOAD_MISSING_DICOM_FILES = (
      'wsi_dicom_payload_missing_dicom_files'
  )
  WSI_DICOM_STUDY_UID_METADATA_DICOM_MISMATCH = (
      'wsi_dicom_study_uid_metadata_dicom_mismatch'
  )
  WSI_TO_DICOM_CONVERSION_FAILED = 'wsi_to_dicom_conversion_failed'
  WSI_TO_DICOM_INVALID_DICOM_GENERATED = 'wsi_to_dicom_invalid_dicom_generated'
  # Slide ID decoding error messages
  BARCODE_IMAGE_MISSING = 'slide_id_error__missing_barcode_containing_image'
  BARCODE_MISSING_FROM_IMAGES = 'slide_id_error__barcode_missing_from_images'
  CANDIDATE_SLIDE_ID_MISSING = 'slide_id_error__candidate_slide_id_missing'
  SLIDE_ID_DEFINED_ON_MULTIPLE_ROWS = (
      'slide_id_error__slide_id_defined_on_multiple_rows'
  )
  FILE_NAME_MISSING_SLIDE_ID_CANDIDATES = (
      'slide_id_error__file_name_does_not_contain_slide_id_candidates'
  )
  FILE_NAME_CONTAINS_MULTIPLE_SLIDE_ID_CANDIDATES = (
      'slide_id_error__file_name_contains_multiple_slide_id_candidates'
  )
  MULTIPLE_BARCODES_SLIDE_ID_CANDIDATES = (
      'slide_id_error__multiple_barcodes_decoded'
  )
  SLIDE_ID_MISSING = 'slide_id_error__slide_id_missing'
  SLIDE_ID_MISSING_FROM_CSV_METADATA = (
      'slide_id_error__slide_id_missing_from_csv_metadata'
  )
  SLIDE_ID_MISSING_FROM_BQ_METADATA = (
      'slide_id_error__slide_id_missing_from_big_query_metadata'
  )
  SLIDE_ID_MISSING_FROM_FILE_NAME = (
      'slide_id_error__slide_id_missing_from_filename'
  )
  INVALID_SLIDE_ID_LENGTH = 'slide_id_error__invalid_slide_id_length'

  # Flat image error messages
  FLAT_IMAGE_FAILED_TO_CONVERT_TIF_TO_JPG = (
      'flat_image_failed_to_convert_tif_to_jpg'
  )
  FLAT_IMAGE_FAILED_TO_OPEN = 'flat_image_failed_to_open'
  FLAT_IMAGE_UNEXPECTED_DIMENSIONS = 'flat_image_unexpected_dimensions'
  FLAT_IMAGE_UNEXPECTED_FORMAT = 'flat_image_unexpected_format'
  FLAT_IMAGE_UNEXPECTED_PIXEL_MODE = 'flat_image_unexpected_pixel_mode'

  # Metadata Mapping Schema Errors
  MISSING_METADATA_FOR_REQUIRED_DICOM_TAG = (
      'missing_metadata_for_required_dicom_tag'
  )

  ERROR_UPLOADING_DICOM_TO_GCS = 'error_uploading_dicom_to_gcs'
  ERROR_DECODING_DICOM_STORE_STUDY_INSTANCE_UID_SEARCH_RESPONSE = (
      'error_decoding_dicom_store_study_instance_uid_search_response'
  )
