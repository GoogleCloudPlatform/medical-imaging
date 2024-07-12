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

HEALTHCARE_API = 'https://healthcare.googleapis.com/v1'

# Private creator tag def.
PRIVATE_TAG_CREATOR = 'GOOGLE'
SC_MANUFACTURER_NAME = 'GOOGLE'

# DICOM UIDs used in ingest pipeline.
# Google Implentation Class UID.
SC_IMPLEMENTATION_CLASS_UID = '1.3.6.1.4.1.11129.5.4.1'
WSI_IMPLEMENTATION_CLASS_UID = '1.3.6.1.4.1.11129.5.4.1'
SCMI_IMPLEMENTATION_CLASS_UID = '1.3.6.1.4.1.11129.5.4.1'
IMPLEMENTATION_VERSION_NAME = 'GooglePathTrans'  # Max len 16 characters

# UID prefix based on Google UID prefix used to generate UIDs.
# https://dicom.nema.org/medical/dicom/current/output/chtml/part05/sect_6.2.html
DPAS_UID_PREFIX = '1.3.6.1.4.1.11129.5.7'

MESSAGE_TTL_S = int(600)

# wsi image and frame type keywords
OVERVIEW = 'OVERVIEW'
THUMBNAIL = 'THUMBNAIL'
LABEL = 'LABEL'
DERIVED = 'DERIVED'
MONOCHROME2 = 'MONOCHROME2'
SECONDARY = 'SECONDARY'
ORIGINAL = 'ORIGINAL'
PRIMARY = 'PRIMARY'
VOLUME = 'VOLUME'
NONE = 'NONE'
RESAMPLED = 'RESAMPLED'
ORIGINAL_PRIMARY_VOLUME = f'{ORIGINAL}\\{PRIMARY}\\{VOLUME}'
ORIGINAL_PRIMARY_VOLUME_RESAMPLED = (
    f'{ORIGINAL}\\{PRIMARY}\\{VOLUME}\\{RESAMPLED}'
)
DERIVED_PRIMARY_VOLUME = f'{DERIVED}\\{PRIMARY}\\{VOLUME}'
DERIVED_PRIMARY_VOLUME_RESAMPLED = (
    f'{DERIVED}\\{PRIMARY}\\{VOLUME}\\{RESAMPLED}'
)

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
  PYRAMID_LABEL = '00200027'
  PYRAMID_DESCRIPTION = '00081088'
  PYRAMID_UID = '00080019'
  SERIES_DESCRIPTION = '0008103E'
  SERIES_INSTANCE_UID = '0020000E'
  SOP_CLASS_UID = '00080016'
  SOP_INSTANCE_UID = '00080018'
  STUDY_INSTANCE_UID = '0020000D'


class DICOMTagKeywords:
  """DICOM Keywords used in ingest project."""

  ACCESSION_NUMBER = 'AccessionNumber'
  ACQUISITION_DATE = 'AcquisitionDate'
  ACQUISITION_DATE_TIME = 'AcquisitionDateTime'
  ACQUISITION_TIME = 'AcquisitionTime'
  BARCODE_VALUE = 'BarcodeValue'
  BITS_ALLOCATED = 'BitsAllocated'
  BITS_STORED = 'BitsStored'
  BURNED_IN_ANNOTATION = 'BurnedInAnnotation'
  COLOR_SPACE = 'ColorSpace'
  COLUMNS = 'Columns'
  CONCATENATION_FRAME_OFFSET_NUMBER = 'ConcatenationFrameOffsetNumber'
  CONCATENATION_UID = 'ConcatenationUID'
  CONTENT_DATE = 'ContentDate'
  CONTENT_TIME = 'ContentTime'
  DEVICE_SERIAL_NUMBER = 'DeviceSerialNumber'
  DICOM_GOOGLE_PRIVATE_CREATOR_BLOCK_TAG = (
      DICOMTagAddress.DICOM_GOOGLE_PRIVATE_CREATOR_BLOCK_TAG
  )
  DIMENSION_ORGANIZATION_TYPE = 'DimensionOrganizationType'
  EXTENDED_DEPTH_OF_FIELD = 'ExtendedDepthOfField'
  FOCUS_METHOD = 'FocusMethod'
  FRAME_OF_REFERENCE_UID = 'FrameOfReferenceUID'
  FRAME_TYPE = 'FrameType'
  GROUP_ADDRESS = '3021'
  HASH_PRIVATE_TAG = DICOMTagAddress.HASH_PRIVATE_TAG
  HIGH_BIT = 'HighBit'
  ICC_PROFILE = 'ICCProfile'
  IMAGE_ORIENTATION_SLIDE = 'ImageOrientationSlide'
  IMAGE_TYPE = 'ImageType'
  IMAGED_VOLUME_DEPTH = 'ImagedVolumeDepth'
  IMAGED_VOLUME_HEIGHT = 'ImagedVolumeHeight'
  IMAGED_VOLUME_WIDTH = 'ImagedVolumeWidth'
  IN_CONCATENATION_NUMBER = 'InConcatenationNumber'
  INGEST_FILENAME_TAG = DICOMTagAddress.INGEST_FILENAME_TAG
  INSTANCE_NUMBER = 'InstanceNumber'
  MANUFACTURER = 'Manufacturer'
  MANUFACTURER_MODEL_NAME = 'ManufacturerModelName'
  MODALITY = 'Modality'
  NUMBER_OF_FRAMES = 'NumberOfFrames'
  OOF_SCORE_PRIVATE_TAG = DICOMTagAddress.OOF_SCORE_PRIVATE_TAG
  OPTICAL_PATH_SEQUENCE = 'OpticalPathSequence'
  PATIENT_ID = 'PatientID'
  PATIENT_NAME = 'PatientName'
  PHOTOMETRIC_INTERPRETATION = 'PhotometricInterpretation'
  PIXEL_REPRESENTATION = 'PixelRepresentation'
  PLANAR_CONFIGURATION = 'PlanarConfiguration'
  POSITION_REFERENCE_INDICATOR = 'PositionReferenceIndicator'
  PUBSUB_MESSAGE_ID_TAG = DICOMTagAddress.PUBSUB_MESSAGE_ID_TAG
  PYRAMID_DESCRIPTION = 'PyramidDescription'
  PYRAMID_LABEL = 'PyramidLabel'
  PYRAMID_UID = 'PyramidUID'
  ROWS = 'Rows'
  SAMPLES_PER_PIXEL = 'SamplesPerPixel'
  SERIES_INSTANCE_UID = 'SeriesInstanceUID'
  SERIES_NUMBER = 'SeriesNumber'
  SHARED_FUNCTIONAL_GROUPS_SEQUENCE = 'SharedFunctionalGroupsSequence'
  SLICE_THICKNESS = 'SliceThickness'
  SOFTWARE_VERSIONS = 'SoftwareVersions'
  SOP_CLASS_UID = 'SOPClassUID'
  SOP_INSTANCE_UID = 'SOPInstanceUID'
  SPECIMEN_LABEL_IN_IMAGE = 'SpecimenLabelInImage'
  STUDY_INSTANCE_UID = 'StudyInstanceUID'
  TOTAL_PIXEL_MATRIX_COLUMNS = 'TotalPixelMatrixColumns'
  TOTAL_PIXEL_MATRIX_FOCAL_PLANES = 'TotalPixelMatrixFocalPlanes'
  TOTAL_PIXEL_MATRIX_ROWS = 'TotalPixelMatrixRows'
  TRANSFER_SYNTAX_UID = 'TransferSyntaxUID'
  VOLUMETRIC_PROPERTIES = 'VolumetricProperties'


SOPClassUID = NewType('SOPClassUID', str)
SOPClassName = NewType('SOPClassName', str)


@dataclasses.dataclass(frozen=True)
class DicomSopClass:
  name: SOPClassName
  uid: SOPClassUID


class DicomSopClasses:
  """DICOM SOP Class values used in ingest project."""

  MICROSCOPY_ANNOTATION = DicomSopClass(
      SOPClassName('Microscopy Bulk Simple Annotations Storage'),
      SOPClassUID('1.2.840.10008.5.1.4.1.1.91.1'),
  )
  MICROSCOPIC_IMAGE = DicomSopClass(
      SOPClassName('VL Microscopic Image Storage'),
      SOPClassUID('1.2.840.10008.5.1.4.1.1.77.1.2'),
  )
  SECONDARY_CAPTURE_IMAGE = DicomSopClass(
      SOPClassName('Secondary Capture Image Storage'),
      SOPClassUID('1.2.840.10008.5.1.4.1.1.7'),
  )
  SLIDE_COORDINATES_IMAGE = DicomSopClass(
      SOPClassName('VL Slide-Coordinates Microscopic Image Storage'),
      SOPClassUID('1.2.840.10008.5.1.4.1.1.77.1.3'),
  )
  WHOLE_SLIDE_IMAGE = DicomSopClass(
      SOPClassName('VL Whole Slide Microscopy Image Storage'),
      SOPClassUID('1.2.840.10008.5.1.4.1.1.77.1.6'),
  )


class DicomImageTransferSyntax:
  """DICOM Transfer Syntaxs."""

  # DICOM transfer syntaxs define pixel data encoding.
  # https://dicom.nema.org/medical/dicom/current/output/chtml/part06/chapter_a.html

  # DICOM default. Private tags not compatible with transfer syntax.
  # And are not returned by store on query.
  # DO NOT USE see b/207162516
  IMPLICIT_VR_LITTLE_ENDIAN = '1.2.840.10008.1.2'
  # DO NOT USE poor performance
  DEFLATED_EXPLICIT_VR_LITTLE_ENDIAN = '1.2.840.10008.1.2.1.99'
  # Prefered transfer syntax
  EXPLICIT_VR_LITTLE_ENDIAN = '1.2.840.10008.1.2.1'
  # DO not use deprecated
  EXPLICIT_VR_BIG_ENDIAN = '1.2.840.10008.1.2.2'
  # encapsulated transfer syntaxs
  JPEG_LOSSY = '1.2.840.10008.1.2.4.50'
  JPEG_2000 = '1.2.840.10008.1.2.4.90'


# https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.7.6.html#sect_C.7.6.1.1.5.1
class DicomImageCompressionMethod:
  JPEG_LOSSY = 'ISO_10918_1'


class RedisLockKeywords:
  """Keywords used to identify unique portions of redis lock values."""

  DICOM_ACCESSION_NUMBER = 'DICOM_ACCESSION_NUMBER:%s'
  GCS_TRIGGERED_INGESTION = 'GCS_TRIGGERED SLIDEID:%s'
  DICOM_STORE_TRIGGERED_INGESTION = 'DICOM_STORE_TRIGGERED STORE:%s SLIDE_ID:%s'
  ML_TRIGGERED_INGESTION = (
      'ML_TRIGGERED STUDY_INSTANCE_UID:%s SERIES_INSTANCE_UID: %s'
  )


class PubSubKeywords:
  PIXEL_SPACING_WIDTH = 'PixelSpacingWidth'
  PIXEL_SPACING_HEIGHT = 'PixelSpacingHeight'


class LogKeywords:
  """Keywords used in structured logs.

  Define all new constants in uppercase.  Refactoring constants to uppercase to
  match global style.
  """

  ACCESSION_NUMBER = 'accession_number'
  ACK_DEADLINE_SEC = 'ack_deadline_sec'
  BARCODE = 'barcode'
  BIGQUERY_TABLE = 'bigquery_table'
  BIGQUERY_TABLE_COLUMN_NAMES = 'bigquery_table_column_names'
  BUCKET_NAME = 'bucket_name'
  DEST_FILE = 'dest_file'
  DEST_URI = 'dest_URI'
  DICOM_INSTANCES_TRIGGERING_TRANSFORM_PIPELINE = (
      'dicom_instances_triggering_transform_pipeline'
  )
  DICOM_TAGS = 'dicom_tags'
  DICOMWEB_PATH = 'dicomweb_path'
  DPAS_INGESTION_TRACE_ID = 'dpas_ingestion_trace_id'
  ELAPSED_TIME_BEYOND_EXTENSION_SEC = 'elapsed_time_beyond_extension_sec'
  EXCEPTION = 'exception'
  EXISTING_DICOM_INSTANCE = 'existing_dicom_instance'
  EXTENDING_ACK_DEADLINE_SEC = 'extending_ack_deadline_sec'
  FILE_EXTENSION = 'file_extension'
  FILE_NAME_PART_REGEX = 'file_name_slide_id_part_regex'
  FILE_NAME_PART_SPLIT_STRING = 'file_name_slide_id_part_split_string'
  FILE_SIZE = 'file_size(bytes)'
  FILENAME = 'filename'
  GCS_IGNORE_FILE_REGEXS = 'gcs_ignore_file_regexs'
  HASH = 'hash'
  IGNORE_FILE_BUCKET = 'ignore_file_bucket'
  INGESTION_HANDLER = 'ingestion_handler'
  INVALID_CHARACTER = 'invalid_character'
  LOCK_HELD_SEC = 'lock_held_sec'
  LOCK_NAME = 'lock_name'
  LOCK_TOKEN = 'lock_token'
  MAIN_DICOM_STORE = 'main_dicom_store'
  MATCHED_REGEX = 'matched_regex'
  MESSAGE_COUNT = 'message_count'
  METADATA = 'metadata'
  METADATA_PRIMARY_KEY = 'metadata_primary_key'
  METADATA_PRIMARY_KEY_COLUMN_NAME = 'metadata_primary_key_column_name'
  METADATA_SOURCE = 'metadata_source'
  NEW_SERIES_INSTANCE_UID = 'new_series_instance_uid'
  OLD_SERIES_INSTANCE_UID = 'old_series_instance_uid'
  OOF_DICOM_STORE = 'oof_dicom_store'
  PATIENT_ID = 'patient_id'
  PIPELINE_GENERATED_DOWNSAMPLE_DICOM_INSTANCE = (
      'pipeline_generated_downsample_dicom_instance'
  )
  PREVIOUS_SERIES_INSTANCE_UID = 'previous_series_instance_uid'
  PREVIOUS_STUDY_INSTANCE_UID = 'previous_study_instance_uid'
  PUBSUB_MESSAGE_ID = 'pubsub_message_id'
  PUBSUB_SUBSCRIPTION = 'pubsub_subscription'
  PUBSUB_TOPIC_NAME = 'pubsub_topic_name'
  RECEIVED_EVENT_TYPE = 'received_event_type'
  REDIS_SERVER_IP = 'redis_server_ip'
  REDIS_SERVER_PORT = 'redis_server_port'
  RETURN_CODE = 'return-code'
  SERIES_INSTANCE_UID = 'series_instance_uid'
  SLIDE_ID = 'slide_id'
  SOP_CLASS_UID = 'sop_class_uid'
  SOP_INSTANCE_UID = 'sop_instance_uid'
  SOURCE_URI = 'source_URI'
  STDERR = 'stderr'
  STDOUT = 'stdout'
  STUDY_INSTANCE_UID = 'study_instance_uid'
  STUDY_INSTANCE_UIDS_FOUND = 'study_instance_uids_found'
  TAG_NUMBER = 'private_tag_number'
  TESTED_DICOM_INSTANCE = 'tested_dicom_instance'
  TOTAL_TIME_SEC = 'total_time_sec'
  TYPE2_AND_2C_TAGS_ADDED = 'type2_and_2c_tags_added'
  TYPE2_TAGS_ADDED = 'type2_tags_added'
  URI = 'uri'
  URL = 'url'


class EnvVarNames:
  DICOM_STORE_TO_CLEAN = 'DICOM_STORE_TO_CLEAN'
  DICOMWEB_URL = 'DICOMWEB_URL'
  GCS_IGNORE_FILE_BUCKET = 'GCS_IGNORE_FILE_BUCKET'
  GCS_IGNORE_FILE_REGEXS = 'GCS_IGNORE_FILE_REGEXS'
  GCS_UPLOAD_IGNORE_FILE_EXT = 'GCS_UPLOAD_IGNORE_FILE_EXT'
  INGEST_COMPLETE_OOF_TRIGGER_PUBSUB_TOPIC = (
      'INGEST_COMPLETE_OOF_TRIGGER_PUBSUB_TOPIC'
  )
  OOF_DICOMWEB_BASE_URL = 'OOF_DICOMWEB_BASE_URL'
  OOF_INFERENCE_CONFIG_PATH = 'OOF_INFERENCE_CONFIG_PATH'


class OofPassThroughKeywords:
  DISABLE_TFEXAMPLE_WRITE = 'DISABLE_TFEXAMPLE_WRITE'
  DPAS_INGESTION_TRACE_ID = 'DICOM_INGESTION_TRACE_ID'
  SOURCE_DICOM_IN_MAIN_STORE = 'SOURCE_DICOM_IN_MAIN_STORE'


class ErrorMsgs:
  """Simplified error messages.

  GCS ingestion may use this simplified error message for GCS failure bucket
  paths, which are passed as an additional args element in exceptions.
  """

  ACQUIRING_LOCK_OUTSIDE_CONTEXT_BLOCK = 'acquiring_lock_outside_context_block'
  BQ_METADATA_NOT_FOUND = 'bigquery_metadata_not_found'
  BQ_METADATA_TABLE_ENV_FORMAT = (
      'big_query_metadata_table_env_incorrectly_formatted'
  )
  COULD_NOT_ACQUIRE_LOCK = 'could_not_acquire_lock'
  INVALID_DICOM = 'invalid_dicom'
  INVALID_DICOM_STANDARD_REPRESENTATION = (
      'invalid_dicom_standard_representation'
  )
  INVALID_ICC_PROFILE = 'invalid_icc_profile'
  INVALID_METADATA_SCHEMA = 'invalid_metadata_schema'
  MISSING_PIXEL_SPACING = 'missing_pixel_spacing'
  MISSING_SERIES_UID = 'missing_series_instance_uid_in_metadata'
  MISSING_STUDY_UID = 'missing_study_instance_uid_in_metadata'
  MISSING_ACCESSION_NUMBER = 'missing_accession_number_metadata'
  MISSING_ACCESSION_NUMBER_UNABLE_TO_CREATE_STUDY_INSTANCE_UID = (
      'metadata_is_missing_accession_number_unable_to_create_study_instance_uid'
  )
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
  FILE_NAME_CONTAINS_MULTIPLE_SLIDE_ID_CANDIDATES = (
      'slide_id_error__file_name_contains_multiple_slide_id_candidates'
  )
  MULTIPLE_BARCODES_SLIDE_ID_CANDIDATES = (
      'slide_id_error__multiple_barcodes_decoded'
  )
  SLIDE_ID_MISSING = 'slide_id_error__slide_id_missing'
  SLIDE_ID_MISSING_FROM_CSV_METADATA = (
      'slide_id_error__slide_metadata_primary_key_missing_from_csv_metadata'
  )
  SLIDE_ID_MISSING_FROM_BQ_METADATA = 'slide_id_error__slide_metadata_primary_key_missing_from_big_query_metadata'
  FILENAME_MISSING_SLIDE_METADATA_PRIMARY_KEY = (
      'slide_id_error__filename_missing_slide_metadata_primary_key'
  )
  INVALID_SLIDE_ID_LENGTH = 'slide_id_error__invalid_slide_id_length'
  UNSUPPORTED_DICOM_SECONDARY_CAPTURE_IMAGE = (
      'unsupported_dicom_secondary_capture_image'
  )
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

  # DICOM Validation Errors
  DICOM_UID_INCORRECTLY_FORMATTED = 'dicom_uid_incorrectly_formatted'
  DICOM_INSTANCE_ENCODED_WITH_UNSUPPORTED_TRANSFER_SYNTAX = (
      'dicom_instance_encoded_with_unsupported_transfer_syntax'
  )
  WSI_DICOM_INSTANCE_PIXEL_NOT_ALLOCATED_WITH_8_BITS_PER_PIXEL = (
      'wsi_dicom_instance_pixel_not_allocated_with_8_bits_per_pixel'
  )
  WSI_DICOM_INSTANCE_PIXEL_NOT_STORED_WITH_8_BITS_PER_PIXEL = (
      'wsi_dicom_instance_pixel_not_stored_with_8_bits_per_pixel'
  )
  WSI_DICOM_INSTANCE_ENCODED_WITH_INVALID_HIGH_PIXEL_BIT = (
      'wsi_dicom_instance_encoded_with_invalid_high_pixel_bit'
  )
  WSI_DICOM_INSTANCE_ENCODED_WITH_INVALID_SAMPLES_PER_PIXEL = (
      'wsi_dicom_instance_encoded_with_invalid_samples_per_pixel'
  )
  WSI_DICOM_INSTANCE_HAS_0_FRAMES = 'wsi_dicom_instance_has_0_frames'
  WSI_DICOM_INSTANCE_HAS_0_ROWS = 'wsi_dicom_instance_has_0_rows'
  WSI_DICOM_INSTANCE_HAS_0_COLUMNS = 'wsi_dicom_instance_has_0_columns'
  WSI_DICOM_INSTANCE_HAS_0_TOTAL_PIXEL_MATRIX_ROWS = (
      'wsi_dicom_instance_has_0_total_pixel_matrix_rows'
  )
  WSI_DICOM_SPECIMEN_LABEL_IN_IMAGE_NOT_YES_OR_NO = (
      'wsi_dicom_specimen_label_in_image_not_yes_or_no'
  )
  WSI_DICOM_BURNED_IN_ANNOTATION_IN_IMAGE_NOT_YES_OR_NO = (
      'wsi_dicom_burned_in_annotation_in_image_not_yes_or_no'
  )
  WSI_DICOM_INSTANCE_DOES_NOT_HAVE_EXPECTED_FRAME_COUNT = (
      'wsi_dicom_instance_does_not_have_expected_frame_count'
  )
  WSI_DICOM_INSTANCE_HAS_INVALID_DIMENSIONAL_ORGANIZATION_TYPE = (
      'wsi_dicom_instance_has_invalid_dimensional_organization_type'
  )
  WSI_DICOM_ANCILLARY_INSTANCE_TYPE_INDETERMINATE = (
      'wsi_dicom_ancillary_instance_type_indeterminate'
  )
  WSI_DICOM_ANCILLARY_INSTANCE_HAS_MORE_THAN_ONE_FRAME = (
      'wsi_dicom_ancillary_instance_has_more_than_one_frame'
  )
  DICOM_INSTANCE_NOT_FOUND = 'dicom_instance_not_found'
  DICOM_INSTANCE_MISSING_STUDY_INSTANCE_UID = (
      'dicom_instance_missing_study_instance_uid'
  )
  DICOM_INSTANCE_MISSING_SERIES_INSTANCE_UID = (
      'dicom_instance_missing_series_instance_uid'
  )
  DICOM_INSTANCE_MISSING_SOP_INSTANCE_UID = (
      'dicom_instance_missing_sop_instance_uid'
  )
  DICOM_INSTANCE_MISSING_SOP_CLASS_UID = 'dicom_instance_missing_sop_class_uid'
  DICOM_INSTANCES_STUDY_INSTANCE_UID_DO_NOT_MATCH = (
      'dicom_instances_study_instance_uid_do_not_match'
  )
  DICOM_INSTANCES_SERIES_INSTANCE_UID_DO_NOT_MATCH = (
      'dicom_instances_series_instance_uid_do_not_match'
  )
  DICOM_INSTANCES_HAVE_DUPLICATE_SOP_INSTANCE_UID = (
      'dicom_instances_have_duplicate_sop_instance_uid'
  )
  DICOM_INSTANCES_BARCODES_DO_NOT_MATCH = (
      'dicom_instances_barcodes_do_not_match'
  )
  DICOM_INSTANCES_HAVE_MULTIPLE_PRIMARY_VOLUME_IMAGES = (
      'dicom_instances_have_multiple_primary_volume_images'
  )
  DICOM_INSTANCES_HAVE_MULTIPLE_LABEL_IMAGES = (
      'dicom_instances_have_multiple_label_images'
  )
  DICOM_INSTANCES_HAVE_MULTIPLE_OVERVIEW_IMAGES = (
      'dicom_instances_have_multiple_overview_images'
  )
  DICOM_INSTANCES_HAVE_MULTIPLE_THUMBNAIL_IMAGES = (
      'dicom_instances_have_multiple_thumbnail_images'
  )
  DICOM_INSTANCES_HAVE_MULTIPLE_ACCESSION_NUMBERS = (
      'dicom_instances_have_multiple_accession_numbers'
  )
  DICOM_INSTANCES_HAVE_MULTIPLE_PATIENT_NAMES = (
      'dicom_instances_have_multiple_patient_names'
  )
  DICOM_INSTANCES_HAVE_MULTIPLE_PATIENT_IDS = (
      'dicom_instances_have_multiple_patient_ids'
  )
  DICOM_INSTANCE_HAS_UNSUPPORTED_TOTAL_PIXEL_MATRIX_FOCAL_PLANE_VALUE = (
      'dicom_instance_has_unsupported_total_pixel_matrix_focal_plane_value'
  )
  DICOM_INSTANCES_DESCRIBE_MULTIPLE_MODALITIES = (
      'dicom_instances_describe_multiple_modalities'
  )
  ERROR_OCCURRED_QUERYING_DICOM_STORE_UNABLE_TO_CREATE_STUDY_INSTANCE_UID = (
      'error_occurred_querying_dicom_store_unable_to_create_study_instance_uid'
  )
  UNABLE_TO_CREATE_STUDY_INSTANCE_UID_ACCESSION_NUMBER_IS_ASSOCIATED_WITH_MULTIPLE_STUDY_INSTANCE_UID = 'unable_to_create_study_instance_uid_accession_number_is_associated_with_multiple_study_instance_uid'
