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
"""Flags used in transformation pipeline."""

import enum
import json
import os
import sys
from typing import Any, Optional

from absl import flags

from shared_libs.flags import flag_utils
from transformation_pipeline.ingestion_lib import ingest_const


def get_value(
    env_var: Optional[str] = None,
    test_default_value: Optional[Any] = None,
    default_value: Any = None,
) -> Any:
  """Returns applicable value for flag.

  Prioritizes env_var if available, then test value, then default value.

  Args:
    env_var: Environment variable name to use.
    test_default_value: Default value to use when running tests.
    default_value: Default value to use.
  """
  if env_var and os.getenv(env_var):
    return os.getenv(env_var)
  if test_default_value and (
      'UNITTEST_ON_FORGE' in os.environ or 'unittest' in sys.modules
  ):
    return test_default_value
  return default_value


class UidSource(enum.Enum):
  DICOM = 1
  METADATA = 2


class Wsi2DcmCompression(enum.Enum):
  JPEG = 'jpeg'
  JPEG2000 = 'jpeg2000'
  RAW = 'raw'


class Wsi2DcmFirstLevelCompression(enum.Enum):
  NONE = None
  JPEG = Wsi2DcmCompression.JPEG.value
  JPEG2000 = Wsi2DcmCompression.JPEG2000.value
  RAW = Wsi2DcmCompression.RAW.value


class Wsi2DcmJpegCompressionSubsample(enum.Enum):
  SUBSAMPLE_444 = '444'
  SUBSAMPLE_440 = '440'
  SUBSAMPLE_442 = '442'
  SUBSAMPLE_420 = '420'


class Wsi2DcmPixelEquivalentTransform(enum.Enum):
  DISABLED = None
  HIGHEST_MAGNIFICATION = 'SVSImportPreferScannerTileingForLargestLevel'
  ALL_LEVELS = 'SVSImportPreferScannerTileingForAllLevels'


### Flag annotations used to control public documentation
# Flags annotated with 'oof' are applicable for OOF mode.
# Flags annotated with 'default' are applicable for default mode.
# Flags annotated with 'required' must be populated for the pipeline to work.
# Flags annotated with 'optional' pipeline will work without populating them.
# Flags annotated with 'obscure' are not included in the documentation but
# can be shared only with targeted customers.
# Flags annotated with 'internal'' should never be shared externally.

TRANSFORMATION_PIPELINE_FLG = flags.DEFINE_string(
    'transformation_pipeline',
    os.getenv('TRANSFORMATION_PIPELINE', ''),
    '[optional|default,oof] Defines transformation pipeline to run. Must be one'
    ' of {default, oof}.',
)

REDIS_HOST_IP_FLG = flags.DEFINE_string(
    'redis_host_ip',
    os.getenv('REDIS_HOST_IP', ''),
    '[optional|default/oof] IP address for Memorystore Redis instance.',
)

EMBED_ICC_PROFILE_FLG = flags.DEFINE_boolean(
    'embed_icc_profile',
    os.getenv('EMBED_ICC_PROFILE', 'true'),
    '[optional|default] embed icc profile into dicom',
)

METADATA_BUCKET_FLG = flags.DEFINE_string(
    'metadata_bucket',
    os.getenv('METADATA_BUCKET', ''),
    '[required|default] Name of bucket non-imaging metadata is pushed to.',
)

BIG_QUERY_METADATA_TABLE_FLG = flags.DEFINE_string(
    'big_query_metadata_table',
    os.getenv('BIG_QUERY_METADATA_TABLE', ''),
    'Id of BigQuery table to ingest metadata from '
    'ex.project_id.dataset_id.table_name.',
)

DICOM_GUID_PREFIX_FLG = flags.DEFINE_string(
    'dicom_guid_prefix',
    os.getenv('DICOM_GUID_PREFIX', None),
    '[obscure|default] Prefix for generated DICOM GUIDs.',
)

VIEWER_DEBUG_URL_FLG = flags.DEFINE_string(
    'viewer_debug_url',
    os.getenv('VIEWER_DEBUG_URL', ''),
    '[optional|default,oof] If defined, a debug url is logged at the end of'
    ' ingestion.',
)

DICOM_QUOTA_ERROR_RETRY_FLG = flags.DEFINE_integer(
    'dicom_quota_error_retry_sec',
    os.getenv('DICOM_QUOTA_ERROR_RETRY', '600'),
    '[obscure|default,oof] Seconds to wait before retying DICOM Store'
    ' upload due to quota failure.',
)

COPY_DICOM_TO_BUCKET_URI_FLG = flags.DEFINE_string(
    'copy_dicom_to_bucket_uri',
    os.getenv('COPY_DICOM_TO_BUCKET_URI', ''),
    '[obscure|default/oof] The bucket to copy generated DICOM to. If empty, '
    'the generated DICOM will not be copied.',
)

# Pub/Sub flags
PROJECT_ID_FLG = flags.DEFINE_string(
    'project_id',
    os.getenv('PROJECT_ID', ''),
    '[required|default,oof] GCP project id to listen on.',
)
GCS_SUBSCRIPTION_FLG = flags.DEFINE_string(
    'gcs_subscription',
    os.getenv('GCS_SUBSCRIPTION', None),
    '[required|default] Pub/Sub GCS subscription id to listen on. Used in'
    ' regular (default) transformation pipeline only.',
)
DICOM_STORE_SUBSCRIPTION_FLG = flags.DEFINE_string(
    'dicom_store_subscription',
    os.getenv('DICOM_STORE_SUBSCRIPTION', None),
    '[optional|default] Pub/Sub DICOM store subscription id to listen on. Used'
    'in regular (default) transformation pipeline only.',
)
OOF_SUBSCRIPTION_FLG = flags.DEFINE_string(
    'oof_subscription',
    os.getenv('OOF_SUBSCRIPTION', None),
    '[required|oof] Pub/Sub Dataflow subscription id to listen on. Used in OOF'
    ' transformation pipeline only.',
)
INGEST_COMPLETE_OOF_TRIGGER_PUBSUB_TOPIC_FLG = flags.DEFINE_string(
    'ingest_complete_oof_trigger_pubsub_topic',
    os.getenv(
        ingest_const.EnvVarNames.INGEST_COMPLETE_OOF_TRIGGER_PUBSUB_TOPIC, ''
    ),
    '[required|oof] Pub/Sub topic to publish to at completion of ingestion to'
    ' trigger OOF.',
)

# GCS ingestion flags
INGEST_SUCCEEDED_URI_FLG = flags.DEFINE_string(
    'ingest_succeeded_uri',
    os.getenv('INGEST_SUCCEEDED_URI', ''),
    '[required|default] Bucket/path to move input to if ingest succeeds.',
)
INGEST_FAILED_URI_FLG = flags.DEFINE_string(
    'ingest_failed_uri',
    os.getenv('INGEST_FAILED_URI', ''),
    '[required|default] Bucket/path to move input to if an error occurs.',
)
GCS_INGEST_STUDY_INSTANCE_UID_SOURCE_FLG = flags.DEFINE_enum_class(
    'gcs_ingest_study_instance_uid_source',
    get_value(env_var='GCS_INGEST_STUDY_INSTANCE_UID_SOURCE'),
    UidSource,
    '[required|default] Which StudyInstanceUID source to use in GCS ingestion.',
)
INGEST_IGNORE_ROOT_DIR_FLG = flags.DEFINE_multi_string(
    'ingest_ignore_root_dirs',
    json.loads(
        os.getenv(
            'INGEST_IGNORE_ROOT_DIR', '["cloud-ingest", "storage-transfer"]'
        )
    ),
    '[optional|default] Root folders ignored in ingestion.',
)
GCS_FILE_INGEST_LIST_FLG = flags.DEFINE_multi_string(
    'gcs_file_to_ingest_list',
    os.getenv('GCS_FILE_INGEST_LIST', None),
    '[optional|default] Fixed list of GCS files to ingest. Client will'
    'terminate at end of ingestion.',
)
GCS_UPLOAD_IGNORE_FILE_EXTS_FLG = flags.DEFINE_list(
    'gcs_upload_ignore_file_exts',
    os.getenv(ingest_const.EnvVarNames.GCS_UPLOAD_IGNORE_FILE_EXT, ''),
    '[optional|default] Comma-separated list of file extensions (e.g., ".json")'
    ' which will be ignored by ingestion if uploaded to GCS. Files without'
    ' extensions can be defined as " ".',
)
FILENAME_SLIDEID_REGEX_FLG = flags.DEFINE_string(
    'wsi2dcm_filename_slideid_regex',
    os.getenv('SLIDEID_REGEX', '^[a-zA-Z0-9]+-[a-zA-Z0-9]+(-[a-zA-Z0-9]+)+'),
    '[optional|default] Regular expression used to identify candidate slide id'
    ' in GCS filename. Default value matches 3 or more hyphen separated'
    ' alpha-numeric blocks, e.g. SR-21-2 and SR-21-2-B1-5.',
)
FILENAME_SLIDEID_SPLIT_STR_FLG = flags.DEFINE_string(
    'filename_slideid_split_str',
    os.getenv('FILENAME_SLIDEID_SPLIT_STR', '_'),
    '[optional|default] Character or string to split GCS filename to find'
    ' slide id using regex defined in FILENAME_SLIDEID_REGEX_FLG.',
)
TEST_WHOLE_FILENAME_AS_SLIDEID_FLG = flags.DEFINE_boolean(
    'test_whole_filename_as_slideid',
    flag_utils.env_value_to_bool('TEST_WHOLE_FILENAME_AS_SLIDEID'),
    '[obscure|default] Include whole filename excluding file extension in'
    ' slide id test.',
)

ENABLE_CREATE_MISSING_STUDY_INSTANCE_UID_FLG = flags.DEFINE_boolean(
    'enable_create_missing_study_instance_uid',
    flag_utils.env_value_to_bool('ENABLE_CREATE_MISSING_STUDY_INSTANCE_UID'),
    '[optional|default] If True pipeline will create instance Study Instance '
    'UID if its missing. Requires metadata to define accession number.',
)


DELETE_FILE_FROM_INGEST_AT_BUCKET_AT_INGEST_SUCCESS_OR_FAILURE_FLG = flags.DEFINE_boolean(
    'delete_file_from_ingest_or_bucket',
    flag_utils.env_value_to_bool(
        'DELETE_FILE_FROM_INGEST_BUCKET_AT_INGEST_SUCCESS_OR_FAILURE',
        undefined_value=True,
    ),
    '[obscure|default] Set flag to False to disable file deletion from the '
    'ingestion bucket. If disabled, file will be copied to the success/failure '
    'bucket at the end of ingestion but not removed from the ingestion bucket '
    'to help de-duplicate uploads.',
)

OOF_INFERENCE_CONFIG_PATH_FLG = flags.DEFINE_string(
    'oof_inference_config_path',
    os.getenv(ingest_const.EnvVarNames.OOF_INFERENCE_CONFIG_PATH, ''),
    '[optional|default] Path to OOF inference config to be included in Pub/Sub '
    'messages generated at the end of ingestion to trigger inference pipeline.',
)
OOF_LEGACY_INFERENCE_PIPELINE_FLG = flags.DEFINE_boolean(
    'oof_legacy_inference_pipeline',
    flag_utils.env_value_to_bool(
        'OOF_LEGACY_INFERENCE_PIPELINE',
        undefined_value=True,
    ),
    '[private|default,oof] Whether to use legacy OOF inference pipeline. Used '
    'both to trigger inference pipeline in default ingestion mode and process '
    'inference pipeline output in OOF ingestion mode.',
)

# DICOM Metadata Schema flags
REQUIRE_TYPE1_DICOM_TAG_METADATA_IS_DEFINED_FLG = flags.DEFINE_boolean(
    'require_type1_dicom_tag_metadata_is_defined',
    flag_utils.env_value_to_bool('REQUIRE_TYPE1_DICOM_TAG_METADATA_IS_DEFINED'),
    '[optional|default] Require all type one tags defined in the metadata'
    ' schema have defined values; if not raise'
    ' MissingRequiredMetadataValueError exception and  fail ingestion.',
)

METADATA_PRIMARY_KEY_COLUMN_NAME_FLG = flags.DEFINE_string(
    'metadata_primary_key_column_name',
    os.getenv('METADATA_PRIMARY_KEY_COLUMN_NAME', 'Bar Code Value'),
    '[optional|default] Defines column name used as primary key for joining '
    'candidate imaging slide id with BigQuery or CSV metadata.',
)

# DICOM Store ingestion flags
DICOM_STORE_INGEST_GCS_URI_FLG = flags.DEFINE_string(
    'dicom_store_ingest_gcs_uri',
    os.getenv('DICOM_STORE_INGEST_GCS_URI', ''),
    '[optional|default] GCS URI for temporarily storing DICOM files when'
    ' ingesting from DICOM Store. Files are stored as backup before deletion'
    ' from DICOM Store.',
)

# WSI specific flags
INGESTION_PYRAMID_LAYER_GENERATION_CONFIG_PATH_FLG = flags.DEFINE_string(
    'ingestion_pyramid_layer_generation_config_path',
    os.getenv('INGESTION_PYRAMID_LAYER_GENERATION_CONFIG_PATH', ''),
    '[optional|default] Path to *.YAML or *.JSON file which defines WSI'
    ' downsampling pyramid layers to generate.',
)
WSI2DCM_DICOM_FRAME_HEIGHT_FLG = flags.DEFINE_integer(
    'wsi2dcm_dicom_frame_height',
    os.getenv('WSI2DCM_DICOM_FRAME_HEIGHT', '256'),
    '[optional|default] wsi2dcm dicom frame height.',
)
WSI2DCM_DICOM_FRAME_WIDTH_FLG = flags.DEFINE_integer(
    'wsi2dcm_dicom_frame_width',
    os.getenv('WSI2DCM_DICOM_FRAME_WIDTH', '256'),
    '[optional|default] wsi2dcm dicom frame width.',
)
WSI2DCM_COMPRESSION_FLG = flags.DEFINE_enum_class(
    'wsi2dcm_compression',
    get_value(
        env_var='WSI2DCM_COMPRESSION',
        default_value=Wsi2DcmCompression.JPEG.name,
    ),
    Wsi2DcmCompression,
    '[optional|default] wsi2dcm compression param used across all levels; '
    'Can be overridden at highest magnification by using the '
    '--first_level_compression flag.',
)
WSI2DCM_FIRST_LEVEL_COMPRESSION_FLG = flags.DEFINE_enum_class(
    'wsi2dcm_first_level_compression',
    get_value(
        env_var='WSI2DCM_FIRST_LEVEL_COMPRESSION',
        default_value=Wsi2DcmFirstLevelCompression.NONE.name,
    ),
    Wsi2DcmFirstLevelCompression,
    '[optional|default] wsi2dcm compression param to use at highest '
    'magnification level.',
)
WSI2DCM_JPEG_COMPRESSION_QUALITY_FLG = flags.DEFINE_integer(
    'wsi2dcm_jpeg_compression_quality',
    os.getenv('WSI2DCM_JPEG_COMPRESSION_QUALITY', '95'),
    '[optional|default] wsi2dcm jpeg compression quality range 1 - 100.',
)
WSI2DCM_JPEG_COMPRESSION_SUBSAMPLING_FLG = flags.DEFINE_enum_class(
    'wsi2dcm_jpeg_compression_subsampling',
    get_value(
        env_var='WSI2DCM_JPEG_COMPRESSION_SUBSAMPLING',
        default_value=Wsi2DcmJpegCompressionSubsample.SUBSAMPLE_444.name,
    ),
    Wsi2DcmJpegCompressionSubsample,
    '[optional|default] wsi2dcm jpeg compression subsampling.',
)
WSI2DCM_PIXEL_EQUIVALENT_TRANSFORM_FLG = flags.DEFINE_enum_class(
    'wsi2dcm_pixel_equivalent_transform',
    get_value(
        env_var='WSI2DCM_PIXEL_EQUIVALENT_TRANSFORM',
        default_value=Wsi2DcmPixelEquivalentTransform.HIGHEST_MAGNIFICATION.name,
    ),
    Wsi2DcmPixelEquivalentTransform,
    '[optional|default] Defines input image magnifications that wsi2dcm will '
    'use to perform pixel equivalent transform on.',
)
INIT_SERIES_INSTANCE_UID_FROM_METADATA_FLG = flags.DEFINE_boolean(
    'initialize_svs_ingestion_series_uid_from_metadata',
    flag_utils.env_value_to_bool(
        'INIT_SVS_INGEST_SERIES_INSTANCE_UID_FROM_METADATA',
        undefined_value=False,
    ),
    '[optional|default] Initialize series instance UID from metadata.'
    ' Recommended setting = False. If true disables auto-reingestion of rescans'
    ' into new series and correct handling of rescans would need to be handled'
    ' outside of DPAS ingestion. Used in golden grahams project.',
)

# Flat image specific flags
FLAT_IMAGES_VL_MICROSCOPIC_IMAGE_IOD_FLG = flags.DEFINE_boolean(
    'flat_images_vl_microscopic_image_iod',
    flag_utils.env_value_to_bool('FLAT_IMAGES_VL_MICROSCOPIC_IMAGE_IOD', False),
    '[optional|default] Whether to use VL Microscopic Image IOD for ingestion'
    ' of flat images without slide coordinates. If unset, all images will use'
    ' VL Slide-Coordinates Microscopic Image IOD.',
)

# DICOM Store URL flags
DICOMWEB_URL_FLG = flags.DEFINE_string(
    'dicomweb_url',
    os.getenv(ingest_const.EnvVarNames.DICOMWEB_URL, ''),
    '[required|default,oof] DICOM Store to upload primary imaging into.',
)
OOF_DICOMWEB_BASE_URL_FLG = flags.DEFINE_string(
    'oof_dicomweb_base_url',
    os.getenv(ingest_const.EnvVarNames.OOF_DICOMWEB_BASE_URL, ''),
    '[required|oof] DICOM Store to upload OOF imaging into.',
)
DICOM_STORE_TO_CLEAN_FLG = flags.DEFINE_string(
    'dicom_store_to_clean',
    os.getenv(ingest_const.EnvVarNames.DICOM_STORE_TO_CLEAN, ''),
    '[optional|oof] DICOM Store to cleanup instances used for OOF pipeline'
    ' after it completes.',
)

# Barcode flags
ZXING_CLI_FLG = flags.DEFINE_string(
    'zxing_cli',
    os.getenv('ZXING_CLI', '/zxing/cpp/build/zxing'),
    '[optional|default] Path to zxing command line tool.',
)
DISABLE_CLOUD_VISION_BARCODE_SEG_FLG = flags.DEFINE_boolean(
    'testing_disable_cloudvision',
    flag_utils.env_value_to_bool('DISABLE_CLOUD_VISION_BARCODE_SEG'),
    '[private] Unit testing flag. Disables cloud vision.',
)
DISABLE_BARCODE_DECODER_FLG = flags.DEFINE_boolean(
    'disable_barcode_decoder',
    flag_utils.env_value_to_bool('DISABLE_BARCODE_DECODER'),
    '[optional|default] Disable barcode decoder.',
)
