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
"""Flags used in transformation pipeline.

Flags annotated with:
* 'oof' are applicable for OOF mode
* 'default' are applicable for default mode
* 'required' must be populated for the pipeline to work
* 'optional' pipeline will work without populating them
"""

import enum
import json
import os
import sys
from typing import Any, List, Optional, Union

from absl import flags

from shared_libs.flags import secret_flag_utils
from transformation_pipeline.ingestion_lib import ingest_const


def _load_multi_string(val: Optional[str]) -> Optional[Union[List[str], str]]:
  if val is None:
    return None
  try:
    return json.loads(val)
  except json.decoder.JSONDecodeError:
    return val


def get_value(
    env_var: str,
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
  if test_default_value and (
      'UNITTEST_ON_FORGE' in os.environ or 'unittest' in sys.modules
  ):
    default_value = test_default_value
  return secret_flag_utils.get_secret_or_env(env_var, default_value)


class DefaultIccProfile(enum.Enum):
  NONE = 'NONE'
  SRGB = 'SRGB'
  ADOBERGB = 'ADOBERGB'
  ROMMRGB = 'ROMMRGB'


class MetadataUidValidation(enum.Enum):
  NONE = 1
  LOG_WARNING = 2
  ERROR = 3


class MetadataTagLengthValidation(enum.Enum):
  NONE = 1
  LOG_WARNING = 2
  LOG_WARNING_AND_CLIP = 3
  ERROR = 4


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


class WsiDicomExtendedDepthOfFieldDefault(enum.Enum):
  YES = 'YES'
  NO = 'NO'


class WsiDicomFocusMethod(enum.Enum):
  AUTO = 'AUTO'
  MANUAL = 'MANUAL'


TRANSFORMATION_PIPELINE_FLG = flags.DEFINE_string(
    'transformation_pipeline',
    secret_flag_utils.get_secret_or_env('TRANSFORMATION_PIPELINE', 'default'),
    '[optional|default,oof] Defines transformation pipeline to run. Must be one'
    ' of {default, oof}.',
)

EMBED_ICC_PROFILE_FLG = flags.DEFINE_boolean(
    'embed_icc_profile',
    secret_flag_utils.get_bool_secret_or_env('EMBED_ICC_PROFILE', True),
    '[optional|default] embed icc profile into dicom',
)

METADATA_BUCKET_FLG = flags.DEFINE_string(
    'metadata_bucket',
    secret_flag_utils.get_secret_or_env('METADATA_BUCKET', ''),
    '[required|default] Name of bucket non-imaging metadata is pushed to.',
)

BIG_QUERY_METADATA_TABLE_FLG = flags.DEFINE_string(
    'big_query_metadata_table',
    secret_flag_utils.get_secret_or_env('BIG_QUERY_METADATA_TABLE', ''),
    'Id of BigQuery table to ingest metadata from '
    'ex.project_id.dataset_id.table_name.',
)

DICOM_GUID_PREFIX_FLG = flags.DEFINE_string(
    'dicom_guid_prefix',
    secret_flag_utils.get_secret_or_env(
        'DICOM_GUID_PREFIX', ingest_const.DPAS_UID_PREFIX
    ),
    '[optional|default] Prefix for generated DICOM GUIDs.',
)

VIEWER_DEBUG_URL_FLG = flags.DEFINE_string(
    'viewer_debug_url',
    secret_flag_utils.get_secret_or_env('VIEWER_DEBUG_URL', ''),
    '[optional|default,oof] If defined, a debug url is logged at the end of'
    ' ingestion.',
)

DICOM_QUOTA_ERROR_RETRY_FLG = flags.DEFINE_integer(
    'dicom_quota_error_retry_sec',
    secret_flag_utils.get_secret_or_env('DICOM_QUOTA_ERROR_RETRY', '600'),
    '[optional|default,oof] Seconds to wait before retying DICOM Store'
    ' upload due to quota failure.',
)

COPY_DICOM_TO_BUCKET_URI_FLG = flags.DEFINE_string(
    'copy_dicom_to_bucket_uri',
    secret_flag_utils.get_secret_or_env('COPY_DICOM_TO_BUCKET_URI', ''),
    '[optional|default/oof] The bucket to copy generated DICOM to. If empty, '
    'the generated DICOM will not be copied.',
)

# Pub/Sub flags
PROJECT_ID_FLG = flags.DEFINE_string(
    'project_id',
    secret_flag_utils.get_secret_or_env('PROJECT_ID', ''),
    '[required|default,oof] GCP project id to listen on.',
)
GCS_SUBSCRIPTION_FLG = flags.DEFINE_string(
    'gcs_subscription',
    secret_flag_utils.get_secret_or_env('GCS_SUBSCRIPTION', None),
    '[required|default] Pub/Sub GCS subscription id to listen on. Used in'
    ' regular (default) transformation pipeline only.',
)
DICOM_STORE_SUBSCRIPTION_FLG = flags.DEFINE_string(
    'dicom_store_subscription',
    secret_flag_utils.get_secret_or_env('DICOM_STORE_SUBSCRIPTION', None),
    '[optional|default] Pub/Sub DICOM store subscription id to listen on. Used'
    'in regular (default) transformation pipeline only.',
)
OOF_SUBSCRIPTION_FLG = flags.DEFINE_string(
    'oof_subscription',
    secret_flag_utils.get_secret_or_env('OOF_SUBSCRIPTION', None),
    '[required|oof] Pub/Sub Dataflow subscription id to listen on. Used in OOF'
    ' transformation pipeline only.',
)
INGEST_COMPLETE_OOF_TRIGGER_PUBSUB_TOPIC_FLG = flags.DEFINE_string(
    'ingest_complete_oof_trigger_pubsub_topic',
    secret_flag_utils.get_secret_or_env(
        ingest_const.EnvVarNames.INGEST_COMPLETE_OOF_TRIGGER_PUBSUB_TOPIC, ''
    ),
    '[required|oof] Pub/Sub topic to publish to at completion of ingestion to'
    ' trigger OOF.',
)

# GCS ingestion flags
INGEST_SUCCEEDED_URI_FLG = flags.DEFINE_string(
    'ingest_succeeded_uri',
    secret_flag_utils.get_secret_or_env('INGEST_SUCCEEDED_URI', ''),
    '[required|default] Bucket/path to move input to if ingest succeeds.',
)
INGEST_FAILED_URI_FLG = flags.DEFINE_string(
    'ingest_failed_uri',
    secret_flag_utils.get_secret_or_env('INGEST_FAILED_URI', ''),
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
    _load_multi_string(
        secret_flag_utils.get_secret_or_env(
            'INGEST_IGNORE_ROOT_DIR', '["cloud-ingest", "storage-transfer"]'
        )
    ),
    '[optional|default] Root folders ignored in ingestion.',
)
GCS_FILE_INGEST_LIST_FLG = flags.DEFINE_multi_string(
    'gcs_file_to_ingest_list',
    _load_multi_string(
        secret_flag_utils.get_secret_or_env('GCS_FILE_INGEST_LIST', None)
    ),
    '[optional|default] Fixed list of GCS files to ingest. Client will'
    'terminate at end of ingestion.',
)
GCS_UPLOAD_IGNORE_FILE_EXTS_FLG = flags.DEFINE_list(
    'gcs_upload_ignore_file_exts',
    secret_flag_utils.get_secret_or_env(
        ingest_const.EnvVarNames.GCS_UPLOAD_IGNORE_FILE_EXT, ''
    ),
    '[optional|default] Comma-separated list of file extensions (e.g., ".json")'
    ' which will be ignored by ingestion if uploaded to GCS. Files without'
    ' extensions can be defined as " ".',
)
GCS_IGNORE_FILE_REGEXS_FLG = flags.DEFINE_multi_string(
    'gcs_ignore_file_regexs',
    _load_multi_string(
        secret_flag_utils.get_secret_or_env(
            ingest_const.EnvVarNames.GCS_IGNORE_FILE_REGEXS, None
        )
    ),
    '[optional|default] List of regular expression used to identify file names '
    'to ignore. The file will will be ingored if any of the regex match.',
)
GCS_IGNORE_FILE_BUCKET_FLG = flags.DEFINE_string(
    'gcs_ignore_file_bucket',
    secret_flag_utils.get_secret_or_env(
        ingest_const.EnvVarNames.GCS_IGNORE_FILE_BUCKET, ''
    ),
    '[optional|default] Optional name of bucket to move files which are ignored'
    ' by the transformation pipeline.',
)
FILENAME_SLIDEID_REGEX_FLG = flags.DEFINE_string(
    'wsi2dcm_filename_slideid_regex',
    secret_flag_utils.get_secret_or_env(
        'SLIDEID_REGEX',
        secret_flag_utils.get_secret_or_env(
            'METADATA_PRIMARY_KEY_REGEX',
            '^[a-zA-Z0-9]+-[a-zA-Z0-9]+(-[a-zA-Z0-9]+)+',
        ),
    ),
    '[optional|default] Regular expression used to identify candidate metadata'
    ' primary key in filename. Default value matches 3 or more hyphen separated'
    ' alpha-numeric blocks, e.g. SR-21-2 and SR-21-2-B1-5.',
)
FILENAME_SLIDEID_SPLIT_STR_FLG = flags.DEFINE_string(
    'filename_slideid_split_str',
    secret_flag_utils.get_secret_or_env(
        'FILENAME_SLIDEID_SPLIT_STR',
        secret_flag_utils.get_secret_or_env(
            'FILENAME_METADATA_PRIMARY_KEY_SPLIT_STR', '_'
        ),
    ),
    '[optional|default] Character or string to split GCS filename to find'
    ' metadata primary key using regex defined in FILENAME_SLIDEID_REGEX_FLG.',
)
TEST_WHOLE_FILENAME_AS_SLIDEID_FLG = flags.DEFINE_boolean(
    'test_whole_filename_as_slideid',
    secret_flag_utils.get_bool_secret_or_env(
        'TEST_WHOLE_FILENAME_AS_SLIDEID',
        secret_flag_utils.get_bool_secret_or_env(
            'TEST_WHOLE_FILENAME_AS_METADATA_PRIMARY_KEY',
        ),
    ),
    '[optional|default] Include slide whole filename excluding file extension'
    ' as a candidate metadata primary key.',
)
INCLUDE_UPLOAD_BUCKET_PATH_IN_WHOLE_FILENAME_SLIDEID_FLG = flags.DEFINE_boolean(
    'include_upload_bucket_path_in_whole_filename_slideid',
    secret_flag_utils.get_bool_secret_or_env(
        'INCLUDE_UPLOAD_BUCKET_PATH_IN_WHOLE_FILENAME_SLIDEID',
        secret_flag_utils.get_bool_secret_or_env(
            'INCLUDE_UPLOAD_BUCKET_PATH_IN_WHOLE_FILENAME_METADATA_PRIMARY_KEY'
        ),
    ),
    '[optional|default] Include bucket upload path in whole filename metadata '
    'primary key. If enabled and file upload to gs://mybucket/foo/bar.svs then '
    'whole filename metadata primary key will be foo/bar. If False (default), '
    'then whole filename metadata primary key will be bar.',
)

ENABLE_CREATE_MISSING_STUDY_INSTANCE_UID_FLG = flags.DEFINE_boolean(
    'enable_create_missing_study_instance_uid',
    secret_flag_utils.get_bool_secret_or_env(
        'ENABLE_CREATE_MISSING_STUDY_INSTANCE_UID'
    ),
    '[optional|default] If True pipeline will create instance Study Instance '
    'UID if its missing. Requires metadata to define accession number.',
)

ENABLE_METADATA_FREE_INGESTION_FLG = flags.DEFINE_boolean(
    'enable_metadata_free_ingestion',
    secret_flag_utils.get_bool_secret_or_env('ENABLE_METADATA_FREE_INGESTION'),
    '[optional|default] If True pipeline will ingest images without requiring '
    'metadata. Each image will be ingested into unique study and series '
    'instance uid. The PatientID will be set to the ingested file name.',
)

METADATA_UID_VALIDATION_FLG = flags.DEFINE_enum_class(
    'metadata_uid_validation',
    get_value(
        env_var='METADATA_UID_VALIDATION',
        default_value=MetadataUidValidation.LOG_WARNING.name,
    ),
    MetadataUidValidation,
    '[optional|default] How to report errors in metadata UID formating.',
)


METADATA_TAG_LENGTH_VALIDATION_FLG = flags.DEFINE_enum_class(
    'metadata_tag_length_validation',
    get_value(
        env_var='METADATA_TAG_LENGTH_VALIDATION',
        default_value=MetadataTagLengthValidation.LOG_WARNING.name,
    ),
    MetadataTagLengthValidation,
    '[optional|default] How to handles DICOM tags which exceed the length/size '
    'limits that are defined by the DICOM standard for the tags VR code.',
)


DELETE_FILE_FROM_INGEST_AT_BUCKET_AT_INGEST_SUCCESS_OR_FAILURE_FLG = flags.DEFINE_boolean(
    'delete_file_from_ingest_or_bucket',
    secret_flag_utils.get_bool_secret_or_env(
        'DELETE_FILE_FROM_INGEST_BUCKET_AT_INGEST_SUCCESS_OR_FAILURE',
        undefined_value=True,
    ),
    '[optional|default] Set flag to False to disable file deletion from the '
    'ingestion bucket. If disabled, file will be copied to the success/failure '
    'bucket at the end of ingestion but not removed from the ingestion bucket '
    'to help de-duplicate uploads.',
)

REDIS_SERVER_IP_FLG = flags.DEFINE_string(
    'redis_server_ip',
    secret_flag_utils.get_secret_or_env('REDIS_SERVER_IP', None),
    '[optional|default] IP address of cloud memory store for redis. Used to '
    'back cross GKE redis locks.',
)

REDIS_SERVER_PORT_FLG = flags.DEFINE_integer(
    'redis_server_port',
    int(secret_flag_utils.get_secret_or_env('REDIS_SERVER_PORT', '6379')),
    '[optional|default] Port of cloud memory store for redis. Used to back '
    'cross GKE redis locks.',
)

REDIS_DB_FLG = flags.DEFINE_integer(
    'redis_db',
    int(secret_flag_utils.get_secret_or_env('REDIS_DB', '0')),
    'Redis database',
)

REDIS_USERNAME_FLG = flags.DEFINE_string(
    'redis_auth_username',
    secret_flag_utils.get_secret_or_env('REDIS_USERNAME', None),
    'Redis auth username.',
)

REDIS_AUTH_PASSWORD_FLG = flags.DEFINE_string(
    'redis_auth_password',
    secret_flag_utils.get_secret_or_env('REDIS_AUTH_PASSWORD', None),
    'Redis auth password.',
)

TRANSFORM_POD_UID_FLG = flags.DEFINE_string(
    'transform_pod_uid',
    secret_flag_utils.get_secret_or_env('MY_POD_UID', None),
    'UID of GKE pod. Do not set unless in test.',
)

TRANSFORMATION_LOCK_RETRY_FLG = flags.DEFINE_integer(
    'transformation_lock_retry',
    int(
        secret_flag_utils.get_secret_or_env('TRANSFORMATION_LOCK_RETRY', '300')
    ),
    '[optional|default] Amount of time in seconds that pipeline should wait '
    'before retrying image ingestion when an image cannot be ingested due to '
    'another instance of the pipleine holding an transformation lock.',
)

OOF_INFERENCE_CONFIG_PATH_FLG = flags.DEFINE_string(
    'oof_inference_config_path',
    secret_flag_utils.get_secret_or_env(
        ingest_const.EnvVarNames.OOF_INFERENCE_CONFIG_PATH, ''
    ),
    '[optional|default] Path to OOF inference config to be included in Pub/Sub '
    'messages generated at the end of ingestion to trigger inference pipeline.',
)
OOF_LEGACY_INFERENCE_PIPELINE_FLG = flags.DEFINE_boolean(
    'oof_legacy_inference_pipeline',
    secret_flag_utils.get_bool_secret_or_env(
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
    secret_flag_utils.get_bool_secret_or_env(
        'REQUIRE_TYPE1_DICOM_TAG_METADATA_IS_DEFINED'
    ),
    '[optional|default] Require all type one tags defined in the metadata'
    ' schema have defined values; if not raise'
    ' MissingRequiredMetadataValueError exception and  fail ingestion.',
)

CREATE_NULL_TYPE2C_DICOM_TAG_IF_METADATA_IS_UNDEFINED_FLG = flags.DEFINE_boolean(
    'create_null_type2c_dicom_tag_if_metadata_if_undefined',
    secret_flag_utils.get_bool_secret_or_env(
        'CREATE_NULL_TYPE2C_DICOM_TAG_IF_METADATA_IS_UNDEFINED'
    ),
    '[optional|default] Undefined mandatory type2 tags are always initalized to'
    ' None. Flag enables automatic initialization of undefined type2c DICOM '
    ' tags to None.',
)

METADATA_PRIMARY_KEY_COLUMN_NAME_FLG = flags.DEFINE_string(
    'metadata_primary_key_column_name',
    secret_flag_utils.get_secret_or_env(
        'METADATA_PRIMARY_KEY_COLUMN_NAME', 'Bar Code Value'
    ),
    '[optional|default] Defines column name used as primary key for joining '
    'candidate imaging with BigQuery or CSV metadata.',
)

# Undefined Metadata default values

DEFAULT_ICCPROFILE_FLG = flags.DEFINE_enum_class(
    'default_iccprofile',
    get_value(
        env_var='DEFAULT_ICCPROFILE',
        default_value=DefaultIccProfile.SRGB.name,
    ),
    DefaultIccProfile,
    '[optional|default] ICC Profile to embedd in wsi imaging that does not '
    'provide an ICCColor profile.',
)

WSI_DICOM_EXTENDED_DEPTH_OF_FIELD_DEFAULT_VALUE_FLG = flags.DEFINE_enum_class(
    'wsi_dicom_extended_depth_of_field_default_value',
    get_value(
        env_var='WSI_DICOM_EXTENDED_DEPTH_OF_FIELD_DEFAULT_VALUE',
        default_value=WsiDicomExtendedDepthOfFieldDefault.NO.name,
    ),
    WsiDicomExtendedDepthOfFieldDefault,
    '[optional|default] Default metadata value for extended depth of field'
    ' DICOM tag metadata in VL Whole Slide Microscopy Images.',
)

WSI_DICOM_FOCUS_METHOD_DEFAULT_VALUE_FLG = flags.DEFINE_enum_class(
    'wsi_dicom_focus_method_default_value',
    get_value(
        env_var='WSI_DICOM_FOCUS_METHOD_DEFAULT_VALUE',
        default_value=WsiDicomFocusMethod.AUTO.name,
    ),
    WsiDicomFocusMethod,
    '[optional|default] Default metadata value for focus method DICOM tag'
    ' metadata in VL Whole Slide Microscopy Images.',
)

WSI_DICOM_SLICE_THICKNESS_DEFAULT_VALUE_FLG = flags.DEFINE_float(
    'wsi_dicom_slice_thickness_default_value',
    float(
        get_value(
            env_var='WSI_DICOM_SLICE_THICKNESS_DEFAULT_VALUE',
            default_value=12.0,
        )
    ),
    '[optional|default] Default metadata value for slice thickness (micrometer)'
    ' DICOM tag metadata in VL Whole Slide Microscopy Images. ',
)


# DICOM Store ingestion flags
DICOM_STORE_INGEST_GCS_URI_FLG = flags.DEFINE_string(
    'dicom_store_ingest_gcs_uri',
    secret_flag_utils.get_secret_or_env('DICOM_STORE_INGEST_GCS_URI', ''),
    '[optional|default] GCS URI for temporarily storing DICOM files when'
    ' ingesting from DICOM Store. Files are stored as backup before deletion'
    ' from DICOM Store.',
)

# Openslide metadata flags
ADD_OPENSLIDE_BACKGROUND_COLOR_METADATA_FLG = flags.DEFINE_boolean(
    'add_openslide_background_color_metadata',
    secret_flag_utils.get_bool_secret_or_env(
        'ADD_OPENSLIDE_BACKGROUND_COLOR_METADATA'
    ),
    '[optional|default] OpenSlide provides hooks to return WSI background color'
    ' however metadata retrieval and embedding is experimental and disabled by '
    'default. At time of writing none of the available openslide supported WSI '
    'imaging encoded this metadata.',
)

ADD_OPENSLIDE_TOTAL_PIXEL_MATRIX_ORIGIN_SEQ_FLG = flags.DEFINE_boolean(
    'add_openslide_total_pixel_matrix_origin_seq',
    secret_flag_utils.get_bool_secret_or_env(
        'ADD_OPENSLIDE_TOTAL_PIXEL_MATRIX_ORIGIN_SEQ'
    ),
    '[optional|default] Openslide provides hooks to return the slide '
    'coordinates of the imaged region however metadata retrieval and embedding '
    'is experimental and disabled by default. At time of writing none of the '
    'available openslide supported WSI imaging encoded this metadata.',
)

# WSI specific flags
INGESTION_PYRAMID_LAYER_GENERATION_CONFIG_PATH_FLG = flags.DEFINE_string(
    'ingestion_pyramid_layer_generation_config_path',
    secret_flag_utils.get_secret_or_env(
        'INGESTION_PYRAMID_LAYER_GENERATION_CONFIG_PATH', ''
    ),
    '[optional|default] Path to *.YAML or *.JSON file which defines WSI'
    ' downsampling pyramid layers to generate.',
)
WSI2DCM_DICOM_FRAME_HEIGHT_FLG = flags.DEFINE_integer(
    'wsi2dcm_dicom_frame_height',
    secret_flag_utils.get_secret_or_env('WSI2DCM_DICOM_FRAME_HEIGHT', '256'),
    '[optional|default] wsi2dcm dicom frame height.',
)
WSI2DCM_DICOM_FRAME_WIDTH_FLG = flags.DEFINE_integer(
    'wsi2dcm_dicom_frame_width',
    secret_flag_utils.get_secret_or_env('WSI2DCM_DICOM_FRAME_WIDTH', '256'),
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
    secret_flag_utils.get_secret_or_env(
        'WSI2DCM_JPEG_COMPRESSION_QUALITY', '95'
    ),
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
    'init_series_instance_uid_from_metadata',
    secret_flag_utils.get_bool_secret_or_env(
        'INIT_SERIES_INSTANCE_UID_FROM_METADATA'
    ),
    '[optional|default] Initialize series instance UID from metadata.'
    ' Recommended setting = False. If true disables auto-reingestion of rescans'
    ' into new series and correct handling of rescans would need to be handled'
    ' outside of DPAS ingestion.',
)

# Flat image specific flags
FLAT_IMAGES_VL_MICROSCOPIC_IMAGE_IOD_FLG = flags.DEFINE_boolean(
    'flat_images_vl_microscopic_image_iod',
    secret_flag_utils.get_bool_secret_or_env(
        'FLAT_IMAGES_VL_MICROSCOPIC_IMAGE_IOD'
    ),
    '[optional|default] Whether to use VL Microscopic Image IOD for ingestion'
    ' of flat images without slide coordinates. If unset, all images will use'
    ' VL Slide-Coordinates Microscopic Image IOD.',
)

# DICOM Store URL flags
DICOMWEB_URL_FLG = flags.DEFINE_string(
    'dicomweb_url',
    secret_flag_utils.get_secret_or_env(
        ingest_const.EnvVarNames.DICOMWEB_URL, ''
    ),
    '[required|default,oof] DICOM Store to upload primary imaging into.',
)
OOF_DICOMWEB_BASE_URL_FLG = flags.DEFINE_string(
    'oof_dicomweb_base_url',
    secret_flag_utils.get_secret_or_env(
        ingest_const.EnvVarNames.OOF_DICOMWEB_BASE_URL, ''
    ),
    '[required|oof] DICOM Store to upload OOF imaging into.',
)
DICOM_STORE_TO_CLEAN_FLG = flags.DEFINE_string(
    'dicom_store_to_clean',
    secret_flag_utils.get_secret_or_env(
        ingest_const.EnvVarNames.DICOM_STORE_TO_CLEAN, ''
    ),
    '[optional|oof] DICOM Store to cleanup instances used for OOF pipeline'
    ' after it completes.',
)

# Barcode flags
ZXING_CLI_FLG = flags.DEFINE_string(
    'zxing_cli',
    secret_flag_utils.get_secret_or_env('ZXING_CLI', '/zxing/cpp/build/zxing'),
    '[optional|default] Path to zxing command line tool.',
)
DISABLE_CLOUD_VISION_BARCODE_SEG_FLG = flags.DEFINE_boolean(
    'testing_disable_cloudvision',
    secret_flag_utils.get_bool_secret_or_env(
        'DISABLE_CLOUD_VISION_BARCODE_SEG',
        not (
            secret_flag_utils.get_bool_secret_or_env(
                'ENABLE_CLOUD_VISION_BARCODE_SEGMENTATION', True
            )
        ),
    ),
    '[private] Unit testing flag. Disables cloud vision.',
)

DISABLE_BARCODE_DECODER_FLG = flags.DEFINE_boolean(
    'disable_barcode_decoder',
    secret_flag_utils.get_bool_secret_or_env(
        'DISABLE_BARCODE_DECODER',
        not (
            secret_flag_utils.get_bool_secret_or_env(
                'ENABLE_BARCODE_DECODER', True
            )
        ),
    ),
    '[optional|default] Disable barcode decoder.',
)
