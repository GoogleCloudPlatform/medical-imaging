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
"""Flags used in Digital Pathology Proxy Server."""
import os
import sys

from absl import flags

from pathology.dicom_proxy import proxy_const
from pathology.shared_libs.flags import secret_flag_utils
from pathology.shared_libs.iap_auth_lib import auth

# Digital Pathology Proxy Server Flask Config.

# Not a flag, requires initialization prior to flag init.
PROXY_SERVER_URL_PATH_PREFIX = secret_flag_utils.get_secret_or_env(
    'URL_PATH_PREFIX', '/tile'
)

GUNICORN_WORKERS_FLG = flags.DEFINE_integer(
    'gunicorn_workers',
    int(
        secret_flag_utils.get_secret_or_env(
            'GUNICORN_WORKERS', int(os.cpu_count() * 3.4)
        )
    ),
    'Number of processes GUnicorn should launch',
)

GUNICORN_THREADS_FLG = flags.DEFINE_integer(
    'gunicorn_threads',
    int(secret_flag_utils.get_secret_or_env('GUNICORN_THREADS', 5)),
    'Number of threads each GUnicorn processes should launch',
)

API_PORT_FLG = flags.DEFINE_integer('port', 8080, 'port to listen on')

ORIGINS_FLG = flags.DEFINE_multi_string(
    'origins',
    secret_flag_utils.get_secret_or_env(
        'ORIGINS', 'http://localhost:5432'
    ),  # Default host:port for local DPAS web host.
    'Sites to allow requests from for CORS.',
)

HEALTH_CHECK_LOG_INTERVAL_FLG = flags.DEFINE_integer(
    'health_check_log_interval',
    int(secret_flag_utils.get_secret_or_env('HEALTH_CHECK_LOG_INTERVAL', 100)),
    'Interval in seconds to log healthcheck.',
)


KEYWORDS_TO_MASK_FROM_CONNECTION_HEADER_LOG_FLG = flags.DEFINE_list(
    'keywords_to_mask_from_connection_header_log',
    secret_flag_utils.get_secret_or_env(
        'KEYWORDS_TO_MASK_FROM_CONNECTION_HEADER_LOG',
        ','.join([
            proxy_const.HeaderKeywords.IAP_JWT_HEADER,
            proxy_const.HeaderKeywords.COOKIE,
            proxy_const.HeaderKeywords.AUTH_HEADER_KEY,
        ]),
    ).split(','),
    'List of keywords to strip from header logs.',
)

DEFAULT_DICOM_STORE_API_VERSION = secret_flag_utils.get_secret_or_env(
    'DEFAULT_DICOM_STORE_API_VERSION', 'v1'
)

# IAP Authentication.

ENABLE_APPLICATION_DEFAULT_CREDENTIALS_FLG = flags.DEFINE_boolean(
    'enable_app_default_credentials',
    secret_flag_utils.get_bool_secret_or_env(
        'ENABLE_APPLICATION_DEFAULT_CREDENTIALS'
    ),
    (
        'Enables application default credentials used to debug should '
        ' be False except for testing.'
    ),
)

VALIDATE_IAP_FLG = auth.VALIDATE_IAP_FLG

# ICC Profile Color Conversion.

THIRD_PARTY_ICC_PROFILE_DICRECTORY_FLG = flags.DEFINE_string(
    'third_party_icc_profile_directory',
    secret_flag_utils.get_secret_or_env(
        'THIRD_PARTY_ICC_PROFILE_DIRECTORY', ''
    ),
    'Directory that contains third party icc profiles.',
)

DISABLE_ICC_PROFILE_CORRECTION_FLG = flags.DEFINE_boolean(
    'disable_icc_profile_color_correction',
    secret_flag_utils.get_bool_secret_or_env('DISABLE_ICC_PROFILE_CORRECTION'),
    'Disable iccprofile color correction.',
)

# ICC Profile Cache Config.

# Maximum number of icc_profile transforms cached in each process. Number
# should be small, total size per machine = cache size * process number
# Using env variable directly to allow it to be used in decorator.
MAX_SIZE_ICC_PROFILE_TRANSFORM_PROCESS_CACHE_FLG = int(
    secret_flag_utils.get_secret_or_env(
        'MAX_SIZE_ICC_PROFILE_TRANSFORM_THREAD_CACHE', 5
    )
)

ICC_PROFILE_REDIS_CACHE_TTL_FLG = flags.DEFINE_integer(
    'icc_profile_redis_cache_ttl',
    int(
        secret_flag_utils.get_secret_or_env('ICC_PROFILE_REDIS_CACHE_TTL', '-1')
    ),
    'TTL (sec) of ICC_PROFILE redis cache. Value < 0 disables ttl.',
)

# Metadata Cache Config.

DISABLE_DICOM_METADATA_CACHING_FLG = flags.DEFINE_boolean(
    'disable_dicom_metadata_caching',
    secret_flag_utils.get_bool_secret_or_env('DISABLE_DICOM_METADATA_CACHING'),
    'Disable DICOM metadata caching.',
)

USER_LEVEL_METADATA_TTL_FLG = flags.DEFINE_integer(
    'user_level_metadata_cache_ttl_sec',
    int(
        secret_flag_utils.get_secret_or_env(
            'USER_LEVEL_METADATA_TTL_SEC', '600'
        )
    ),
    'TTL (sec) of DICOM metadata cache.',
)

# Bearer Token Email Caching

SIZE_OF_BEARER_TOKEN_EMAIL_CACHE_FLG = flags.DEFINE_integer(
    'size_of_bearer_token_email_cache',
    int(
        secret_flag_utils.get_secret_or_env(
            'BEARER_TOKEN_EMAIL_CACHE_SIZE', 1000
        )
    ),
    'Maximum size of bearer token to email cache.',
)


# DICOM Store Instance Downloads.

DICOM_INSTANCE_DOWNLOAD_STREAMING_CHUNKSIZE_FLG = flags.DEFINE_integer(
    'streaming_chunksize',
    max(
        int(
            secret_flag_utils.get_secret_or_env('STREAMING_CHUNKSIZE', '512000')
        ),
        1,
    ),
    'Size in bytes of streaming chunks.',
)

# Client side image caching.

CACHE_CONTROL_TTL_RENDERED_FRAMES_FLG = flags.DEFINE_integer(
    'cache_control_ttl_rendered_frames',
    int(
        secret_flag_utils.get_secret_or_env(
            'CACHE_CONTROL_TTL_RENDERED_FRAMES', 3600
        )
    ),
    'Client side cache control control ttl for rendered single frames.',
)

# Image Generation.

JPEG_ENCODER_FLG = flags.DEFINE_string(
    'jpeg_encoder_flg',
    secret_flag_utils.get_secret_or_env('JPEG_ENCODER', 'CV2'),
    'CV2 = OpenCV (Default); PIL=PIL.Image',
)

MAX_PARALLEL_FRAME_DOWNLOADS_FLG = flags.DEFINE_integer(
    'max_thread_pool_size',
    int(secret_flag_utils.get_secret_or_env('MAX_THREAD_POOL_SIZE', 20)),
    'Max size of frame retrieval thread pool.',
)

MAX_NUMBER_OF_FRAMES_PER_REQUEST_FLG = flags.DEFINE_integer(
    'max_number_of_frame_per_request',
    min(
        int(
            secret_flag_utils.get_secret_or_env(
                'MAX_NUMBER_OF_FRAMES_PER_REQUEST', 100
            )
        ),
        int(
            secret_flag_utils.get_secret_or_env(
                'MAX_MINI_BATCH_FRAME_REQUEST_SIZE', 500
            )
        ),
    ),
    'Maximum number of frames which can be requested at once.',
)

MAX_MINI_BATCH_FRAME_REQUEST_SIZE_FLG = flags.DEFINE_integer(
    'max_mini_batch_frame_request_size',
    int(
        secret_flag_utils.get_secret_or_env(
            'MAX_MINI_BATCH_FRAME_REQUEST_SIZE', 500
        )
    ),
    'Maximum number of frames which can be requested in a mini-batch.',
)

# Preemptive Frame Cache Loader.

DISABLE_PREEMPTIVE_INSTANCE_FRAME_CACHE_FLG = flags.DEFINE_boolean(
    'disable_preemptive_instance_frame_cache',
    secret_flag_utils.get_bool_secret_or_env(
        'DISABLE_PREEMPTIVE_INSTANCE_FRAME_CACHE'
    ),
    'Disable preemptive instance frame cache.',
)

PREEMPTIVE_INSTANCE_CACHE_MAX_INSTANCE_FRAME_NUMBER_FLG = flags.DEFINE_integer(
    'preemptive_instance_cache_max_instance_frame_number',
    int(
        secret_flag_utils.get_secret_or_env(
            'PREEMPTIVE_INSTANCE_CACHE_MAX_INSTANCE_FRAME_NUMBER', 500000
        )
    ),
    'Maximum number of frames in an instance.',
)

PREEMPTIVE_INSTANCE_CACHE_BLOCK_PIXEL_DIM_FLG = flags.DEFINE_integer(
    'preemptive_instance_cache_block_pixel_dim',
    int(
        secret_flag_utils.get_secret_or_env(
            'PREEMPTIVE_INSTANCE_CACHE_PIXEL_DIM', 8000
        )
    ),
    'Dimension in pixels of large cache block premptively loaded to cache.',
)

PREEMPTIVE_DISPLAY_CACHE_MIN_INSTANCE_FRAME_NUMBER_FLG = flags.DEFINE_integer(
    'preemptive_display_cache_min_instance_frame_number',
    int(
        secret_flag_utils.get_secret_or_env(
            'PREEMPTIVE_DISPLAY_CACHE_MIN_INSTANCE_FRAME_NUMBER', 10000
        )
    ),
    'Minimum number of frames in an instance required for to use display'
    ' cache.',
)

PREEMPTIVE_DISPLAY_CACHE_PIXEL_DIM_FLG = flags.DEFINE_integer(
    'preemptive_display_cache_pixel_dim',
    int(
        secret_flag_utils.get_secret_or_env(
            'PREEMPTIVE_DISPLAY_CACHE_PIXEL_DIM', 3000
        )
    ),
    'Number of pixels loaded on x and y dim by display cache.',
)

ENABLE_PREEMPTIVE_WHOLEINSTANCE_CACHING_FLG = flags.DEFINE_boolean(
    'enable_preemptive_wholeinstance_caching',
    secret_flag_utils.get_bool_secret_or_env(
        'ENABLE_PREEMPTIVE_WHOLEINSTANCE_CACHING', True
    ),
    'If True, enables caching whole instance at once. If False, cache frame'
    ' block size block of frames in instance at once.',
)

MAX_WHOLE_INSTANCE_CACHING_CPU_LOAD_FLG = flags.DEFINE_integer(
    'max_whole_instance_caching_cpu_load',
    int(
        secret_flag_utils.get_secret_or_env(
            'MAX_WHOLE_INSTANCE_CACHING_CPU_LOAD', 80
        )
    ),
    'Maximum CPU load to use whole instance caching.',
)
MAX_WHOLE_INSTANCE_CACHING_MEMORY_PRECENT_LOAD_FLG = flags.DEFINE_integer(
    'max_whole_instance_caching_memory_precent_load',
    int(
        secret_flag_utils.get_secret_or_env(
            'MAX_WHOLE_INSTANCE_CACHING_MEMORY_PRECENT_LOAD', 70
        )
    ),
    'Maximum precentage of memory load for whole instance caching.',
)
PREEMPTIVE_INSTANCE_CACHE_MAX_THREADS_FLG = flags.DEFINE_integer(
    'preemptive_instance_cache_max_threads',
    int(
        secret_flag_utils.get_secret_or_env(
            'PREEMPTIVE_INSTANCE_CACHE_MAX_THREADS', 1
        )
    ),
    (
        'Maximum number of instance frame cache ingestion threads to run per'
        ' GUnicorn worker (process). Server hosted on C2N30 should default to'
        ' ~26 workers enabling by default 26 parallel cache loads. Keeping'
        ' number of cache loads per worker small (e.g. 1) will improve server'
        ' performance by limiting total thread load per-worker.  Total number'
        ' of threads per worker =  gunicorn worker threads + cache loading'
        ' threads + additional.'
    ),
)

PREEMPTIVE_INSTANCE_CACHE_MIN_INSTANCE_FRAME_NUMBER_FLG = flags.DEFINE_integer(
    'preemptive_instance_cache_min_instance_frame_number',
    int(
        secret_flag_utils.get_secret_or_env(
            'PREEMPTIVE_INSTANCE_CACHE_MIN_INSTANCE_FRAME_NUMBER', 10
        )
    ),
    (
        'Min number of frames in an instance required to start preemptive frame'
        ' caching.'
    ),
)

PREEMPTIVE_WHOLEINSTANCE_RECACHING_TTL_FLG = flags.DEFINE_integer(
    'preemptive_wholeinstance_recaching_ttl',
    int(
        secret_flag_utils.get_secret_or_env(
            'PREEMPTIVE_WHOLEINSTANCE_RECACHING_TTL', 300
        )
    ),
    'Time in seconds before an instance frame caching request can be repeated.',
)

FRAME_CACHE_TTL_FLG = flags.DEFINE_integer(
    'frame_cache_ttl',
    int(secret_flag_utils.get_secret_or_env('FRAME_CACHE_TTL', 3600)),
    'Time in seconds frames are stored in redis cache. Set to value < 0 to '
    'disable ttl and keep frames in cache until removed via other mechanisms '
    '(e.g. LRU).',
)

# Redis Cross Process Cache Flags.

REDIS_CACHE_HOST_IP_FLG = flags.DEFINE_string(
    'redis_cache_host_ip',
    secret_flag_utils.get_secret_or_env('REDIS_CACHE_HOST_IP', '127.0.0.1'),
    'Redis cache host IP',
)

REDIS_CACHE_HOST_PORT_FLG = flags.DEFINE_integer(
    'redis_cache_host_port',
    int(secret_flag_utils.get_secret_or_env('REDIS_CACHE_HOST_PORT', '6379')),
    'Redis cache host port',
)

REDIS_CACHE_DB_FLG = flags.DEFINE_integer(
    'redis_cache_host_db',
    int(secret_flag_utils.get_secret_or_env('REDIS_CACHE_HOST_DB', '0')),
    'Redis cache host database',
)

REDIS_CACHE_MAXMEMORY_FLG = flags.DEFINE_integer(
    'redis_cache_maxmemory',
    int(secret_flag_utils.get_secret_or_env('REDIS_CACHE_MAXMEMORY', -1)),
    'Max memory in gigabytes to allocate to local redis instance.',
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

REDIS_TLS_CERTIFICATE_AUTHORITY_GCS_URI_FLG = flags.DEFINE_string(
    'redis_tls_certificate_authority_gcs_uri',
    secret_flag_utils.get_secret_or_env(
        'REDIS_TLS_CERTIFICATE_AUTHORITY_GCS_URI', ''
    ),
    'GS uri for blob in buket which holds Redis Certificate Authority for TLS.',
)

REDIS_MIN_CACHE_FLUSH_INTERVAL_FLG = flags.DEFINE_integer(
    'redis_min_cache_flush_interval',
    int(
        secret_flag_utils.get_secret_or_env(
            'REDIS_MIN_CACHE_FLUSH_INTERVAL', 600
        )
    ),
    'Minimum time in sec between Redis cache flush operations.',
)

DISABLE_REDIS_INSTANCE_PROCESS_CACHE_IN_DEBUG_FLG = flags.DEFINE_boolean(
    'disable_redis_instance_process_cache_in_debug',
    bool('UNITTEST_ON_FORGE' in os.environ or 'unittest' in sys.modules),
    'Automatically disable redis instance cache when debugging.',
)

# Cached Sparse Dicom Per frame functional groups metadata ttl

SPARSE_DICOM_PER_FRAME_FUNCTIONAL_GROUPS_SEQUENCE_METADATA_CACHE_TTL_FLG = flags.DEFINE_integer(
    'sparse_dicom_per_frame_functional_groups_sequence_metadata_cache_ttl',
    int(
        secret_flag_utils.get_secret_or_env(
            'SPARSE_DICOM_PER_FRAME_FUNCTIONAL_GROUPS_SEQUENCE_METADATA_CACHE_TTL',
            3600,
        )
    ),
    'Time to live (sec) for cached sparse dicom per frame functional group'
    ' sequence metadata.',
)

# Annotations Flags.

DICOM_ANNOTATIONS_STORE_ALLOW_LIST = flags.DEFINE_list(
    'dicom_annotations_store_allow_list',
    secret_flag_utils.get_secret_or_env(
        'DICOM_ANNOTATIONS_STORE_ALLOW_LIST', ''
    ).split(','),
    'List of allowed base dicom url addresses that can be written to for'
    ' annotations. projects/.../locations/.../dicomWeb',
)

ENABLE_ANNOTATIONS_ENDPOINT_FLG = flags.DEFINE_boolean(
    'enable_annotations_endpoint',
    secret_flag_utils.get_bool_secret_or_env('ENABLE_ANNOTATIONS_ENDPOINT'),
    'Enable annotations storage and deletion endpoint.',
)

ENABLE_DEBUG_FUNCTION_TIMING_FLG = flags.DEFINE_boolean(
    'enable_debug_function_timing',
    secret_flag_utils.get_bool_secret_or_env('ENABLE_DEBUG_FUNCTION_TIMING'),
    'Enable logging of timing for decorated functions and methods.',
)

# ENABLE_AUGMENTED_STUDY_SEARCH_FLG adds the following tags to the study
# tag search response:
# https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_10.6.3.3.html
# Modalities in Study     (0008,0061)
# Retrieve URL    (0008,1190)
# Number of Study Related Series  (0020,1206)
# Number of Study Related Instances       (0020,1208)
# Instance Availability 	(0008,0056)
ENABLE_AUGMENTED_STUDY_SEARCH_FLG = flags.DEFINE_boolean(
    'enable_augmented_study_search',
    secret_flag_utils.get_bool_secret_or_env('ENABLE_AUGMENTED_STUDY_SEARCH'),
    'Enable the return of augmented metadata for study search.',
)

# ENABLE_AUGMENTED_SERIES_SEARCH_FLG adds the following tags to the series
# tag search response:
# https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_10.6.3.3.2.html
# Retrieve URL    (0008,1190)
# Number of Study Related Instances       (0020,1208)
ENABLE_AUGMENTED_SERIES_SEARCH_FLG = flags.DEFINE_boolean(
    'enable_augmented_series_search',
    secret_flag_utils.get_bool_secret_or_env('ENABLE_AUGMENTED_SERIES_SEARCH'),
    'Enable the return of augmented metadata for series search.',
)

# ENABLE_AUGMENTED_INSTANCE_SEARCH_FLG adds the following tags to the instance
# tag search response:
# https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_10.6.3.3.3.html
# Retrieve URL    (0008,1190)
# Instance Availability 	(0008,0056)
ENABLE_AUGMENTED_INSTANCE_SEARCH_FLG = flags.DEFINE_boolean(
    'enable_augmented_instance_search',
    secret_flag_utils.get_bool_secret_or_env(
        'ENABLE_AUGMENTED_INSTANCE_SEARCH', True
    ),
    'Enable the return of augmented metadata for instance search.',
)

MAX_AUGMENTED_METADATA_DOWNLOAD_THREAD_COUNT_FLG = flags.DEFINE_integer(
    'max_augmented_metadata_thread_count',
    int(
        secret_flag_utils.get_secret_or_env(
            'MAX_AUGMENTED_METADATA_DOWNLOAD_THREAD_COUNT', '4'
        )
    ),
    'Maximum number of threads used to download instance tag for binary tag'
    ' metadata augmentation.',
)

MAX_AUGMENTED_METADATA_DOWNLOAD_SIZE_FLG = flags.DEFINE_integer(
    'max_augmented_metadata_download_size',
    int(
        secret_flag_utils.get_secret_or_env(
            'MAX_AUGMENTED_METADATA_DOWNLOAD_SIZE', str(3 * pow(2, 30))
        )
    ),
    'Maximum size of augmented metadata requests.',
)

DEFAULT_ANNOTATOR_INSTITUTION_FLG = flags.DEFINE_string(
    'default_annotator_institution',
    secret_flag_utils.get_secret_or_env(
        'DEFAULT_ANNOTATOR_INSTITUTION', 'default_institution'
    ),
    'Default annotator institution.',
)

# Bulk DATA URI
BULK_DATA_URI_PROTOCOL_FLG = flags.DEFINE_string(
    'bulkdata_uri_protocol',
    secret_flag_utils.get_secret_or_env('BULK_DATA_URI_PROTOCOL', 'https'),
    'Protocal to return in bulkdata responses.',
)

PROXY_DICOM_STORE_BULK_DATA_FLG = flags.DEFINE_boolean(
    'proxy_dicom_store_bulk_data',
    secret_flag_utils.get_bool_secret_or_env('PROXY_DICOM_STORE_BULK_DATA'),
    'If true rewrites DICOM store metadata to proxy bulkdata requests through'
    ' proxy.',
)

BULK_DATA_PROXY_URL_FLG = flags.DEFINE_string(
    'bulk_data_proxy_url',
    secret_flag_utils.get_secret_or_env('BULK_DATA_PROXY_URL', ''),
    'If defined sets base url for proxy bulk data responses',
)

ENABLE_ANNOTATION_BULKDATA_METADATA_PATCH = flags.DEFINE_boolean(
    'enable_annotation_bulkdata_metadata_patch',
    secret_flag_utils.get_bool_secret_or_env(
        'ENABLE_ANNOTATION_BULKDATA_METADATA_PATCH'
    ),
    'If true downloads dicom metadata for annotations and merges it with store'
    ' returned metadata to fix store bug which results in store not returning'
    ' short binary tags.',
)
