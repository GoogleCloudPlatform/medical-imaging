# Copyright 2024 Google LLC
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
"""Starts Transformation Docker Container."""
import argparse
from concurrent import futures
import contextlib
import dataclasses
import json
import logging
import os
import pathlib
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Any, List, Mapping, Optional, TextIO


_LOCALHOST_SPECIFIC_ENVS = [
    'GOOGLE_APPLICATION_CREDENTIALS',
    'LOCAL_UID',
    'LOCAL_GID',
    'POLL_IMAGE_INGESTION_DIR',
    'GOOGLE_CLOUD_PROJECT',
    'ENABLE_CLOUD_OPS_LOGGING',
    'LOCALHOST_DICOM_STORE',
]

_LINUX_ADC_PATH = '.config/gcloud/application_default_credentials.json'
_WINDOWS_ADC_PATH = r'%APPDATA%\gcloud\application_default_credentials.json'


@dataclasses.dataclass(frozen=True)
class _Directory:
  path: str
  is_temp: bool


class _StartLocalHostError(Exception):
  pass


def _str_to_bool(val: str) -> bool:
  """Converts a string representation of truth.

    True values are 'y', 'yes', 't', 'true', 'on', and '1';
    False values are 'n', 'no', 'f', 'false', 'off', and '0'.

  Args:
    val: String to convert to bool.

  Returns:
    Boolean result

  Raises:
    _StartLocalHostError: if val is anything else.
  """
  val = val.strip().lower()
  if val in ('y', 'yes', 't', 'true', 'on', '1'):
    return True
  if val in ('n', 'no', 'f', 'false', 'off', '0'):
    return False
  raise _StartLocalHostError(f'invalid truth value {str(val)}')


def _discover_adc_credentials() -> str:
  """Returns application default credentials (ADC) for the current user.

  Looks for user ADC credentials in known locations.
  https://cloud.google.com/docs/authentication/application-default-credentials#personal

  Returns:
    Path to application default credentials or empty string if not found.
  """
  if platform.system() == 'Windows':
    test_path = os.path.expandvars(_WINDOWS_ADC_PATH)
  else:
    try:
      test_path = os.path.join(pathlib.Path.home(), _LINUX_ADC_PATH)
    except RuntimeError:
      test_path = ''
  if test_path and os.path.isfile(test_path):
    logging.info('Using ADC credentials: %s', test_path)
    return test_path
  logging.info('Application default credentials (ADC) are undefined.')
  return ''


def _get_arg_parser() -> argparse.Namespace:
  """Parses commandline arguments."""
  arg_parser = argparse.ArgumentParser(
      description='Local Transformation Pipeline Configuration.'
  )
  # ----------------------------------------------------------------------------
  # General Container Authentication Configuration
  # ----------------------------------------------------------------------------
  arg_parser.add_argument('-H', action='help')

  arg_parser.add_argument(
      '-log',
      default=os.getenv('LOG', ''),
      help='File path to write transformation pipeline output to.',
  )

  arg_parser.add_argument(
      '-user_id',
      default=os.getenv('USER_ID', str(os.getuid())),
      help='Container user id; defaults to user id of the current user.',
  )

  arg_parser.add_argument(
      '-group_id',
      default=os.getenv('GROUP_ID', str(os.getgid())),
      help='Container group id; defaults to group id of the current user.',
  )

  arg_parser.add_argument(
      '-google_application_credentials',
      default=os.getenv(
          'GOOGLE_APPLICATION_CREDENTIALS', _discover_adc_credentials()
      ),
      help=(
          'File path to user credential json file that container will user to'
          ' authenticate to GCP. Only required if GCP services are used.'
      ),
  )

  arg_parser.add_argument(
      '-google_cloud_project',
      default=os.getenv('GOOGLE_CLOUD_PROJECT', ''),
      help=(
          'Google Cloud project hosting used DICOM store and other GCP'
          ' services. Only needed if GCP services are used.'
      ),
  )

  # ----------------------------------------------------------------------------
  # Input, Output, and DICOM store configurations.
  # ----------------------------------------------------------------------------
  arg_parser.add_argument(
      '-input_images',
      nargs='*',
      default=json.loads(os.getenv('INPUT_IMAGES', '[]')),
      help=(
          'Paths to image files to ingest. Path may describe either file on the'
          ' local file system or a file within a cloud bucket (gs style path).'
      ),
  )

  arg_parser.add_argument(
      '-poll',
      default=os.getenv('POLL_IMAGE_INGESTION_DIR', 'False'),
      type=_str_to_bool,
      help=(
          'Poll image ingestion directory for new images, container run'
          ' indefinitely.'
      ),
  )

  arg_parser.add_argument(
      '-metadata_dir',
      default=os.getenv('METADATA_DIR', ''),
      help='Local directory metadata is ingested from.',
  )

  arg_parser.add_argument(
      '-image_ingestion_dir',
      default=os.getenv('IMAGE_INGESTION_DIR', ''),
      help='Local directory containing imaging to be ingested.',
  )

  arg_parser.add_argument(
      '-processed_image_dir',
      default=os.getenv('PROCESSED_IMAGE_DIR', ''),
      help='Local directory images are copied/moved to after being processed.',
  )

  arg_parser.add_argument(
      '-dicom_store',
      default=os.getenv('DICOM_STORE'),
      help='Local directory or GCP DICOM store to write DICOM files.',
  )

  arg_parser.add_argument(
      '-pyramid_generation_config_path',
      default=os.getenv('PYRAMID_LAYER_GENERATION_CONFIG_PATH', ''),
      help='Path to file defining pyramid generation config.',
  )

  # ----------------------------------------------------------------------------
  # Metadata Input Configuration
  # ----------------------------------------------------------------------------
  arg_parser.add_argument(
      '-metadata_free_ingestion',
      default=os.getenv('METADATA_FREE_INGESTION', 'True'),
      type=_str_to_bool,
      help=(
          'If True pipeline will ingest images without requiring metadata. Each'
          ' image will be ingested into unique study and series instance uid.'
          ' DICOM PatientID will be set to the ingested file name.'
      ),
  )

  arg_parser.add_argument(
      '-create_missing_study_instance_uid',
      default=os.getenv('CREATE_MISSING_STUDY_INSTANCE_UID', 'True'),
      type=_str_to_bool,
      help=(
          'If CREATE_MISSING_STUDY_INSTANCE_UID="True" then the pipeline will'
          ' attempt to create missing Study Instance UID using slide accession'
          ' number. Requires Slide Accession numbers have a 1:1 mapping with'
          ' StudyInstanceUID.'
      ),
  )

  arg_parser.add_argument(
      '-dicom_study_instance_uid_source',
      default=os.getenv('DICOM_STUDY_INSTANCE_UID_SOURCE', 'DICOM'),
      choices=['DICOM', 'METADATA'],
      help=(
          'Defines the source of the DICOM Study Instance UID for image'
          ' transformation triggered using DICOM imaging. DICOM: Sets the study'
          ' instance uid in generated imaging to the value encoded in the'
          ' source imaging. METADATA: Sets the study instance uid in generated'
          ' imaging to the value encoded in the source imaging metadata.'
      ),
  )

  arg_parser.add_argument(
      '-whole_filename_metadata_primary_key',
      default=os.getenv('WHOLE_FILENAME_METADATA_PRIMARY_KEY', 'True'),
      type=_str_to_bool,
      help=(
          'If True then the whole file name will be tested as candidate'
          ' metadata primary key.'
      ),
  )

  arg_parser.add_argument(
      '-include_upload_path_in_whole_filename_metadata_primary_key',
      default=os.getenv(
          'INCLUDE_UPLOAD_PATH_IN_WHOLE_FILENAME_METADATA_PRIMARY_KEY', 'False'
      ),
      type=_str_to_bool,
      help=(
          'If true and file is placed within a sub-directory in the image'
          ' ingestion bucket then whole filename metadata primary key will'
          ' include the sub-directorys. If false the the metadata primary key'
          ' is just the base portion of the filename.'
      ),
  )

  arg_parser.add_argument(
      '-filename_metadata_primary_key_split_str',
      default=os.getenv('FILENAME_METADATA_PRIMARY_KEY_SPLIT_STR', '_'),
      help=(
          'Character or string that file name will split on to identify'
          ' candidate slide metadata primary keys.'
      ),
  )

  arg_parser.add_argument(
      '-metadata_primary_key_regex',
      default=os.getenv(
          'METADATA_PRIMARY_KEY_REGEX',
          '^[a-zA-Z0-9]+-[a-zA-Z0-9]+(-[a-zA-Z0-9]+)+',
      ),
      help=(
          'Regular expression used to validate candidate metadata primary keys'
          ' in filename. Whole file names are not tested against the regular'
          ' expression. Default regular expression matches 3 or more hyphen'
          ' separated alpha-numeric blocks, e.g. SR-21-2 and SR-21-2-B1-5.'
      ),
  )

  arg_parser.add_argument(
      '-metadata_primary_key_column_name',
      default=os.getenv('METADATA_PRIMARY_KEY_COLUMN_NAME', 'Bar Code Value'),
      help=(
          'Column name used as primary key for joining  BigQuery or CSV'
          ' metadata with imaging.'
      ),
  )

  arg_parser.add_argument(
      '-barcode_decoder',
      default=os.getenv('BARCODE_DECODER', 'True'),
      type=_str_to_bool,
      help=(
          'If the metadata primary key cannot be identified using other'
          ' mechanisms attempts to identify key by decoding barcodes placed in'
          ' slide label imaging.'
      ),
  )

  arg_parser.add_argument(
      '-cloud_vision_barcode_segmentation',
      default=os.getenv('CLOUD_VISION_BARCODE_SEGMENTATION', 'False'),
      type=_str_to_bool,
      help=(
          'Use cloud vision barcode segmentation to improve barcode decoding.'
          ' Requires GCP connectivitiy. mechanisms attempts to identify key by'
          ' decoding barcodes placed in slide label imaging.'
      ),
  )

  arg_parser.add_argument(
      '-big_query',
      default=os.getenv('BIG_QUERY', ''),
      help=(
          'Sets Big Query as the source for slide metadata. Requires GCP'
          ' connectivity To configure set value to: '
          ' "project_id.dataset_id.table_name" of table holding metadata.'
          ' Disabled if initalized to "". If undefined, slide metadata defined'
          ' by placing CSV files in metadata ingestion directory.'
      ),
  )

  # ----------------------------------------------------------------------------
  # Metadata Validation Configuration
  # ----------------------------------------------------------------------------

  arg_parser.add_argument(
      '-metadata_uid_validation',
      default=os.getenv('METADATA_UID_VALIDATION', 'LOG_WARNING'),
      choices=['NONE', 'LOG_WARNING', 'ERROR'],
      help=(
          'Test and optionally error if UID values defined in metadata are'
          ' formatted incorrectly.'
          ' https://dicom.nema.org/dicom/2013/output/chtml/part05/chapter_9.html'
      ),
  )

  arg_parser.add_argument(
      '-metadata_tag_length_validation',
      default=os.getenv('METADATA_TAG_LENGTH_VALIDATION', 'LOG_WARNING'),
      choices=['NONE', 'LOG_WARNING', 'LOG_WARNING_AND_CLIP', 'ERROR'],
      help=(
          'Test and optionally error if values defined in metadata exceed dicom'
          ' standard length limits.'
          ' https://dicom.nema.org/dicom/2013/output/chtml/part05/sect_6.2.html'
      ),
  )

  arg_parser.add_argument(
      '-require_type1_dicom_tag_metadata_are_defined',
      default=os.getenv(
          'REQUIRE_TYPE1_DICOM_TAG_METADATA_ARE_DEFINED', 'False'
      ),
      type=_str_to_bool,
      help=(
          'Require all type one tags defined in the metadata schema have'
          ' defined values; if not raise MissingRequiredMetadataValueError'
          ' exception and fail ingestion.'
      ),
  )

  # ----------------------------------------------------------------------------
  # WSI Pyramid Generation Configuration.
  # ----------------------------------------------------------------------------
  # When imaging is ingested using pixel equivalent methods frame dimensions are
  # set to match those in the source imaging. When non-pixel equivalent methods
  # are used frame dimensions in pixels are defined by FRAME_HEIGHT
  # and FRAME_WIDTH. It is generally recommended that frame
  # dimensions be defined as being square with, edge dimensions 256 to 512
  # pixels.
  arg_parser.add_argument(
      '-frame_height',
      default=os.getenv('FRAME_HEIGHT', '256'),
      type=int,
      help=(
          'DICOM frame height in generated imaging when non-pixel equivalent'
          ' transform is used'
      ),
  )

  arg_parser.add_argument(
      '-frame_width',
      default=os.getenv('FRAME_WIDTH', '256'),
      type=int,
      help=(
          'DICOM frame width in generated imaging when non-pixel equivalent'
          ' transform is used'
      ),
  )

  arg_parser.add_argument(
      '-compression',
      default=os.getenv('COMPRESSION', 'JPEG'),
      choices=['JPEG', 'JPEG2000', 'RAW'],
      help=(
          'Image compression to use when encoding pixels; supported formats:'
          ' JPEG, JPEG2000, or RAW (uncompressed).'
      ),
  )

  arg_parser.add_argument(
      '-jpeg_quality',
      default=os.getenv('JPEG_QUALITY', '95'),
      type=int,
      help='JPEG compression quality range 1 - 100.',
  )

  arg_parser.add_argument(
      '-jpeg_subsampling',
      default=os.getenv('JPEG_SUBSAMPLING', 'SUBSAMPLE_444'),
      choices=[
          'SUBSAMPLE_444',
          'SUBSAMPLE_440',
          'SUBSAMPLE_442',
          'SUBSAMPLE_420',
      ],
      help='JPEG compression subsampling.',
  )

  arg_parser.add_argument(
      '-icc_profile',
      default=os.getenv('ICC_PROFILE', 'True'),
      type=_str_to_bool,
      help='Embed source imaging ICC profile in generated imaging.',
  )

  arg_parser.add_argument(
      '-pixel_equivalent_transform',
      default=os.getenv('PIXEL_EQUIVALENT_TRANSFORM', 'HIGHEST_MAGNIFICATION'),
      choices=['DISABLED', 'HIGHEST_MAGNIFICATION', 'ALL_LEVELS'],
      help='Levels are processed using pixel equivalent transformation.',
  )

  # ----------------------------------------------------------------------------
  # MISC WSI Pyramid Configuration.
  # ----------------------------------------------------------------------------
  arg_parser.add_argument(
      '-uid_prefix',
      default=os.getenv('UID_PREFIX', '1.3.6.1.4.1.11129.5.7'),
      help=(
          'DICOM UID prefix to prefix generated DICOM with. The prefix is'
          ' required to'
          ' start with the Google Digital Pathology prefix'
          ' "1.3.6.1.4.1.11129.5.7"'
          ' the prefix may include an optional customer suffix, additional 7'
          ' characters. Characters must conform to the DICOM standard UID'
          ' requirements. '
          ' https://dicom.nema.org/dicom/2013/output/chtml/part05/chapter_9.html'
      ),
  )

  arg_parser.add_argument(
      '-ignore_root_dirs',
      default=json.loads(
          os.getenv('IGNORE_ROOT_DIRS', '["cloud-ingest", "storage-transfer"]')
      ),
      nargs='*',
      help=(
          'Files placed in listed root directories within the ingestion'
          ' directory will be ignored.'
      ),
  )

  arg_parser.add_argument(
      '-ignore_file_exts',
      default=json.loads(os.getenv('IGNORE_FILE_EXT', '[]')),
      nargs='*',
      help=(
          'list of file extensions (e.g., ".json") which will be ignored by the'
          ' transformation pipeline.'
      ),
  )

  arg_parser.add_argument(
      '-move_image_on_ingest_sucess_or_failure',
      default=os.getenv('MOVE_IMAGE_ON_INGEST_SUCCESS_OR_FAILURE', 'True'),
      type=_str_to_bool,
      help=(
          'If true imaging will be moved from ingestion bucket to success or'
          ' failure folder in the output folder when transformation completes.'
          ' If False, the file will be copied to the success/failure and not'
          ' removed from the ingestion bucket. This setting is to support RSYNC'
          ' or similar driven streaming pipeline execution which require copied'
          ' files to remain in place to avoid repeated file transfers.'
      ),
  )

  # ----------------------------------------------------------------------------
  # MISC Local host runner configuration.
  # ----------------------------------------------------------------------------

  arg_parser.add_argument(
      '-cloud_ops_logging',
      default=os.getenv('CLOUD_OPS_LOGGING', 'False'),
      type=_str_to_bool,
      help='Enables publishing transformation logs to cloud operations.',
  )

  arg_parser.add_argument(
      '-ops_log_name',
      default=os.getenv('CLOUD_OPS_LOG_NAME', 'transformation_pipeline'),
      help='Cloud ops log name to write logs to.',
  )

  arg_parser.add_argument(
      '-ops_log_project',
      default=os.getenv('CLOUD_OPS_LOG_PROJECT'),
      help='GCP project name to write cloud ops log to. Undefined = default',
  )

  # ----------------------------------------------------------------------------
  # MISC Local host runner configuration.
  # ----------------------------------------------------------------------------

  arg_parser.add_argument(
      '-docker_container_name',
      default=os.getenv(
          'DOCKER_CONTAINER_NAME', 'local_transform_pipeline_docker_container'
      ),
      help='Name of the docker container.',
  )

  arg_parser.add_argument(
      '-running_docker_instance_name',
      default=os.getenv(
          'RUNNING_DOCKER_INSTANCE_NAME', 'local_transform_pipeline'
      ),
      help='Name of the running docker instance.',
  )

  arg_parser.add_argument(
      '-max_file_copy_threads',
      default=os.getenv('MAX_FILE_COPY_THREADS', '3'),
      type=int,
      help='Maximum number of file copy threads.',
  )

  arg_parser.add_argument(
      '-log_environment_variables',
      default=os.getenv('LOG_ENVIRONMENT_VARIABLES', 'False'),
      type=_str_to_bool,
      help=(
          'Echo enviromental variable settings used to start transform'
          ' pipeline; does not start pipleine.'
      ),
  )

  arg_parser.add_argument(
      '-write_docker_run_shellscript',
      default=os.getenv('WRITE_DOCKER_RUN_SHELLSCRIPT', ''),
      help=(
          'Name of path to a file to write a shell script which launches the'
          ' configured localhost transformation pipeline; does'
          ' not start pipleine.'
      ),
  )
  return arg_parser.parse_args()


_parsed_args = _get_arg_parser()


def _mount(source: str, target: str, is_readonly: bool = False) -> List[str]:
  read_only = ',readonly' if is_readonly else ''
  return ['--mount', f'type=bind,source={source},target={target}{read_only}']


def _env(name: str, value: Any) -> List[str]:
  return ['-e', f'{name}={value}']


def _are_gcp_credentials_defined() -> bool:
  credential_path = _parsed_args.google_application_credentials
  if not credential_path:
    return False
  if not os.path.isfile(credential_path):
    raise _StartLocalHostError(
        'GOOGLE_APPLICATION_CREDENTIALS does not define a credential file;'
        f' value: {credential_path}'
    )
  return True


def _copy_file_to_image_ingestion_dir(source: str, dest: str) -> None:
  """Copies file on file system or contents gcs bucket to dest file path."""
  gs_prefix = 'gs://'
  if source.lower().startswith(gs_prefix):
    match = re.fullmatch('.*?/(.+)', source[len(gs_prefix) :])
    if match is not None:
      bucket_path = match.groups()[0]
      dest = os.path.join(dest, bucket_path.lstrip('/'))
    try:
      subprocess.run(
          [
              'gcloud',
              'storage',
              'cp',
              '--recursive',
              source.rstrip('/'),
              dest.rstrip('/'),
          ],
          check=True,
      )
    except subprocess.CalledProcessError:
      logging.warning('Failed to copy: %s to %s.', source, dest)
    return
  try:
    if not os.path.exists(source):
      logging.warning(
          'File does not exist. Failed to copy: %s to %s.', source, dest
      )
      return
    if not os.path.exists(dest):
      logging.warning(
          'Destination directory does not exist. Failed to copy: %s to %s.',
          source,
          dest,
      )
      return
    shutil.copy(source, dest)
  except OSError:
    logging.warning('Failed to copy: %s to %s.', source, dest)


def _is_dicom_store_gcp_store() -> bool:
  dicom_store = _parsed_args.dicom_store
  if dicom_store is None:
    return False
  return dicom_store.lower().startswith(
      'http://'
  ) or dicom_store.lower().startswith('https://')


def _get_metadata_free_ingestion(metadata_dir: _Directory) -> bool:
  """Returns true if metadata free ingestion is enabled."""
  enable_metadata_free_ingestion = _parsed_args.metadata_free_ingestion
  if metadata_dir.is_temp and not enable_metadata_free_ingestion:
    logging.warning(
        'Metadata ingested from temporary directory. Enabling metadata free'
        ' ingestion.'
    )
    return True
  return enable_metadata_free_ingestion


def _get_cloud_vision_barcode_segmentation_enabled() -> bool:
  """Returns true if cloud vision barcode segmentation is enabled."""
  cloud_vision_barcode_segmentation_enabled = (
      _parsed_args.cloud_vision_barcode_segmentation
  )
  if (
      not _are_gcp_credentials_defined()
      and cloud_vision_barcode_segmentation_enabled
  ):
    logging.warning(
        'GOOGLE_APPLICATION_CREDENTIALS are not defined. Disabling cloud'
        ' vision segmentation.'
    )
    return False
  return cloud_vision_barcode_segmentation_enabled


def _get_big_query_metadata_table() -> str:
  big_query_metadata_table = _parsed_args.big_query
  if big_query_metadata_table and not _are_gcp_credentials_defined():
    raise _StartLocalHostError(
        'Configuring Big Query as the metadata source requires the definition'
        ' of GOOGLE_APPLICATION_CREDENTIALS'
    )
  if big_query_metadata_table and len(big_query_metadata_table.split('.')) != 3:
    raise _StartLocalHostError('Big Query table name is incorrectly formatted.')
  return big_query_metadata_table


def _is_logging_to_cloud_ops() -> bool:
  return _parsed_args.cloud_ops_logging


def _get_logging_ops() -> List[str]:
  """Sets container logging option envs."""
  if not _is_logging_to_cloud_ops():
    return _env('ENABLE_CLOUD_OPS_LOGGING', False)
  if not _are_gcp_credentials_defined():
    raise _StartLocalHostError(
        'Logging to cloud operations requires the definition of'
        ' GOOGLE_APPLICATION_CREDENTIALS'
    )
  args = _env('ENABLE_CLOUD_OPS_LOGGING', True)
  args.extend(_env('CLOUD_OPS_LOG_NAME', _parsed_args.ops_log_name))
  gcp_to_log_to = _parsed_args.ops_log_project
  if gcp_to_log_to is not None and gcp_to_log_to:
    args.extend(_env('CLOUD_OPS_LOG_PROJECT', gcp_to_log_to))
  elif not _parsed_args.google_cloud_project:
    raise _StartLocalHostError(
        'Logging to cloud operations requires the definition of'
        ' GOOGLE_APPLICATION_CREDENTIALS'
    )
  return args


def _gen_docker_execution_args(
    image_ingestion: _Directory,
    metadata: _Directory,
    image_output: _Directory,
    dicom_store: str,
    poll_image_ingestion_dir: bool,
) -> List[str]:
  """Returns commandline to start localhost docker."""
  args = ['docker', 'run']
  if not _is_logging_to_cloud_ops():
    args.append('-it')
  args.append('--init')
  if _are_gcp_credentials_defined():
    default_credentials = _parsed_args.google_application_credentials
    container_crediential_path = (
        '/.config/gcloud/application_default_credentials.json'
    )
    args.extend(
        _env('GOOGLE_APPLICATION_CREDENTIALS', container_crediential_path)
    )
    if _parsed_args.google_cloud_project:
      args.extend(
          _env('GOOGLE_CLOUD_PROJECT', _parsed_args.google_cloud_project)
      )
    args.extend(_mount(default_credentials, container_crediential_path, True))

  args.extend(_env('LOCAL_UID', _parsed_args.user_id))
  args.extend(_env('LOCAL_GID', _parsed_args.group_id))
  args.extend(_env('POLL_IMAGE_INGESTION_DIR', poll_image_ingestion_dir))
  args.extend(_get_logging_ops())
  pyramid_config_path = _parsed_args.pyramid_generation_config_path.strip()
  if not pyramid_config_path:
    args.extend(_env('INGESTION_PYRAMID_LAYER_GENERATION_CONFIG_PATH', ''))
  elif pyramid_config_path.lower() == 'default':
    args.extend(
        _env(
            'INGESTION_PYRAMID_LAYER_GENERATION_CONFIG_PATH',
            '/default_config.json',
        )
    )
  else:
    if not os.path.isfile(pyramid_config_path):
      raise _StartLocalHostError(
          'Cannot find pyramid generation config file: {}.'
      )
    config_filename = os.path.basename(pyramid_config_path)
    args.extend(
        _env(
            'INGESTION_PYRAMID_LAYER_GENERATION_CONFIG_PATH',
            f'/{config_filename}',
        )
    )
    args.extend(_mount(pyramid_config_path, f'/{config_filename}', True))

  args.extend(
      _env(
          'ENABLE_METADATA_FREE_INGESTION',
          _get_metadata_free_ingestion(metadata),
      )
  )
  args.extend(
      _env(
          'ENABLE_CREATE_MISSING_STUDY_INSTANCE_UID',
          _parsed_args.create_missing_study_instance_uid,
      )
  )
  args.extend(
      _env(
          'GCS_INGEST_STUDY_INSTANCE_UID_SOURCE',
          _parsed_args.dicom_study_instance_uid_source,
      )
  )
  args.extend(
      _env(
          'TEST_WHOLE_FILENAME_AS_METADATA_PRIMARY_KEY',
          _parsed_args.whole_filename_metadata_primary_key,
      )
  )
  args.extend(
      _env(
          'INCLUDE_UPLOAD_BUCKET_PATH_IN_WHOLE_FILENAME_SLIDEID',
          _parsed_args.include_upload_path_in_whole_filename_metadata_primary_key,
      )
  )
  args.extend(
      _env(
          'FILENAME_METADATA_PRIMARY_KEY_SPLIT_STR',
          _parsed_args.filename_metadata_primary_key_split_str,
      )
  )
  args.extend(
      _env(
          'METADATA_PRIMARY_KEY_REGEX', _parsed_args.metadata_primary_key_regex
      )
  )
  args.extend(
      _env(
          'METADATA_PRIMARY_KEY_COLUMN_NAME',
          _parsed_args.metadata_primary_key_column_name,
      )
  )
  args.extend(_env('ENABLE_BARCODE_DECODER', _parsed_args.barcode_decoder))
  args.extend(
      _env(
          'ENABLE_CLOUD_VISION_BARCODE_SEGMENTATION',
          _get_cloud_vision_barcode_segmentation_enabled(),
      )
  )
  args.extend(_env('BIG_QUERY_METADATA_TABLE', _get_big_query_metadata_table()))
  args.extend(
      _env('METADATA_UID_VALIDATION', _parsed_args.metadata_uid_validation)
  )
  args.extend(
      _env(
          'METADATA_TAG_LENGTH_VALIDATION',
          _parsed_args.metadata_tag_length_validation,
      )
  )
  args.extend(
      _env(
          'REQUIRE_TYPE1_DICOM_TAG_METADATA_IS_DEFINED',
          _parsed_args.require_type1_dicom_tag_metadata_are_defined,
      )
  )
  args.extend(_env('WSI2DCM_DICOM_FRAME_HEIGHT', _parsed_args.frame_height))
  args.extend(_env('WSI2DCM_DICOM_FRAME_WIDTH', _parsed_args.frame_width))
  args.extend(_env('WSI2DCM_COMPRESSION', _parsed_args.compression))
  args.extend(
      _env('WSI2DCM_JPEG_COMPRESSION_QUALITY', _parsed_args.jpeg_quality)
  )
  args.extend(
      _env(
          'WSI2DCM_JPEG_COMPRESSION_SUBSAMPLING', _parsed_args.jpeg_subsampling
      )
  )
  args.extend(_env('EMBED_ICC_PROFILE', _parsed_args.icc_profile))
  args.extend(
      _env(
          'WSI2DCM_PIXEL_EQUIVALENT_TRANSFORM',
          _parsed_args.pixel_equivalent_transform,
      )
  )
  args.extend(_env('DICOM_GUID_PREFIX', _parsed_args.uid_prefix))
  args.extend(
      _env('INGEST_IGNORE_ROOT_DIR', json.dumps(_parsed_args.ignore_root_dirs))
  )
  args.extend(
      _env(
          'GCS_UPLOAD_IGNORE_FILE_EXT',
          ','.join(_parsed_args.ignore_file_exts),
      )
  )

  delete_image_from_ingestion_dir = (
      _parsed_args.move_image_on_ingest_sucess_or_failure
  )
  if image_output.is_temp:
    if delete_image_from_ingestion_dir:
      logging.warning(
          'Image output is defined as a temporary directory. Images will not be'
          ' removed from ingestion directory.'
      )
      delete_image_from_ingestion_dir = False
  args.extend(
      _env(
          'DELETE_FILE_FROM_INGEST_BUCKET_AT_INGEST_SUCCESS_OR_FAILURE',
          delete_image_from_ingestion_dir,
      )
  )
  args.extend(_env('LOCALHOST_DICOM_STORE', dicom_store))
  args.extend(_mount(metadata.path, '/input_metadata', False))
  args.extend(_mount(image_ingestion.path, '/input_imaging', False))
  args.extend(_mount(image_output.path, '/processed_imaging', False))
  if not _is_dicom_store_gcp_store():
    args.extend(_mount(dicom_store, '/mock_dicom_store', False))
  args.extend(['--name', _parsed_args.running_docker_instance_name])
  args.append(_parsed_args.docker_container_name)
  print(args)
  return args


def _handle_log(
    proc: subprocess.Popen[bytes], log_file: Optional[TextIO]
) -> None:
  if _is_logging_to_cloud_ops():
    return
  for line in proc.stdout:
    line_str = line.decode('utf-8')
    sys.stdout.write(line_str)
    if log_file is not None:
      log_file.write(line_str)


def _get_dir(
    name: str, stack_list: contextlib.ExitStack, path: str
) -> _Directory:
  if path:
    if not os.path.isdir(path):
      raise _StartLocalHostError(f'{name} directory: {path} does not exist.')
    return _Directory(path, False)
  temp_dir = stack_list.enter_context(tempfile.TemporaryDirectory())
  return _Directory(temp_dir, True)


def _test_dicom_store() -> None:
  """Tests dicom store is configured."""
  dicom_store = _parsed_args.dicom_store
  if _is_dicom_store_gcp_store():
    if not _are_gcp_credentials_defined():
      raise _StartLocalHostError(
          'GOOGLE_APPLICATION_CREDENTIALS are not defined. Connecting to GCP'
          ' DICOM stor requires GOOGLE_APPLICATION_CREDENTIALS.'
      )
    return
  if dicom_store is not None and not os.path.isdir(dicom_store):
    raise _StartLocalHostError(
        f'DICOM store directory: {dicom_store} does not exist.'
    )


def _copy_file_list_to_input_dir(
    stack_list: contextlib.ExitStack,
    copy_file_list: List[str],
    image_ingestion_dir: str,
    poll_image_ingestion_dir: bool,
) -> None:
  """Copies list images into input imaging directory, blocks if not polling."""
  if not copy_file_list:
    return
  fc_thread_pool = stack_list.enter_context(
      futures.ThreadPoolExecutor(
          max_workers=min(
              len(copy_file_list), _parsed_args.max_file_copy_threads
          )
      )
  )
  future_list = [
      fc_thread_pool.submit(
          _copy_file_to_image_ingestion_dir, file_path, image_ingestion_dir
      )
      for file_path in copy_file_list
  ]
  if poll_image_ingestion_dir:
    return
  # If not polling input directory then transformation pipeline will
  # create a list of imaging in input dir when it starts and exit when
  # list of item is complete. If not configured to poll the pipeline
  # launcher waits here for the all input imaging to finish being copied
  # into the image input directory.
  while future_list:
    logging.info(
        'Waiting for input %d imaging input sources to be copied into the'
        ' imaging ingestion folder.',
        len(future_list),
    )
    futures.wait(future_list, timeout=5, return_when=futures.ALL_COMPLETED)
    future_list = [future for future in future_list if not future.done()]


def _get_argument_settings(argument: str, args: List[str]) -> List[str]:
  index = 0
  element_list = []
  while index < len(args):
    if args[index] != argument:
      index += 1
      continue
    element_list.append(args[index + 1])
    index += 2
  return element_list


def _get_env_settings(args: List[str]) -> Mapping[str, str]:
  env_dict = {}
  for env_element in _get_argument_settings('-e', args):
    match = re.fullmatch('(.*?)=(.*)', env_element)
    if match is None:
      raise _StartLocalHostError('unexpectedly formated environmental var')
    env_dict[match.groups()[0]] = match.groups()[1]
  return env_dict


def _quote(val: str) -> str:
  if '"' not in val:
    return f'"{val}"'
  if "'" not in val:
    return f"'{val}'"
  return val


def _get_transform_env_log(env: Mapping[str, str]) -> str:
  """Returns String representation of transform pipleine ENVS set by runner."""
  env = dict(env)
  # Add entry for DICOM store if configured to run against actual store.
  if 'LOCALHOST_DICOM_STORE' in env:
    if env['LOCALHOST_DICOM_STORE'].lower().startswith('http'):
      env['DICOMWEB_URL'] = env['LOCALHOST_DICOM_STORE']
    del env['LOCALHOST_DICOM_STORE']
  # strip envs which only have meaning for localhost_main.
  for key in _LOCALHOST_SPECIFIC_ENVS:
    if key in env:
      del env[key]
  return '\n'.join(
      [f'  {key}: {_quote(env[key])}' for key in sorted(list(env))]
  )


def _get_poll_image_ingestion_dir(image_ingestion_dir: _Directory) -> bool:
  poll_image_ingestion_dir = _parsed_args.poll
  if poll_image_ingestion_dir and image_ingestion_dir.is_temp:
    logging.warning(
        'Images ingested from temporary dir. Image ingestion directory'
        ' polling disabled.'
    )
    return False
  return poll_image_ingestion_dir


def _write_docker_run_shellscript(path: str, args: List[str]) -> None:
  """Writes docker run shell script to path."""
  remove_docker = f'docker rm {_parsed_args.running_docker_instance_name}'
  with open(path, 'wt') as output:
    lines_to_write = [
        '# Copyright 2024 Google LLC\n',
        '#\n',
        '# Licensed under the Apache License, Version 2.0 (the "License");\n',
        '# you may not use this file except in compliance with the License.\n',
        '# You may obtain a copy of the License at\n',
        '#\n',
        '#     http://www.apache.org/licenses/LICENSE-2.0\n',
        '#\n',
        '# Unless required by applicable law or agreed to in writing, software',
        '\n',
        '# distributed under the License is distributed on an "AS IS" BASIS,\n',
        '# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or ',
        'implied.\n',
        '# See the License for the specific language governing permissions and',
        '\n',
        '# limitations under the License.\n\n',
    ]
    output.writelines(lines_to_write)
    output.write('\n'.join([remove_docker, ' '.join(args), remove_docker]))
  os.chmod(path, 0o755)


def main(unused_argv):
  if _parsed_args.log and _is_logging_to_cloud_ops():
    raise _StartLocalHostError(
        'Invalid configuration, can not capture logs locally and send logs to'
        ' cloud operations.'
    )
  with contextlib.ExitStack() as stack_list:
    log_file = (
        stack_list.enter_context(open(_parsed_args.log, 'wt'))
        if _parsed_args.log
        else None
    )
    metadata_dir = _get_dir('Metadata', stack_list, _parsed_args.metadata_dir)
    image_ingestion_dir = _get_dir(
        'Image ingestion', stack_list, _parsed_args.image_ingestion_dir
    )
    processed_image_dir = _get_dir(
        'Processed image', stack_list, _parsed_args.processed_image_dir
    )
    _test_dicom_store()
    poll_image_ingestion_dir = _get_poll_image_ingestion_dir(
        image_ingestion_dir
    )

    _copy_file_list_to_input_dir(
        stack_list,
        _parsed_args.input_images,
        image_ingestion_dir.path,
        poll_image_ingestion_dir,
    )
    if _parsed_args.dicom_store is None:
      dicom_store = stack_list.enter_context(tempfile.TemporaryDirectory())
      print(
          '\n'.join([
              f'DICOM STORE: {dicom_store}',
              (
                  'Warning: DICOM Store was not set and is initialized a'
                  ' temporary directory. DICOM images created will not persist'
                  ' after termination of the executing python script.'
              ),
              '',
          ])
      )
    else:
      dicom_store = _parsed_args.dicom_store
      print(f'DICOM STORE: {_parsed_args.dicom_store}')
    docker_args = _gen_docker_execution_args(
        image_ingestion_dir,
        metadata_dir,
        processed_image_dir,
        dicom_store,
        poll_image_ingestion_dir,
    )

    if _parsed_args.log_environment_variables:
      log = _get_transform_env_log(_get_env_settings(docker_args))
      msg = [
          '\n\nTransform Pipleine Enviromental Variables',
          '-' * 41,
          log,
          '-' * 41,
      ]
      logging.info('\n'.join(msg))
      return

    shell_script_path = _parsed_args.write_docker_run_shellscript
    if shell_script_path:
      if (
          metadata_dir.is_temp
          or image_ingestion_dir.is_temp
          or processed_image_dir.is_temp
      ):
        raise _StartLocalHostError(
            'Invalid configuration, metadata, image ingestion, and processed'
            ' image dirs must be defined.'
        )
      _write_docker_run_shellscript(shell_script_path, docker_args)
      return

    docker_instance_name = _parsed_args.running_docker_instance_name
    # remove prexisting running docker.
    subprocess.run(['docker', 'rm', docker_instance_name], check=False)
    # start docker container and log output.
    proc = subprocess.Popen(
        args=docker_args,
        stdout=None if _is_logging_to_cloud_ops() else subprocess.PIPE,
        stderr=None if _is_logging_to_cloud_ops() else subprocess.STDOUT,
    )
    if _is_logging_to_cloud_ops():
      logging.info('Logs are being sent to cloud operations.')
    try:
      _handle_log(proc, log_file)
    finally:
      proc.terminate()
      # remove stopped docker.
      subprocess.run(['docker', 'rm', docker_instance_name], check=False)
    _handle_log(proc, log_file)


if __name__ == '__main__':
  main(None)
