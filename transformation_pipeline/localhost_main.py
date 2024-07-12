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
"""DOCKER CLI entrypoint for transformation pipeline."""

import contextlib
import dataclasses
import os
import shutil
from typing import Any, List, Mapping, Optional
from unittest import mock
import uuid

from absl import app
from absl import flags
from absl.testing import flagsaver

from shared_libs.flags import flag_utils
from shared_libs.logging_lib import cloud_logging_client
from shared_libs.test_utils.dicom_store_mock import dicom_store_mock
from shared_libs.test_utils.gcs_mock import gcs_mock
from shared_libs.test_utils.gcs_mock import gcs_pubsub_mock
from transformation_pipeline import gke_main
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client

_CONTAINER_BASE_DIR = ''

_MOCK_DICOM_STORE_URL = 'https://mock.dicom.store.com/dicomWeb'
_EXAMPLE_METADATA_MAPPING_SCHEMA_PATH = '/example/metadata_mapping_schemas'

_POLL_IMAGE_INGESTION_DIR_FLG = flags.DEFINE_boolean(
    'poll_image_ingestion_dir',
    flag_utils.env_value_to_bool('POLL_IMAGE_INGESTION_DIR', False),
    'Monitor image ingest dir for new images; transformation pipeline will'
    ' continue to run after all images in the ingestion dir have been'
    ' processed.',
)

_ENABLE_CLOUD_OPS_LOGGING_FLG = flags.DEFINE_boolean(
    'enable_cloud_ops_logging',
    flag_utils.env_value_to_bool('ENABLE_CLOUD_OPS_LOGGING', False),
    'Enables publishing transformation logs to cloud operations',
)

_ENABLE_TRANSFORMATION_PIPELINE_GENERATED_PUBSUB_MESSAGES_FLG = (
    flags.DEFINE_boolean(
        'enable_transformation_pipeline_generated_published_pubsub_messages',
        flag_utils.env_value_to_bool(
            'ENABLE_TRANSFORMATION_PIPELINE_GENERATED_PUBSUB_MESSAGES', False
        ),
        'Enables transformation pipeline generation of pub/sub messages.',
    )
)

_PUBSUB_MOCK_MESSAGE_DELAY_SEC_FLG = flags.DEFINE_float(
    'pubsub_mock_message_delay_sec',
    float(os.getenv('PUBSUB_MOCK_MESSAGE_DELAY_SEC', '30.0')),
    'Delay to wait before processing mock gcs pub/sub msg to ensure file has'
    ' finished copying into image input bucket.',
)

_LOCALHOST_DICOM_STORE_FLG = flags.DEFINE_string(
    'localhost_dicom_store',
    os.getenv('LOCALHOST_DICOM_STORE', ''),
    'External directory or http server being connected to (Required).',
)


class _LocalhostMissingDicomStoreConfigurationError(Exception):
  pass


class _UnableToReadWriteFromDirError(Exception):
  pass


@dataclasses.dataclass(frozen=True)
class _ContainerDirs:
  metadata: str
  imaging: str
  processed_images: str
  dicom_store: str


def _has_metadata_schema_in_dir(metadata_dir: str) -> bool:
  for path in os.listdir(metadata_dir):
    if metadata_storage_client.is_schema(path):
      return True
  return False


def _copy_default_schema_to_metadata_dir(metadata_dir: str):
  """Copy example metadata mapping schemas to metadata dir."""
  for schema_filesname in os.listdir(_EXAMPLE_METADATA_MAPPING_SCHEMA_PATH):
    example_schema_path = os.path.join(
        _EXAMPLE_METADATA_MAPPING_SCHEMA_PATH, schema_filesname
    )
    if not metadata_storage_client.is_schema(example_schema_path):
      continue
    try:
      shutil.copyfile(
          example_schema_path, os.path.join(metadata_dir, schema_filesname)
      )
    except OSError as exp:
      cloud_logging_client.warning(
          'Error occured copying example metadata mapping schema to metadata'
          ' dir.',
          exp,
      )
      continue


def _is_mocked_dicom_store() -> bool:
  store = _LOCALHOST_DICOM_STORE_FLG.value.lower()
  return not store.startswith('http://') and not store.startswith('https://')


def _get_container_dirs() -> _ContainerDirs:
  base_dir = _CONTAINER_BASE_DIR.rstrip('/')
  return _ContainerDirs(
      f'{base_dir}/input_metadata',
      f'{base_dir}/input_imaging',
      f'{base_dir}/processed_imaging',
      f'{base_dir}/mock_dicom_store' if _is_mocked_dicom_store() else '',
  )


def _can_read_write_dir(path: str, readonly: bool):
  """Validates that mounted path supports expected, read/write."""
  if not os.path.isdir(path):
    cloud_logging_client.critical(f'Directory {path} does not exist.')
    raise _UnableToReadWriteFromDirError(f'Directory {path} does not exist.')
  if not os.access(path, os.R_OK):
    cloud_logging_client.critical(f'Cannot read from directory: {path}')
    raise _UnableToReadWriteFromDirError(f'Cannot read directory {path}.')
  if readonly:
    return
  if not os.access(path, os.W_OK):
    cloud_logging_client.critical(f'Cannot write to directory: {path}')
    raise _UnableToReadWriteFromDirError(f'Cannot write to directory {path}.')


def _call_if_no_files() -> None:
  """Call back called when mock gcs pub/sub queue has no files."""
  return


def _build_ingest_file_list(bucket: str) -> Optional[List[str]]:
  """Returns a list of files to ingest."""
  if _POLL_IMAGE_INGESTION_DIR_FLG.value:
    return None
  base_dir = _get_container_dirs().imaging.rstrip('/')
  base_dir_len = len(base_dir)
  return_files = []
  for root, _, files in os.walk(base_dir):
    gcs_root = f'gs://{bucket}{root[base_dir_len:]}'
    for fname in files:
      if fname.startswith('.'):
        continue
      return_files.append(os.path.join(gcs_root, fname))
  return return_files


def _get_logging_destination(project_id: str) -> Mapping[str, Any]:
  if _ENABLE_CLOUD_OPS_LOGGING_FLG.value:
    return dict()
  return dict(
      ops_log_project=project_id,
      ops_log_name='transformation_pipeline',
      debug_logging_use_absl_logging=True,
  )


def main(unused_argv):
  container_dirs = _get_container_dirs()
  test_dirs_exist = [
      container_dirs.metadata,
      container_dirs.imaging,
      container_dirs.processed_images,
  ]
  if _is_mocked_dicom_store():
    test_dirs_exist.append(container_dirs.dicom_store)
  if not _LOCALHOST_DICOM_STORE_FLG.value:
    cloud_logging_client.critical('Localhost DICOM Store is undefined.')
    raise _LocalhostMissingDicomStoreConfigurationError(
        'Localhost DICOM Store is undefined.'
    )
  for path in test_dirs_exist:
    _can_read_write_dir(path, False)

  if not _has_metadata_schema_in_dir(container_dirs.metadata):
    _copy_default_schema_to_metadata_dir(container_dirs.metadata)
  try:
    gcs_bucket_name_generating_pubsub = 'image_ingest'
    gcs_bucket_name_holding_processed_imaging = 'transform_output'
    gcs_bucket_name_holding_metadata = 'metadata'
    gcs_file_to_ingest_list = _build_ingest_file_list(
        gcs_bucket_name_generating_pubsub
    )
    # Project id hosting storage buckets and pub/sub subscription
    project_id = 'mock-project-id'
    transform_pod_uid = str(uuid.uuid4())
    # Name of pub/sub subscription listening on.
    gcs_subscription = 'mock-gcs-subscription'
    # dicom store image are uploaded into
    if _is_mocked_dicom_store():
      dicomweb_url = _MOCK_DICOM_STORE_URL
    else:
      dicomweb_url = _LOCALHOST_DICOM_STORE_FLG.value
    with contextlib.ExitStack() as context_list:
      context_list.enter_context(
          flagsaver.flagsaver(
              metadata_bucket=gcs_bucket_name_holding_metadata,
              gcs_subscription=gcs_subscription,
              ingest_succeeded_uri=(
                  f'gs://{gcs_bucket_name_holding_processed_imaging}/success'
              ),
              ingest_failed_uri=(
                  f'gs://{gcs_bucket_name_holding_processed_imaging}/failure'
              ),
              dicomweb_url=dicomweb_url,
              project_id=project_id,
              gcs_file_to_ingest_list=gcs_file_to_ingest_list,
              # Send logs to to container standard out
              **_get_logging_destination(project_id),
              transform_pod_uid=transform_pod_uid,
              pod_uid=transform_pod_uid,
              # Pod host is not set.
              pod_hostname='',
          )
      )
      if gcs_file_to_ingest_list is None:
        context_list.enter_context(
            gcs_pubsub_mock.MockPubSub(
                project_id,
                gcs_subscription,
                gcs_bucket_name_generating_pubsub,
                _get_container_dirs().imaging,
                call_if_no_files=_call_if_no_files,
                message_queue_delay_sec=_PUBSUB_MOCK_MESSAGE_DELAY_SEC_FLG.value,
            )
        )
      if (
          not _ENABLE_TRANSFORMATION_PIPELINE_GENERATED_PUBSUB_MESSAGES_FLG.value
      ):
        context_list.enter_context(
            mock.patch('google.cloud.pubsub_v1.PublisherClient', autospec=True)
        )
      context_list.enter_context(
          gcs_mock.GcsMock({
              gcs_bucket_name_holding_metadata: container_dirs.metadata,
              gcs_bucket_name_holding_processed_imaging: (
                  container_dirs.processed_images
              ),
              gcs_bucket_name_generating_pubsub: container_dirs.imaging,
          })
      )
      if _is_mocked_dicom_store():
        mock_dicom_store = context_list.enter_context(
            dicom_store_mock.MockDicomStores(
                _MOCK_DICOM_STORE_URL,
                real_http=True,  # Pass unhandled requests through mock.
            )
        )
        mock_dicom_store[_MOCK_DICOM_STORE_URL].set_dicom_store_disk_storage(
            container_dirs.dicom_store
        )
      gke_main.main(unused_argv=None)
    cloud_logging_client.info('Transformation pipeline done.')
  except Exception as exp:
    cloud_logging_client.critical(
        'Unexpected error running transformation pipeline', exp
    )
    raise


if __name__ == '__main__':
  app.run(main)
