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
"""DPAS DICOM transformation pipeline.

Overview:
 - Pipeline hosted in GKE
 - Sync polls cloud storage pub/sub subscription or
   ingests specified list of files.
 - Downloads image to container.
 - Converts downloaded image to DICOM using specified conversion tooling.
 - Performs hash based duplicate testing DICOMs for duplication.
 - Uploads generated non duplicated DICOM to DICOM Store.
 - Moves image trigging pipeline from input to output/success or
   output/failure bucket.
 - Published ingest complete pub/sub msg.
 - All Operations logged in cloud operations.
"""
import threading
from typing import Mapping, Optional

from absl import app

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import abstract_pubsub_message_handler
from transformation_pipeline.ingestion_lib import polling_client
from transformation_pipeline.ingestion_lib import redis_client
from transformation_pipeline.ingestion_lib.dicom_gen import ingestion_dicom_store_urls
from transformation_pipeline.ingestion_lib.dicom_gen.ai_to_dicom import png_to_dicom
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_dicom_store_handler
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_gcs_handler
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingestion_complete_oof_trigger_pubsub_topic
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client

_LICENSE_FILE_PATH = './thirdparty_licenses.txt'


class LicenseMissingError(Exception):
  pass


class RedisConnectionFailedError(Exception):
  pass


_running_polling_client: Optional[polling_client.PollingClient] = None
_running_polling_client_lock = threading.Lock()


def stop_polling_client() -> None:
  with _running_polling_client_lock:
    if _running_polling_client is not None:
      _running_polling_client.stop_polling_client()


def _set_running_polling_client(
    client: Optional[polling_client.PollingClient],
) -> None:
  with _running_polling_client_lock:
    global _running_polling_client
    _running_polling_client = client


def _run_polling_client_ingest(
    project: str,
    sub_to_handler: Mapping[
        str, abstract_pubsub_message_handler.AbstractPubSubMsgHandler
    ],
) -> None:
  """Runs polling client ingestion.

  Args:
    project: GCP project id for Pub/Sub subscriptions.
    sub_to_handler: Map of Pub/Sub subscription id to message handler.
  """
  # Wrap the polling client in a 'with' block to automatically call close() to
  # close the underlying gRPC channel when done.
  with polling_client.PollingClient(project, sub_to_handler) as client:
    _set_running_polling_client(client)
    try:
      client.run()
    finally:
      _set_running_polling_client(None)


def _run_oof_ingest() -> None:
  """Runs OOF ingestion."""
  if not ingest_flags.OOF_SUBSCRIPTION_FLG.value:
    oof_msg = (
        'Missing OOF subscription for OOF transformation pipeline. Either '
        '--oof_subscription flag or OOF_SUBSCRIPTION env variable must be set.'
    )
    cloud_logging_client.critical(oof_msg)
    raise ValueError(oof_msg)

  ingest_handler = png_to_dicom.AiPngtoDicomSecondaryCapture(
      dicom_store_web_path=ingestion_dicom_store_urls.get_main_dicom_web_url(),
      use_oof_legacy_pipeline=ingest_flags.OOF_LEGACY_INFERENCE_PIPELINE_FLG.value,
  )
  sub_to_handler = {ingest_flags.OOF_SUBSCRIPTION_FLG.value: ingest_handler}
  _run_polling_client_ingest(ingest_flags.PROJECT_ID_FLG.value, sub_to_handler)


def _get_oof_trigger_config() -> (
    Optional[ingest_gcs_handler.InferenceTriggerConfig]
):
  oof_dicom_store_url = ingestion_dicom_store_urls.get_oof_dicom_web_url()
  if not oof_dicom_store_url:
    return None
  return ingest_gcs_handler.InferenceTriggerConfig(
      dicom_store_web_path=oof_dicom_store_url,
      pubsub_topic=ingestion_complete_oof_trigger_pubsub_topic.get_oof_trigger_pubsub_topic(),
      use_oof_legacy_pipeline=ingest_flags.OOF_LEGACY_INFERENCE_PIPELINE_FLG.value,
      inference_config_path=ingest_flags.OOF_INFERENCE_CONFIG_PATH_FLG.value,
  )


def _run_default_ingest() -> None:
  """Runs default ingestion.

  If --dicom_store_subscription is set, runs round robin with GCS and DICOM
  store polling.
  """
  if not ingest_flags.GCS_SUBSCRIPTION_FLG.value:
    gcs_msg = (
        'Missing GCS subscription for default transformation pipeline. Either '
        '--gcs_subscription flag or GCS_SUBSCRIPTION env variable must be set.'
    )
    cloud_logging_client.critical(gcs_msg)
    raise ValueError(gcs_msg)

  sub_to_handler = {}
  metadata_client = metadata_storage_client.MetadataStorageClient()
  gcs_handler = ingest_gcs_handler.IngestGcsPubSubHandler(
      ingest_succeeded_uri=ingest_flags.INGEST_SUCCEEDED_URI_FLG.value,
      ingest_failed_uri=ingest_flags.INGEST_FAILED_URI_FLG.value,
      dicom_store_web_path=ingestion_dicom_store_urls.get_main_dicom_web_url(),
      ingest_ignore_root_dirs=frozenset(
          ingest_flags.INGEST_IGNORE_ROOT_DIR_FLG.value
      ),
      metadata_client=metadata_client,
      oof_trigger_config=_get_oof_trigger_config(),
  )
  sub_to_handler[ingest_flags.GCS_SUBSCRIPTION_FLG.value] = gcs_handler

  if ingest_flags.DICOM_STORE_SUBSCRIPTION_FLG.value:
    dicom_store_handler = (
        ingest_dicom_store_handler.IngestDicomStorePubSubHandler(
            metadata_client
        )
    )
    sub_to_handler[ingest_flags.DICOM_STORE_SUBSCRIPTION_FLG.value] = (
        dicom_store_handler
    )
  else:
    cloud_logging_client.info(
        'Running GCS ingest only. To include DICOM store, either '
        '--dicom_store_subscription flag or DICOM_STORE_SUBSCRIPTION env '
        'variable must be set. '
    )

  _run_polling_client_ingest(ingest_flags.PROJECT_ID_FLG.value, sub_to_handler)


def main(unused_argv):
  copyright_notification = (
      'Copyright 2021 Google LLC.\n\n'
      'Your use of this software is subject to your agreement with Google. '
      'You  may not copy, modify, or distribute this software except as '
      'permitted under your agreement with Google.'
  )
  cloud_logging_client.info(copyright_notification)
  try:
    with open(_LICENSE_FILE_PATH, 'rt') as infile:
      software_license = infile.read()
  except FileNotFoundError:
    software_license = None
  if not software_license:
    cloud_logging_client.critical('GKE container is missing software license.')
    raise LicenseMissingError('Missing software license')
  cloud_logging_client.info(
      'Opensource software licenses', {'licenses': software_license}
  )

  log = {'redis_server': ingest_flags.REDIS_SERVER_IP_FLG.value}
  if not redis_client.redis_client().has_redis_client():
    cloud_logging_client.info('Redis is not configured', log)
  else:
    cloud_logging_client.info('Redis is configured', log)
    if redis_client.redis_client().ping():
      cloud_logging_client.info('Successfully pinged redis server', log)
    else:
      cloud_logging_client.critical('Could not ping redis server', log)
      raise RedisConnectionFailedError(
          'Could not connect to redis server '
          f'{ingest_flags.REDIS_SERVER_IP_FLG.value}.'
      )

  transformation_pipeline = (
      ingest_flags.TRANSFORMATION_PIPELINE_FLG.value.strip().lower()
  )
  if transformation_pipeline == 'oof':
    _run_oof_ingest()
  elif transformation_pipeline == 'default':
    _run_default_ingest()
  else:
    err_msg = (
        f'Invalid TRANSFORMATION_PIPELINE value: {transformation_pipeline}. '
        'Expected: default or oof.'
    )
    cloud_logging_client.critical(err_msg)
    raise ValueError(err_msg)


if __name__ == '__main__':
  app.run(main)
