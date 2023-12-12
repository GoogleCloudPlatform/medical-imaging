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
"""DPP DICOM transformation pipeline.

 Overview:
  - Pipeline hosted in GKE
  - Sync polls cloud storage pub/sub subscription or
    ingests specified list of files.
  - Downloads image to container.
  - Converts downloaded image to DICOM using specified conversion tooling.
  - Performs hash based duplicate testing DICOMs for duplication.
  - Uploads generated non duplicated DICOM to DICOM Store.
  - Moves SVS from ingest to output/success or output/failure bucket.
  - Published ingest complete pub/sub msg.
  - All Operations logged in cloud operations.

 WSI-to-DICOM conversion:
  - Detects if metadata changed.
    - If changed downloads metadata (csv & schema) to container.
  - Identifies slide_id
    - 1st: parts of file name
    - 2nd: tries to decode barcode in label, macro, thumbnail images
    - if no valid slide ID is found, image will be placed in actionable error
      bucket
  - Generates DICOM metadata (JSON DICOM) from csv & schema.
  - Converts image to WSI using conversion tooling.
  - Converts label, macro, thumbnail images to secondary captures.
  - All Operations logged in cloud operations.

Debugging/ingesting specific set of images
  To ingest a pre-defined set of images and exit the container.
  Run container with "gcs_file_to_ingest_list" parameter defined or define
  "GCS_FILE_INGEST_LIST" environmental variable to point to paths of SVS to
  ingest (see parameter definition in polling_client.py for more info).

  example: gke_main.py --gcs_file_to_ingest_list gs://mybucket/image.svs
"""
from typing import Mapping

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


class LicenseMissingError(Exception):
  pass


class RedisConnectionFailedError(Exception):
  pass


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
    client.run()


def _run_oof_ingest() -> None:
  """Runs OOF ingestion."""
  if not ingest_flags.OOF_SUBSCRIPTION_FLG.value:
    oof_msg = (
        'Missing OOF subscription for OOF transformation pipeline. Either '
        '--oof_subscription flag or OOF_SUBSCRIPTION env variable must be set.'
    )
    cloud_logging_client.logger().critical(oof_msg)
    raise ValueError(oof_msg)

  ingest_handler = png_to_dicom.AiPngtoDicomSecondaryCapture(
      dicom_store_web_path=ingestion_dicom_store_urls.get_main_dicom_web_url(),
      use_oof_legacy_pipeline=ingest_flags.OOF_LEGACY_INFERENCE_PIPELINE_FLG.value,
  )
  sub_to_handler = {ingest_flags.OOF_SUBSCRIPTION_FLG.value: ingest_handler}
  _run_polling_client_ingest(ingest_flags.PROJECT_ID_FLG.value, sub_to_handler)


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
    cloud_logging_client.logger().critical(gcs_msg)
    raise ValueError(gcs_msg)

  sub_to_handler = {}
  gcs_handler = ingest_gcs_handler.IngestGcsPubSubHandler(
      ingest_succeeded_uri=ingest_flags.INGEST_SUCCEEDED_URI_FLG.value,
      ingest_failed_uri=ingest_flags.INGEST_FAILED_URI_FLG.value,
      dicom_store_web_path=ingestion_dicom_store_urls.get_main_dicom_web_url(),
      ingest_ignore_root_dirs=frozenset(
          ingest_flags.INGEST_IGNORE_ROOT_DIR_FLG.value
      ),
      oof_trigger_config=ingest_gcs_handler.InferenceTriggerConfig(
          dicom_store_web_path=ingestion_dicom_store_urls.get_oof_dicom_web_url(),
          pubsub_topic=ingestion_complete_oof_trigger_pubsub_topic.get_oof_trigger_pubsub_topic(),
          use_oof_legacy_pipeline=ingest_flags.OOF_LEGACY_INFERENCE_PIPELINE_FLG.value,
          inference_config_path=ingest_flags.OOF_INFERENCE_CONFIG_PATH_FLG.value,
      ),
  )
  sub_to_handler[ingest_flags.GCS_SUBSCRIPTION_FLG.value] = gcs_handler

  if ingest_flags.DICOM_STORE_SUBSCRIPTION_FLG.value:
    dicom_store_handler = (
        ingest_dicom_store_handler.IngestDicomStorePubSubHandler()
    )
    sub_to_handler[ingest_flags.DICOM_STORE_SUBSCRIPTION_FLG.value] = (
        dicom_store_handler
    )
  else:
    cloud_logging_client.logger().info(
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
  cloud_logging_client.logger().info(copyright_notification)
  try:
    with open('./thirdparty_licenses.txt', 'rt') as infile:
      software_license = infile.read()
  except FileNotFoundError:
    software_license = None
  if not software_license:
    cloud_logging_client.logger().critical(
        'GKE container is missing software license.'
    )
    raise LicenseMissingError('Missing software license')
  cloud_logging_client.logger().info(
      'Opensource software licenses', {'licenses': software_license}
  )

  if not redis_client.redis_client(
      ingest_flags.REDIS_HOST_IP_FLG.value
  ).has_redis_client():
    cloud_logging_client.logger().info(
        'Redis is not configured',
        {'redis_server': ingest_flags.REDIS_HOST_IP_FLG.value},
    )
  else:
    cloud_logging_client.logger().info(
        'Redis is configured',
        {'redis_server': ingest_flags.REDIS_HOST_IP_FLG.value},
    )
    if redis_client.redis_client(ingest_flags.REDIS_HOST_IP_FLG.value).ping():
      cloud_logging_client.logger().info(
          'Successfully pinged redis server',
          {'redis_server': ingest_flags.REDIS_HOST_IP_FLG.value},
      )
    else:
      cloud_logging_client.logger().critical(
          'Could not ping redis server',
          {'redis_server': ingest_flags.REDIS_HOST_IP_FLG.value},
      )
      raise RedisConnectionFailedError(
          'Could not connect to redis server '
          f'{ingest_flags.REDIS_HOST_IP_FLG.value}.'
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
    cloud_logging_client.logger().critical(err_msg)
    raise ValueError(err_msg)


if __name__ == '__main__':
  app.run(main)
