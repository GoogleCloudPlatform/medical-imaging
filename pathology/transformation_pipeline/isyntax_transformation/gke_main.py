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
"""Isyntax conversion server."""

from __future__ import annotations

import contextlib
import json
import os
import subprocess
import tempfile
import threading
import time
from typing import Optional, Sequence

from absl import app as absl_app
from absl import flags
from google.api_core import retry
from google.cloud import pubsub_v1
from google.cloud import storage

from pathology.shared_libs.flags import secret_flag_utils
from pathology.shared_libs.logging_lib import cloud_logging_client
from pathology.transformation_pipeline.ingestion_lib import cloud_storage_client

_ISYNTAX_CONVERSION_TRACE_ID = 'isyntax_conversion_trace_id'
_PUBSUB_MESSAGE_ID = 'pubsub_message_id'
_GKE_RUNNING = True  # Used as flag to exit when running in unit tests.

PUBSUB_SUBSCRIPTION_PROJECT_ID_FLG = flags.DEFINE_string(
    'pubsub_subscription_project_id',
    secret_flag_utils.get_secret_or_env(
        'PUBSUB_SUBSCRIPTION_PROJECT_ID',
        secret_flag_utils.get_secret_or_env('PROJECT_ID', ''),
    ),
    'PubSub project id.',
)

PUBSUB_SUBSCRIPTION_FLG = flags.DEFINE_string(
    'pubsub_subscription',
    secret_flag_utils.get_secret_or_env('PUBSUB_SUBSCRIPTION', ''),
    'PubSub project id.',
)

INGEST_SUCCEEDED_URI_FLG = flags.DEFINE_string(
    'ingest_succeeded_bucket',
    secret_flag_utils.get_secret_or_env('INGEST_SUCCEEDED_URI', ''),
    'GS style path to bucket that successful ingest files are moved to.',
)

INGEST_FAILED_URI_FLG = flags.DEFINE_string(
    'ingest_failed_uri',
    secret_flag_utils.get_secret_or_env('INGEST_FAILED_URI', ''),
    'GS style path that defines output bucket that failed ingested files are'
    ' moved to.',
)

OUTPUT_BUCKET_FLG = flags.DEFINE_string(
    'output_bucket',
    secret_flag_utils.get_secret_or_env('OUTPUT_BUCKET', ''),
    'GS style path that defines output bucket generated isyntax files are'
    ' uploaded to.',
)

DELETE_FROM_SOURCE_BUCKET__FLG = flags.DEFINE_bool(
    'delete_from_source_bucket',
    secret_flag_utils.get_bool_secret_or_env(
        'DELETE_FROM_SOURCE_BUCKET', False
    ),
    'Delete isyntax files from source bucket after successful conversion and'
    ' upload to output bucket.',
)

JPEG_QUALITY_FLG = flags.DEFINE_integer(
    'jpeg_quality',
    min(
        100,
        max(1, int(secret_flag_utils.get_secret_or_env('JPEG_QUALITY', '95'))),
    ),
    'JPEG quality of generated output.',
)

FRAME_WIDTH_FLG = flags.DEFINE_integer(
    'frame_width',
    max(1, int(secret_flag_utils.get_secret_or_env('FRAME_WIDTH', '512'))),
    'Width of frames in output tiff.',
)

FRAME_HEIGHT_FLG = flags.DEFINE_integer(
    'frame_height',
    max(1, int(secret_flag_utils.get_secret_or_env('FRAME_HEIGHT', '512'))),
    'Height of frames in output tiff.',
)


class PubSubKeepAliveThread:
  """Thread to keep pub/sub message alive."""

  def __init__(
      self,
      pubsub_subscriber: Optional[pubsub_v1.SubscriberClient],
      subscription_path: str,
      ack_id: str,
  ):
    self._pubsub_subscriber = pubsub_subscriber
    self._subscription_path = subscription_path
    self._ack_id = ack_id
    self._thread = None
    self._thread_running = False

  def __enter__(self) -> PubSubKeepAliveThread:
    if self._pubsub_subscriber is None:
      return self
    self._thread = threading.Thread(target=self._keep_alive)
    self._thread.start()
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    if self._thread is None:
      return
    self._thread.join()
    self._thread_running = False
    self._thread = None

  def _keep_alive(self):
    """Thread to keep pub/sub message alive."""
    if self._pubsub_subscriber is None:
      return
    count = 0
    while self._thread_running:
      time.sleep(1)
      count += 1
      if count % 60 == 0:
        self._pubsub_subscriber.modify_ack_deadline(
            request={
                'subscription': self._subscription_path,
                'ack_ids': [self._ack_id],
                'ack_deadline_seconds': 1000,  # ack_deadline_seconds
            }
        )
        count = 0
        cloud_logging_client.info('Extending Pub/sub keep alive')


def _convert_isyntax_to_tiff(
    client: storage.Client, local_filename: str, source_blob_file_name: str
) -> bool:
  """Coverts isyntax file to tiff file.

  Args:
    client: Storage client to use for uploading converted file.
    local_filename: Local filename of isyntax file to convert.
    source_blob_file_name: Source blob file name of isyntax file to convert.

  Returns:
    True if conversion was successful, False otherwise.
  """
  source_blob_name, ext = os.path.splitext(source_blob_file_name)
  if ext.lower() != '.isyntax':
    cloud_logging_client.error(f'Unexpected filename: {local_filename}')
    return False

  current_dir = os.getcwd()
  try:
    os.chdir(os.path.dirname(local_filename))
    cloud_logging_client.info('Running isyntax conversion')
    result = subprocess.run(
        [
            'python3',
            '/pythontools/isyntax_to_tiff.py',
            os.path.basename(local_filename),
            '0',
            '0',
            '0',
            str(JPEG_QUALITY_FLG.value),
            str(FRAME_WIDTH_FLG.value),
            str(FRAME_HEIGHT_FLG.value),
        ],
        check=True,
        capture_output=True,
    )
    console_output = result.stdout.decode('utf-8')
    cloud_logging_client.info(f'ISyntax conversion output: {console_output}')
  except subprocess.CalledProcessError as process_exp:
    cloud_logging_client.error('Error running syntax conversion.', process_exp)
    return False
  finally:
    os.chdir(current_dir)
  local_upload_filename, _ = os.path.splitext(local_filename)
  local_upload_filename = f'{local_upload_filename}.tiff'
  if not os.path.exists(local_upload_filename):
    cloud_logging_client.error(
        f'ISyntax conversion failed. {local_upload_filename} does not exist.'
    )
    return False
  upload_blob_path = f'{source_blob_name}.tiff'
  upload_blob_path = upload_blob_path.lstrip('/')
  output_bucket = OUTPUT_BUCKET_FLG.value
  try:
    blob = storage.Blob.from_string(OUTPUT_BUCKET_FLG.value)
    bucket_name = blob.bucket.name
    base_blob_path = blob.name.rstrip('/')
    upload_blob_path = f'{base_blob_path}/{upload_blob_path}'
  except ValueError:
    bucket_name = storage.Bucket.from_string(output_bucket).name.rstrip('/')
  cloud_logging_client.info(
      'Uploading converted file to bucket:'
      f' gs://{bucket_name}/{upload_blob_path}'
  )
  bucket = client.bucket(bucket_name)
  blob = bucket.blob(upload_blob_path)
  blob.upload_from_filename(local_upload_filename)
  return True


def _ack(
    pubsub_subscriber: Optional[pubsub_v1.SubscriberClient],
    subscription_path,
    message: pubsub_v1.types.ReceivedMessage,
) -> None:
  if pubsub_subscriber is None:
    return
  pubsub_subscriber.acknowledge(
      request={
          'subscription': subscription_path,
          'ack_ids': [message.ack_id],
      }
  )


def main(argv: Sequence[str]) -> None:
  cloud_logging_client.set_per_thread_log_signatures(False)
  cloud_logging_client.set_log_trace_key(_ISYNTAX_CONVERSION_TRACE_ID)
  blob_list = list(argv[1:]) if len(argv) > 1 else []
  process_file_list = bool(blob_list)
  while _GKE_RUNNING and (not process_file_list or blob_list):
    with contextlib.ExitStack() as stack:
      if process_file_list:
        pubsub_subscriber = None
        blob = storage.Blob.from_string(blob_list.pop())
        filename = blob.name
        bucket_name = blob.bucket.name
        subscription_path = ''
        message = pubsub_v1.types.ReceivedMessage()
      else:
        pubsub_subscriber = stack.enter_context(pubsub_v1.SubscriberClient())
        subscription_path = pubsub_subscriber.subscription_path(
            PUBSUB_SUBSCRIPTION_PROJECT_ID_FLG.value,
            PUBSUB_SUBSCRIPTION_FLG.value,
        )
        response = pubsub_subscriber.pull(
            request={
                'subscription': subscription_path,
                'max_messages': 1,
            },
            return_immediately=False,
            retry=retry.Retry(deadline=1000),
        )
        if not response.received_messages:
          cloud_logging_client.clear_log_signature()
          cloud_logging_client.info('No messages received from pub/sub.')
          time.sleep(30)
          continue
        message = response.received_messages[0]
        cloud_logging_client.set_log_signature({
            _PUBSUB_MESSAGE_ID: message.message_id,
            _ISYNTAX_CONVERSION_TRACE_ID: message.message_id,
        })
        event_type = message.attributes.get('eventType', '')
        if event_type != 'OBJECT_FINALIZE':
          cloud_logging_client.info(
              f'Ignoring pub/sub msg with event type: {event_type}'
          )
          _ack(pubsub_subscriber, subscription_path, message)
          continue
        try:
          pubsub_msg_data_dict = json.loads(message.data.decode('utf-8'))
        except json.JSONDecodeError as json_decode_exception:
          cloud_logging_client.error(
              'Error decoding pub/sub msg.', json_decode_exception
          )
          _ack(pubsub_subscriber, subscription_path, message)
          continue
        try:
          filename = pubsub_msg_data_dict['name']
          bucket_name = pubsub_msg_data_dict['bucket']
        except (ValueError, TypeError, KeyError) as pub_sub_msg_decode_exp:
          cloud_logging_client.error(
              'Error decoding pub/sub msg.',
              pubsub_msg_data_dict,
              pub_sub_msg_decode_exp,
          )
          _ack(pubsub_subscriber, subscription_path, message)
          continue
      cloud_logging_client.info(
          f'Received pub/sub msg; filename: {filename}, bucket_name:'
          f' {bucket_name}'
      )

      client = storage.Client()
      bucket = client.get_bucket(bucket_name)
      with tempfile.TemporaryDirectory() as tmp_dir:
        with PubSubKeepAliveThread(
            pubsub_subscriber, subscription_path, message.ack_id
        ):
          local_filename = os.path.join(tmp_dir, os.path.basename(filename))
          try:
            bucket.blob(filename).download_to_filename(local_filename)
            success = _convert_isyntax_to_tiff(client, local_filename, filename)
          except:
            success = False
            raise
          finally:
            if success:  # pylint: disable=undefined-variable
              dest_uri = INGEST_SUCCEEDED_URI_FLG.value
              status = 'complete'
            else:
              dest_uri = INGEST_FAILED_URI_FLG.value
              status = 'failed'
            source_uri = f'gs://{bucket_name}/{filename}'
            if (
                dest_uri
                and cloud_storage_client.copy_blob_to_uri(
                    source_uri=source_uri,
                    dst_uri=dest_uri,
                    local_source=local_filename,
                    destination_blob_filename=filename,
                )
                and DELETE_FROM_SOURCE_BUCKET__FLG.value
            ):
              cloud_storage_client.del_blob(
                  uri=source_uri, ignore_file_not_found=True
              )
            _ack(pubsub_subscriber, subscription_path, message)
            cloud_logging_client.info(f'Conversion {status}: {filename}')


if __name__ == '__main__':
  try:
    absl_app.run(main)
  except Exception as exp:
    cloud_logging_client.error(
        'Exception raised in isyntax conversion server', exp
    )
    raise
