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
"""Update metadata and replace DICOM instances in DICOM store."""
import dataclasses
import os
import re
from typing import Mapping, Optional

from google.cloud import pubsub_v1
import requests

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import abstract_polling_client
from transformation_pipeline.ingestion_lib import cloud_storage_client
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_dicom
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client
from transformation_pipeline.ingestion_lib.pubsub_msgs import abstract_pubsub_msg
from transformation_pipeline.ingestion_lib.pubsub_msgs import dicom_store_pubsub_msg


_DICOM_STORE_INSTANCE_PATTERN = re.compile(
    '(.*(projects/.*/locations/.*/datasets/.*/dicomStores/.*/dicomWeb))/studies/(.*)/series/(.*)/instances/(.*)'
)

_DICOMWEB_RESOURCE_PATTERN = re.compile('projects/.*/dicomWeb')


@dataclasses.dataclass(frozen=True)
class _IngestionInstance:
  gcs_recovery_uri: str
  viewer_debug_url: str
  dicom_store_client: Optional[dicom_store_client.DicomStoreClient]


class IngestDicomStorePubSubHandler(
    abstract_dicom_generation.AbstractDicomGeneration
):
  """Handler to update metadata and replace DICOM instances in DICOM store."""

  def __init__(
      self, metadata_client: metadata_storage_client.MetadataStorageClient
  ):
    super().__init__(dicom_store_web_path='')
    # Current ingestion instance (i.e. corresponding to last decoded pub/sub
    # message).
    self._current_instance = _IngestionInstance('', '', None)
    self._ingest_dicom_handler = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=True,
        override_study_uid_with_metadata=False,
        metadata_client=metadata_client,
    )
    if not ingest_flags.DICOM_STORE_INGEST_GCS_URI_FLG.value:
      err_msg = (
          '--dicom_store_ingest_gcs_uri flag or DICOM_STORE_INGEST_GCS_URI env '
          'variable must be set for DICOM store ingestion.'
      )
      cloud_logging_client.critical(err_msg)
      raise ValueError(err_msg)
    self._gcs_recovery_uri = (
        ingest_flags.DICOM_STORE_INGEST_GCS_URI_FLG.value.strip()
    )

  def decode_pubsub_msg(
      self, msg: pubsub_v1.types.ReceivedMessage
  ) -> abstract_pubsub_msg.AbstractPubSubMsg:
    """Returns decoded pub/sub message.

    Args:
      msg: Pubsub msg to decode.

    Returns:
      DICOM store pub/sub message.
    """
    dicom_msg = dicom_store_pubsub_msg.DicomStorePubSubMsg(msg)
    if not dicom_msg.filename:
      dicom_msg.ignore = True
      return dicom_msg
    # Message filename corresponds to DICOM instance resource in DICOM Store:
    # projects/<proj>/locations/<..>/datasets/<..>/dicomStores/<..>/dicomWeb/
    # studies/<study uid>/series/<series uid>/instances/<sop instance uid>
    msg_gcs_recovery_uri = os.path.join(
        self._gcs_recovery_uri, f'{dicom_msg.filename}.dcm'
    )
    m = _DICOM_STORE_INSTANCE_PATTERN.match(dicom_msg.uri)
    if m is None or len(m.groups()) != 5:
      cloud_logging_client.error(
          f'Invalid data in DICOM store pub/sub message: {dicom_msg.filename}'
      )
      dicom_msg.ignore = True
      return dicom_msg
    (
        dcm_webpath,
        dicomweb_resource,
        study_instance_uid,
        series_instance_uid,
        sop_instance_uid,
    ) = m.groups(default='')
    if sop_instance_uid.startswith(ingest_const.DPAS_UID_PREFIX):
      # DICOM instance already ingested
      dicom_msg.ignore = True
      return dicom_msg
    msg_viewer_debug_url = _DICOMWEB_RESOURCE_PATTERN.sub(
        dicomweb_resource, self._viewer_debug_url
    )
    msg_dicom_store_client = dicom_store_client.DicomStoreClient(dcm_webpath)

    try:
      if self._ingest_dicom_handler.is_dicom_instance_already_ingested(
          msg_dicom_store_client,
          study_instance_uid,
          series_instance_uid,
          sop_instance_uid,
      ):
        # DICOM instance already ingested
        dicom_msg.ignore = True
        return dicom_msg
    except ingest_dicom.UnexpectedDicomMetadataError:
      pass
    self._current_instance = _IngestionInstance(
        msg_gcs_recovery_uri, msg_viewer_debug_url, msg_dicom_store_client
    )
    cloud_logging_client.debug(
        'Decoded DICOM store pub/sub msg.',
        {ingest_const.LogKeywords.URI: dicom_msg.uri},
    )
    return dicom_msg

  def get_pubsub_file(
      self, uri: str, download_filepath: str
  ) -> abstract_dicom_generation.GeneratedDicomFiles:
    """Downloads pub/sub referenced files to container.

    Args:
      uri: pub/sub resource URI to download file from.
      download_filepath: Path to download file to.

    Returns:
      GeneratedDicomFiles with the downloaded file.

    Raises:
      abstract_dicom_generation.FileDownloadError: if failed to download.
    """
    if not self._current_instance.dicom_store_client:
      raise abstract_dicom_generation.FileDownloadError(
          f'Unable to download DICOM from {uri}: DICOM store client undefined.'
      )
    read_from_dcm_store = False
    try:
      self._current_instance.dicom_store_client.download_instance_from_uri(
          uri, download_filepath
      )
      read_from_dcm_store = True
    except requests.HTTPError as exp:
      cloud_logging_client.warning(
          f'Failed to download DICOM from {uri} with error {exp}. '
          'Attempting to download from recovery GCS: '
          f'{self._current_instance.gcs_recovery_uri}'
      )
      if not cloud_storage_client.download_to_container(
          self._current_instance.gcs_recovery_uri, download_filepath
      ):
        raise abstract_dicom_generation.FileDownloadError(
            f'Failed to download {uri} DICOM from both DICOM store and GCS.',
            exp,
        )
    # Write to GCS temporarily. Will be deleted once updated instance is
    # uploaded to DICOM store.
    if read_from_dcm_store:
      cloud_logging_client.debug(
          f'Downloaded DICOM from {uri}. Writing to recovery GCS: '
          f'{self._current_instance.gcs_recovery_uri}'
      )
      if not cloud_storage_client.upload_blob_to_uri(
          download_filepath, self._current_instance.gcs_recovery_uri
      ):
        raise abstract_dicom_generation.FileDownloadError(
            f'Failed to write {uri} DICOM to recovery GCS: '
            f'{self._current_instance.gcs_recovery_uri}.'
        )
    return abstract_dicom_generation.GeneratedDicomFiles(download_filepath, uri)

  def _delete_recovery_instance(self):
    if cloud_storage_client.del_blob(self._current_instance.gcs_recovery_uri):
      cloud_logging_client.debug(
          'Deleted temporary DICOM instance from GCS: '
          f'{self._current_instance.gcs_recovery_uri}.'
      )
    else:
      cloud_logging_client.error(
          'Failed to delete temporary DICOM instance from GCS:'
          f' {self._current_instance.gcs_recovery_uri}. Instance may have'
          ' already been deleted or will require manual deletion.'
      )

  def _update_dicom_instance_in_dicom_store(
      self,
      dcm: ingest_base.GenDicomResult,
      gcs_metadata: Optional[Mapping[str, str]] = None,
  ):
    """Uploads updated DICOM instance to DICOM store.

    Original DICOM is deleted and updated DICOM uploaded from DICOM store.
    Original (recovery) DICOM is deleted from GCS only if upload is
    successful.

    Args:
      dcm: DICOM result to be uploaded.
      gcs_metadata: Optional metadata to attach to for files copied to GCS.
    """
    if (
        not self._current_instance.gcs_recovery_uri
        or not self._current_instance.dicom_store_client
        or not dcm.files_to_upload.main_store_instances
    ):
      return
    # Delete original DICOM from store.
    if not self._current_instance.dicom_store_client.delete_resource_from_dicom_store(
        dcm.dicom_gen.source_uri
    ):
      cloud_logging_client.warning(
          'Failed to delete original DICOM instance from DICOM store: '
          f'{dcm.dicom_gen.source_uri}. Instance may have already been '
          'deleted.'
      )
    # Write updated DICOM to store.
    cloud_logging_client.info(
        f'Uploading {dcm.dicom_gen.source_uri} to DICOM store.'
    )
    try:
      upload_result = (
          self._current_instance.dicom_store_client.upload_to_dicom_store(
              dicom_paths=dcm.files_to_upload.main_store_instances,
              discover_existing_series_option=(
                  dicom_store_client.DiscoverExistingSeriesOptions.IGNORE
              ),
              copy_to_bucket_metadata=gcs_metadata,
          )
      )
      if upload_result.slide_has_instances_in_dicom_store():
        self.log_debug_url(
            viewer_debug_url=self._current_instance.viewer_debug_url,
            ingested_dicom=upload_result.slide_instances_in_dicom_store[0],
        )
    except (requests.HTTPError, dicom_store_client.DicomUploadToGcsError):
      # Either DICOM Store upload or GCS copy failure.
      return
    if upload_result.previously_ingested:  # DICOM instance already in store.
      return
    # Delete temporary DICOM from GCS.
    self._delete_recovery_instance()

  def handle_unexpected_exception(
      self,
      msg: abstract_pubsub_msg.AbstractPubSubMsg,
      ingest_file: Optional[abstract_dicom_generation.GeneratedDicomFiles],
      exp: Exception,
  ):
    """See base class."""
    cloud_logging_client.critical(
        (
            'An unexpected exception occurred during DICOM store ingestion.'
            ' Recovery instance will remain in GCS:'
            f' {self._current_instance.gcs_recovery_uri}'
        ),
        {
            ingest_const.LogKeywords.URI: msg.uri,
            ingest_const.LogKeywords.PUBSUB_MESSAGE_ID: msg.message_id,
        },
        exp,
    )

  def get_slide_transform_lock(
      self,
      ingest_file: abstract_dicom_generation.GeneratedDicomFiles,
      polling_client: abstract_polling_client.AbstractPollingClient,
  ) -> abstract_dicom_generation.TransformationLock:
    """Returns slide transform lock for direct dicom transformation.

       Handles pub/sub messages for store. Ensure that an instance cannot be
       processed on the same thread twice.

    Args:
      ingest_file: File payload to generate into DICOM.
      polling_client: Polling client receiving triggering pub/sub msg.

    Returns:
      TransformationLock
    """
    if not self._current_instance.gcs_recovery_uri:
      cloud_logging_client.error(
          'Unable to ingest instance: missing DICOM instance GCS recovery URI.'
      )
      polling_client.ack()
      return abstract_dicom_generation.TransformationLock()
    if not self._current_instance.dicom_store_client:
      cloud_logging_client.error(
          'Unable to ingest instance: DICOM store client undefined.'
      )
      self._delete_recovery_instance()
      polling_client.ack()
      return abstract_dicom_generation.TransformationLock()

    if self._ingest_dicom_handler.is_dicom_file_already_ingested(
        ingest_file.localfile
    ):
      cloud_logging_client.debug('DICOM instance already ingested.')
      self._delete_recovery_instance()
      polling_client.ack()
      return abstract_dicom_generation.TransformationLock()

    cloud_logging_client.info('Processing DICOM store ingestion.')
    try:
      self._ingest_dicom_handler.update_metadata()
    except metadata_storage_client.MetadataDownloadExceptionError as exp:
      cloud_logging_client.error(
          'Error downloading metadata. Ignoring DICOM store pub/sub message.',
          exp,
      )
      self._delete_recovery_instance()
      polling_client.ack()
      return abstract_dicom_generation.TransformationLock()

    self._ingest_dicom_handler.init_handler_for_ingestion()
    try:
      slide_id = self._ingest_dicom_handler.get_slide_id(ingest_file, self)
    except ingest_base.DetermineSlideIDError as exp:
      cloud_logging_client.error(
          'Cannot find metadata for DICOM instance. Ignoring DICOM store'
          ' pub/sub message.',
          exp,
      )
      self._delete_recovery_instance()
      polling_client.ack()
      return abstract_dicom_generation.TransformationLock()
    slide_lock_str = (
        ingest_const.RedisLockKeywords.DICOM_STORE_TRIGGERED_INGESTION
        % (ingest_file.source_uri, slide_id)
    )
    return abstract_dicom_generation.TransformationLock(slide_lock_str)

  def generate_dicom_and_push_to_store(
      self,
      transform_lock: abstract_dicom_generation.TransformationLock,
      ingest_file: abstract_dicom_generation.GeneratedDicomFiles,
      polling_client: abstract_polling_client.AbstractPollingClient,
  ):
    """Updates DICOM instance with metadata and re-uploads to DICOM store.

    Args:
      transform_lock: Transformation pipeline lock.
      ingest_file: File payload to generate into DICOM.
      polling_client: Polling client receiving triggering pub/sub msg.
    """
    dicom_gen_dir = os.path.join(self.root_working_dir, 'gen_dicom')
    os.mkdir(dicom_gen_dir)
    dcm = self._ingest_dicom_handler.generate_dicom(
        dicom_gen_dir, ingest_file, polling_client.current_msg.message_id, self
    )
    if not self.validate_redis_lock_held(transform_lock):
      polling_client.nack(
          retry_ttl=ingest_flags.TRANSFORMATION_LOCK_RETRY_FLG.value
      )
      self._delete_recovery_instance()
      return
    self._update_dicom_instance_in_dicom_store(
        dcm,
        gcs_metadata={
            'pubsub_message_id': str(polling_client.current_msg.message_id)
        },
    )
    polling_client.ack()
