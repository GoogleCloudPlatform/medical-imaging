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
"""Converts image to DICOM. Conversion based on file extension."""
import dataclasses
import os
import re
from typing import FrozenSet, Mapping, Optional
import zipfile

from google.cloud import pubsub_v1
import pydicom
import tifffile

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import abstract_polling_client
from transformation_pipeline.ingestion_lib import cloud_storage_client
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import gcs_storage_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_dicom
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_flat_image
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_svs
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_wsi_dicom
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client
from transformation_pipeline.ingestion_lib.pubsub_msgs import abstract_pubsub_msg
from transformation_pipeline.ingestion_lib.pubsub_msgs import cloud_storage_pubsub_msg
from transformation_pipeline.ingestion_lib.pubsub_msgs import ingestion_complete_pubsub


def _get_ignored_gcs_upload_file_exts() -> FrozenSet[str]:
  """Returns file extensions which should be ignored by gcs ingestion."""
  candidate_extensions = ingest_flags.GCS_UPLOAD_IGNORE_FILE_EXTS_FLG.value
  if not candidate_extensions:
    return frozenset()
  found_extensions = set()
  for test_ext in candidate_extensions:
    test_ext = test_ext.strip('\t "\'')
    if test_ext and not test_ext.startswith('.'):
      cloud_logging_client.error(
          (
              f'ENV {ingest_const.EnvVarNames.GCS_UPLOAD_IGNORE_FILE_EXT} contains'
              ' a non-empty file extension that does not start with a ".".'
              ' Value will be ignored.'
          ),
          {
              ingest_const.EnvVarNames.GCS_UPLOAD_IGNORE_FILE_EXT: (
                  candidate_extensions
              ),
              'file_extension': test_ext,
          },
      )
      continue
    found_extensions.add(test_ext.lower())
  if found_extensions:
    ignored_extensions = '", "'.join(sorted(list(found_extensions)))
    cloud_logging_client.info(
        (
            'GCS ingestion is configured to ignore files with extensions: '
            f'"{ignored_extensions}"'
        ),
        {
            ingest_const.EnvVarNames.GCS_UPLOAD_IGNORE_FILE_EXT: (
                candidate_extensions
            )
        },
    )
  return frozenset(found_extensions)


_DICOM_INGEST_EXTENSIONS = frozenset(['.dicom', '.dcm', '.zip'])
_FLAT_IMAGE_INGEST_EXTENSIONS = frozenset(['.jpg', '.jpeg', '.png'])
_WSI_INGEST_EXTENSIONS = frozenset(['.ndpi', '.svs'])
_TIFF_EXTENSIONS = frozenset(['.tif', '.tiff'])
# See TIFF tags in
# https://www.loc.gov/preservation/digital/formats/content/tiff_tags.shtml
_TIFF_TILE_TAGS = frozenset([322, 323, 324, 325])


class StorageBucketNotSpecifiedError(Exception):
  pass


class InferenceTriggerConfigError(Exception):
  pass


@dataclasses.dataclass(frozen=True)
class InferenceTriggerConfig:
  """Config for triggering inference pipeline."""

  dicom_store_web_path: str
  pubsub_topic: str
  use_oof_legacy_pipeline: bool = True
  inference_config_path: str = ''
  _dicom_store = None
  _inference_config = None

  def validate(self):
    """Validates config.

    Raises:
      InferenceTriggerConfigError: if config is not valid.
    """
    if not self.dicom_store_web_path:
      raise InferenceTriggerConfigError(
          'Missing inference pipeline DICOM store web path. Enabling OOF '
          f'requires {ingest_const.EnvVarNames.OOF_DICOMWEB_BASE_URL} '
          '(value may equal main store path).'
      )
    object.__setattr__(
        self,
        '_dicom_store',
        dicom_store_client.DicomStoreClient(self.dicom_store_web_path),
    )
    if not self.pubsub_topic:
      raise InferenceTriggerConfigError(
          'Missing inference pipeline Pub/Sub topic. Enabling OOF requires '
          f'{ingest_const.EnvVarNames.INGEST_COMPLETE_OOF_TRIGGER_PUBSUB_TOPIC}'
      )
    if not self.use_oof_legacy_pipeline and not self.inference_config_path:
      raise InferenceTriggerConfigError(
          'Missing inference pipeline config path. Enabling OOF requires '
          f'{ingest_const.EnvVarNames.OOF_INFERENCE_CONFIG_PATH}.'
          ' Alternatively, to use legacy pipeline, set '
          '--oof_legacy_inference_pipeline=true.'
      )
    if self.inference_config_path:
      object.__setattr__(
          self,
          '_inference_config',
          ingestion_complete_pubsub.read_inference_pipeline_config_from_json(
              self.inference_config_path
          ),
      )
    cloud_logging_client.info(f'OOF ingestion is enabled with config: {self}.')

  @property
  def inference_config(self):
    """Inference config to be used in pub/sub messages to inference pipeline."""
    return self._inference_config

  @property
  def dicom_store(self):
    return self._dicom_store


@dataclasses.dataclass(frozen=True)
class _UploadToDicomStoresResult:
  main_store_results: dicom_store_client.UploadSlideToDicomStoreResults
  oof_ingest_results: Optional[
      dicom_store_client.UploadSlideToDicomStoreResults
  ]
  ingest_complete_oof_trigger_msg: Optional[ingestion_complete_pubsub.PubSubMsg]


class IngestGcsPubSubHandler(abstract_dicom_generation.AbstractDicomGeneration):
  """Creates or updates DICOM instance and stores into DICOM Store.

  Conversion based on ingested file ext.
  """

  def __init__(
      self,
      ingest_succeeded_uri: str,
      ingest_failed_uri: str,
      dicom_store_web_path: str,
      ingest_ignore_root_dirs: FrozenSet[str],
      metadata_client: metadata_storage_client.MetadataStorageClient,
      oof_trigger_config: Optional[InferenceTriggerConfig] = None,
  ):
    """Constructor.

    Args:
      ingest_succeeded_uri: Path to move input to if ingestion succeeds.
      ingest_failed_uri: Path to move input to if ingestion fails.
      dicom_store_web_path: DICOM Store to upload ingestion images to.
      ingest_ignore_root_dirs: Directories to ignore for ingestion of images.
      metadata_client: Metadata storage client.
      oof_trigger_config: Config for triggering OOF inference pipeline at the
        end of WSI ingestion.

    Raises:
      ValueError if any of the parameters is invalid.
    """
    super().__init__(dicom_store_web_path)
    if oof_trigger_config:
      try:
        oof_trigger_config.validate()
      except InferenceTriggerConfigError as exp:
        cloud_logging_client.warning('OOF ingestion is disabled.', exp)
        oof_trigger_config = None
    self._oof_trigger_config = oof_trigger_config

    self._ingest_buckets = self._verify_success_and_failure_buckets(
        ingest_succeeded_uri, ingest_failed_uri
    )
    self._ignored_gcs_upload_file_extensions = (
        _get_ignored_gcs_upload_file_exts()
    )
    self._ingest_ignore_root_dirs = ingest_ignore_root_dirs
    self._decoded_file_ext = None
    if (
        ingest_flags.GCS_INGEST_STUDY_INSTANCE_UID_SOURCE_FLG.value
        == ingest_flags.UidSource.METADATA
    ):
      override_study_uid_with_metadata = True
    elif (
        ingest_flags.GCS_INGEST_STUDY_INSTANCE_UID_SOURCE_FLG.value
        == ingest_flags.UidSource.DICOM
    ):
      override_study_uid_with_metadata = False
    else:
      raise ValueError(
          'Unsupported StudyInstanceUID source value for GCS ingestion: '
          f'{ingest_flags.GCS_INGEST_STUDY_INSTANCE_UID_SOURCE_FLG.value}.'
      )

    self._ingest_wsi_handler = ingest_svs.IngestSVS(
        self._ingest_buckets, self.is_oof_ingestion_enabled, metadata_client
    )
    self._ingest_flat_handler = ingest_flat_image.IngestFlatImage(
        self._ingest_buckets, metadata_client
    )
    self._ingest_dicom_handler = ingest_dicom.IngestDicom(
        dicom_store_triggered_ingest=False,
        override_study_uid_with_metadata=override_study_uid_with_metadata,
        metadata_client=metadata_client,
        ingest_buckets=self._ingest_buckets,
    )
    self._ingest_wsi_dicom_handler = ingest_wsi_dicom.IngestWsiDicom(
        self._ingest_buckets,
        self.is_oof_ingestion_enabled,
        override_study_uid_with_metadata,
        metadata_client,
    )
    self._current_handler: Optional[ingest_base.IngestBase] = None
    if (
        ingest_flags.GCS_IGNORE_FILE_REGEXS_FLG.value is not None
        and ingest_flags.GCS_IGNORE_FILE_REGEXS_FLG.value
    ):
      self._gcs_ignore_file_regexes = [
          re.compile(regex)
          for regex in ingest_flags.GCS_IGNORE_FILE_REGEXS_FLG.value
      ]
    else:
      self._gcs_ignore_file_regexes = []

  @property
  def current_handler(self) -> Optional[ingest_base.IngestBase]:
    return self._current_handler

  @current_handler.setter
  def current_handler(self, val: Optional[ingest_base.IngestBase]) -> None:
    self._current_handler = val

  def _init_gcs_handler_ingestion(self) -> None:
    self._current_handler = None
    self._decoded_file_ext = None

  @property
  def is_oof_ingestion_enabled(self) -> bool:
    """Returns true if OOF ingestion is enabled."""
    return self._oof_trigger_config is not None

  @property
  def decoded_file_ext(self) -> Optional[str]:
    """Returns file extension of last decoded pub/sub message."""
    return self._decoded_file_ext

  def failure_bucket_exception_path(self, exception_str: str) -> str:
    if not self._ingest_buckets.failure_uri:
      return ''
    return f'{self._ingest_buckets.failure_uri}/{exception_str}'

  def _test_ignore_file_regex(self, filename: str) -> bool:
    for index, regex in enumerate(self._gcs_ignore_file_regexes):
      if regex.fullmatch(filename) is not None:
        cloud_logging_client.debug(
            'Name of the file uploaded to ingestion bucket matched a ignore'
            ' file regular expression.',
            {
                ingest_const.LogKeywords.MATCHED_REGEX: (
                    ingest_flags.GCS_IGNORE_FILE_REGEXS_FLG.value[index]
                ),
                ingest_const.LogKeywords.GCS_IGNORE_FILE_REGEXS: (
                    ingest_flags.GCS_IGNORE_FILE_REGEXS_FLG.value
                ),
                ingest_const.LogKeywords.FILENAME: filename,
            },
        )
        return True
    return False

  def _move_file_to_ignore_bucket(
      self, storage_msg: cloud_storage_pubsub_msg.CloudStoragePubSubMsg
  ) -> None:
    ignore_file_bucket = ingest_flags.GCS_IGNORE_FILE_BUCKET_FLG.value
    if not ignore_file_bucket:
      return
    dst_blob_metadata = {'pubsub_message_id': str(storage_msg.message_id)}
    struct_msg = {
        ingest_const.LogKeywords.URI: storage_msg.uri,
        ingest_const.LogKeywords.IGNORE_FILE_BUCKET: ignore_file_bucket,
    }
    if not cloud_storage_client.copy_blob_to_uri(
        source_uri=storage_msg.uri,
        local_source=storage_msg.filename,
        dst_uri=ignore_file_bucket,
        dst_metadata=dst_blob_metadata,
    ):
      cloud_logging_client.error(
          'Failed to copy file to ignore bucket.', struct_msg
      )
    elif not cloud_storage_client.del_blob(
        uri=storage_msg.uri, ignore_file_not_found=True
    ):
      cloud_logging_client.error(
          'Failed to delete file from ignore bucket.', struct_msg
      )
    else:
      cloud_logging_client.info('File moved to ignore bucket.', struct_msg)

  def decode_pubsub_msg(
      self, msg: pubsub_v1.types.ReceivedMessage
  ) -> abstract_pubsub_msg.AbstractPubSubMsg:
    """Returns decoded GCS pub/sub message.

    Args:
      msg: Pubsub msg to decode.

    Returns:
      Implementation of CloudStoragePubSubMsg
    """
    self._init_gcs_handler_ingestion()
    storage_msg = cloud_storage_pubsub_msg.CloudStoragePubSubMsg(msg)
    if storage_msg.event_type != 'OBJECT_FINALIZE':
      cloud_logging_client.warning(
          (
              'Pub/sub subscription received message with event_type != '
              'OBJECT_FINALIZE. Set subscription to publish only msgs with '
              'OBJECT_FINALIZE event type.'
          ),
          {
              ingest_const.LogKeywords.RECEIVED_EVENT_TYPE: (
                  storage_msg.event_type
              )
          },
      )
      storage_msg.ignore = True
      return storage_msg
    if not storage_msg.filename or not storage_msg.bucket_name:
      storage_msg.ignore = True
      return storage_msg
    _, self._decoded_file_ext = os.path.splitext(
        os.path.basename(storage_msg.filename)
    )
    if self._decoded_file_ext:
      self._decoded_file_ext = self._decoded_file_ext.strip().lower()
    file_parts = storage_msg.filename.split('/')
    if self._decoded_file_ext in self._ignored_gcs_upload_file_extensions:
      cloud_logging_client.warning(
          (
              'A file with an ignored file extension was uploaded to the'
              ' ingestion bucket.'
          ),
          {
              ingest_const.LogKeywords.FILE_EXTENSION: self._decoded_file_ext,
              ingest_const.LogKeywords.URI: storage_msg.uri,
          },
      )
      storage_msg.ignore = True
      self._move_file_to_ignore_bucket(storage_msg)
    elif self._test_ignore_file_regex(storage_msg.filename):
      cloud_logging_client.warning(
          (
              'A file matching the ignored file regular expression was '
              'uploaded to the ingestion bucket.'
          ),
          {ingest_const.LogKeywords.URI: storage_msg.uri},
      )
      storage_msg.ignore = True
      self._move_file_to_ignore_bucket(storage_msg)
    elif len(file_parts) > 1:
      storage_msg.ignore = file_parts[0] in self._ingest_ignore_root_dirs
    cloud_logging_client.info(
        'Decoded cloud storage pub/sub msg.',
        {ingest_const.LogKeywords.URI: storage_msg.uri},
    )
    return storage_msg

  def _verify_success_and_failure_buckets(
      self, ingest_succeeded_uri: str, ingest_failed_uri: str
  ) -> ingest_base.GcsIngestionBuckets:
    """Verifies success and failure buckets are defined.

    Args:
      ingest_succeeded_uri: GCS URI to success bucket.
      ingest_failed_uri: GCS URI to failure bucket.

    Returns:
      GcsIngestionBuckets on success.

    Raises:
      StorageBucketNotSpecifiedError: if buckets not defined.
    """
    bucket_not_specified = []
    if not ingest_succeeded_uri:
      bucket_not_specified.append('success')
    if not ingest_failed_uri:
      bucket_not_specified.append('failure')
    if bucket_not_specified:
      bucket_not_specified_msg = ' and '.join(bucket_not_specified)
      msg = (
          f'Ingest {bucket_not_specified_msg} bucket(s) env variable and '
          'command line param not specified.'
      )
      cloud_logging_client.critical(msg)
      raise StorageBucketNotSpecifiedError(msg)
    return ingest_base.GcsIngestionBuckets(
        success_uri=ingest_succeeded_uri, failure_uri=ingest_failed_uri
    )

  def _is_wsi_dicom(self, dicom_path: str) -> bool:
    """Returns true for ZIP and WSI DICOM files.

    Defaults to False if unable to read file.

    Args:
      dicom_path: Path to a DICOM file.
    """
    if zipfile.is_zipfile(dicom_path):
      return True
    try:
      with pydicom.dcmread(dicom_path, defer_size='512 KB', force=False) as dcm:
        return (
            dcm[ingest_const.DICOMTagKeywords.SOP_CLASS_UID].value
            == ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
        )
    except (
        KeyError,
        TypeError,
        FileNotFoundError,
        pydicom.errors.InvalidDicomError,
    ) as exp:
      cloud_logging_client.warning(
          'Unable to read DICOM SOP class UID. Defaulting to non-WSI.', exp
      )
      return False

  def _is_flat_image(self, image_path: str) -> bool:
    """Returns true for TIFF files with a single frame and no tile tags.

    Args:
      image_path: Path to a TIFF image.
    """
    try:
      with tifffile.TiffFile(image_path) as tiff:
        if len(tiff.pages) != 1:
          return False
        if _TIFF_TILE_TAGS.intersection(tiff.pages[0].tags.keys()):
          return False
        return True
    except FileNotFoundError:
      return False
    except tifffile.TiffFileError:
      return False

  def _get_message_dicom_handler(
      self, ingest_file: abstract_dicom_generation.GeneratedDicomFiles
  ) -> ingest_base.IngestBase:
    """Returns DICOM handler for last pub/sub message file extension.

    Defaults to WSI ingestion for unknown file extensions.

    Args:
      ingest_file: File payload to generate into DICOM.
    """
    # Inspect DICOM files to decide between WSI and generic DICOM ingestion.
    if self.decoded_file_ext in _DICOM_INGEST_EXTENSIONS:
      if self._is_wsi_dicom(ingest_file.localfile):
        return self._ingest_wsi_dicom_handler
      else:  # Default to generic DICOM ingestion.
        return self._ingest_dicom_handler
    if self.decoded_file_ext in _FLAT_IMAGE_INGEST_EXTENSIONS:
      return self._ingest_flat_handler
    # Inspect TIFF files to decide between WSI and flat image ingestion.
    if self.decoded_file_ext in _TIFF_EXTENSIONS:
      if self._is_flat_image(ingest_file.localfile):
        return self._ingest_flat_handler
      else:
        return self._ingest_wsi_handler
    if self.decoded_file_ext in _WSI_INGEST_EXTENSIONS:
      return self._ingest_wsi_handler
    # Default file handler
    cloud_logging_client.warning(
        f'Using default WSI file handler. Image format: {self.decoded_file_ext}'
    )
    return self._ingest_wsi_handler

  def handle_unexpected_exception(
      self,
      msg: abstract_pubsub_msg.AbstractPubSubMsg,
      ingest_file: Optional[abstract_dicom_generation.GeneratedDicomFiles],
      exp: Exception,
  ):
    """Moves input to failure bucket in response to unexpected error.

    Args:
      msg: Current pub/sub message being processed.
      ingest_file: File payload to generate into DICOM.
      exp: Exception which triggered method

    Raises:
       gcs_storage_util.CloudStorageBlobMoveError: if file copy or delete fails.
    """
    dest_uri = self.failure_bucket_exception_path(
        ingest_const.ErrorMsgs.UNEXPECTED_EXCEPTION
    )
    dst_blob_metadata = {'pubsub_message_id': str(msg.message_id)}
    if not ingest_file or not cloud_storage_client.blob_exists(
        ingest_file.source_uri
    ):
      additional_error_msg = 'Original file not found.'
    elif not cloud_storage_client.copy_blob_to_uri(
        source_uri=ingest_file.source_uri,
        local_source=ingest_file.localfile,
        dst_uri=dest_uri,
        dst_metadata=dst_blob_metadata,
    ):
      additional_error_msg = 'Failed to copy to failure bucket.'
    # Very last step is to delete blob from ingest bucket.
    elif not cloud_storage_client.del_blob(
        uri=ingest_file.source_uri, ignore_file_not_found=True
    ):
      additional_error_msg = 'Failed to delete from ingest bucket.'
    else:
      additional_error_msg = 'File moved to failure bucket.'
    cloud_logging_client.critical(
        (
            'An unexpected exception occurred during GCS ingestion. '
            f'{additional_error_msg}'
        ),
        {
            ingest_const.LogKeywords.URI: msg.uri,
            ingest_const.LogKeywords.PUBSUB_MESSAGE_ID: msg.message_id,
            ingest_const.LogKeywords.DEST_URI: dest_uri,
        },
        exp,
    )

  def _create_ingest_complete_pubsub_msg(
      self,
      store_results: dicom_store_client.UploadSlideToDicomStoreResults,
      ds_client: Optional[dicom_store_client.DicomStoreClient] = None,
  ) -> Optional[ingestion_complete_pubsub.PubSubMsg]:
    """Creates ingestion complete pub/sub message(OOF trigger).

    Args:
      store_results: DICOM store ingestion results.
      ds_client: DICOM store client imaging was ingested into if null defaults
        to class's DICOM store.

    Returns:
      Pub/sub message if DICOM store results defined ingestion else None
    """
    if not self.is_oof_ingestion_enabled:
      return None
    if not store_results.slide_has_instances_in_dicom_store():
      return None
    if ds_client is None:
      ds_client = self.dcm_store_client
    pipeline_passthrough_params = {}
    pipeline_passthrough_params[
        ingest_const.OofPassThroughKeywords.DISABLE_TFEXAMPLE_WRITE
    ] = True
    pipeline_passthrough_params[
        ingest_const.OofPassThroughKeywords.DPAS_INGESTION_TRACE_ID
    ] = cloud_logging_client.get_log_signature().get(
        ingest_const.LogKeywords.DPAS_INGESTION_TRACE_ID,
        ingest_const.MISSING_INGESTION_TRACE_ID,
    )
    pipeline_passthrough_params[
        ingest_const.OofPassThroughKeywords.SOURCE_DICOM_IN_MAIN_STORE
    ] = (ds_client.dicomweb_path == self.dcm_store_client.dicomweb_path)
    try:
      return ingestion_complete_pubsub.create_ingest_complete_pubsub_msg(
          ds_client.dicomweb_path,
          self._oof_trigger_config.pubsub_topic,
          store_results.ingested,
          store_results.previously_ingested,
          pipeline_passthrough_params,
          self._oof_trigger_config.use_oof_legacy_pipeline,
          self._oof_trigger_config.inference_config,
      )
    except ingestion_complete_pubsub.CreatePubSubMessageError:
      return None

  def _upload_to_dicom_stores(
      self,
      files_to_upload: ingest_base.DicomInstanceIngestionSets,
      discover_series_option: dicom_store_client.DiscoverExistingSeriesOptions,
      dst_metadata: Optional[Mapping[str, str]],
  ) -> _UploadToDicomStoresResult:
    """Uploads generated DICOMS to target DICOM stores (main & OOF).

       Files are uploaded to the OOF store only if OOF describes ingestion
       which differs from main ingestion and OOF ingestion targets a store
       other than the main store. If no OOF store is defined or
       no OOF pub/sub trigger is defined then no files will be uploaded to
       the OOF store.

    Args:
      files_to_upload: Lists of files to upload to main & OOF DICOM stores.
      discover_series_option: Defines approach used to identify. ingested series
        uid.
      dst_metadata: Optional metadata to add to DICOMs copied to GCS.

    Returns:
      _UploadToDicomStoresResult

    Raises:
       requests.HTTPError: DICOM upload failed.
       DicomUploadToGcsError: GCS upload failed.
    """
    ingest_complete_oof_trigger_msg = None
    oof_ingest_results = None
    if (
        files_to_upload.oof_store_instances
        and self.is_oof_ingestion_enabled
        and self._oof_trigger_config.dicom_store.dicomweb_path
        == self.dcm_store_client.dicomweb_path
    ):
      # If OOF is enabled and files are to be uploaded to OOF and the OOF DICOM
      # store and Main store are both defined as the same DICOM store then
      # combine the main and OOF ingestions and ingest files to the main store.
      files_to_upload.combine_main_and_oof_ingestion_and_main_store_sets()

    if not files_to_upload.main_store_instances:
      main_store_results = dicom_store_client.UploadSlideToDicomStoreResults(
          ingested=[], previously_ingested=[]
      )
    else:
      cloud_logging_client.info(
          'Uploading main results.',
          {
              ingest_const.LogKeywords.MAIN_DICOM_STORE: (
                  self.dcm_store_client.dicomweb_path
              )
          },
      )
      main_store_results = self.dcm_store_client.upload_to_dicom_store(
          list(files_to_upload.main_store_instances),
          discover_series_option,
          dst_metadata,
      )
      # Messages is to use to trigger OOF if OOF specific DICOM store is not
      # used and OOF imaging is written into main store only; MSG overwritten
      # if OOF specific ingestion is used.
      ingest_complete_oof_trigger_msg = self._create_ingest_complete_pubsub_msg(
          main_store_results
      )

    if self.is_oof_ingestion_enabled:
      oof_set_includes_files_not_ingested_into_main = (
          files_to_upload.oof_store_instances
          - files_to_upload.main_store_instances
      )
      if oof_set_includes_files_not_ingested_into_main:
        # If OOF trigger (ingestion_complete_topic is defined) and OOF instances
        # are not a subset of the instances uploaded to the main store.
        # OOF upload must occure after actual image ingestion to enable OOF
        # series uid in OOF images to match those in main store if series
        # instance uid were modified during ingestion into the store to merge
        # imaging into a pre-existing series.
        cloud_logging_client.info(
            'Uploading OOF results.',
            {
                ingest_const.LogKeywords.MAIN_DICOM_STORE: (
                    self._oof_trigger_config.dicom_store.dicomweb_path
                )
            },
        )
        dicom_store_client.set_dicom_series_uid(
            files_to_upload.oof_store_instances,
            main_store_results.slide_series_instance_uid_in_dicom_store,
        )

        oof_ingest_results = (
            self._oof_trigger_config.dicom_store.upload_to_dicom_store(
                list(files_to_upload.oof_store_instances),
                not main_store_results.slide_has_instances_in_dicom_store(),
                dst_metadata,
                copy_to_bucket_enabled=False,
            )
        )
        # Overwrite ingest_complete_msg with OOF specific message.
        ingest_complete_oof_trigger_msg = (
            self._create_ingest_complete_pubsub_msg(
                oof_ingest_results,
                self._oof_trigger_config.dicom_store,
            )
        )
      if ingest_complete_oof_trigger_msg is None:
        cloud_logging_client.warning(
            'OOF ingestion is enabled but no OOF pub/sub complete message was'
            ' generated.'
        )
    return _UploadToDicomStoresResult(
        main_store_results, oof_ingest_results, ingest_complete_oof_trigger_msg
    )

  def get_slide_transform_lock(
      self,
      ingest_file: abstract_dicom_generation.GeneratedDicomFiles,
      polling_client: abstract_polling_client.AbstractPollingClient,
  ) -> abstract_dicom_generation.TransformationLock:
    """Returns lock to ensure transform processes only one instance of a slide at a time.

    Args:
      ingest_file: File payload to generate into DICOM.
      polling_client: Polling client receiving triggering pub/sub msg.

    Returns:
      Name of lock
    """
    ingest_file.generated_dicom_files = []
    handler = self._get_message_dicom_handler(ingest_file)
    handler.init_handler_for_ingestion()
    cloud_logging_client.info(
        'Processing ingestion with handler',
        {
            ingest_const.LogKeywords.INGESTION_HANDLER: handler.name,
            ingest_const.LogKeywords.FILE_EXTENSION: str(
                self._decoded_file_ext
            ),
        },
    )
    try:
      handler.update_metadata()
    except metadata_storage_client.MetadataDownloadExceptionError as exp:
      cloud_logging_client.error('Error downloading metadata', exp)
      polling_client.nack()
      return abstract_dicom_generation.TransformationLock()
    self.current_handler = handler
    try:
      return abstract_dicom_generation.TransformationLock(
          ingest_const.RedisLockKeywords.GCS_TRIGGERED_INGESTION
          % handler.get_slide_id(ingest_file, self)
      )
    except ingest_base.DetermineSlideIDError as exp:
      # Move files triggering ingestion to exp.dest_uri failure bucket.
      dicom_result = ingest_base.GenDicomResult(
          exp.dicom_gen,
          exp.dest_uri,
          ingest_base.DicomInstanceIngestionSets([], None, []),
          True,  # Series not generated; meaningless placeholder value
      )
      self._move_ingested_file_to_success_or_failure_bucket(
          dicom_result, polling_client, None
      )
      return abstract_dicom_generation.TransformationLock()

  def generate_dicom_and_push_to_store(
      self,
      transform_lock: abstract_dicom_generation.TransformationLock,
      ingest_file: abstract_dicom_generation.GeneratedDicomFiles,
      polling_client: abstract_polling_client.AbstractPollingClient,
  ):
    """Converts downloaded image to DICOM based on file extension.

    Args:
      transform_lock: Transformation pipeline lock.
      ingest_file: File payload to generate into DICOM.
      polling_client: Polling client receiving triggering pub/sub msg.

    Raises:
      StorageBucketNotSpecifiedError: if storage buckets not defined.
    """
    dicom_gen_dir = os.path.join(self.root_working_dir, 'gen_dicom')
    os.mkdir(dicom_gen_dir)
    dicom = self.current_handler.generate_dicom(
        dicom_gen_dir,
        ingest_file,
        polling_client.current_msg.message_id,
        self,
    )
    cloud_logging_client.info('DICOM generation complete.')
    dst_metadata = {
        'pubsub_message_id': str(polling_client.current_msg.message_id)
    }
    dicom_upload_discover_existing_series_option = (
        dicom_store_client.DiscoverExistingSeriesOptions.USE_HASH
        if dicom.generated_series_instance_uid
        else dicom_store_client.DiscoverExistingSeriesOptions.USE_STUDY_AND_SERIES
    )
    if not self.validate_redis_lock_held(transform_lock):
      polling_client.nack(
          retry_ttl=ingest_flags.TRANSFORMATION_LOCK_RETRY_FLG.value
      )
      return
    try:
      ds_upload_result = self._upload_to_dicom_stores(
          dicom.files_to_upload,
          dicom_upload_discover_existing_series_option,
          dst_metadata,
      )
    except dicom_store_client.DicomUploadToGcsError:
      polling_client.nack(
          retry_ttl=ingest_flags.DICOM_QUOTA_ERROR_RETRY_FLG.value
      )
      return
    self._move_ingested_file_to_success_or_failure_bucket(
        dicom, polling_client, ds_upload_result
    )

  def _move_ingested_file_to_success_or_failure_bucket(
      self,
      dicom: ingest_base.GenDicomResult,
      polling_client: abstract_polling_client.AbstractPollingClient,
      ds_upload_result: Optional[_UploadToDicomStoresResult],
  ) -> None:
    dst_metadata = {
        'pubsub_message_id': str(polling_client.current_msg.message_id)
    }
    del_file = (
        ingest_flags.DELETE_FILE_FROM_INGEST_AT_BUCKET_AT_INGEST_SUCCESS_OR_FAILURE_FLG.value
    )
    try:
      gcs_storage_util.move_ingested_dicom_and_publish_ingest_complete(
          dicom.dicom_gen.localfile,
          dicom.dicom_gen.source_uri,
          destination_uri=dicom.dest_uri,
          dst_metadata=dst_metadata,
          files_copied_msg=ds_upload_result.ingest_complete_oof_trigger_msg
          if ds_upload_result is not None
          else None,
          delete_file_in_ingestion_bucket_at_ingest_success_or_failure=del_file,
      )
      polling_client.ack()
    except gcs_storage_util.CloudStorageBlobMoveError as exp:
      cloud_logging_client.error(
          'Cloud storage blob move error',
          {
              'dest_uri': dicom.dest_uri,
              'dicom_gen.source_uri': dicom.dicom_gen.source_uri,
              'dicom_gen.localfile': dicom.dicom_gen.localfile,
              'dicom_gen.generated_dicom_files': (
                  dicom.dicom_gen.generated_dicom_files
              ),
          },
          exp,
      )
      polling_client.nack()
      return

    if (
        ds_upload_result is not None
        and ds_upload_result.main_store_results.slide_has_instances_in_dicom_store()
    ):
      self.log_debug_url(
          viewer_debug_url=self._viewer_debug_url,
          ingested_dicom=ds_upload_result.main_store_results.slide_instances_in_dicom_store[
              0
          ],
      )
