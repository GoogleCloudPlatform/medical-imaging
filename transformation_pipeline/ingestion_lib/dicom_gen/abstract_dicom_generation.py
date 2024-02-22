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
"""Base class for image to DICOM generation implementations."""
import abc
import dataclasses
import os
import re
import tempfile
import time
from typing import List, Optional

from google.cloud import pubsub_v1
import requests

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import abstract_polling_client
from transformation_pipeline.ingestion_lib import abstract_pubsub_message_handler
from transformation_pipeline.ingestion_lib import cloud_storage_client
from transformation_pipeline.ingestion_lib import hash_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib import redis_client
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_private_tag_generator
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen import wsi_dicom_file_ref
from transformation_pipeline.ingestion_lib.pubsub_msgs import abstract_pubsub_msg

# Regular expression concatenation of three byte arrays. First starts regex
# second defines list of excluded bytes
# third excludes bytes in ranges 0-31 and 127-255 and terminates regex
INVALID_FILENAME_BYTES = re.compile(
    b'[' + r'\?#@:&/\\"`$~\''.encode('utf-8') + rb'\x7F-\xFF\x00-\x1f]'
)


class GeneratedDicomFiles:
  """Holds references to ingested file triggering ingestion and generated dicom.

  Attributes:
    generated_dicom_files: List of paths to DICOM instances generated.
    localfile: File path in container for source file (svs, dcm, zip) that
      triggered ingestion.
    hash: SHA512 hash of ingested file. Used to identify bits triggering
      ingestion.  Written into DICOM instance.  Used in SVS ingestion to
      identify DICOM instances from the same source which were written to DICOM
      store.  E.g., SVS ingestion started, crashed while uploading to tge DICOM
      store and is then being run again with some but not all required
      instances. Hash allows the series uid to be discovered.
    source_uri: Source URI of the file that triggered ingestion.
  """

  def __init__(self, localfile: str, source_uri: Optional[str]):
    self._generated_dicom_files = list()
    self._localfile = localfile
    self._hash = None
    self._source_uri = source_uri

  @property
  def generated_dicom_files(self) -> List[str]:
    return self._generated_dicom_files

  @generated_dicom_files.setter
  def generated_dicom_files(self, val: List[str]) -> None:
    self._generated_dicom_files = val

  @property
  def localfile(self) -> str:
    return self._localfile

  @property
  def source_uri(self) -> Optional[str]:
    return self._source_uri

  @property
  def hash(self) -> Optional[str]:
    return self._hash

  @hash.setter
  def hash(self, val: str) -> None:
    self._hash = val

  def within_bucket_file_path(self) -> str:
    """Returns the path with in the source GS style uri to the file.

    Returns:
      path within bucket to file.

    Raises:
      ValueError: Not GS style URI or undefined URI.
    """
    if self._source_uri is None or not self._source_uri:
      raise ValueError('Source URI is undefined.')
    if not self._source_uri.startswith('gs://'):
      raise ValueError('Source URI is is not google cloud storage URI.')
    _, filename = os.path.split(self._localfile)
    bucket_path = re.fullmatch(
        (r'gs://.+?/(.*)/' f'{filename}'), self._source_uri
    )
    if bucket_path is None:
      return ''
    return bucket_path.groups()[0]


class FileDownloadError(Exception):
  pass


class FileNameContainsInvalidCharError(Exception):

  def __init__(self, invalid_char: str):
    super().__init__('Filename contains invalid character.')
    self.invalid_char = invalid_char


def get_ingest_triggering_file_path(
    gen_dicom: GeneratedDicomFiles, include_file_path_within_bucket: bool = True
) -> str:
  """Returns path to file within bucket that triggerted ingestion pipeline.

  Args:
    gen_dicom: Reference to files triggering ingestion.
    include_file_path_within_bucket: If true include within bucket file path.

  Returns:
    Name of file or path within bucket to file triggering ingestion.
  """
  _, filename = os.path.split(gen_dicom.localfile)
  if not include_file_path_within_bucket:
    return filename
  try:
    bucket_file_path = gen_dicom.within_bucket_file_path()
  except ValueError:
    return filename
  if not bucket_file_path:
    return filename
  return os.path.join(bucket_file_path, filename)


def get_private_tags_for_gen_dicoms(
    gen_dicom: GeneratedDicomFiles,
    pubsub_msg_id: str,
    include_integest_filename_tag: bool = True,
) -> List[dicom_private_tag_generator.DicomPrivateTag]:
  """Returns List of private tags to add to generated DICOM instances.

     SHA512 hash of bytes in source imaging finger prints the input imaging
     used to generate the DICOM. Private tag facilitates de-duplication
     ingestion of same source bytes. (e.g., copying same file twice to ingest
     bucket, or duplication of pub/sub msg).

  Args:
    gen_dicom: Generated DICOM files.
    pubsub_msg_id: Pub/sub message ID triggering DICOM file generation.
    include_integest_filename_tag: Include private tag identifying ingested
      file.

  Returns:
    List of private tags to add to DICOM files.
  """
  private_tag_list = [
      dicom_private_tag_generator.DicomPrivateTag(
          ingest_const.DICOMTagKeywords.PUBSUB_MESSAGE_ID_TAG,
          'LT',
          pubsub_msg_id,
      ),
  ]
  if include_integest_filename_tag:
    private_tag_list.append(
        dicom_private_tag_generator.DicomPrivateTag(
            ingest_const.DICOMTagKeywords.INGEST_FILENAME_TAG,
            'LT',
            get_ingest_triggering_file_path(gen_dicom),
        )
    )
  # dicom ingestion does not add hash
  if gen_dicom.hash is not None and gen_dicom.hash:
    private_tag_list.append(
        dicom_private_tag_generator.DicomPrivateTag(
            ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG, 'LT', gen_dicom.hash
        )
    )
  return private_tag_list


def _check_filename_for_invalid_chars(filename: str):
  """Raises exception if filename contains invalid character.

     Code uses google.cloud.storage.Blob.from_string to resolve Blob from
     filename uri. This code relies on urllib.parse which in turn does not
     allow url control characters to be present.

  Args:
    filename: Filename to test.

  Raises:
     FileNameContainsInvalidCharError: If file name contains invalid char.
  """
  result = re.search(INVALID_FILENAME_BYTES, filename.encode('utf-8'))
  if result:
    raise FileNameContainsInvalidCharError(str(result.group()))


@dataclasses.dataclass(frozen=True)
class TransformationLock:
  name: str = ''

  def is_defined(self) -> bool:
    """Returns True if lock is defined."""
    return bool(self.name)


class AbstractDicomGeneration(
    abstract_pubsub_message_handler.AbstractPubSubMsgHandler
):
  """Base class for image to dicom generation implementations."""

  def __init__(
      self,
      dicom_store_web_path: str,
  ):
    try:
      # Test at startup that DICOM UID prefix configured correctly.
      uid_generator.validate_uid_prefix()
    except uid_generator.InvalidUIDPrefixError as exp:
      cloud_logging_client.logger().critical(str(exp))
      raise
    self._dicom_store_client = None
    self._root_working_dir = None
    self._dicomweb_path = dicom_store_web_path
    self._viewer_debug_url = ingest_flags.VIEWER_DEBUG_URL_FLG.value.strip()

  @property
  def name(self) -> str:
    """Name of conversion class."""
    return self.__class__.__name__

  @abc.abstractmethod
  def generate_dicom_and_push_to_store(
      self,
      transform_lock: TransformationLock,
      ingest_file: GeneratedDicomFiles,
      polling_client: abstract_polling_client.AbstractPollingClient,
  ):
    """Converts downloaded image to DICOM.

    Args:
      transform_lock: Transformation pipeline lock.
      ingest_file: File payload to generate into DICOM.
      polling_client: Polling client receiving triggering pub/sub msg.
    """

  @property
  def dcm_store_client(self) -> Optional[dicom_store_client.DicomStoreClient]:
    if self._dicom_store_client is None:
      self._dicom_store_client = dicom_store_client.DicomStoreClient(
          dicomweb_path=self._dicomweb_path
      )
    return self._dicom_store_client

  @abc.abstractmethod
  def decode_pubsub_msg(
      self, msg: pubsub_v1.types.ReceivedMessage
  ) -> abstract_pubsub_msg.AbstractPubSubMsg:
    """Pass pubsub msg to decoder described in DICOM Gen.

    Allows decoder to implement decoder specific pubsub msg decoding.

    Args:
      msg: Pubsub msg.

    Returns:
      implementation of AbstractPubSubMsg
    """

  @property
  def root_working_dir(self) -> str:
    """Root working dir of container."""
    return self._root_working_dir

  @root_working_dir.setter
  def root_working_dir(self, val: str):
    """Root working dir of container."""
    self._root_working_dir = val

  @property
  def img_dir(self) -> str:
    """Container dir to download images into."""
    return os.path.join(self.root_working_dir, 'img_dir')

  def _get_download_filepath(self, msg_filename: str) -> str:
    """Returns download filepath for given filename.

    Args:
      msg_filename: Pub/sub message filename.

    Raises:
       FileNameContainsInvalidCharError: If file name contains invalid char.
    """
    download_filename = os.path.basename(msg_filename)
    _check_filename_for_invalid_chars(download_filename)
    return os.path.join(self.img_dir, download_filename)

  def get_pubsub_file(
      self, uri: str, download_filepath: str
  ) -> GeneratedDicomFiles:
    """Downloads pub/sub referenced files to container.

    Default implementation processes GCS generated Finalize events.
    Generates a single reference.

    Args:
      uri: Pub/sub resource URI to download file from.
      download_filepath: Path to download file to.

    Returns:
      GeneratedDicomFiles with the downloaded file.

    Raises:
      FileDownloadError: if failed to download.
    """
    if not cloud_storage_client.download_to_container(
        uri=uri, local_file=download_filepath
    ):
      raise FileDownloadError(f'Failed to download file from {uri}.')
    return GeneratedDicomFiles(download_filepath, uri)

  def log_debug_url(
      self,
      viewer_debug_url: str,
      ingested_dicom: wsi_dicom_file_ref.WSIDicomFileRef,
  ):
    """Logs DICOM store debug URL for ingested DICOM.

    Args:
      viewer_debug_url: viewer debug URL to use for logging.
      ingested_dicom: DICOM ref to use for logging.
    """
    study_uid = ingested_dicom.study_instance_uid
    series_uid = ingested_dicom.series_instance_uid
    debug_url = f'{viewer_debug_url}/studies/{study_uid}/series/{series_uid}'
    cloud_logging_client.logger().debug('Debug_Link', {'url': debug_url})

  @abc.abstractmethod
  def handle_unexpected_exception(
      self,
      msg: abstract_pubsub_msg.AbstractPubSubMsg,
      ingest_file: Optional[GeneratedDicomFiles],
      exp: Exception,
  ):
    """Handles unexpected errors.

    Args:
      msg: Current pub/sub message being processed.
      ingest_file: Ingested DICOM.
      exp: Exception which triggered method.
    """

  @abc.abstractmethod
  def get_slide_transform_lock(
      self,
      ingest_file: GeneratedDicomFiles,
      polling_client: abstract_polling_client.AbstractPollingClient,
  ) -> TransformationLock:
    """Returns lock to ensure transform processes only one instance of a slide at a time.

    Args:
      ingest_file: File payload to generate into DICOM.
      polling_client: Polling client receiving triggering pub/sub msg.

    Returns:
      Transformation pipeline lock
    """

  def validate_redis_lock_held(
      self,
      transform_lock: TransformationLock,
  ) -> bool:
    """Validates transformation pipeline is not using locks or the lock is held.

    Args:
      transform_lock: Transformation pipeline lock.

    Returns:
      True if locks are not being used or lock is held. False if locks are being
      used and lock is not held.
    """
    r_client = redis_client.redis_client()
    if not r_client.has_redis_client() or r_client.is_lock_owned(
        transform_lock.name
    ):
      return True
    cloud_logging_client.logger().warning(
        'Slide ingestion lock is no longer owned; slide ingestion will be'
        ' retried later.',
        {'redis_lock_name': transform_lock.name},
    )
    return False

  def _generate_dicom_and_push_to_store_lock_wrapper(
      self,
      ingest_file: GeneratedDicomFiles,
      polling_client: abstract_polling_client.AbstractPollingClient,
  ):
    """Wrapper for generate_dicom_and_push_to_store.

    Args:
      ingest_file: File payload to generate into DICOM.
      polling_client: Polling client receiving triggering pub/sub msg.
    """
    slide_lock = self.get_slide_transform_lock(ingest_file, polling_client)
    if not slide_lock.is_defined():
      return
    r_client = redis_client.redis_client()
    if not r_client.has_redis_client():
      self.generate_dicom_and_push_to_store(
          slide_lock, ingest_file, polling_client
      )
      return
    # Lock used to protects against other processes interacting with
    # dicom and or metadata stores prior to the data getting written into
    # dicomstore. It is safe to move the ingested bits to success folder
    # and acknowledge pub/sub msg in unlocked context.
    expiry_seconds = ingest_const.MESSAGE_TTL_S
    token = ingest_flags.TRANSFORM_POD_UID_FLG.value
    lock_log = {
        'lock_name': slide_lock.name,
        'lock_token': token,
        'redis_server_ip': r_client.redis_ip,
        'redis_server_port': r_client.redis_port,
    }
    if not r_client.acquire_non_blocking_lock(
        slide_lock.name, token, expiry_seconds
    ):
      retry_delay = ingest_flags.TRANSFORMATION_LOCK_RETRY_FLG.value
      cloud_logging_client.logger().info(
          'Could not acquire lock. Slide transformation will be retried in'
          f' about {retry_delay} seconds.',
          lock_log,
      )
      polling_client.nack(retry_delay)
      return
    start_time = time.time()
    try:
      cloud_logging_client.logger().info(
          f'Acquired transformation lock: {slide_lock.name}', lock_log
      )
      self.generate_dicom_and_push_to_store(
          slide_lock, ingest_file, polling_client
      )
    finally:
      r_client.release_lock(slide_lock.name, ignore_redis_exception=True)
      lock_log['sec_lock_held'] = time.time() - start_time
      cloud_logging_client.logger().info(
          f'Released transformation lock: {slide_lock.name}', lock_log
      )

  def process_message(
      self, polling_client: abstract_polling_client.AbstractPollingClient
  ):
    """Called to process received pub/sub msg.

    Args:
      polling_client: instance of polling client receiving msg.
    """
    ingest_file = None
    try:
      # Creates a temp working directory inside the container.
      with tempfile.TemporaryDirectory() as working_root_dir:
        self.root_working_dir = working_root_dir
        os.mkdir(self.img_dir)  # Could raise OSError

        # Download to local filename excluding any subfolders in direct path.
        download_filepath = self._get_download_filepath(
            polling_client.current_msg.filename
        )
        ingest_file = self.get_pubsub_file(
            polling_client.current_msg.uri, download_filepath
        )

        ingest_file.hash = hash_util.sha512hash(
            ingest_file.localfile,
            {ingest_const.LogKeywords.URI: ingest_file.source_uri},
        )
        self._generate_dicom_and_push_to_store_lock_wrapper(
            ingest_file, polling_client
        )
        if polling_client.is_acked():
          cloud_logging_client.logger().info('Ingest pipeline completed')
    except FileNameContainsInvalidCharError as exp:
      cloud_logging_client.logger().error(
          (
              'Ingested file contains invalid character in filename. File will '
              'not be processed or moved from ingestion bucket.'
          ),
          exp,
          {
              'invalid_character': exp.invalid_char,
              'filename': os.path.basename(polling_client.current_msg.filename),
              'uri': polling_client.current_msg.uri,
          },
      )
      polling_client.ack()
      return
    except FileDownloadError as exp:
      # Assume file was deleted and msg just failed to ack.
      cloud_logging_client.logger().info(
          'Ingest pipeline completed. Ingest blob not found.',
          exp,
          {
              'filename': os.path.basename(polling_client.current_msg.filename),
              'uri': polling_client.current_msg.uri,
          },
      )
      polling_client.ack()
      return
    except requests.HTTPError as exp:
      # Error occurred uploading the DICOM. Retry ingest.
      # Error logged in self.dcm_store_client.upload_to_dicom_store
      retry_ttl = 0
      opt_quota_str = ''
      if exp.response.status_code == 429:
        retry_ttl = ingest_flags.DICOM_QUOTA_ERROR_RETRY_FLG.value
        opt_quota_str = (
            f'; insufficient DICOM Store quota retrying in {retry_ttl} sec'
        )
      cloud_logging_client.logger().error(
          f'HTTPError occurred in the GKE container{opt_quota_str}', exp
      )
      polling_client.nack(retry_ttl=retry_ttl)
      return
    except Exception as exp:
      self.handle_unexpected_exception(
          polling_client.current_msg, ingest_file, exp
      )
      polling_client.ack()
      raise exp
