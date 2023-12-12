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
import os
import re
import tempfile
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


class FileDownloadError(Exception):
  pass


class FileNameContainsInvalidCharError(Exception):

  def __init__(self, invalid_char: str):
    super().__init__('Filename contains invalid character.')
    self.invalid_char = invalid_char


def get_private_tags_for_gen_dicoms(
    gen_dicom: GeneratedDicomFiles, pubsub_msg_id: str
) -> List[dicom_private_tag_generator.DicomPrivateTag]:
  """Returns List of private tags to add to generated DICOM instances.

     SHA512 hash of bytes in source imaging finger prints the input imaging
     used to generate the DICOM. Private tag facilitates de-duplication
     ingestion of same source bytes. (e.g., copying same file twice to ingest
     bucket, or duplication of pub/sub msg).

  Args:
    gen_dicom: Generated DICOM files.
    pubsub_msg_id: Pub/sub message ID triggering DICOM file generation.

  Returns:
    List of private tags to add to DICOM files.
  """
  _, ingest_filename = os.path.split(gen_dicom.localfile)
  private_tag_list = [
      dicom_private_tag_generator.DicomPrivateTag(
          ingest_const.DICOMTagKeywords.PUBSUB_MESSAGE_ID_TAG,
          'LT',
          pubsub_msg_id,
      ),
      dicom_private_tag_generator.DicomPrivateTag(
          ingest_const.DICOMTagKeywords.INGEST_FILENAME_TAG,
          'LT',
          ingest_filename,
      ),
  ]
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
      ingest_file: GeneratedDicomFiles,
      polling_client: abstract_polling_client.AbstractPollingClient,
  ):
    """Converts downloaded image to DICOM.

    Args:
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
            {ingest_const.LogKeywords.uri: ingest_file.source_uri},
        )

        self.generate_dicom_and_push_to_store(ingest_file, polling_client)

        if polling_client.is_acked():
          cloud_logging_client.logger().info('Ingest pipeline completed')
        return
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
