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
"""Wrapper for cloud storage operations (e.g. read, write, copy, delete)."""
import base64
import dataclasses
import os
from typing import Mapping, Optional

import google.api_core
from google.cloud import storage as cloud_storage

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import hash_util
from transformation_pipeline.ingestion_lib import ingest_const


@dataclasses.dataclass
class GSFileParts:
  bucket_name: str
  file_path: str


def _get_blob_path_gsuri(blob: cloud_storage.Blob) -> str:
  """Get blob gsuri path.

  Args:
    blob: blob to return gs:// uri for

  Returns:
     gs uri prefix path
  """
  return f'gs://{blob.bucket.name}/{blob.name}'


def _add_metadata_to_blob(
    blob: cloud_storage.Blob, metadata: Optional[Mapping[str, str]] = None
):
  """Add metadata to blob.

  Args:
    blob: Blob to add metadata to.
    metadata: Metadata to add to blob.
  """
  if metadata is None:
    return
  blob.reload()  # reload blob metadata to make sure blob metadata is current.
  if blob.metadata is None:
    blob.metadata = metadata
  else:
    blob_metadata = blob.metadata  # get current blob metadata
    blob_metadata.update(metadata)  # merge blob metadata with new metadata
    blob.metadata = blob_metadata  # set blob metadata.
  # Update pushes metadata to GCS blob
  blob.update()
  cloud_logging_client.info(
      'Adding metadata to blob',
      {
          'blob_uri': _get_blob_path_gsuri(blob),
      },
      metadata,
  )


def _get_blob(
    gs_uri: str, filename: Optional[str] = None
) -> cloud_storage.Blob:
  """Returns storage blob for given URI and filename.

  Args:
    gs_uri: GCS URI.
    filename: Optional filename. May be structured as 'foo/bar.txt'.

  Returns:
    cloud_storage.Blob

  Raises:
    ValueError if invalid GCS URI.
  """
  if filename is not None and filename:
    gs_uri = os.path.join(gs_uri, filename)
  return cloud_storage.Blob.from_string(
      gs_uri, client=_CloudStorageClientProjectState.storage_client()
  )


class _CloudStorageClientProjectState:
  """Global state for cloud storage client methods."""

  _client = None
  _project = None

  @classmethod
  def gcp_project(cls) -> str:
    return _CloudStorageClientProjectState._project

  @classmethod
  def reset_storage_client(cls, project: str):
    """Reset Cloud storage client should only be called by polling client.

       Not thread safe to switch projects if cached state is in use elsewhere.

    Args:
      project: GCP project reading/writing to.
    """
    _CloudStorageClientProjectState._client = None
    _CloudStorageClientProjectState._project = project

  @classmethod
  def storage_client(cls) -> cloud_storage.Client:
    if _CloudStorageClientProjectState._client is None:
      _CloudStorageClientProjectState._client = cloud_storage.Client(
          project=_CloudStorageClientProjectState._project
      )
    return _CloudStorageClientProjectState._client


def reset_storage_client(project: str):
  """Reset Cloud storage client should only be called by polling client.

       Not thread safe to switch projects if cached state is in use elsewhere.

  Args:
    project: GCP project reading/writing to.
  """
  _CloudStorageClientProjectState.reset_storage_client(project)


def download_to_container(uri: str, local_file: str) -> bool:
  """Download file referenced in msg to the container.

  Args:
    uri: URI to remote file.
    local_file: Local file path to save to.

  Returns:
    True if file downloaded.

  Raises:
      OSError: If failed to write file in container.
  """
  blob = cloud_storage.Blob.from_string(
      uri, client=_CloudStorageClientProjectState.storage_client()
  )
  try:
    blob.download_to_filename(local_file)
  except google.api_core.exceptions.NotFound as exp:
    cloud_logging_client.warning(
        'Blob download failed. Source blob not found',
        {ingest_const.LogKeywords.URI: uri},
        exp,
    )
    return False
  except OSError as exp:
    cloud_logging_client.warning(
        'Failed to write blob to container', {'local_file': local_file}, exp
    )
    # re-raise exception to retry pub/sub message in another container.
    raise
  cloud_logging_client.info(
      'Downloaded blob',
      {
          ingest_const.LogKeywords.SOURCE_URI: uri,
          ingest_const.LogKeywords.DEST_FILE: local_file,
          'filesize_bytes': str(os.path.getsize(local_file)),
      },
  )
  return True


def get_gsuri_bucket_and_path(dst_gsuri: str) -> GSFileParts:
  """Returns bucket name and path to write file referenced by msg to uri.

  Args:
    dst_gsuri: URI referencing cloud bucket; 'gs://bucketname/path'.

  Returns:
    Tuple(BucketName: str, Path: str)
  """
  blob = cloud_storage.Blob.from_string(dst_gsuri)
  return GSFileParts(blob.bucket.name, blob.name)


def blob_exists(uri: Optional[str] = None) -> bool:
  """Returns true if blob exists.

  Args:
    uri: URI to bucket.

  Returns:
    True if blob exists.
  """
  if uri is not None and uri:
    blob = cloud_storage.Blob.from_string(
        uri, client=_CloudStorageClientProjectState.storage_client()
    )
    return blob.exists()
  return False


def upload_blob_to_uri(
    local_source: str,
    dst_uri: str,
    dst_filename: Optional[str] = None,
    additional_log: Optional[Mapping[str, str]] = None,
    dst_metadata: Optional[Mapping[str, str]] = None,
) -> bool:
  """Upload blob from local file to a subdir in another bucket.

  Args:
    local_source: Path to local file to upload to dest.
    dst_uri: Destination bucket and path to copy blob to.
    dst_filename: Destination file name.
    additional_log: Optional additional elements to add to logs.
    dst_metadata: Metadata to merge copy into dst_uri blob.

  Returns:
    Bool indicating if blob upload operation succeeds.
  """
  if additional_log is None:
    additional_log = {}
  if not local_source:
    cloud_logging_client.info(
        'local file is not specified. File not uploaded to GCS bucket.',
        additional_log,
    )
    return True
  if not dst_uri:
    cloud_logging_client.info(
        'Dest uri not specified. File not uploaded to GCS bucket.',
        additional_log,
    )
    return True

  try:
    dst_blob = _get_blob(dst_uri, dst_filename)
  except ValueError:
    cloud_logging_client.info(f'Dest uri invalid: {dst_uri}')
    return False
  dst_bucket = dst_blob.bucket
  if not dst_bucket.exists():
    cloud_logging_client.error(
        'Dest storage bucket does not exist. Upload to GCS bucket failed.',
        {ingest_const.LogKeywords.BUCKET_NAME: dst_bucket.name},
        additional_log,
    )
    return False

  # Tests if source blob has already been copied.
  if dst_blob.exists():
    dst_blob.reload()
    local_hash_hex = hash_util.md5hash(local_source)
    local_hash_b64 = base64.b64encode(bytes.fromhex(local_hash_hex)).decode(
        'utf-8'
    )
    if local_hash_b64 == dst_blob.md5_hash:
      cloud_logging_client.info(
          (
              'Copy of local file already exists in dest. '
              'Upload to GCS bucket skipped.,'
          ),
          {
              'local_file': local_source,
              ingest_const.LogKeywords.DEST_URI: _get_blob_path_gsuri(dst_blob),
          },
          additional_log,
      )
      _add_metadata_to_blob(dst_blob, dst_metadata)
      return True

  try:
    dst_blob.upload_from_filename(local_source, checksum='md5')
    cloud_logging_client.info(
        'Uploaded local file to GCS bucket.',
        {
            'local_file': local_source,
            ingest_const.LogKeywords.DEST_URI: _get_blob_path_gsuri(dst_blob),
        },
        additional_log,
    )
    _add_metadata_to_blob(dst_blob, dst_metadata)
    return True
  except google.api_core.exceptions.NotFound as second_exp:
    cloud_logging_client.warning(
        'Exception occurred uploading local file to GCS bucket.',
        {
            'local_file': local_source,
            ingest_const.LogKeywords.DEST_URI: _get_blob_path_gsuri(dst_blob),
        },
        additional_log,
        second_exp,
    )
  return False


def copy_blob_to_uri(
    source_uri: Optional[str],
    dst_uri: Optional[str],
    local_source: Optional[str] = None,
    dst_metadata: Optional[Mapping[str, str]] = None,
    source_uri_gcp_project: Optional[str] = None,
    destination_blob_filename: Optional[str] = None,
) -> bool:
  """Copy blob from one bucket to a subdir in another bucket.

  Args:
    source_uri: Source bucket to and path to copy from.
    dst_uri: Destination bucket and path to copy blob to.
    local_source: Optional path to local file to upload to dest if source is not
      available.
    dst_metadata: Metadata to merge copy into dst_uri blob.
    source_uri_gcp_project: optional name of source uri gcp project. enables
      cross project copying.
    destination_blob_filename: Optional filename to rename uploaded file to.

  Returns:
   bool indicating if blob copy operation succeeds.
  """
  if source_uri is None or not source_uri:
    cloud_logging_client.info(
        'Source uri not specified. File not copied to output bucket.'
    )
    return True
  if dst_uri is None or not dst_uri:
    cloud_logging_client.info(
        'Destination uri not specified. File not copied to output bucket.'
    )
    return True

  source_client = cloud_storage.Client(project=source_uri_gcp_project)
  source_blob = cloud_storage.Blob.from_string(source_uri, client=source_client)
  if destination_blob_filename is None or not destination_blob_filename:
    destination_blob_filename = source_blob.name
  try:
    dst_blob = _get_blob(dst_uri, destination_blob_filename)
  except ValueError as exp:
    cloud_logging_client.info(f'Dest uri invalid: {dst_uri}', exp)
    return False
  dst_bucket = dst_blob.bucket
  try:
    if not dst_bucket.exists():
      cloud_logging_client.error(
          'Dest storage bucket does not exist',
          {ingest_const.LogKeywords.BUCKET_NAME: dst_bucket.name},
      )
      return False
  except google.api_core.exceptions.Forbidden:
    cloud_logging_client.error(
        'Storage bucket access forbidden.',
        {ingest_const.LogKeywords.BUCKET_NAME: dst_bucket.name},
    )
    return False
  try:
    if source_blob.exists():
      source_blob.reload()
      source_blob_exists = True
    else:
      source_blob_exists = False
      cloud_logging_client.warning(
          'Source blob does not exist',
          {ingest_const.LogKeywords.URI: source_uri},
      )
  except google.api_core.exceptions.Forbidden:
    cloud_logging_client.error(
        'Source blob access forbidden.',
        {ingest_const.LogKeywords.URI: source_uri},
    )
    source_blob_exists = False
  # Tests if source blob has already been copied. Possible message is being
  # rehandeled that the source has alreadly been copied to the destination.
  if dst_blob.exists():
    dst_blob.reload()
    if source_blob_exists and source_blob.md5_hash == dst_blob.md5_hash:
      cloud_logging_client.info(
          'File exists in dest; file copy skipped',
          {
              ingest_const.LogKeywords.SOURCE_URI: source_uri,
              ingest_const.LogKeywords.DEST_URI: _get_blob_path_gsuri(dst_blob),
          },
      )
      _add_metadata_to_blob(dst_blob, dst_metadata)
      return True
  if source_blob_exists:
    try:
      # start source_bucket.copy_blob alternative
      # improved reliability for large files
      rewrite_token = None
      while True:
        rewrite_token, _, _ = dst_blob.rewrite(
            source=source_blob, timeout=600, token=rewrite_token
        )
        if not rewrite_token:
          break
      # end source_bucket.copy_blob alternative
      cloud_logging_client.info(
          'Copied blob from GCS bucket to GCS bucket',
          {
              ingest_const.LogKeywords.SOURCE_URI: source_uri,
              ingest_const.LogKeywords.DEST_URI: _get_blob_path_gsuri(dst_blob),
          },
      )
      _add_metadata_to_blob(dst_blob, dst_metadata)
      return True
    except google.api_core.exceptions.NotFound as exp:
      # error handeling handeled below
      cloud_logging_client.warning(
          'Exception occurred copying blob',
          {
              ingest_const.LogKeywords.SOURCE_URI: source_uri,
              ingest_const.LogKeywords.DEST_URI: _get_blob_path_gsuri(dst_blob),
          },
          exp,
      )
  if local_source is not None and local_source:
    return upload_blob_to_uri(
        local_source,
        dst_uri,
        destination_blob_filename,
        dst_metadata=dst_metadata,
    )
  return False


def del_blob(uri: str, ignore_file_not_found: bool = False) -> bool:
  """Deletes blob from ingest bucket.

  Args:
    uri: URI to blob to delete.
    ignore_file_not_found: If true ignores file not found errors.

  Returns:
   bool indicating if blob copy operation succeeds.
  """
  source_blob = cloud_storage.Blob.from_string(
      uri, client=_CloudStorageClientProjectState.storage_client()
  )
  try:
    source_blob.delete()
    cloud_logging_client.info('Deleted file', {'file': uri})
    return True
  except google.api_core.exceptions.NotFound as exp:
    if ignore_file_not_found:
      cloud_logging_client.warning(
          'File already deleted.',
          {
              ingest_const.LogKeywords.URI: uri,
          },
          exp,
      )
      return True
    else:
      cloud_logging_client.error(
          'Exception occurred deleting file',
          {
              ingest_const.LogKeywords.URI: uri,
          },
          exp,
      )
      # source file still exists. retry
  except google.api_core.exceptions.Forbidden as exp:
    cloud_logging_client.error(
        'Error deleting file access forbidden.',
        {
            ingest_const.LogKeywords.URI: uri,
        },
        exp,
    )
  return False  # Delete failed
