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
"""Wrapper for Cloud Storage client providing util methods for Orchestrator."""
from typing import List

from absl import flags
from google.api_core import exceptions
from google.cloud import storage as gcs

from pathology.shared_libs.flags import secret_flag_utils
from pathology.shared_libs.logging_lib import cloud_logging_client


FILTER_FILE_BUCKET_FLG = flags.DEFINE_string(
    'filter_file_bucket',
    secret_flag_utils.get_secret_or_env('FILTER_FILE_BUCKET', None),
    'Bucket to store filter files in.',
)
DEID_FILES_PREFIX = 'deid_filter_'
EXPORT_FILES_PREFIX = 'export_filter_'


class GcsUtil(object):
  """Wrapper for Cloud Storage client."""

  def __init__(self):
    """Initializes the client and bucket to store temp files."""
    self._client = gcs.Client()
    self._bucket = self._client.bucket(FILTER_FILE_BUCKET_FLG.value)

  def write_filter_file(
      self,
      filtered_paths: List[str],
      file_path: str,
  ) -> str:
    """Writes a filter file to Cloud Storage of DICOM paths provided.

    Args:
      filtered_paths: Paths to write to file.
      file_path: Path name for filter file.

    Returns:
      str - full path to created file.
    """
    full_file_path = f'gs://{FILTER_FILE_BUCKET_FLG.value}/{file_path}'
    cloud_logging_client.info(f'Writing filter file to {full_file_path}.')

    blob = self._bucket.blob(file_path)

    with blob.open(mode='w') as f:
      for path in filtered_paths:
        f.write(f'{path}\n')

    return full_file_path

  def delete_file(self, file_path: str) -> None:
    """Deletes a file in Cloud Storage.

    Args:
      file_path: Path to file.

    Returns:
      None
    """
    blob = self._bucket.blob(file_path)

    try:
      blob.delete()
      cloud_logging_client.info(f'Deleted file {file_path}.')
    except exceptions.NotFound:
      # Assume file has already been deleted.
      cloud_logging_client.warning(f'File {file_path} was not found.')
      pass

  def list_files(self) -> List[str]:
    """Lists files in Cloud Storage bucket.

    Returns:
      List of file names.
    """
    blobs = self._client.list_blobs(self._bucket.name)
    return [blob.name for blob in blobs]
