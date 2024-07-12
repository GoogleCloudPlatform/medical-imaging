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
"""Hashing utility functions."""
import hashlib
from typing import Any, Mapping, Optional
from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import ingest_const


def _hash_file(path: str, hash_function: Any) -> str:
  """Return hash of file in hex.

  Args:
    path: Path to file.
    hash_function: Hashlib.hash function.

  Returns:
    Hash in hex.
  """
  with open(path, 'rb') as infile:
    block = infile.read(16384)
    while block:
      hash_function.update(block)
      block = infile.read(16384)
  return hash_function.hexdigest()


def sha512hash(
    path: str, additional_logs: Optional[Mapping[str, str]] = None
) -> str:
  """Generates hash of file in hex.

  Args:
    path: Path to file.
    additional_logs: Additional elements to log with hash method.

  Returns:
    Sha512 hash in hex.
  """
  if not additional_logs:
    additional_logs = {}
  blob_hash = _hash_file(path, hashlib.sha512())
  cloud_logging_client.info(
      'sha512 hash computed',
      {ingest_const.LogKeywords.HASH: blob_hash},
      additional_logs,
  )
  return blob_hash


def md5hash(
    path: str, additional_logs: Optional[Mapping[str, str]] = None
) -> str:
  """Generates md5 hash of file in hex.

  Args:
    path: Path to file.
    additional_logs: Additional elements to log with hash method.

  Returns:
    MD5 hash in hex.
  """
  if not additional_logs:
    additional_logs = {}
  blob_hash = _hash_file(path, hashlib.md5())
  cloud_logging_client.info(
      'local file hash computed',
      {ingest_const.LogKeywords.HASH: blob_hash},
      additional_logs,
  )
  return blob_hash
