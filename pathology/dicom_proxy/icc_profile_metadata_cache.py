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
"""DICOM instance iccprofile caching utility."""

import os
import sys
import threading
from typing import NewType, Optional
import uuid

import cachetools

from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import redis_cache


ICCProfileHash = NewType('ICCProfileHash', str)


INSTANCE_MISSING_ICC_PROFILE_METADATA = ICCProfileHash(
    'DICOM_INSTANCE_HAS_NO_ICC_PROFILE'
)

# Metadata Flag Cache Const
_LOCALMETADATA_CACHESIZE = 30


_metadata_cache = cachetools.LRUCache(_LOCALMETADATA_CACHESIZE)
_metadata_cache_lock = threading.Lock()
_is_debugging = 'UNITTEST_ON_FORGE' in os.environ or 'unittest' in sys.modules


def _init_fork_module_state() -> None:
  global _metadata_cache
  global _metadata_cache_lock
  _metadata_cache = cachetools.LRUCache(_LOCALMETADATA_CACHESIZE)
  _metadata_cache_lock = threading.Lock()


def _cache_key(
    series_url: dicom_url_util.DicomSeriesUrl,
    instance: dicom_url_util.SOPInstanceUID,
) -> str:
  """Returns redis cache key for metadata request.

  All DICOM Proxy responses require retrieval of metadata.

  DICOM store metadata request functions as a mechanism to validate that the
  user
  has read privileges on DICOM store. User authentication attached to metadata
  cache key to ensure metadata cache functions at user level. Metadata cache has
  TTL to ensure the user's ability to read from the DICOM store is validated at
  a
  miniumum interval.

  Args:
    series_url: DICOM series requested.
    instance: DICOM SOPInstanceUID for instance in series requested.  Returns
      cache key for metadata request (str)
  """
  return f'icc_profile_metadata url:{series_url}/{instance}'


def _cache_tools_hash_key(key: str) -> str:
  return key if not _is_debugging else f'{key}_{uuid.uuid1()}'


def icc_profile_metadata_redis_cache_ttl() -> Optional[int]:
  return (
      None
      if dicom_proxy_flags.ICC_PROFILE_REDIS_CACHE_TTL_FLG.value < 0
      else dicom_proxy_flags.ICC_PROFILE_REDIS_CACHE_TTL_FLG.value
  )


def set_cached_instance_icc_profile_metadata(
    redis: redis_cache.RedisCache,
    series_url: dicom_url_util.DicomSeriesUrl,
    instance: dicom_url_util.SOPInstanceUID,
    metadata: ICCProfileHash,
) -> None:
  """Caches instance metadata."""
  with _metadata_cache_lock:
    cache_key = _cache_key(series_url, instance)
    redis.set(
        cache_key,
        metadata.encode('utf-8'),
        ttl_sec=icc_profile_metadata_redis_cache_ttl(),
    )
    _metadata_cache[_cache_tools_hash_key(cache_key)] = metadata


def get_cached_instance_icc_profile_metadata(
    redis: redis_cache.RedisCache,
    series_url: dicom_url_util.DicomSeriesUrl,
    instance: dicom_url_util.SOPInstanceUID,
) -> Optional[ICCProfileHash]:
  """Returns metadata structure initialized from DICOM instance in Store.

  Args:
    redis: Redis cache.
    series_url: URL of series in store.
    instance: DICOM SOP Instance UID of instance metadata to return.

  Returns:
    DicomInstanceMetadata

  Raises:
    ReadDicomMetadataError: Error occurred requesting metadata.
  """
  with _metadata_cache_lock:
    cache_key = _cache_key(series_url, instance)
    hash_tools_cache_key = _cache_tools_hash_key(cache_key)
    result = _metadata_cache.get(hash_tools_cache_key)
    if result is not None:
      return result
    result = redis.get(cache_key)
    if result is None:
      _metadata_cache[hash_tools_cache_key] = None
      return None
    if result.value is not None:
      result = ICCProfileHash(result.value.decode('utf-8'))
      _metadata_cache[hash_tools_cache_key] = result
      return result


os.register_at_fork(after_in_child=_init_fork_module_state)
