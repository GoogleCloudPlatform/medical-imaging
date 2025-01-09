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
"""DICOM sparse dicom metadata utility."""
from concurrent import futures
import json
from typing import Any, Mapping, MutableMapping, Union
import zlib

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import dicom_store_util
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import flask_util
from pathology.dicom_proxy import metadata_util
from pathology.dicom_proxy import redis_cache
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.logging_lib import cloud_logging_client


_ALL = 'all'
_PIXELDATA_DICOM_TAG_KEYWORD = 'PixelData'
DIMENSIONAL_ORGANIZATION_TYPE_DICOM_TAG = '00209311'
_PER_FRAME_FUNCTIONAL_GROUPS_SEQUENCE = '52009230'
_INCLUDE_FIELD_WILL_RETURN_DIM_ORG_METADATA = frozenset([
    _ALL,
    'dimensionorganizationtype',
    DIMENSIONAL_ORGANIZATION_TYPE_DICOM_TAG,
])
_INCLUDE_FIELD_WILL_PERFRAME_FUNC_SEQ = frozenset([
    _ALL,
    'perframefunctionalgroupssequence',
    _PER_FRAME_FUNCTIONAL_GROUPS_SEQUENCE,
])


def do_includefields_request_dimensional_organization_type() -> bool:
  for tag in flask_util.get_includefields():
    tag = tag.lower()
    if tag in _INCLUDE_FIELD_WILL_RETURN_DIM_ORG_METADATA:
      return True
  return False


def do_includefields_request_perframe_functional_group_seq() -> bool:
  for tag in flask_util.get_includefields():
    tag = tag.lower()
    if tag in _INCLUDE_FIELD_WILL_PERFRAME_FUNC_SEQ:
      return True
  return False


def _is_missing_per_frame_functional_groups_sequence(
    metadata: Mapping[str, Any],
) -> bool:
  try:
    return len(metadata[_PER_FRAME_FUNCTIONAL_GROUPS_SEQUENCE]['Value']) <= 0
  except (KeyError, ValueError, TypeError) as _:
    return True


def _instance_per_frame_functional_group_cache_key(
    uid: metadata_util.MetadataUID,
) -> str:
  return f'perframefuncgroupsq_{uid.study_instance_uid}_{uid.series_instance_uid}_{uid.sop_instance_uid}'


def _add_per_frame_functional_group_seq_metadata(
    user_auth: user_auth_util.AuthSession,
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL,
    uid: metadata_util.MetadataUID,
    metadata: MutableMapping[str, Any],
    enable_caching: cache_enabled_type.CachingEnabled,
    manager: dicom_store_util.MetadataThreadPoolDownloadManager,
) -> str:
  """Download annotation and update metadata instance in place."""
  cache_key = _instance_per_frame_functional_group_cache_key(uid)
  cache = redis_cache.RedisCache()
  cache_result = cache.get(cache_key) if enable_caching else None
  if cache_result is not None:
    if cache_result.value is not None:
      metadata[_PER_FRAME_FUNCTIONAL_GROUPS_SEQUENCE] = json.loads(
          zlib.decompress(cache_result.value).decode('utf-8')
      )
    return json.dumps(metadata)

  instance_metadata = dicom_store_util.download_instance_return_metadata(
      user_auth,
      dicom_web_base_url,
      uid.study_instance_uid,
      uid.series_instance_uid,
      uid.sop_instance_uid,
      [_PIXELDATA_DICOM_TAG_KEYWORD],
  )
  per_frame_sq = instance_metadata.get(_PER_FRAME_FUNCTIONAL_GROUPS_SEQUENCE)
  if per_frame_sq is None:
    group_sq = None
  else:
    metadata[_PER_FRAME_FUNCTIONAL_GROUPS_SEQUENCE] = per_frame_sq
    group_sq = json.dumps(per_frame_sq).encode('utf-8')
    manager.inc_data_downloaded(len(group_sq))
  if enable_caching:
    cache.set(
        cache_key,
        zlib.compress(group_sq) if group_sq is not None else None,
        allow_overwrite=True,
        ttl_sec=dicom_proxy_flags.SPARSE_DICOM_PER_FRAME_FUNCTIONAL_GROUPS_SEQUENCE_METADATA_CACHE_TTL_FLG.value,
    )
  return json.dumps(metadata)


def download_and_return_sparse_dicom_metadata(
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL,
    study_uid: dicom_url_util.StudyInstanceUID,
    series_uid: dicom_url_util.SeriesInstanceUID,
    instance_uid: dicom_url_util.SOPInstanceUID,
    metadata: MutableMapping[str, Any],
    enable_caching: cache_enabled_type.CachingEnabled,
    manager: dicom_store_util.MetadataThreadPoolDownloadManager,
) -> Union[futures.Future[str], str]:
  """Tests if instance describes WSI annotation and returns instance metadata.

  Args:
    dicom_web_base_url: Base DICOMweb URL for store.
    study_uid: StudyInstanceUID for request or if empty init from metadata.
    series_uid: SeriesInstanceUID for request or if empty init from metadata.
    instance_uid: SOPInstanceUID for request or if empty init from metadata.
    metadata: DICOM instance json formatted metadata,
    enable_caching: Cache per frame functional group sequence.
    manager: Dicom instance thread pool manager.

  Returns:
    True if updated metadata
  """
  uid = metadata_util.get_metadata_uid(
      study_uid, series_uid, instance_uid, metadata
  )
  if not uid.is_defined():
    return json.dumps(metadata)
  if not _is_missing_per_frame_functional_groups_sequence(metadata):
    return json.dumps(metadata)
  cloud_logging_client.debug('Returning metadata for sparse dicom')
  user_auth = user_auth_util.AuthSession(flask_util.get_headers())
  return manager.submit(
      _add_per_frame_functional_group_seq_metadata,
      user_auth,
      dicom_web_base_url,
      uid,
      metadata,
      enable_caching,
      manager,
  )
