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
"""Util for transforming color in DICOM from embedded ICCProfile to sRGB.

Public Functions:

  transform_image_color: Transforms raw pixels data (RGB) to transform
                        defined colorspace.

  get_icc_profile_srgb_transform: Get transformation to transform pixels from
                                  encoded colorspace to SRGB.

  Functions are thread safe.
"""

import hashlib
import io
import os
import threading
from typing import Any, Mapping, Optional, Union

import cachetools
from ez_wsi_dicomweb import dicom_slide
import numpy as np
import PIL
from PIL import ImageCms

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import dicom_store_util
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import execution_timer
from pathology.dicom_proxy import icc_color_transform
from pathology.dicom_proxy import icc_profile_metadata_cache
from pathology.dicom_proxy import image_util
from pathology.dicom_proxy import metadata_util
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import pydicom_single_instance_read_cache
from pathology.dicom_proxy import redis_cache
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.logging_lib import cloud_logging_client


# String Constants
_RAW = 'raw'
_RGB = 'RGB'
_VALUE = 'Value'

# DICOM Tag Keywords
_IMAGE_TYPE = 'ImageType'

# DICOM Tag Addresses
_IMAGE_TYPE_DICOM_TAG_ADDRESS = '00080008'
_NUMBER_OF_FRAMES_DICOM_TAG_ADDRESS = '00280008'
_SOP_CLASS_UID_DICOM_TAG_ADDRESS = '00080016'
_SOP_INSTANCE_UID_DICOM_TAG_ADDRESS = '00080018'
_COLUMNS_TAG_ADDRESS = '00280011'
_ROWS_TAG_ADDRESS = '00280010'

# DICOM IMAGE TYPE Keywords
_LABEL = 'LABEL'
_OVERVIEW = 'OVERVIEW'
_THUMBNAIL = 'THUMBNAIL'
_LOCALIZER = 'LOCALIZER'
_ORIGINAL_PRIMARY_VOLUME = 'ORIGINAL\\PRIMARY\\VOLUME'
_DERIVED_PRIMARY_VOLUME = 'DERIVED\\PRIMARY\\VOLUME'

# DICOM IOD UID
# https://dicom.nema.org/dicom/2013/output/chtml/part04/sect_i.4.html
_VL_WHOLE_SLIDE_MICROSCOPY_IMAGE_IOD = '1.2.840.10008.5.1.4.1.1.77.1.6'
_EMPTY_BYTES = b''

_cache_lock = threading.Lock()
_icc_transform_cache = cachetools.LRUCache(
    maxsize=dicom_proxy_flags.MAX_SIZE_ICC_PROFILE_TRANSFORM_PROCESS_CACHE_FLG,
)


class _MissingIccProfileBulkDataUriError(Exception):
  pass


class UnableToLoadIccProfileError(Exception):
  pass


def _init_fork_module_state() -> None:
  global _cache_lock
  global _icc_transform_cache
  _cache_lock = threading.Lock()
  _icc_transform_cache = cachetools.LRUCache(
      maxsize=dicom_proxy_flags.MAX_SIZE_ICC_PROFILE_TRANSFORM_PROCESS_CACHE_FLG,
  )


def _clear_cache_before_fork() -> None:
  with _cache_lock:
    _icc_transform_cache.clear()


def _read_icc_profile_file(*file_parts: str) -> bytes:
  with open(os.path.join(*file_parts), 'rb') as f:
    return f.read()


def _does_icc_profile_file_name_match(file_name: str, search_name: str) -> bool:
  fname, ext = os.path.splitext(file_name.lower())
  return fname == search_name and ext == '.icc'


def _read_dir_icc_profile(dir_path: str) -> bytes:
  for entry in os.scandir(dir_path):
    if not entry.is_file():
      continue
    entry_name = entry.name.lower()
    if entry_name.endswith('.icc'):
      return _read_icc_profile_file(dir_path, entry.name)
  return _EMPTY_BYTES


def read_icc_profile_plugin_file(name: str) -> bytes:
  """Returns ICC Profile bytes read from plugin directory."""
  icc_profile_plugin_dir = (
      dicom_proxy_flags.THIRD_PARTY_ICC_PROFILE_DICRECTORY_FLG.value
  )
  if not icc_profile_plugin_dir:
    return _EMPTY_BYTES
  name = name.lower()
  try:
    file_list = os.scandir(icc_profile_plugin_dir)
  except FileNotFoundError:
    return _EMPTY_BYTES
  profile = _EMPTY_BYTES
  for entry in file_list:
    if entry.is_file() and _does_icc_profile_file_name_match(entry.name, name):
      profile = _read_icc_profile_file(icc_profile_plugin_dir, entry.name)
    elif entry.is_dir() and entry.name.lower() == name:
      profile = _read_dir_icc_profile(
          os.path.join(icc_profile_plugin_dir, entry.name)
      )
    if profile:
      return profile
  return _EMPTY_BYTES


def _get_srgb_iccprofile() -> bytes:
  profile = read_icc_profile_plugin_file('srgb')
  if profile:
    return profile
  return dicom_slide.get_srgb_icc_profile_bytes()


def _get_adobergb_iccprofile() -> bytes:
  for filename in ('adobergb1998', 'adobergb'):
    profile = read_icc_profile_plugin_file(filename)
    if profile:
      return profile
  return dicom_slide.get_adobergb_icc_profile_bytes()


def _get_rommrgb_iccprofile() -> bytes:
  profile = read_icc_profile_plugin_file('rommrgb')
  if profile:
    return profile
  return dicom_slide.get_rommrgb_icc_profile_bytes()


def _get_displayp3_iccprofile() -> bytes:
  profile = read_icc_profile_plugin_file('displayp3')
  if profile:
    return profile
  return dicom_slide.get_displayp3_icc_profile_bytes()


def _create_icc_profile_transform(
    dicom_icc_profile_bytes: Optional[bytes], transform: enum_types.ICCProfile
) -> Optional[icc_color_transform.IccColorTransform]:
  """Returns ImageCms transform to transform pixel RGB from to ICCProfile.

  Args:
    dicom_icc_profile_bytes: source images icc_profile(bytes).
    transform: ICC profile colorspace transform configuration.

  Returns:
    ImageCms transform to transform pixel RGB from ICCProfile to
    defind colorspace or None if icc_profile_bytes is None.

  Raises:
    UnableToLoadIccProfileError: if transform cannot be loaded.
  """
  u_transform = transform.upper()
  if (
      dicom_icc_profile_bytes is None
      or not dicom_icc_profile_bytes
      or not u_transform
      or u_transform == proxy_const.ICCProfile.NO
  ):
    return None
  if u_transform == proxy_const.ICCProfile.YES:
    return icc_color_transform.IccColorTransform(
        transform, None, dicom_icc_profile_bytes
    )
  elif u_transform == proxy_const.ICCProfile.SRGB:
    rendered_icc_profile_bytes = _get_srgb_iccprofile()
  elif u_transform == proxy_const.ICCProfile.ADOBERGB:
    rendered_icc_profile_bytes = _get_adobergb_iccprofile()
  elif u_transform == proxy_const.ICCProfile.ROMMRGB:
    rendered_icc_profile_bytes = _get_rommrgb_iccprofile()
  elif u_transform == proxy_const.ICCProfile.DISPLAYP3:
    rendered_icc_profile_bytes = _get_displayp3_iccprofile()
  else:
    rendered_icc_profile_bytes = read_icc_profile_plugin_file(transform)
  if not rendered_icc_profile_bytes:
    msg = f'Could not load ICC Profile for transform: {transform}'
    cloud_logging_client.warning(msg)
    raise UnableToLoadIccProfileError(msg)
  dicom_input_profile = ImageCms.getOpenProfile(
      io.BytesIO(dicom_icc_profile_bytes)
  )
  rendered_icc_profile = ImageCms.getOpenProfile(
      io.BytesIO(rendered_icc_profile_bytes)
  )
  return icc_color_transform.IccColorTransform(
      transform,
      ImageCms.buildTransform(
          dicom_input_profile,
          rendered_icc_profile,
          _RGB,
          _RGB,
          renderingIntent=ImageCms.Intent.PERCEPTUAL,
      ),
      rendered_icc_profile_bytes,
  )


def _get_tag_value(instance_metadata: Mapping[str, Any], tag: str) -> Any:
  return instance_metadata[tag][_VALUE][0]


def _is_wsi_instance(instance_metadata: Mapping[str, Any]) -> bool:
  """Returns true if instance metadata represents WSI instance.

  Args:
    instance_metadata: DICOM JSON metadata for instance.

  Returns:
    True if metadata defines WSI instance for primary volume.
  """
  try:
    image_type = instance_metadata[_IMAGE_TYPE_DICOM_TAG_ADDRESS][_VALUE]
    number_of_frames = int(
        _get_tag_value(instance_metadata, _NUMBER_OF_FRAMES_DICOM_TAG_ADDRESS)
    )
    sop_class_uid = _get_tag_value(
        instance_metadata, _SOP_CLASS_UID_DICOM_TAG_ADDRESS
    )
  except (IndexError, KeyError, ValueError) as _:
    return False
  if sop_class_uid != _VL_WHOLE_SLIDE_MICROSCOPY_IMAGE_IOD:
    return False
  if number_of_frames <= 0:
    return False
  image_type = '\\'.join(image_type).upper()
  if any(
      [im_type in image_type for im_type in [_LABEL, _OVERVIEW, _LOCALIZER]]
  ):
    return False
  for im_type in [
      _ORIGINAL_PRIMARY_VOLUME,
      _DERIVED_PRIMARY_VOLUME,
      _THUMBNAIL,
  ]:
    if im_type in image_type:
      return True
  return False


def _icc_profile_cache_key(icc_profile_hash_value: str) -> str:
  return f'{icc_profile_hash_value}_ICC_PROFILE_BYTES'


def _icc_profile_hash_value(icc_profile_bytes: bytes) -> str:
  return hashlib.sha256(icc_profile_bytes).hexdigest()


def _get_cached_icc_profile_bytes(
    metadata: icc_profile_metadata_cache.ICCProfileHash,
    redis: redis_cache.RedisCache,
) -> Optional[redis_cache.RedisResult]:
  """Returns ICC Profile bytes in cache.

  Args:
    metadata: ICC Profile Metadata for instance.
    redis: redis cache.

  Returns:
    redis_cache.RedisResult or None if hash value of icc profile bytes has
    not been determined.
  """
  if not metadata:
    # Hash not initalized can not return bytes.
    # Hash may not be uninitalized if cache was set and then instance metadata
    # retrieval failed to retrieve and set bytes hash.
    return None
  if (
      metadata
      == icc_profile_metadata_cache.INSTANCE_MISSING_ICC_PROFILE_METADATA
  ):
    return redis_cache.RedisResult(None)  # Does not have icc profile.
  # look up result in redis cache.
  return redis.get(_icc_profile_cache_key(metadata))


def _set_cached_icc_profile(
    redis: redis_cache.RedisCache,
    series_url: dicom_url_util.DicomSeriesUrl,
    instance: dicom_url_util.SOPInstanceUID,
    icc_profile_bytes: Optional[bytes],
    icc_profile_metadata: icc_profile_metadata_cache.ICCProfileHash,
) -> None:
  """Sets ICC Profile bytes in cache.

  The key for the ICC Profile byte redis cache is the hash of the ICC profile
  bytes. This enables the proxy to cache ICC profiles to be easily reuse used
  across instances which have been encoded with the same profile.  The hash
  of the instances ICC profile is stored cached in the instance metadata. This
  enables the ICC profile to be retrieved at most once per instance.

  Args:
    redis: redis cache.
    series_url: DICOMweb url identifying series.
    instance: SOPInstanceUID frame request that is triggering ICCProfile
      request.
    icc_profile_bytes: ICC profile bytes.
    icc_profile_metadata: ICC profile metadata.

  Returns:
    None
  """
  if icc_profile_bytes is not None:
    # TTL for ICC profile cache disable TTL if flag value is < 0
    ttl_sec = icc_profile_metadata_cache.icc_profile_metadata_redis_cache_ttl()
    redis.set(
        _icc_profile_cache_key(icc_profile_metadata),
        icc_profile_bytes,
        ttl_sec=ttl_sec,
    )
  icc_profile_metadata_cache.set_cached_instance_icc_profile_metadata(
      redis,
      series_url,
      instance,
      icc_profile_metadata,
  )


def _set_icc_profile_cache_and_return(
    redis: redis_cache.RedisCache,
    dicom_series_url: dicom_url_util.DicomSeriesUrl,
    requested_sop_instance_uid: dicom_url_util.SOPInstanceUID,
    icc_profile_metadata: icc_profile_metadata_cache.ICCProfileHash,
    icc_profile_bytes: Optional[bytes],
) -> Optional[bytes]:
  """Sets ICC Profile cache and returns requested cached result."""
  _set_cached_icc_profile(
      redis,
      dicom_series_url,
      requested_sop_instance_uid,
      icc_profile_bytes,
      icc_profile_metadata,
  )
  return icc_profile_bytes


def _retrieve_instance_bulkdata_to_get_icc_profile(
    redis: redis_cache.RedisCache,
    session: user_auth_util.AuthSession,
    dicom_series_url: dicom_url_util.DicomSeriesUrl,
    requested_sop_instance_uid: dicom_url_util.SOPInstanceUID,
    enable_caching: cache_enabled_type.CachingEnabled,
) -> Optional[bytes]:
  """Gets ICC Profile & metadata for a instance using bulkdata."""
  cloud_logging_client.debug('Using bulkdata to retrieve ICC profile.')
  # DICOM stores supporting bulk data return bulk data uri to the ICC
  # profile when instance metadata is returned. If the dicom store supports
  # bulkdata the and no instance metadata was found in the cache then
  # re-init the icc profile metadata from the metadata stored within the
  # the instance. The retreival of the instance metadata is itself cached
  # to reduce its cost.
  try:
    metadata = metadata_util.get_instance_metadata(
        session, dicom_series_url, requested_sop_instance_uid, enable_caching
    )
  except metadata_util.ReadDicomMetadataError:
    return None
  if not metadata.has_icc_profile:
    _set_cached_icc_profile(
        redis,
        dicom_series_url,
        requested_sop_instance_uid,
        None,
        icc_profile_metadata_cache.INSTANCE_MISSING_ICC_PROFILE_METADATA,
    )
    return None
  bulk_data_uri = metadata.icc_profile_bulkdata_uri
  # if instance does not have uri. return None.
  if not bulk_data_uri:
    raise _MissingIccProfileBulkDataUriError()
  with io.BytesIO() as bytes_read:
    dicom_store_util.download_bulkdata(session, bulk_data_uri, bytes_read)
    icc_profile_bytes = bytes_read.getvalue()
    icc_profile_metadata = icc_profile_metadata_cache.ICCProfileHash(
        _icc_profile_hash_value(icc_profile_bytes)
    )
  return _set_icc_profile_cache_and_return(
      redis,
      dicom_series_url,
      requested_sop_instance_uid,
      icc_profile_metadata,
      icc_profile_bytes,
  )


@execution_timer.log_execution_time(
    'color_conversion_util.get_instance_icc_profile'
)
def get_instance_icc_profile_bytes(
    session: user_auth_util.AuthSession,
    series_url: dicom_url_util.DicomSeriesUrl,
    requested_sop_instance_uid: dicom_url_util.SOPInstanceUID,
    enable_caching: cache_enabled_type.CachingEnabled,
) -> Optional[bytes]:
  """Return DICOM ICCProfile bytes.

  Args:
    session: Identifies calling user's session.
    series_url: DICOMweb url identifying series.
    requested_sop_instance_uid: SOPInstanceUID frame request that is triggering
      ICCProfile request.
    enable_caching: Is cache enabled, used in testing, to disable caching for
      performance testing.

  Returns:
    ICCProfile bytes or tag path to ICCProfile
  """
  if not series_url:
    return None
  if not requested_sop_instance_uid.sop_instance_uid:
    return None
  if not enable_caching:
    cloud_logging_client.warning('ICC_PROFILE caching disabled.')
  redis = redis_cache.RedisCache(enable_caching)
  icc_profile_metadata = (
      icc_profile_metadata_cache.get_cached_instance_icc_profile_metadata(
          redis,
          series_url,
          requested_sop_instance_uid,
      )
  )
  if icc_profile_metadata is not None:
    result = _get_cached_icc_profile_bytes(icc_profile_metadata, redis)
    if result is not None:
      return result.value
  try:
    return _retrieve_instance_bulkdata_to_get_icc_profile(
        redis,
        session,
        series_url,
        requested_sop_instance_uid,
        enable_caching,
    )
  except dicom_store_util.DicomInstanceRequestError:
    cloud_logging_client.warning(
        'ICC Profile retrieval failed using bulk data. Using non-bulk data'
        ' methods.'
    )
    pass
  except _MissingIccProfileBulkDataUriError:
    pass
  return None


def _hash_key(
    icc_profile_metadata: icc_profile_metadata_cache.ICCProfileHash,
    transform: enum_types.ICCProfile,
) -> str:
  """Returns cachetools LRU hash key for ICC Profile transformation.

  Args:
    icc_profile_metadata: Metadata describing the DICOM ICC Profile.
    transform: ICC profile colorspace transform configuration.

  Returns:
    Key used for cachetools LRU cache.
  """
  return f'cache_tools_{icc_profile_metadata}_{transform}_ICCPROFILE_TRANSFORM'


def get_icc_profile_transform_for_dicom_url(
    session: user_auth_util.AuthSession,
    dicom_series_url: dicom_url_util.DicomSeriesUrl,
    requested_sop_instance_uid: dicom_url_util.SOPInstanceUID,
    enable_caching: cache_enabled_type.CachingEnabled,
    transform: enum_types.ICCProfile,
) -> Optional[icc_color_transform.IccColorTransform]:
  """Color transfrom to convert color in instance to defined colorspace.

  Caches transform results in local redis.  Cache mannaged by LRU.
  Cache works across users and process.

  Args:
    session: User authentication session to issue DICOM Store requests.
    dicom_series_url: DICOM series url to retrieve Color transform.
    requested_sop_instance_uid: SOPInstanceUID frame request that is triggering
      ICCProfile request.
    enable_caching: Enable ICC profile caching.
    transform: ICC profile colorspace transform configuration.

  Returns:
    Color transform to convert from ICCProfile colorspace to sRGB or None
    if no valid ICCprofile could be found for the instance.

  Raises:
    UnableToLoadIccProfileError: if transform cannot be loaded.
  """
  if proxy_const.ICCProfile.NO == transform:
    return None
  with _cache_lock:
    redis = redis_cache.RedisCache(enable_caching)
    if enable_caching:
      icc_profile_metadata = (
          icc_profile_metadata_cache.get_cached_instance_icc_profile_metadata(
              redis,
              dicom_series_url,
              requested_sop_instance_uid,
          )
      )
      if icc_profile_metadata is not None:
        if (
            icc_profile_metadata
            == icc_profile_metadata_cache.INSTANCE_MISSING_ICC_PROFILE_METADATA
        ):
          return None
        if icc_profile_metadata:
          result = _icc_transform_cache.get(
              _hash_key(icc_profile_metadata, transform)
          )
          if result is not None:
            return result
    icc_profile = get_instance_icc_profile_bytes(
        session, dicom_series_url, requested_sop_instance_uid, enable_caching
    )

    cloud_logging_client.debug('Creating ICC Profile Transform')
    result = _create_icc_profile_transform(icc_profile, transform)
    if enable_caching:
      # Retrieval of ICC Profile bytes updates icc profile metadata cache.
      icc_profile_metadata = (
          icc_profile_metadata_cache.get_cached_instance_icc_profile_metadata(
              redis,
              dicom_series_url,
              requested_sop_instance_uid,
          )
      )
      if (
          icc_profile_metadata is not None
          and icc_profile_metadata
          != icc_profile_metadata_cache.INSTANCE_MISSING_ICC_PROFILE_METADATA
          and icc_profile_metadata
      ):
        _icc_transform_cache[_hash_key(icc_profile_metadata, transform)] = (
            result
        )
    return result


def get_icc_profile_transform_for_local_file(
    cache: pydicom_single_instance_read_cache.PyDicomSingleInstanceCache,
    transform: enum_types.ICCProfile,
) -> Optional[icc_color_transform.IccColorTransform]:
  """Returns color transform to convert to transform color space.

  Args:
    cache: Single instance read Cache.
    transform: ICC profile colorspace transform configuration.

  Raises:
    UnableToLoadIccProfileError: if transform cannot be loaded.
  """
  # Not caching local file icc profile transforms. If caching is helpful
  # assume it will be handled externally.
  return _create_icc_profile_transform(cache.icc_profile, transform)


@execution_timer.log_execution_time(
    'color_conversion_util.transform_image_color'
)
def transform_image_color(
    buffer: np.ndarray,
    icc_profile_transform: Optional[icc_color_transform.IccColorTransform],
) -> Union[np.ndarray, image_util.PILImage]:
  """Transforms raw pixels data (RGB) to transform defined colorspace.

  Args:
    buffer: Raw pixel data to transform (RGB) 8 bytes per pixel e.g., RGBRGBRGB
    icc_profile_transform: Color space transformation to perfrom on image.

  Returns:
    Image returned unchanged as numpy array if no transform is defined.
    Otherwise, Returns PILImage encoding raw pixels (RGB) transformed to target
    colorspace.
  """
  if (
      icc_profile_transform is None
      or icc_profile_transform.color_transform is None
  ):
    return buffer
  # image color conversion transform conducted in PIL RGB color space
  image_util.bgr2rgb(buffer)
  img = PIL.Image.fromarray(buffer)
  ImageCms.applyTransform(
      img, icc_profile_transform.color_transform, inPlace=True
  )
  return image_util.PILImage(img)


# The digitial_pathology_dicom proxy runs using gunicorn, which forks worker
# processes. Forked processes do not re-init global state and assume their
# values at the time of the fork. This can result in forked modules being
# started with invalid global state, e.g., acquired locks that will not release
# or references state. os.register at fork, defines a function run in child
# forked processes following the fork to re-initalize the forked global module
# state.
os.register_at_fork(
    before=_clear_cache_before_fork, after_in_child=_init_fork_module_state
)
