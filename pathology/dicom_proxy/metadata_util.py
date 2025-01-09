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
"""DICOM instance metadata & retrieval utility."""

from __future__ import annotations

import contextlib
import dataclasses
import math
import os
import sys
import threading
from typing import Any, List, Mapping, Optional, Union
import uuid

import cachetools
import dataclasses_json
import numpy as np
import pydicom

from pathology.dicom_proxy import bulkdata_util
from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import dicom_store_util
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import icc_profile_metadata_cache
from pathology.dicom_proxy import redis_cache
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.logging_lib import cloud_logging_client


# Metadata Flag Cache Const
_SECONDS_IN_HOUR = 3600
_SECONDS_IN_MIN = 60
_LOCALMETADATA_TTL = 300
_LOCALMETADATA_CACHESIZE = 30

# Metadata Dicom Tags
_STUDY_INSTANCE_UID_DICOM_TAG = '0020000D'
_SERIES_INSTANCE_UID_DICOM_TAG = '0020000E'
_SOP_INSTANCE_UID_DICOM_TAG = '00080018'
_SOP_CLASS_UID_DICOM_ADDRESS_TAG = '00080016'
_TOTAL_PIXEL_MATRIX_COLUMNS_DICOM_ADDRESS_TAG = '00480006'
_TOTAL_PIXEL_MATRIX_ROWS_DICOM_ADDRESS_TAG = '00480007'
_COLUMNS_DICOM_ADDRESS_TAG = '00280011'
_ROWS_DICOM_ADDRESS_TAG = '00280010'
_LOSSY_COMPRESSION_METHOD_DICOM_ADDRESS_TAG = '00282114'
_LOSSY_IMAGE_COMPRESSION_DICOM_ADDRESS_TAG = '00282110'
_SAMPLES_PER_PIXEL_DICOM_ADDRESS_TAG = '00280002'
_PLANAR_CONFIGURATION_DICOM_ADDRESS_TAG = '00280006'
_BITS_ALLOCATED_DICOM_ADDRESS_TAG = '00280100'
_BITS_STORED_DICOM_ADDRESS_TAG = '00280101'
_HIGH_BITS_DICOM_ADDRESS_TAG = '00280102'
_NUMBER_OF_FRAMES_DICOM_ADDRESS_TAG = '00280008'
_DIMENSION_ORGANIZATION_TYPE = '00209311'
_COLOR_SPACE_DICOM_ADDRRESS_TAG = '00282002'
_ICC_PROFILE_ADDRESS_TAG = '00282000'
_SHARED_FUNCTIONAL_GROUPS_SEQUENCE_TAG = '52009229'
_PIXEL_MEASURES_SEQUENCE_TAG = '00289110'
_PIXEL_SPACING_TAG = '00280030'
_DICOM_TRANSFER_SYNTAX = '00020010'
_OPTICAL_PATH_SEQ_TAG = '00480105'
_SERIES_INSTANCE_UID = '0020000E'

_TILED_FULL = 'TILED_FULL'
_TILED_SPARSE = 'TILED_SPARSE'


# Constants
_VALUE = 'Value'
_VL_WHOLE_SLIDE_MICROSCOPY_IMAGE_IOD = '1.2.840.10008.5.1.4.1.1.77.1.6'
_MICROSCOPY_BULK_SIMPLE_ANNOTATIONS_IOD = '1.2.840.10008.5.1.4.1.1.91.1'

_IMPLICIT_VR_ENDIAN = '1.2.840.10008.1.2'
_EXPLICIT_VR_LITTLE_ENDIAN = '1.2.840.10008.1.2.1'
_DEFLATED_EXPLICIT_VR_LITTLE_ENDIAN = '1.2.840.10008.1.2.1.99'
_EXPLICIT_VR_BIG_ENDIAN = '1.2.840.10008.1.2.2'

_UNENCAPSULATED_TRANSFER_SYNTAXS = {
    _IMPLICIT_VR_ENDIAN,
    _EXPLICIT_VR_LITTLE_ENDIAN,
    _DEFLATED_EXPLICIT_VR_LITTLE_ENDIAN,
    _EXPLICIT_VR_BIG_ENDIAN,
}


class JsonMetadataDownsampleError(Exception):
  pass


class ReadDicomMetadataError(Exception):
  pass


class GetSeriesInstanceUIDError(Exception):
  pass


@dataclasses.dataclass(frozen=True)
class MetadataUID:
  study_instance_uid: str
  series_instance_uid: str
  sop_instance_uid: str

  def is_defined(self) -> bool:
    return (
        bool(self.study_instance_uid)
        and bool(self.series_instance_uid)
        and bool(self.sop_instance_uid)
    )


_metadata_cache = cachetools.TTLCache(
    _LOCALMETADATA_CACHESIZE, ttl=_LOCALMETADATA_TTL
)
_metadata_cache_lock = threading.Lock()
_is_debugging = 'UNITTEST_ON_FORGE' in os.environ or 'unittest' in sys.modules


def _init_fork_module_state() -> None:
  global _metadata_cache
  global _metadata_cache_lock
  _metadata_cache = cachetools.TTLCache(
      _LOCALMETADATA_CACHESIZE, ttl=_LOCALMETADATA_TTL
  )
  _metadata_cache_lock = threading.Lock()


def clear_local_metadata_cache() -> None:
  with _metadata_cache_lock:
    _metadata_cache.clear()


class ClearLocalMetadata(contextlib.ContextDecorator):

  def __enter__(self):
    clear_local_metadata_cache()

  def __exit__(self, exc_type, exc_value, traceback):
    return


def is_transfer_syntax_encapsulated(transfer_syntax_uid: str) -> bool:
  """Returns True if DICOM Transfer Syntax describes and encapsulated format.

  DICOM Pixel data is encoded in the PixelData tag. The encoding format is
  described by the DICOM transfer syntax. DICOM transfer syntaxs and UID are
  listed here: https://www.dicomlibrary.com/dicom/transfer-syntax/

  Unencapsulated transfer syntaxs encode the pixel data uncompressed as a byte
  array in the in PixelData tag. Note, the Deflated transfer syntax, is special,
  it encodes the pixel data uncompressed and then compresses the entire DICOM
  dataset, excluding the Group 2 header.

  The format for the Pixel data byte array is defined by tags in in
  the Image Pixel Module:
  https://dicom.nema.org/medical/Dicom/2018d/output/chtml/part03/sect_C.7.6.3.html
  Using the Samples Per Pixel, Bits Allocated, Bits Stored, High Bit, and
  Pixel representation tags.

  Also, due to its current implementation the Google DICOM store currently
  limits unencapsulated Pixel Data to ~2 Gigs.

  The UID listed below are unencapsulated transfer syntaxs:
    1.2.840.10008.1.2   Implicit VR Endian: Default Transfer Syntax for DICOM
    1.2.840.10008.1.2.1 Explicit VR Little Endian
    1.2.840.10008.1.2.1.99      Deflated Explicit VR Little Endian
    1.2.840.10008.1.2.2 Explicit VR Big Endian

  All other Transfer syntaxs describe encapsulated formats:
  Examples:
    1.2.840.10008.1.2.4.50      JPEG Baseline (Process 1)
    1.2.840.10008.1.2.4.90      JPEG 2000 Image Compression
  These encodings represent the pixel data as one or more encapsulated binary
  encodings. All encapsulated formats describe compressed data. The format the
  encapsulated pixel data is defined by encapsulated encoding and the
  Image Module tag metadata will define the decompressed represention of the
  encapsulated data. Encapsulated data is bounded by additional formatting so
  the total size of the encapsulated PixelData will be slightly larger than the
  encoded data. In the Google DICOM store, encapsulated formats much larger
  amounts of pixel data; currently total instance sizes for encapsulated
  instances is ~20 gigs, this will likely increase.

  Args:
    transfer_syntax_uid: String UID DICOM transfer syntax.

  Returns:
    True if syntax is an encapsulated format.
  """
  return transfer_syntax_uid not in _UNENCAPSULATED_TRANSFER_SYNTAXS


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class MetadataSource:
  store_url: dicom_url_util.DicomSeriesUrl
  sop_instance_uid: dicom_url_util.SOPInstanceUID
  caching_enabled: cache_enabled_type.CachingEnabled


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class DicomInstanceMetadata:
  """DICOM Instance Metadata."""

  sop_class_uid: str  # 00080016
  total_pixel_matrix_columns: int  # 00480006
  total_pixel_matrix_rows: int  # 00480007
  columns: int  # 00280011
  rows: int  # 00280010
  number_of_frames: int  # 00280008

  samples_per_pixel: int  # 00280002
  planar_configuration: int  # 00280006
  bits_allocated: int  # 00280100
  bits_stored: int  # 00280101
  high_bit: int  # 00280102
  lossy_image_compression: str  # 00282110
  lossy_compression_method: str  # 00282114
  dimension_organization_type: str  # 00209311  TILED_FULL
  dicom_transfer_syntax: str  # 00020010

  # Instance icc_profile metadata should not be accessed directly.
  # contains metadata discovered when instance metadata was read and will only
  # contain values for DICOM stores supporting bukdata retrieval. It is used
  # internally to reinitalize ICC Profile metadata cache.
  #
  # Access instance ICC profile metadata using accessors on the
  # Classes derived from DICOM Instance Request or through methods
  # (get_series_icc_profile_colorspace, get_series_icc_profile_bytes,
  #  and get_series_icc_profile_path) exposed on color_conversion_util module.
  _instance_icc_profile_metadata: Optional[
      icc_profile_metadata_cache.ICCProfileMetadata
  ]

  # Source metadata used to request instance metadata.
  metadata_source: MetadataSource

  @property
  def is_initialized(self) -> bool:
    return bool(self.sop_class_uid)

  @property
  def is_tiled_full(self) -> bool:
    return self.dimension_organization_type.strip().upper() == _TILED_FULL

  @property
  def is_tiled_sparse(self) -> bool:
    return self.dimension_organization_type.strip().upper() == _TILED_SPARSE

  @property
  def is_tiled_not_set(self) -> bool:
    return not bool(self.dimension_organization_type.strip())

  def replace(self, changes: Mapping[str, Any]) -> DicomInstanceMetadata:
    """Returns new instance reflecting metadata + changes."""
    return dataclasses.replace(self, **changes)

  @property
  def is_wsi_iod(self) -> bool:
    """Returns true if instance metadata defines WSI DICOM IOD."""
    return self.sop_class_uid == _VL_WHOLE_SLIDE_MICROSCOPY_IMAGE_IOD

  @property
  def frames_per_row(self) -> int:
    return int(
        math.ceil(float(self.total_pixel_matrix_columns) / float(self.columns))
    )

  @property
  def frames_per_column(self) -> int:
    return int(
        math.ceil(float(self.total_pixel_matrix_rows) / float(self.rows))
    )

  def downsample(self, downsample: float) -> DicomInstanceMetadata:
    """Returns downsampled representation of DICOM instance metadata.

    Args:
      downsample: Factor to downsample metadata by.

    Returns:
      Downsampled metadata.
    """
    total_pixel_matrix_columns = max(
        int(self.total_pixel_matrix_columns / downsample), 1
    )
    total_pixel_matrix_rows = max(
        int(self.total_pixel_matrix_rows / downsample), 1
    )
    frames_per_row = int(
        math.ceil(float(total_pixel_matrix_columns) / float(self.columns))
    )
    frames_per_column = int(
        math.ceil(float(total_pixel_matrix_rows) / float(self.rows))
    )
    number_of_frames = frames_per_row * frames_per_column
    return DicomInstanceMetadata(
        self.sop_class_uid,
        total_pixel_matrix_columns,
        total_pixel_matrix_rows,
        self.columns,
        self.rows,
        number_of_frames,
        self.samples_per_pixel,
        self.planar_configuration,
        self.bits_allocated,
        self.bits_stored,
        self.high_bit,
        self.lossy_image_compression,
        self.lossy_compression_method,
        self.dimension_organization_type,
        self.dicom_transfer_syntax,
        self._instance_icc_profile_metadata,
        self.metadata_source,
    )

  def pad_image_to_frame(self, img: np.ndarray) -> np.ndarray:
    if img.shape[0] == self.rows and img.shape[1] == self.columns:
      return img
    if img.shape[0] > self.rows or img.shape[1] > self.columns:
      raise ValueError('Image dimensions exceed frame dimensions.')
    mem = np.zeros((self.rows, self.columns, img.shape[2]), dtype=img.dtype)
    mem[0 : img.shape[0], 0 : img.shape[1], ...] = img[...]
    return mem

  @property
  def is_baseline_jpeg(self) -> bool:
    """Tests if metadata appears to reference lossy jpeg encoded data.

    Returns:
      True if DICOM metadata appears to be lossy jpeg.
    """
    return self.dicom_transfer_syntax == '1.2.840.10008.1.2.4.50'

  @property
  def is_jpeg2000(self) -> bool:
    """Tests if metadata reference jpeg2000 encoded data.

    Returns:
      True if DICOM metadata appears to be jpeg2000.
    """
    return self.dicom_transfer_syntax in (
        '1.2.840.10008.1.2.4.90',
        '1.2.840.10008.1.2.4.91',
    )

  @property
  def is_jpegxl(self) -> bool:
    """Tests if metadata references jpegxl encoded data.

    Returns:
      True if DICOM metadata appears to be jpegxl.
    """
    return self.dicom_transfer_syntax in (
        '1.2.840.10008.1.2.4.110',
        '1.2.840.10008.1.2.4.111',
        '1.2.840.10008.1.2.4.112',
    )

  @property
  def is_jpg_transcoded_to_jpegxl(self) -> bool:
    """Tests if metadata reference jpeg transcoded tojpegxl encoded data.

    Returns:
      True if DICOM metadata appears to be jpeg transcoded to jpegxl.
    """
    return self.dicom_transfer_syntax == '1.2.840.10008.1.2.4.111'

  @property
  def image_compression(self) -> Optional[enum_types.Compression]:
    """Returns compression encoding of source image bytes; None = Unknown."""
    if self.is_baseline_jpeg:
      return enum_types.Compression.JPEG
    if self.is_jpeg2000:
      return enum_types.Compression.JPEG2000
    if self.is_jpg_transcoded_to_jpegxl:
      return enum_types.Compression.JPEG_TRANSCODED_TO_JPEGXL
    if self.is_jpegxl:
      return enum_types.Compression.JPEGXL
    return None

  def init_instance_icc_profile_metadata(self) -> None:
    if self._instance_icc_profile_metadata is None:
      return
    store_url = self.metadata_source.store_url
    icc_profile_metadata_cache.set_cached_instance_icc_profile_metadata(
        redis_cache.RedisCache(self.metadata_source.caching_enabled),
        store_url,
        self.metadata_source.sop_instance_uid,
        bulkdata_util.does_dicom_store_support_bulkdata(store_url),
        self._instance_icc_profile_metadata,
    )


def _get_value(metadata: Mapping[str, Any], key: str) -> Any:
  """Returns DICOM tag JSON value.

  Args:
    metadata: DICOM formatted JSON metadata.
    key: Metadata key to return value for.

  Returns:
    Metadata key value.
  """
  return metadata[key][_VALUE][0]


def _get_str_value(metadata: Mapping[str, Any], key: str) -> str:
  """Returns string DICOM tag JSON value.

  Args:
    metadata: DICOM formatted JSON metadata.
    key: Metadata key to return value for.

  Returns:
    String metadata key value.
  """
  try:
    return str(_get_value(metadata, key))
  except (IndexError, KeyError, TypeError) as _:
    return ''


def _get_bulkdatauri(metadata: Mapping[str, Any], key: str) -> str:
  """Returns string DICOM tag JSON value.

  Args:
    metadata: DICOM formatted JSON metadata.
    key: Metadata key to return value for.

  Returns:
    String metadata key value.
  """
  try:
    return str(metadata[key].get(bulkdata_util.BULK_DATA_URI_KEY, ''))
  except (IndexError, KeyError, TypeError) as _:
    return ''


def _has_tag_value(metadata: Mapping[str, Any], key: str) -> bool:
  """Returns true if DICOM tag has value.

  Args:
    metadata: DICOM formatted JSON metadata.
    key: Metadata key to return value for.

  Returns:
    True if tag value defined.
  """
  try:
    _ = _get_value(metadata, key)
    return True
  except (IndexError, KeyError, ValueError, TypeError) as _:
    return False


def _set_tag_int_value(
    metadata: Mapping[str, Any], key: str, value: int
) -> None:
  """Sets DICOM JSON tag value.

  Args:
    metadata: JSON metadata.
    key: DICOM tag hex address (not starting with 0x).
    value: Int value to set tag to.

  Returns:
    None
  Raises:
    ValueError: Tag value contains more than one value.
  """
  if len(metadata[key][_VALUE]) != 1:
    raise ValueError('Tag value does not contain one value as expected.')
  # DICOM JSON values define an array of values. Setting first element.  All
  # callers should have only one array value.
  metadata[key][_VALUE][0] = int(value)


def _get_int_value(metadata: Mapping[str, Any], key: str) -> int:
  """Returns int DICOM tag JSON value.

  Args:
    metadata: DICOM formatted JSON metadata.
    key: Metadata key to return value for.

  Returns:
    Int metadata key value.
  """
  try:
    return int(_get_value(metadata, key))
  except (IndexError, KeyError, ValueError, TypeError) as _:
    return 0


def _get_iccprofile_and_colorspace_(
    path: str,
    metadata: Mapping[str, Any],
) -> List[icc_profile_metadata_cache.ICCProfileMetadata]:
  """Returns ICC Profile metadata, if any, from DICOM Dataset."""
  color_space = _get_str_value(metadata, _COLOR_SPACE_DICOM_ADDRRESS_TAG)
  icc_profile_address = _get_bulkdatauri(metadata, _ICC_PROFILE_ADDRESS_TAG)
  if icc_profile_address:
    return [
        icc_profile_metadata_cache.ICCProfileMetadata(
            path, color_space, icc_profile_address, ''
        )
    ]
  return []


def _init_instance_icc_profile_from_metadata(
    metadata: Mapping[str, Any],
) -> List[icc_profile_metadata_cache.ICCProfileMetadata]:
  """Returns a list of refs to ICC profile metadata defined in a DICOM instance.

  Args:
    metadata: DICOM formatted JSON metadata for a single instance.

  Returns:
    List of the ICC profile metadata described within an instance.
  """
  try:
    result_list = []
    optical_path_sequence = metadata.get(_OPTICAL_PATH_SEQ_TAG)
    if optical_path_sequence is not None:
      for index, optical_path in enumerate(
          optical_path_sequence.get(_VALUE, [])
      ):
        result_list.extend(
            _get_iccprofile_and_colorspace_(
                f'OpticalPathSequence/{index}/ICCProfile', optical_path
            )
        )
    result_list.extend(_get_iccprofile_and_colorspace_('ICCProfile', metadata))
    if len(result_list) > 1:
      cloud_logging_client.warning(
          'DICOM instance defines multiple ICC Profiles; server side correction'
          ' will use the first profile found.'
      )
    return result_list
  except AttributeError:
    return []


def _init_metadata_from_json(
    store_url: dicom_url_util.DicomSeriesUrl,
    metadata: Mapping[str, Any],
    caching_enabled: cache_enabled_type.CachingEnabled,
) -> DicomInstanceMetadata:
  """Init metadata structure from DICOM JSON.

  Args:
    store_url: URL encoding DICOM store and HCLS store version.
    metadata: DICOM formatted JSON metadata.
    caching_enabled: Metadata caching enabled.

  Returns:
    DicomInstanceMetadata
  """
  sop_class_uid = _get_str_value(metadata, _SOP_CLASS_UID_DICOM_ADDRESS_TAG)
  sop_instance_uid = _get_str_value(metadata, _SOP_INSTANCE_UID_DICOM_TAG)
  total_pixel_matrix_columns = _get_int_value(
      metadata, _TOTAL_PIXEL_MATRIX_COLUMNS_DICOM_ADDRESS_TAG
  )
  total_pixel_matrix_rows = _get_int_value(
      metadata, _TOTAL_PIXEL_MATRIX_ROWS_DICOM_ADDRESS_TAG
  )
  columns = _get_int_value(metadata, _COLUMNS_DICOM_ADDRESS_TAG)
  rows = _get_int_value(metadata, _ROWS_DICOM_ADDRESS_TAG)
  samples_per_pixel = _get_int_value(
      metadata, _SAMPLES_PER_PIXEL_DICOM_ADDRESS_TAG
  )
  planar_configuration = _get_int_value(
      metadata, _PLANAR_CONFIGURATION_DICOM_ADDRESS_TAG
  )
  bits_allocated = _get_int_value(metadata, _BITS_ALLOCATED_DICOM_ADDRESS_TAG)
  bits_stored = _get_int_value(metadata, _BITS_STORED_DICOM_ADDRESS_TAG)
  high_bit = _get_int_value(metadata, _HIGH_BITS_DICOM_ADDRESS_TAG)

  lossy_image_compression = _get_str_value(
      metadata, _LOSSY_IMAGE_COMPRESSION_DICOM_ADDRESS_TAG
  )
  lossy_compression_method = _get_str_value(
      metadata, _LOSSY_COMPRESSION_METHOD_DICOM_ADDRESS_TAG
  )
  number_of_frames = _get_int_value(
      metadata, _NUMBER_OF_FRAMES_DICOM_ADDRESS_TAG
  )
  dimension_organization_type = _get_str_value(
      metadata, _DIMENSION_ORGANIZATION_TYPE
  )
  dicom_transfer_syntax = _get_str_value(metadata, _DICOM_TRANSFER_SYNTAX)

  icc_profiles = _init_instance_icc_profile_from_metadata(metadata)
  has_bulk_data_icc_profile = any(
      [bool(profile.bulkdata_uri) for profile in icc_profiles]
  )
  if has_bulk_data_icc_profile:
    bulkdata_util.set_dicom_store_supports_bulkdata(store_url)
  else:
    bulkdata_util.test_dicom_store_metadata_for_bulkdata_uri_support(
        store_url, metadata
    )
  if icc_profiles:
    instance_icc_profile_metadata = icc_profiles[0]
  elif bulkdata_util.does_dicom_store_support_bulkdata(store_url):
    instance_icc_profile_metadata = (
        icc_profile_metadata_cache.INSTANCE_MISSING_ICC_PROFILE_METADATA
    )
  else:
    # Instance may have ICC Profile metadata but none was detected in JASON at
    # time of metadata retrieval.
    instance_icc_profile_metadata = None
  md = DicomInstanceMetadata(
      sop_class_uid,
      total_pixel_matrix_columns,
      total_pixel_matrix_rows,
      columns,
      rows,
      number_of_frames,
      samples_per_pixel,
      planar_configuration,
      bits_allocated,
      bits_stored,
      high_bit,
      lossy_image_compression,
      lossy_compression_method,
      dimension_organization_type,
      dicom_transfer_syntax,
      instance_icc_profile_metadata,
      MetadataSource(
          store_url,
          dicom_url_util.SOPInstanceUID(sop_instance_uid),
          caching_enabled,
      ),
  )
  md.init_instance_icc_profile_metadata()
  return md


def _cache_key(
    user_auth: user_auth_util.AuthSession,
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
    user_auth: User authentication.
    series_url: DICOM series requested.
    instance: DICOM SOPInstanceUID for instance in series requested.  Returns
      cache key for metadata request (str)
  """
  iap_user_id = user_auth.iap_user_id  # If IAP not enabled == ''
  user_bearer_token = user_auth.authorization
  return (
      f'metadata url:{series_url}/{instance} '
      f'bearer:{user_bearer_token} iap:{iap_user_id} email:{user_auth.email}'
  )


def _dicom_metadata_cache_ttl() -> int:
  """Returns TTL to attach to cached metadata request."""
  return min(
      _SECONDS_IN_HOUR,
      max(
          _SECONDS_IN_MIN,
          dicom_proxy_flags.USER_LEVEL_METADATA_TTL_FLG.value,
      ),
  )


def _cache_tools_hash_key(
    user_auth: user_auth_util.AuthSession,
    series_url: dicom_url_util.DicomSeriesUrl,
    instance: dicom_url_util.SOPInstanceUID,
    unused_caching_enabled: cache_enabled_type.CachingEnabled,
) -> str:
  key = _cache_key(user_auth, series_url, instance)
  return key if not _is_debugging else f'{key}_{uuid.uuid1()}'


def get_series_instance_uid_for_study_and_instance_uid(
    user_auth: user_auth_util.AuthSession,
    baseurl: dicom_url_util.DicomWebBaseURL,
    study_instance_uid: dicom_url_util.StudyInstanceUID,
    sop_instance_uid: dicom_url_util.SOPInstanceUID,
) -> str:
  """Returns series instance UID for study that contains instance uid.

  Args:
    user_auth: User authentication session to use to connect to store.
    baseurl: Base URL for connecting to DICOM store.
    study_instance_uid: DICOM Study Instance UID.
    sop_instance_uid: DICOM Study Instance UID.

  Returns:
    series instance uid which is passed study instance uid and contains
    passed sop instance uid.

  Raises:
    GetSeriesInstanceUIDError: Unable to find study instance UID.
  """
  if (
      not study_instance_uid.study_instance_uid
      or not sop_instance_uid.sop_instance_uid
  ):
    raise GetSeriesInstanceUIDError(
        'Undefined StudyInstanceUID and/or SOPInstanceUID.'
    )
  try:
    metadata = dicom_store_util.get_dicom_study_instance_metadata(
        user_auth,
        dicom_url_util.base_dicom_study_url(baseurl, study_instance_uid),
        sop_instance_uid,
    )
  except dicom_store_util.DicomMetadataRequestError as exp:
    raise GetSeriesInstanceUIDError('Error reading metadata.') from exp
  if len(metadata) != 1:
    raise GetSeriesInstanceUIDError(
        'DICOM metadata search found multiple instances.'
    )
  try:
    return metadata[0][_SERIES_INSTANCE_UID][_VALUE][0]
  except (IndexError, KeyError, TypeError) as exp:
    raise GetSeriesInstanceUIDError(
        'Error retrieving SeriesInstanceUID from DICOM instance.'
    ) from exp


@cachetools.cached(
    _metadata_cache, key=_cache_tools_hash_key, lock=_metadata_cache_lock
)
def get_instance_metadata(
    user_auth: user_auth_util.AuthSession,
    series_url: dicom_url_util.DicomSeriesUrl,
    instance: dicom_url_util.SOPInstanceUID,
    caching_enabled: cache_enabled_type.CachingEnabled,
) -> DicomInstanceMetadata:
  """Returns metadata structure initialized from DICOM instance in Store.

  Args:
    user_auth: User authentication session to use to connect to store.
    series_url: URL of series in store.
    instance: DICOM SOP Instance UID of instance metadata to return.
    caching_enabled: Metadata caching enabled.

  Returns:
    DicomInstanceMetadata

  Raises:
    ReadDicomMetadataError: Error occurred requesting metadata.
  """
  cache_key = _cache_key(user_auth, series_url, instance)
  redis = redis_cache.RedisCache(
      cache_enabled_type.CachingEnabled(
          caching_enabled
          and not dicom_proxy_flags.DISABLE_DICOM_METADATA_CACHING_FLG.value
      )
  )
  result = redis.get(cache_key)
  if result is not None:
    if result.value is not None:
      return DicomInstanceMetadata.from_json(result.value.decode('utf-8'))
  try:
    result = dicom_store_util.get_dicom_instance_metadata(
        user_auth, series_url, instance
    )
  except Exception as exp:
    cloud_logging_client.error(
        'Exception occurred requesting DICOM metadata', exp
    )
    raise ReadDicomMetadataError(str(exp)) from exp
  if len(result) != 1:
    msg = 'Download metadata returned invalid results.'
    cloud_logging_client.error(msg, {'results': result})
    raise ReadDicomMetadataError(msg)
  return_metadata = _init_metadata_from_json(
      series_url, result[0], caching_enabled
  )
  if return_metadata.is_initialized:
    # set cache only if metadata is read.
    redis.set(
        cache_key,
        return_metadata.to_json(sort_keys=True).encode('utf-8'),
        ttl_sec=_dicom_metadata_cache_ttl(),
    )
  return return_metadata


def get_instance_metadata_from_local_instance(
    path: Union[str, pydicom.FileDataset],
) -> DicomInstanceMetadata:
  """Returns DicomInstanceMetadata initialized from loaded instance or file path.

  Args:
    path: File path to local DICOM instance or loaded pydicom.FileDataset

  Returns:
    DicomInstanceMetadata
  """
  if isinstance(path, pydicom.FileDataset):
    file_meta = path.file_meta.to_json_dict()
    dicom_md = path.to_json_dict()
  else:
    with pydicom.dcmread(path) as ds:
      file_meta = ds.file_meta.to_json_dict()
      dicom_md = ds.to_json_dict()
  dicom_md.update(file_meta)
  return _init_metadata_from_json(
      dicom_url_util.DicomSeriesUrl(bulkdata_util.LOCALHOST),
      dicom_md,
      cache_enabled_type.CachingEnabled(False),
  )


def is_vl_wholeside_microscopy_iod(metadata: Mapping[str, Any]) -> bool:
  return (
      _get_str_value(metadata, _SOP_CLASS_UID_DICOM_ADDRESS_TAG)
      == _VL_WHOLE_SLIDE_MICROSCOPY_IMAGE_IOD
  )


def is_bulk_microscopy_simple_annotation(metadata: Mapping[str, Any]) -> bool:
  return (
      _get_str_value(metadata, _SOP_CLASS_UID_DICOM_ADDRESS_TAG)
      == _MICROSCOPY_BULK_SIMPLE_ANNOTATIONS_IOD
  )


def is_sparse_dicom(metadata: Mapping[str, Any]) -> bool:
  return (
      _get_str_value(metadata, _DIMENSION_ORGANIZATION_TYPE).strip().upper()
      == _TILED_SPARSE
  )


def get_metadata_uid(
    study_uid: dicom_url_util.StudyInstanceUID,
    series_uid: dicom_url_util.SeriesInstanceUID,
    instance_uid: dicom_url_util.SOPInstanceUID,
    metadata: Mapping[str, Any],
) -> MetadataUID:
  """Return UID for DICOM instance, get values from metadata if param undefined."""
  study_instance_uid = (
      study_uid.study_instance_uid
      if study_uid.study_instance_uid
      else _get_str_value(metadata, _STUDY_INSTANCE_UID_DICOM_TAG)
  )
  series_instance_uid = (
      series_uid.series_instance_uid
      if series_uid.series_instance_uid
      else _get_str_value(metadata, _SERIES_INSTANCE_UID_DICOM_TAG)
  )
  sop_instance_uid = (
      instance_uid.sop_instance_uid
      if instance_uid.sop_instance_uid
      else _get_str_value(metadata, _SOP_INSTANCE_UID_DICOM_TAG)
  )
  return MetadataUID(study_instance_uid, series_instance_uid, sop_instance_uid)


def downsample_json_metadata(
    metadata: Mapping[str, Any], downsample: float
) -> bool:
  """Downsamples DICOM JSON metadata.

  Args:
    metadata: DICOM JSON metadata to downsample.
    downsample: Downsampling factor.

  Returns:
    True if metadata altered.

  Raises:
    JsonMetadataDownsampleError: Metadata does not contain sufficient data to
                                 generate downsampled view of NumberOfFrames
  """
  if downsample <= 1.0:
    return False
  if not is_vl_wholeside_microscopy_iod(metadata):
    return False
  if is_sparse_dicom(metadata):
    msg = 'Metadata downsampling is not supported on sparse DICOM.'
    cloud_logging_client.error(msg)
    raise JsonMetadataDownsampleError(msg)
  if _has_tag_value(metadata, _TOTAL_PIXEL_MATRIX_COLUMNS_DICOM_ADDRESS_TAG):
    total_pixel_matrix_columns = _get_int_value(
        metadata, _TOTAL_PIXEL_MATRIX_COLUMNS_DICOM_ADDRESS_TAG
    )
    total_pixel_matrix_columns = max(
        int(total_pixel_matrix_columns / downsample), 1
    )
    _set_tag_int_value(
        metadata,
        _TOTAL_PIXEL_MATRIX_COLUMNS_DICOM_ADDRESS_TAG,
        total_pixel_matrix_columns,
    )
  else:
    total_pixel_matrix_columns = None

  if _has_tag_value(metadata, _TOTAL_PIXEL_MATRIX_ROWS_DICOM_ADDRESS_TAG):
    total_pixel_matrix_rows = _get_int_value(
        metadata, _TOTAL_PIXEL_MATRIX_ROWS_DICOM_ADDRESS_TAG
    )
    total_pixel_matrix_rows = max(int(total_pixel_matrix_rows / downsample), 1)
    _set_tag_int_value(
        metadata,
        _TOTAL_PIXEL_MATRIX_ROWS_DICOM_ADDRESS_TAG,
        total_pixel_matrix_rows,
    )
  else:
    total_pixel_matrix_rows = None

  if _has_tag_value(metadata, _COLUMNS_DICOM_ADDRESS_TAG):
    columns = _get_int_value(metadata, _COLUMNS_DICOM_ADDRESS_TAG)
  else:
    columns = None

  if _has_tag_value(metadata, _ROWS_DICOM_ADDRESS_TAG):
    rows = _get_int_value(metadata, _ROWS_DICOM_ADDRESS_TAG)
  else:
    rows = None

  if _has_tag_value(metadata, _NUMBER_OF_FRAMES_DICOM_ADDRESS_TAG):
    tag_address = (
        _TOTAL_PIXEL_MATRIX_COLUMNS_DICOM_ADDRESS_TAG,
        _TOTAL_PIXEL_MATRIX_ROWS_DICOM_ADDRESS_TAG,
        _COLUMNS_DICOM_ADDRESS_TAG,
        _ROWS_DICOM_ADDRESS_TAG,
    )
    tag_values = (
        total_pixel_matrix_columns,
        total_pixel_matrix_rows,
        columns,
        rows,
    )
    for address, value in zip(tag_address, tag_values):
      if value is None:
        raise JsonMetadataDownsampleError(
            f'DICOM tag {address} is undefined cannot return number of frames'
            ' in downsampled DICOM instance.'
        )
    frames_per_row = math.ceil(float(total_pixel_matrix_rows) / float(rows))
    frames_per_column = math.ceil(
        float(total_pixel_matrix_columns) / float(columns)
    )
    number_of_frames = int(frames_per_row * frames_per_column)
    _set_tag_int_value(
        metadata, _NUMBER_OF_FRAMES_DICOM_ADDRESS_TAG, number_of_frames
    )
    if number_of_frames == 1:
      _set_tag_int_value(
          metadata, _COLUMNS_DICOM_ADDRESS_TAG, total_pixel_matrix_columns
      )
      _set_tag_int_value(
          metadata, _ROWS_DICOM_ADDRESS_TAG, total_pixel_matrix_rows
      )

  try:
    pixel_spacing = metadata[_SHARED_FUNCTIONAL_GROUPS_SEQUENCE_TAG][_VALUE][0][
        _PIXEL_MEASURES_SEQUENCE_TAG
    ][_VALUE][0][_PIXEL_SPACING_TAG][_VALUE]
    for index, value in enumerate(list(pixel_spacing)):
      pixel_spacing[index] = float(value) * downsample
  except (IndexError, KeyError, ValueError) as _:
    pass
  return True


os.register_at_fork(after_in_child=_init_fork_module_state)
