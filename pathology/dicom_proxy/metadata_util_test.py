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
"""Tests for metadata util."""

import copy
import dataclasses
import json
import math
from typing import Any, Mapping
from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import cachetools
import numpy as np
import redis

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import dicom_store_util
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import metadata_util
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import shared_test_util
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.test_utils.dicom_store_mock import dicom_store_mock


# Mark flags as parsed to avoid UnparsedFlagAccessError for tests using
# --test_srcdir flag in parameters.
flags.FLAGS.mark_as_parsed()

_MOCK_DICOM_STORE_URL = 'https://healthcare.googleapis.com/v1'
_NONEXISTENT_DICOM_TAG = '123'
_SOP_CLASS_UID = 'sop_class_uid'
_COLUMNS = 'columns'
_ROWS = 'rows'
_TOTAL_PIXEL_MATRIX_COLUMNS = 'total_pixel_matrix_columns'
_TOTAL_PIXEL_MATRIX_ROWS = 'total_pixel_matrix_rows'
_NUMBER_OF_FRAMES = 'number_of_frames'

_MOCK_TOKEN_STR = 'mock_token_123'
_MOCK_SERIES_URL = dicom_url_util.DicomSeriesUrl(
    f'{_MOCK_DICOM_STORE_URL}/mock_series_url'
)
_MOCK_INSTANCE_UID = dicom_url_util.SOPInstanceUID('1.2.3.4')
_MOCK_IAP_EMAIL = 'mock@email.com'
_MOCK_IAP_USERID = 'mock_user_id'
_MOCK_BASE_URL = dicom_url_util.DicomWebBaseURL(
    'v1',
    'mock_project',
    'earth',
    'mock_dateset',
    'mock_store',
)

_EXPECTED_METADATA = {
    'bits_allocated': 8,
    'bits_stored': 8,
    'columns': 256,
    'high_bit': 7,
    'lossy_compression_method': 'ISO_10918_1',
    'lossy_image_compression': '01',
    'planar_configuration': 0,
    'rows': 256,
    'samples_per_pixel': 3,
    _SOP_CLASS_UID: '1.2.840.10008.5.1.4.1.1.77.1.6',
    _TOTAL_PIXEL_MATRIX_COLUMNS: 1152,
    _TOTAL_PIXEL_MATRIX_ROWS: 700,
    'number_of_frames': 15,
    'dimension_organization_type': 'TILED_FULL',
    'dicom_transfer_syntax': '1.2.840.10008.1.2.4.50',
    '_instance_icc_profile_metadata': None,
    'metadata_source': {
        'store_url': 'https://healthcare.googleapis.com/v1',
        'sop_instance_uid': {
            'sop_instance_uid': (
                '1.2.276.0.7230010.3.1.4.296485376.89.1688794081.412405'
            )
        },
        'caching_enabled': True,
    },
}


def _expected_metadata(metadata: Mapping[str, Any]) -> Mapping[str, Any]:
  md = dict(_EXPECTED_METADATA)
  md.update(metadata)
  return md


def _get_mock_mock_user_auth_session() -> user_auth_util.AuthSession:
  return user_auth_util.AuthSession({
      proxy_const.HeaderKeywords.AUTH_HEADER_KEY: _MOCK_TOKEN_STR,
      proxy_const.HeaderKeywords.IAP_USER_ID_KEY: _MOCK_IAP_USERID,
      proxy_const.HeaderKeywords.IAP_EMAIL_KEY: _MOCK_IAP_EMAIL,
  })


class MetadataUtilTest(parameterized.TestCase):

  @parameterized.parameters([
      ('1.2.840.10008.1.2', False),
      ('1.2.840.10008.1.2.1', False),
      ('1.2.840.10008.1.2.1.99', False),
      ('1.2.840.10008.1.2.2', False),
      ('1.2.840.10008.1.2.4.50', True),
      ('1.2.840.10008.1.2.4.90', True),
      ('1.2.840.10008.1.2.4.91', True),
  ])
  def test_is_transfer_syntax_encapsulated(self, transfer_syntax, expectation):
    self.assertEqual(
        metadata_util.is_transfer_syntax_encapsulated(transfer_syntax),
        expectation,
    )

  def test_metadata_flag_defaults(self):
    self.assertFalse(dicom_proxy_flags.DISABLE_DICOM_METADATA_CACHING_FLG.value)
    self.assertEqual(metadata_util._dicom_metadata_cache_ttl(), 600)

  @flagsaver.flagsaver(user_level_metadata_cache_ttl_sec=0)
  def test_metadata_flag_min(self):
    self.assertEqual(metadata_util._dicom_metadata_cache_ttl(), 60)

  @flagsaver.flagsaver(user_level_metadata_cache_ttl_sec=99999)
  def test_metadata_flag_max(self):
    self.assertEqual(metadata_util._dicom_metadata_cache_ttl(), 3600)

  def test_is_wsi_dicom_iod_true(self):
    self.assertTrue(
        shared_test_util.jpeg_encoded_dicom_instance_metadata().is_wsi_iod
    )

  def test_is_wsi_dicom_iod_false(self):
    self.assertFalse(
        shared_test_util.jpeg_encoded_dicom_instance_metadata(
            dict(sop_class_uid='1.2.3')
        ).is_wsi_iod
    )

  def test_is_tiled_full_true(self):
    self.assertTrue(
        shared_test_util.jpeg_encoded_dicom_instance_metadata().is_tiled_full
    )

  def test_is_tiled_full_false(self):
    self.assertFalse(
        shared_test_util.jpeg_encoded_dicom_instance_metadata(
            dict(dimension_organization_type='3D')
        ).is_tiled_full
    )

  def test_s_baseline_jpeg_true(self):
    self.assertTrue(
        shared_test_util.jpeg_encoded_dicom_instance_metadata().is_baseline_jpeg
    )

  def test_image_compression_is_jpeg(self):
    self.assertEqual(
        shared_test_util.jpeg_encoded_dicom_instance_metadata().image_compression,
        enum_types.Compression.JPEG,
    )

  @parameterized.parameters(
      ['1.2.840.10008.1.2.4.90', '1.2.840.10008.1.2.4.91']
  )
  def test_image_compression_is_jpeg2000(self, uid):
    self.assertEqual(
        shared_test_util.jpeg_encoded_dicom_instance_metadata(
            dict(dicom_transfer_syntax=uid)
        ).image_compression,
        enum_types.Compression.JPEG2000,
    )

  def test_image_compression_other_is_none(self):
    self.assertIsNone(
        shared_test_util.jpeg_encoded_dicom_instance_metadata(
            dict(dicom_transfer_syntax='1.2.840.10008.1.2.4.57')
        ).image_compression
    )

  @parameterized.parameters(
      ['1.2.840.10008.1.2.4.90', '1.2.840.10008.1.2.4.91', '1.2.3']
  )
  def test_is_baseline_jpeg_false(self, uid):
    self.assertFalse(
        shared_test_util.jpeg_encoded_dicom_instance_metadata(
            dict(dicom_transfer_syntax=uid)
        ).is_baseline_jpeg
    )

  @parameterized.parameters(
      ['1.2.840.10008.1.2.4.90', '1.2.840.10008.1.2.4.91']
  )
  def test_is_jpeg2000_true(self, uid):
    self.assertTrue(
        shared_test_util.jpeg_encoded_dicom_instance_metadata(
            dict(dicom_transfer_syntax=uid)
        ).is_jpeg2000
    )

  @parameterized.parameters(['1.2.840.10008.1.2.4.50', '1.2.3'])
  def test_is_jpeg2000_false(self, uid):
    self.assertFalse(
        shared_test_util.jpeg_encoded_dicom_instance_metadata(
            dict(dicom_transfer_syntax=uid)
        ).is_jpeg2000
    )

  def test_get_value(self):
    expected_value = 1
    self.assertEqual(
        metadata_util._get_value(
            {'test': {'Value': [expected_value, 2, 3, 4]}}, 'test'
        ),
        expected_value,
    )

  def test_get_str_value(self):
    str_value = metadata_util._get_str_value(
        shared_test_util.jpeg_encoded_dicom_instance_json(),
        metadata_util._SOP_CLASS_UID_DICOM_ADDRESS_TAG,
    )
    self.assertEqual(str_value, _EXPECTED_METADATA[_SOP_CLASS_UID])

  def test_get_str_value_missing(self):
    str_value = metadata_util._get_str_value(
        shared_test_util.jpeg_encoded_dicom_instance_json(),
        _NONEXISTENT_DICOM_TAG,
    )
    self.assertEqual(str_value, '')

  def test_get_int_value(self):
    int_value = metadata_util._get_int_value(
        shared_test_util.jpeg_encoded_dicom_instance_json(),
        metadata_util._BITS_STORED_DICOM_ADDRESS_TAG,
    )
    self.assertEqual(int_value, 8)

  def test_get_int_value_missing(self):
    int_value = metadata_util._get_int_value(
        shared_test_util.jpeg_encoded_dicom_instance_json(),
        _NONEXISTENT_DICOM_TAG,
    )
    self.assertEqual(int_value, 0)

  def test_init_metadata_from_json(self):
    metadata = metadata_util._init_metadata_from_json(
        dicom_url_util.DicomSeriesUrl(_MOCK_DICOM_STORE_URL),
        shared_test_util.jpeg_encoded_dicom_instance_json(),
        cache_enabled_type.CachingEnabled(True),
    )

    self.assertEqual(dataclasses.asdict(metadata), _EXPECTED_METADATA)

  def test_metadata_frames_per_row(self):
    metadata = metadata_util._init_metadata_from_json(
        dicom_url_util.DicomSeriesUrl(_MOCK_DICOM_STORE_URL),
        shared_test_util.jpeg_encoded_dicom_instance_json(),
        cache_enabled_type.CachingEnabled(True),
    )
    self.assertEqual(metadata.frames_per_row, 5)

  def test_metadata_downsample(self):
    metadata = metadata_util._init_metadata_from_json(
        dicom_url_util.DicomSeriesUrl(_MOCK_DICOM_STORE_URL),
        shared_test_util.jpeg_encoded_dicom_instance_json(),
        cache_enabled_type.CachingEnabled(True),
    )
    downsampled_metadata = copy.copy(_EXPECTED_METADATA)
    downsampled_metadata[_TOTAL_PIXEL_MATRIX_COLUMNS] = max(
        int(downsampled_metadata[_TOTAL_PIXEL_MATRIX_COLUMNS] / 2), 1
    )
    downsampled_metadata[_TOTAL_PIXEL_MATRIX_ROWS] = max(
        int(downsampled_metadata[_TOTAL_PIXEL_MATRIX_ROWS] / 2), 1
    )
    downsampled_metadata[_NUMBER_OF_FRAMES] = int(
        math.ceil(
            float(downsampled_metadata[_TOTAL_PIXEL_MATRIX_ROWS])
            / float(downsampled_metadata[_ROWS])
        )
        * math.ceil(
            float(downsampled_metadata[_TOTAL_PIXEL_MATRIX_COLUMNS])
            / float(downsampled_metadata[_COLUMNS])
        )
    )

    metadata = metadata.downsample(2.0)

    self.assertEqual(dataclasses.asdict(metadata), downsampled_metadata)

  def test_metadata_pad_image_to_frame_same_dim_is_no_op(self):
    expected_columns = _EXPECTED_METADATA[_COLUMNS]
    expected_rows = _EXPECTED_METADATA[_ROWS]
    img = np.zeros((expected_rows, expected_columns, 3), dtype=np.uint8)
    metadata = shared_test_util.jpeg_encoded_dicom_instance_metadata()
    self.assertIs(metadata.pad_image_to_frame(img), img)

  def test_metadata_pad_image_to_frame_larger_col_dim_raise(self):
    metadata = shared_test_util.jpeg_encoded_dicom_instance_metadata()
    img = np.zeros((1, 500, 3), dtype=np.uint8)
    with self.assertRaises(ValueError):
      metadata.pad_image_to_frame(img)

  def test_metadata_pad_image_to_frame_larger_row_dim_raise(self):
    metadata = shared_test_util.jpeg_encoded_dicom_instance_metadata()
    img = np.zeros((500, 1, 3), dtype=np.uint8)
    with self.assertRaises(ValueError):
      metadata.pad_image_to_frame(img)

  def test_metadata_pad_image_to_frame_smaller_succeeds(self):
    metadata = shared_test_util.jpeg_encoded_dicom_instance_metadata()

    img = metadata.pad_image_to_frame(np.zeros((25, 25, 3), dtype=np.uint8))
    expected_columns = _EXPECTED_METADATA[_COLUMNS]
    expected_rows = _EXPECTED_METADATA[_ROWS]

    self.assertEqual(img.shape, (expected_rows, expected_columns, 3))

  def test_get_instance_metadata_from_local_instance(self):
    expected_from_path = dataclasses.asdict(
        shared_test_util.jpeg_encoded_dicom_instance_metadata()
    )
    self.assertEqual(
        expected_from_path,
        _expected_metadata({
            'metadata_source': {
                'store_url': 'localhost',
                'sop_instance_uid': {
                    'sop_instance_uid': (
                        '1.2.276.0.7230010.3.1.4.296485376.89.1688794081.412405'
                    )
                },
                'caching_enabled': False,
            }
        }),
    )

  def test_get_instance_metadata_from_pydicom_instance(self):
    with shared_test_util.jpeg_encoded_dicom_instance() as ds:
      self.assertEqual(
          dataclasses.asdict(
              metadata_util.get_instance_metadata_from_local_instance(ds)
          ),
          _expected_metadata({
              'metadata_source': {
                  'store_url': 'localhost',
                  'sop_instance_uid': {
                      'sop_instance_uid': '1.2.276.0.7230010.3.1.4.296485376.89.1688794081.412405'
                  },
                  'caching_enabled': False,
              }
          }),
      )

  @parameterized.parameters([
      ({}, False),
      ({metadata_util._PIXEL_SPACING_TAG: {}}, False),
      ({metadata_util._PIXEL_SPACING_TAG: {'vr': 'SQ'}}, False),
      (
          {
              metadata_util._PIXEL_SPACING_TAG: {
                  metadata_util._VALUE: [],
                  'vr': 'SQ',
              }
          },
          False,
      ),
      (
          {
              metadata_util._PIXEL_SPACING_TAG: {
                  metadata_util._VALUE: [1],
                  'vr': 'SQ',
              }
          },
          True,
      ),
      (
          {
              metadata_util._PIXEL_SPACING_TAG: {
                  metadata_util._VALUE: [1, 2],
                  'vr': 'SQ',
              }
          },
          True,
      ),
  ])
  def test_has_tag_value(self, metadata, expected):
    test_tag = metadata_util._PIXEL_SPACING_TAG
    self.assertEqual(metadata_util._has_tag_value(metadata, test_tag), expected)

  def test_set_tag_int_value(self):
    new_value = 5
    metadata = {
        metadata_util._PIXEL_SPACING_TAG: {
            metadata_util._VALUE: [0],
            'vr': 'SQ',
        }
    }
    metadata_util._set_tag_int_value(
        metadata, metadata_util._PIXEL_SPACING_TAG, new_value
    )

    self.assertEqual(
        metadata[metadata_util._PIXEL_SPACING_TAG][metadata_util._VALUE],
        [new_value],
    )

  def test_set_tag_int_value_throws(self):
    new_value = 5
    metadata = {
        metadata_util._PIXEL_SPACING_TAG: {
            metadata_util._VALUE: [0, 1],
            'vr': 'SQ',
        }
    }
    with self.assertRaises(ValueError):
      metadata_util._set_tag_int_value(
          metadata, metadata_util._PIXEL_SPACING_TAG, new_value
      )

  @parameterized.parameters([
      metadata_util._TOTAL_PIXEL_MATRIX_COLUMNS_DICOM_ADDRESS_TAG,
      metadata_util._TOTAL_PIXEL_MATRIX_ROWS_DICOM_ADDRESS_TAG,
      metadata_util._COLUMNS_DICOM_ADDRESS_TAG,
      metadata_util._ROWS_DICOM_ADDRESS_TAG,
  ])
  def test_missing_metadata_throws(self, tag_address):
    metadata = shared_test_util.jpeg_encoded_dicom_instance_json()
    del metadata[tag_address]
    with self.assertRaises(metadata_util.JsonMetadataDownsampleError):
      metadata_util.downsample_json_metadata(metadata, 2.0)

  def test_downsample_sparse_metadata_throws(self):
    metadata = shared_test_util.jpeg_encoded_dicom_instance_json()
    metadata['00209311']['Value'][0] = 'TILED_SPARSE'
    with self.assertRaises(metadata_util.JsonMetadataDownsampleError):
      metadata_util.downsample_json_metadata(metadata, 2.0)

  def test_downsample_json_metadata_invalid_sop_class_id(self):
    metadata = shared_test_util.jpeg_encoded_dicom_instance_json()
    metadata[metadata_util._SOP_CLASS_UID_DICOM_ADDRESS_TAG][
        metadata_util._VALUE
    ][0] = '1.2.3'
    original = copy.deepcopy(metadata)

    self.assertFalse(metadata_util.downsample_json_metadata(metadata, 2.0))
    self.assertEqual(metadata, original)

  def test_downsample_json_metadata_no_downsample(self):
    metadata = shared_test_util.jpeg_encoded_dicom_instance_json()
    original = copy.deepcopy(metadata)

    self.assertFalse(metadata_util.downsample_json_metadata(metadata, 1.0))
    self.assertEqual(metadata, original)

  def test_downsample_json_metadata(self):
    metadata = shared_test_util.jpeg_encoded_dicom_instance_json()
    expected_number_of_frames = 6
    downsample = 2.0
    value = metadata_util._VALUE
    original = copy.deepcopy(metadata)
    original_px_spacing = original[
        metadata_util._SHARED_FUNCTIONAL_GROUPS_SEQUENCE_TAG
    ][value][0][metadata_util._PIXEL_MEASURES_SEQUENCE_TAG][value][0][
        metadata_util._PIXEL_SPACING_TAG
    ][
        value
    ]

    self.assertTrue(
        metadata_util.downsample_json_metadata(metadata, downsample)
    )
    for key in (
        metadata_util._TOTAL_PIXEL_MATRIX_ROWS_DICOM_ADDRESS_TAG,
        metadata_util._TOTAL_PIXEL_MATRIX_COLUMNS_DICOM_ADDRESS_TAG,
    ):
      self.assertEqual(
          metadata[key][value], [int(original[key][value][0] / downsample)]
      )
    self.assertEqual(
        metadata[metadata_util._SHARED_FUNCTIONAL_GROUPS_SEQUENCE_TAG][value][
            0
        ][metadata_util._PIXEL_MEASURES_SEQUENCE_TAG][value][0][
            metadata_util._PIXEL_SPACING_TAG
        ][
            value
        ],
        [px_spacing * downsample for px_spacing in original_px_spacing],
    )
    self.assertEqual(
        metadata[metadata_util._NUMBER_OF_FRAMES_DICOM_ADDRESS_TAG][value],
        [expected_number_of_frames],
    )

  def test_cache_key(self):
    user_auth = _get_mock_mock_user_auth_session()
    cache_key = metadata_util._cache_key(
        user_auth, _MOCK_SERIES_URL, _MOCK_INSTANCE_UID
    )
    self.assertEqual(
        cache_key,
        (
            f'metadata url:{_MOCK_SERIES_URL}/{_MOCK_INSTANCE_UID} '
            f'bearer:{_MOCK_TOKEN_STR} iap:{_MOCK_IAP_USERID} '
            f'email:{user_auth.email}'
        ),
    )

  @mock.patch.object(dicom_store_util, '_get_dicom_json', autospec=True)
  def test_get_instance_metadata_from_dicom_store_cache_disabled_param(
      self, mock_download_metadata
  ):
    user_auth = _get_mock_mock_user_auth_session()
    mock_download_metadata.return_value = [
        shared_test_util.jpeg_encoded_dicom_instance_json()
    ]

    metadata = metadata_util.get_instance_metadata(
        user_auth,
        _MOCK_SERIES_URL,
        _MOCK_INSTANCE_UID,
        cache_enabled_type.CachingEnabled(False),
    )

    self.assertEqual(
        dataclasses.asdict(metadata),
        _expected_metadata({
            'metadata_source': {
                'store_url': (
                    'https://healthcare.googleapis.com/v1/mock_series_url'
                ),
                'sop_instance_uid': {
                    'sop_instance_uid': (
                        '1.2.276.0.7230010.3.1.4.296485376.89.1688794081.412405'
                    )
                },
                'caching_enabled': False,
            }
        }),
    )

  @flagsaver.flagsaver(disable_dicom_metadata_caching=True)
  @mock.patch.object(dicom_store_util, '_get_dicom_json', autospec=True)
  def test_get_instance_metadata_from_dicom_store_disabled_flag(
      self, mock_download_metadata
  ):
    user_auth = _get_mock_mock_user_auth_session()
    mock_download_metadata.return_value = [
        shared_test_util.jpeg_encoded_dicom_instance_json()
    ]

    metadata = metadata_util.get_instance_metadata(
        user_auth,
        _MOCK_SERIES_URL,
        _MOCK_INSTANCE_UID,
        cache_enabled_type.CachingEnabled(True),
    )

    self.assertEqual(
        dataclasses.asdict(metadata),
        _expected_metadata({
            'metadata_source': {
                'store_url': (
                    'https://healthcare.googleapis.com/v1/mock_series_url'
                ),
                'sop_instance_uid': {
                    'sop_instance_uid': (
                        '1.2.276.0.7230010.3.1.4.296485376.89.1688794081.412405'
                    )
                },
                'caching_enabled': True,
            }
        }),
    )

  @mock.patch.object(redis.Redis, 'set', return_value=None)
  @mock.patch.object(redis.Redis, 'get', return_value=None)
  @mock.patch.object(dicom_store_util, '_get_dicom_json', autospec=True)
  def test_get_instance_metadata_from_dicom_stored_not_cached(
      self, mock_download_metadata, mock_redis_get, mock_redis_set
  ):
    user_auth = _get_mock_mock_user_auth_session()
    mock_download_metadata.return_value = [
        shared_test_util.jpeg_encoded_dicom_instance_json()
    ]
    redis_key = (
        f'metadata url:{_MOCK_SERIES_URL}/{_MOCK_INSTANCE_UID} '
        f'bearer:{_MOCK_TOKEN_STR} '
        f'iap:{_MOCK_IAP_USERID} email:{user_auth.email}'
    )

    metadata = metadata_util.get_instance_metadata(
        user_auth,
        _MOCK_SERIES_URL,
        _MOCK_INSTANCE_UID,
        cache_enabled_type.CachingEnabled(True),
    )

    mock_redis_get.assert_called_once_with(redis_key)
    expected_metadata = _expected_metadata({
        'metadata_source': {
            'store_url': 'https://healthcare.googleapis.com/v1/mock_series_url',
            'sop_instance_uid': {
                'sop_instance_uid': (
                    '1.2.276.0.7230010.3.1.4.296485376.89.1688794081.412405'
                )
            },
            'caching_enabled': True,
        }
    })
    self.assertEqual(dataclasses.asdict(metadata), expected_metadata)
    mock_redis_set.assert_called_once_with(
        redis_key,
        json.dumps(expected_metadata, sort_keys=True).encode('utf-8'),
        nx=False,
        ex=dicom_proxy_flags.USER_LEVEL_METADATA_TTL_FLG.value,
    )

  @mock.patch.object(redis.Redis, 'set', return_value=None)
  @mock.patch.object(redis.Redis, 'get')
  @mock.patch.object(dicom_store_util, '_get_dicom_json', autospec=True)
  def test_get_instance_metadata_from_dicom_stored_cached(
      self, mock_download_metadata, mock_redis_get, mock_redis_set
  ):
    user_auth = _get_mock_mock_user_auth_session()
    mock_redis_get.return_value = json.dumps(
        _EXPECTED_METADATA, sort_keys=True
    ).encode('utf-8')
    redis_key = (
        f'metadata url:{_MOCK_SERIES_URL}/{_MOCK_INSTANCE_UID} '
        f'bearer:{_MOCK_TOKEN_STR} iap:{_MOCK_IAP_USERID} '
        f'email:{user_auth.email}'
    )

    metadata = metadata_util.get_instance_metadata(
        user_auth,
        _MOCK_SERIES_URL,
        _MOCK_INSTANCE_UID,
        cache_enabled_type.CachingEnabled(True),
    )

    mock_redis_get.assert_called_once_with(redis_key)
    mock_download_metadata.assert_not_called()
    mock_redis_set.assert_not_called()
    self.assertEqual(dataclasses.asdict(metadata), _EXPECTED_METADATA)

  @mock.patch.object(redis.Redis, 'set', return_value=None)
  @mock.patch.object(redis.Redis, 'get', return_value=None)
  @mock.patch.object(dicom_store_util, '_get_dicom_json', autospec=True)
  def test_get_instance_metadata_from_dicom_stored_invalid_response(
      self, mock_download_metadata, mock_redis_get, mock_redis_set
  ):
    user_auth = _get_mock_mock_user_auth_session()
    redis_key = (
        f'metadata url:{_MOCK_SERIES_URL}/{_MOCK_INSTANCE_UID} '
        f'bearer:{_MOCK_TOKEN_STR} '
        f'iap:{_MOCK_IAP_USERID} email:{user_auth.email}'
    )
    mock_download_metadata.return_value = [{}, {}]

    with self.assertRaises(metadata_util.ReadDicomMetadataError):
      metadata_util.get_instance_metadata(
          user_auth,
          _MOCK_SERIES_URL,
          _MOCK_INSTANCE_UID,
          cache_enabled_type.CachingEnabled(True),
      )
    mock_redis_get.assert_called_once_with(redis_key)
    mock_redis_set.assert_not_called()

  @mock.patch.object(redis.Redis, 'set', return_value=None)
  @mock.patch.object(redis.Redis, 'get', return_value=None)
  @mock.patch.object(dicom_store_util, '_get_dicom_json', autospec=True)
  def test_get_instance_metadata_from_dicom_stored_download_raises(
      self, mock_download_metadata, mock_redis_get, mock_redis_set
  ):
    user_auth = _get_mock_mock_user_auth_session()
    redis_key = (
        f'metadata url:{_MOCK_SERIES_URL}/{_MOCK_INSTANCE_UID} '
        f'bearer:{_MOCK_TOKEN_STR} '
        f'iap:{_MOCK_IAP_USERID} email:{user_auth.email}'
    )
    mock_download_metadata.side_effect = RuntimeError

    with self.assertRaises(metadata_util.ReadDicomMetadataError):
      metadata_util.get_instance_metadata(
          user_auth,
          _MOCK_SERIES_URL,
          _MOCK_INSTANCE_UID,
          cache_enabled_type.CachingEnabled(True),
      )
    mock_redis_get.assert_called_once_with(redis_key)
    mock_redis_set.assert_not_called()

  def test_init_fork_module_state(self):
    metadata_util._metadata_cache = None
    metadata_util._metadata_cache_lock = None
    metadata_util._init_fork_module_state()
    self.assertIsNotNone(metadata_util._metadata_cache_lock)
    self.assertIsInstance(metadata_util._metadata_cache, cachetools.TTLCache)

  def test_get_series_instance_uid_for_study_and_instance_uid_success(self):
    dcm = shared_test_util.jpeg_encoded_dicom_instance()
    with dicom_store_mock.MockDicomStores(
        _MOCK_BASE_URL.full_url
    ) as mocked_dicom_stores:
      mocked_dicom_stores[_MOCK_BASE_URL.full_url].add_instance(dcm)
      series_instance_uid = (
          metadata_util.get_series_instance_uid_for_study_and_instance_uid(
              _get_mock_mock_user_auth_session(),
              _MOCK_BASE_URL,
              dicom_url_util.StudyInstanceUID(dcm.StudyInstanceUID),
              dicom_url_util.SOPInstanceUID(dcm.SOPInstanceUID),
          )
      )
    self.assertEqual(series_instance_uid, dcm.SeriesInstanceUID)

  def test_get_series_instance_uid_missing_study_id_raises(self):
    with self.assertRaises(metadata_util.GetSeriesInstanceUIDError):
      metadata_util.get_series_instance_uid_for_study_and_instance_uid(
          _get_mock_mock_user_auth_session(),
          _MOCK_BASE_URL,
          dicom_url_util.StudyInstanceUID(''),
          dicom_url_util.SOPInstanceUID('1.2.3'),
      )

  def test_get_series_instance_uid_missing_series_id_raises(self):
    with self.assertRaises(metadata_util.GetSeriesInstanceUIDError):
      metadata_util.get_series_instance_uid_for_study_and_instance_uid(
          _get_mock_mock_user_auth_session(),
          _MOCK_BASE_URL,
          dicom_url_util.StudyInstanceUID('1.2.3'),
          dicom_url_util.SOPInstanceUID(''),
      )

  def test_get_series_bad_url_raises(self):
    with dicom_store_mock.MockDicomStores(_MOCK_BASE_URL.full_url):
      with self.assertRaises(metadata_util.GetSeriesInstanceUIDError):
        metadata_util.get_series_instance_uid_for_study_and_instance_uid(
            _get_mock_mock_user_auth_session(),
            _MOCK_BASE_URL,
            dicom_url_util.StudyInstanceUID('1.2.3'),
            dicom_url_util.SOPInstanceUID('1.3.4'),
        )

  @mock.patch.object(
      dicom_store_util, 'get_dicom_study_instance_metadata', autospec=True
  )
  def test_get_series_missing_series_raises(self, mk_study_instance_metadata):
    dcm = shared_test_util.jpeg_encoded_dicom_instance()
    del dcm['SeriesInstanceUID']
    mk_study_instance_metadata.return_value = [dcm.to_json_dict()]
    with self.assertRaises(metadata_util.GetSeriesInstanceUIDError):
      metadata_util.get_series_instance_uid_for_study_and_instance_uid(
          _get_mock_mock_user_auth_session(),
          _MOCK_BASE_URL,
          dicom_url_util.StudyInstanceUID('1.2.3'),
          dicom_url_util.SOPInstanceUID('1.3.4'),
      )

  @mock.patch.object(
      dicom_store_util, 'get_dicom_study_instance_metadata', autospec=True
  )
  def test_get_series_returns_multiple_raises(self, mk_study_instance_metadata):
    dcm_json = shared_test_util.jpeg_encoded_dicom_instance().to_json_dict()
    mk_study_instance_metadata.return_value = [dcm_json, dcm_json]
    with self.assertRaises(metadata_util.GetSeriesInstanceUIDError):
      metadata_util.get_series_instance_uid_for_study_and_instance_uid(
          _get_mock_mock_user_auth_session(),
          _MOCK_BASE_URL,
          dicom_url_util.StudyInstanceUID('1.2.3'),
          dicom_url_util.SOPInstanceUID('1.3.4'),
      )


if __name__ == '__main__':
  absltest.main()
