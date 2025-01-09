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
"""Tests for color conversion util."""

import copy
import os
from typing import Any, Mapping, Optional, Union
from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import cachetools
import numpy as np
from PIL import ImageCms
import pydicom
import redis

from pathology.dicom_proxy import bulkdata_util
from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import color_conversion_util
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import icc_profile_metadata_cache
from pathology.dicom_proxy import image_util
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import redis_cache
from pathology.dicom_proxy import shared_test_util
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.test_utils.dicom_store_mock import dicom_store_mock


# Mark flags as parsed to avoid UnparsedFlagAccessError for tests using
# --test_srcdir flag in parameters.
flags.FLAGS.mark_as_parsed()

MOCK_TEST_API_VERSION = 'v1'

_NUMBER_OF_FRAMES_DICOM_TAG_ADDRESS = (
    color_conversion_util._NUMBER_OF_FRAMES_DICOM_TAG_ADDRESS
)
_UNDEFINED_SOP_INSTANCE_UID = dicom_url_util.SOPInstanceUID('')


def _set_tag_value(metadata: Mapping[str, Any], address: str, value: Any):
  if isinstance(value, list):
    metadata[address][color_conversion_util._VALUE] = copy.copy(value)
  else:
    metadata[address][color_conversion_util._VALUE] = [value]


def _test_dcm_with_icc_profile(
    icc_profile_path: str,
    dirname: str,
    icc_profile_filename: str,
    colorspace: str = '',
) -> pydicom.FileDataset:
  icc_profile = color_conversion_util._read_internal_icc_profile(
      dirname, icc_profile_filename
  )
  with pydicom.dcmread(
      shared_test_util.jpeg_encoded_dicom_instance_test_path()
  ) as dcm:
    if icc_profile_path == 'ICCProfile':
      dcm.ICCProfile = icc_profile
      if colorspace:
        dcm.ColorSpace = colorspace
    else:
      index = int(icc_profile_path.split('/')[1])
      while index >= len(dcm.OpticalPathSequence):
        dcm.OpticalPathSequence.append({})
      dcm.OpticalPathSequence[index].ICCProfile = icc_profile
      if colorspace:
        dcm.OpticalPathSequence[index].ColorSpace = colorspace
    return dcm


class ColorConversionUtilTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    color_conversion_util._init_fork_module_state()
    self.enter_context(
        mock.patch.object(
            user_auth_util.AuthSession,
            'email',
            new_callable=mock.PropertyMock,
            return_value='mock@email.com',
        )
    )

  def test_get_tag_value(self):
    tag_value = '123'
    test_tag_address = color_conversion_util._IMAGE_TYPE_DICOM_TAG_ADDRESS
    test_metadata = {
        test_tag_address: {color_conversion_util._VALUE: [tag_value]}
    }

    self.assertEqual(
        color_conversion_util._get_tag_value(test_metadata, test_tag_address),
        tag_value,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='none_bytes',
          dicom_icc_profile=None,
          transform=proxy_const.ICCProfile.SRGB,
      ),
      dict(
          testcase_name='empty_bytes',
          dicom_icc_profile=b'',
          transform=proxy_const.ICCProfile.SRGB,
      ),
      dict(
          testcase_name='no_transform',
          dicom_icc_profile=b'123',
          transform=proxy_const.ICCProfile.NO,
      ),
  ])
  def test_create_icc_profile_tranfrom_passed_none_empty_or_no_returns_none(
      self,
      dicom_icc_profile: Optional[bytes],
      transform: enum_types.ICCProfile,
  ):
    self.assertIsNone(
        color_conversion_util._create_icc_profile_transform(
            dicom_icc_profile, transform
        )
    )

  def test_create_icc_profile_transform_yes(self):
    result = color_conversion_util._create_icc_profile_transform(
        b'123', proxy_const.ICCProfile.YES
    )
    self.assertEqual(result.config, proxy_const.ICCProfile.YES)  # pytype: disable=attribute-error
    self.assertIsNone(result.color_transform)  # pytype: disable=attribute-error
    self.assertEqual(result.rendered_icc_profile, b'123')  # pytype: disable=attribute-error

  def test_create_custom_icc_profile_transform(self):
    profile_name = 'foo'
    srgb_bytes = color_conversion_util._get_srgb_iccprofile()
    tdir = self.create_tempdir()
    with open(os.path.join(tdir, f'{profile_name}.icc'), 'wb') as tfile:
      tfile.write(srgb_bytes)
    with flagsaver.flagsaver(third_party_icc_profile_directory=tdir.full_path):
      result = color_conversion_util._create_icc_profile_transform(
          srgb_bytes, enum_types.ICCProfile(profile_name)
      )
    self.assertEqual(result.config, profile_name)  # pytype: disable=attribute-error
    self.assertIsNotNone(result.color_transform)  # pytype: disable=attribute-error
    self.assertEqual(result.rendered_icc_profile, srgb_bytes)  # pytype: disable=attribute-error

  def test_create_custom_icc_profile_transform_not_found(self):
    tdir = self.create_tempdir()
    srgb_bytes = color_conversion_util._get_srgb_iccprofile()
    with flagsaver.flagsaver(third_party_icc_profile_directory=tdir.full_path):
      with self.assertRaises(color_conversion_util.UnableToLoadIccProfileError):
        color_conversion_util._create_icc_profile_transform(
            srgb_bytes, enum_types.ICCProfile('foo')
        )

  @parameterized.named_parameters([
      dict(
          testcase_name='srgb',
          transform=proxy_const.ICCProfile.SRGB,
          expected_bytes=color_conversion_util._get_srgb_iccprofile(),
      ),
      dict(
          testcase_name='rommrgb',
          transform=proxy_const.ICCProfile.ROMMRGB,
          expected_bytes=color_conversion_util._get_rommrgb_iccprofile(),
      ),
  ])
  def test_create_icc_profile_color_transform(
      self, transform: enum_types.ICCProfile, expected_bytes: bytes
  ):
    with mock.patch.object(ImageCms, 'getOpenProfile', autospec=True):
      with mock.patch.object(
          ImageCms,
          'buildTransform',
          autospec=True,
          return_value='MockTransform',
      ) as icc_transform:
        result = color_conversion_util._create_icc_profile_transform(
            b'123', transform
        )
        self.assertEqual(result.config, transform)  # pytype: disable=attribute-error
        self.assertEqual(result.color_transform, 'MockTransform')  # pytype: disable=attribute-error
        self.assertEqual(result.rendered_icc_profile, expected_bytes)  # pytype: disable=attribute-error
        icc_transform.assert_called_once()

  @parameterized.parameters([
      'srgb',
      'adobergb',
      'rommrgb',
      'foo',
  ])
  def test_read_icc_profile_plugin_dir(self, name):
    expected_value = b'test'
    tdir = self.create_tempdir()
    profile_dir = os.path.join(tdir, name)
    os.mkdir(profile_dir)
    with open(os.path.join(profile_dir, 'foo.icc'), 'wb') as tfile:
      tfile.write(expected_value)
    with flagsaver.flagsaver(third_party_icc_profile_directory=tdir):
      self.assertEqual(
          expected_value,
          color_conversion_util.read_icc_profile_plugin_file(name),
      )

  def test_read_icc_profile_plugin_dir_not_found(self):
    tdir = self.create_tempdir()
    with flagsaver.flagsaver(third_party_icc_profile_directory=tdir.full_path):
      self.assertEqual(
          color_conversion_util.read_icc_profile_plugin_file('srgb'), b''
      )

  @parameterized.parameters([
      'srgb',
      'adobergb',
      'rommrgb',
      'foo',
  ])
  def test_get_plugin_icc_profile(self, name):
    expected_value = b'test'
    tdir = self.create_tempdir()
    with open(os.path.join(tdir, f'{name}.icc'), 'wb') as tfile:
      tfile.write(expected_value)
    with flagsaver.flagsaver(third_party_icc_profile_directory=tdir):
      self.assertEqual(
          expected_value,
          color_conversion_util.read_icc_profile_plugin_file(name),
      )

  def test_get_plugin_icc_profile_no_plugin_dir(self):
    with flagsaver.flagsaver(third_party_icc_profile_directory=''):
      self.assertEqual(
          color_conversion_util.read_icc_profile_plugin_file('foo'), b''
      )

  def test_get_plugin_icc_profile_dir_not_found(self):
    with flagsaver.flagsaver(
        third_party_icc_profile_directory='/foo/bar/sandwich'
    ):
      self.assertEqual(
          color_conversion_util.read_icc_profile_plugin_file('foo'), b''
      )

  def test_get_plugin_icc_profile_not_found(self):
    tdir = self.create_tempdir()
    with flagsaver.flagsaver(third_party_icc_profile_directory=tdir.full_path):
      self.assertEqual(
          color_conversion_util.read_icc_profile_plugin_file('foo'), b''
      )

  def test_get_plugin_icc_profile_not_found_inside_dir(self):
    tdir = self.create_tempdir()
    os.mkdir(os.path.join(tdir, 'foo'))
    os.mkdir(os.path.join(tdir, 'foo', 'foo'))
    with flagsaver.flagsaver(third_party_icc_profile_directory=tdir.full_path):
      self.assertEqual(
          color_conversion_util.read_icc_profile_plugin_file('foo'), b''
      )

  def test_transform_image_color_passed_none_is_nop(self):
    img = np.zeros((4, 4, 3), dtype=np.uint8)
    self.assertIs(img, color_conversion_util.transform_image_color(img, None))

  def test_transform_image_color_passed(self):
    img = np.zeros((4, 4, 3), dtype=np.uint8)
    icc_profile_transform = mock.Mock()
    with mock.patch.object(ImageCms, 'applyTransform', autospec=True):
      result = color_conversion_util.transform_image_color(
          img, icc_profile_transform
      )
    self.assertIsInstance(result, image_util.PILImage)
    self.assertEqual(result.image.tobytes(), img.tobytes())

  def test_is_wsi_instance_requires_number_of_frames(self):
    metadata = shared_test_util.jpeg_encoded_dicom_instance_json()
    _set_tag_value(metadata, _NUMBER_OF_FRAMES_DICOM_TAG_ADDRESS, 0)

    self.assertFalse(color_conversion_util._is_wsi_instance(metadata))

  def test_is_wsi_instance_missing_number_of_frames(self):
    metadata = shared_test_util.jpeg_encoded_dicom_instance_json()
    del metadata[_NUMBER_OF_FRAMES_DICOM_TAG_ADDRESS]

    self.assertFalse(color_conversion_util._is_wsi_instance(metadata))

  def test_is_wsi_instance_requires_wsi_sop_class_uid(self):
    metadata = shared_test_util.jpeg_encoded_dicom_instance_json()
    _set_tag_value(
        metadata,
        color_conversion_util._SOP_CLASS_UID_DICOM_TAG_ADDRESS,
        '1.2.3.4',
    )

    self.assertFalse(color_conversion_util._is_wsi_instance(metadata))

  def test_get_instance_with_fewest_frames(self):
    metadata_list = []
    for frame_number in range(4):
      metadata = shared_test_util.jpeg_encoded_dicom_instance_json()
      _set_tag_value(
          metadata,
          color_conversion_util._SOP_INSTANCE_UID_DICOM_TAG_ADDRESS,
          f'1.2.3.{frame_number}',
      )
      _set_tag_value(
          metadata, _NUMBER_OF_FRAMES_DICOM_TAG_ADDRESS, frame_number
      )
      metadata_list.append(metadata)

    inst = color_conversion_util._get_instance_with_fewest_frames(
        metadata_list, _UNDEFINED_SOP_INSTANCE_UID
    )

    self.assertIsNotNone(inst)
    self.assertEqual(inst.sop_instance_uid, '1.2.3.1')

  def test_get_instance_with_fewest_frames_returns_none_if_not_found(self):
    metadata_list = []
    for frame_number in range(4):
      metadata = shared_test_util.jpeg_encoded_dicom_instance_json()
      _set_tag_value(
          metadata,
          color_conversion_util._SOP_INSTANCE_UID_DICOM_TAG_ADDRESS,
          f'1.2.3.{frame_number}',
      )
      _set_tag_value(
          metadata,
          color_conversion_util._SOP_CLASS_UID_DICOM_TAG_ADDRESS,
          '1.2.3',
      )
      _set_tag_value(
          metadata, _NUMBER_OF_FRAMES_DICOM_TAG_ADDRESS, frame_number
      )
      metadata_list.append(metadata)

    self.assertIsNone(
        color_conversion_util._get_instance_with_fewest_frames(
            metadata_list, _UNDEFINED_SOP_INSTANCE_UID
        )
    )

  def test_is_wsi_instance_true_image_type_fails_if_image_type_invalid(self):
    metadata = shared_test_util.jpeg_encoded_dicom_instance_json()
    img_type = 'ORIGINAL\\SECONDARY\\VOLUME'
    _set_tag_value(
        metadata,
        color_conversion_util._IMAGE_TYPE_DICOM_TAG_ADDRESS,
        img_type.split('\\'),
    )
    self.assertFalse(color_conversion_util._is_wsi_instance(metadata))

  @parameterized.parameters([
      color_conversion_util._LABEL,
      color_conversion_util._OVERVIEW,
      color_conversion_util._LOCALIZER,
  ])
  def test_is_wsi_instance_false_image_type(self, img_type):
    metadata = shared_test_util.jpeg_encoded_dicom_instance_json()
    metadata[color_conversion_util._IMAGE_TYPE_DICOM_TAG_ADDRESS][
        color_conversion_util._VALUE
    ].append(img_type)

    self.assertFalse(color_conversion_util._is_wsi_instance(metadata))

  @parameterized.parameters([
      color_conversion_util._ORIGINAL_PRIMARY_VOLUME,
      color_conversion_util._DERIVED_PRIMARY_VOLUME,
      color_conversion_util._THUMBNAIL,
  ])
  def test_is_wsi_instance_true_image_type(self, img_type):
    metadata = shared_test_util.jpeg_encoded_dicom_instance_json()
    _set_tag_value(
        metadata,
        color_conversion_util._IMAGE_TYPE_DICOM_TAG_ADDRESS,
        img_type.split('\\'),
    )
    self.assertTrue(color_conversion_util._is_wsi_instance(metadata))

  def test_get_icc_profile_srgb_transform_from_undefined_series(self):
    session = user_auth_util.AuthSession({})
    dicom_series_url = dicom_url_util.DicomSeriesUrl('')

    self.assertIsNone(
        color_conversion_util.get_icc_profile_transform_for_dicom_url(
            session,
            dicom_series_url,
            _UNDEFINED_SOP_INSTANCE_UID,
            cache_enabled_type.CachingEnabled(True),
            proxy_const.ICCProfile.SRGB,
        )
    )

  @mock.patch.object(
      redis.Redis,
      'get',
      return_value=icc_profile_metadata_cache.INSTANCE_MISSING_ICC_PROFILE_METADATA.to_json().encode(
          'utf-8'
      ),
      autospec=True,
  )
  def test_get_icc_profile_srgb_transform_from_instance_cached_none(
      self, *unused_mocks
  ):
    session = user_auth_util.AuthSession({})
    dicom_series_url = dicom_url_util.DicomSeriesUrl(
        'https://healthcare.googleapis.com/v1/test_url_2'
    )

    self.assertIsNone(
        color_conversion_util.get_icc_profile_transform_for_dicom_url(
            session,
            dicom_series_url,
            _UNDEFINED_SOP_INSTANCE_UID,
            cache_enabled_type.CachingEnabled(True),
            proxy_const.ICCProfile.SRGB,
        )
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='no_bulkdata_support',
          store_version='v1',
          supports_bulkdata=False,
      ),
      dict(
          testcase_name='bulkdata_support',
          store_version='v1beta1',
          supports_bulkdata=True,
      ),
  ])
  @mock.patch.object(
      color_conversion_util, '_create_icc_profile_transform', autospec=True
  )
  def test_get_icc_profile_transform_for_dicom_url_cached_profile_value(
      self, color_transform_mock, store_version, supports_bulkdata
  ):
    key_value = b'1234'
    session = user_auth_util.AuthSession({})
    dicom_series_url = dicom_url_util.DicomSeriesUrl(
        f'https://healthcare.googleapis.com/{store_version}/test_url'
    )
    color_transform_mock.return_value = key_value

    # Set mock redis cache to hold reference ICC Profile metadata for instance
    # and the ICC Profile bytes.
    redis_mk = shared_test_util.RedisMock()
    mock_hash = 'mock_icc_profile_byteshash'
    icc_profile_metadata = icc_profile_metadata_cache.ICCProfileMetadata(
        'path', 'srgb', 'bulkdata_uri', mock_hash
    )
    redis_mk.set(
        icc_profile_metadata_cache._cache_key(
            dicom_series_url, _UNDEFINED_SOP_INSTANCE_UID, supports_bulkdata
        ),
        icc_profile_metadata.to_json().encode('utf-8'),
        nx=False,
        ex=24,
    )
    redis_mk.set(
        color_conversion_util._icc_profile_cache_key(mock_hash),
        key_value,
        nx=False,
        ex=24,
    )
    with mock.patch.object(
        bulkdata_util,
        'does_dicom_store_support_bulkdata',
        autospec=True,
        return_value=supports_bulkdata,
    ):
      with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
        self.assertEqual(
            color_conversion_util.get_icc_profile_transform_for_dicom_url(
                session,
                dicom_series_url,
                _UNDEFINED_SOP_INSTANCE_UID,
                cache_enabled_type.CachingEnabled(True),
                proxy_const.ICCProfile.SRGB,
            ),
            key_value,
        )
    color_transform_mock.assert_called_once_with(
        key_value, proxy_const.ICCProfile.SRGB
    )
    hash_key = color_conversion_util._hash_key(
        icc_profile_metadata, proxy_const.ICCProfile.SRGB
    )
    self.assertEqual(
        color_conversion_util._icc_transform_cache[hash_key], key_value
    )

  @mock.patch.object(
      color_conversion_util, '_create_icc_profile_transform', autospec=True
  )
  def test_get_icc_profile_transform_for_dicom_url_caching_disabled(
      self, color_transform_mock
  ):
    key_value = b'1234'
    session = user_auth_util.AuthSession({})
    color_transform_mock.return_value = key_value
    baseurl = dicom_url_util.DicomWebBaseURL(
        MOCK_TEST_API_VERSION,
        'mock_project',
        'earth',
        'mock_dateset',
        'mock_store',
    )
    dcm = _test_dcm_with_icc_profile(
        'OpticalPathSequence/0/ICCProfile',
        'srgb',
        'sRGB_v4_ICC_preference.icc',
    )
    icc_profile = dcm.OpticalPathSequence[0].ICCProfile
    with dicom_store_mock.MockDicomStores(
        baseurl.full_url
    ) as mocked_dicom_stores:
      mocked_dicom_stores[baseurl.full_url].add_instance(dcm)
      self.assertEqual(
          color_conversion_util.get_icc_profile_transform_for_dicom_url(
              session,
              dicom_url_util.base_dicom_series_url(
                  baseurl,
                  dicom_url_util.StudyInstanceUID(dcm.StudyInstanceUID),
                  dicom_url_util.SeriesInstanceUID(dcm.SeriesInstanceUID),
              ),
              _UNDEFINED_SOP_INSTANCE_UID,
              cache_enabled_type.CachingEnabled(False),
              proxy_const.ICCProfile.SRGB,
          ),
          key_value,
      )
    color_transform_mock.assert_called_once_with(
        icc_profile, proxy_const.ICCProfile.SRGB
    )
    self.assertEmpty(color_conversion_util._icc_transform_cache)

  def test_get_icc_profile_transform_for_dicom_url_loaded_from_cache(self):
    key_value = b'1234'
    session = user_auth_util.AuthSession({})
    dicom_series_url = dicom_url_util.DicomSeriesUrl(
        'https://healthcare.googleapis.com/v1/cached_result'
    )
    mock_hash = 'mock_icc_profile_byteshash'
    icc_profile_metadata = icc_profile_metadata_cache.ICCProfileMetadata(
        'path', 'srgb', 'bulkdata_uri', mock_hash
    )
    hash_key = color_conversion_util._hash_key(
        icc_profile_metadata, proxy_const.ICCProfile.SRGB
    )
    color_conversion_util._icc_transform_cache[hash_key] = key_value
    redis_mk = shared_test_util.RedisMock()
    with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
      icc_profile_metadata_cache.set_cached_instance_icc_profile_metadata(
          redis_cache.RedisCache(cache_enabled_type.CachingEnabled(True)),
          dicom_series_url,
          _UNDEFINED_SOP_INSTANCE_UID,
          does_dicom_store_support_bulkdata=False,
          metadata=icc_profile_metadata,
      )
      self.assertEqual(
          color_conversion_util.get_icc_profile_transform_for_dicom_url(
              session,
              dicom_series_url,
              _UNDEFINED_SOP_INSTANCE_UID,
              cache_enabled_type.CachingEnabled(True),
              proxy_const.ICCProfile.SRGB,
          ),
          key_value,
      )

  @parameterized.parameters([
      proxy_const.ICCProfile.SRGB,
      proxy_const.ICCProfile.ADOBERGB,
      proxy_const.ICCProfile.ROMMRGB,
  ])
  def test_iccprofile_transform_cache_tools_hash_key(
      self, transform: enum_types.ICCProfile
  ):
    mock_hash = 'mock_icc_profile_byteshash'
    icc_profile_metadata = icc_profile_metadata_cache.ICCProfileMetadata(
        'path', 'srgb', 'bulkdata_uri', mock_hash
    )
    self.assertEqual(
        color_conversion_util._hash_key(
            icc_profile_metadata,
            transform,
        ),
        f'cache_tools_{icc_profile_metadata.hash}_{transform}_ICCPROFILE_TRANSFORM',
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='iccprofile_bytes',
          dcm=_test_dcm_with_icc_profile(
              'ICCProfile', 'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
          param_to_return=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_BYTES,
          expected_result=color_conversion_util._read_internal_icc_profile(
              'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
      ),
      dict(
          testcase_name='opticalpathsq_iccprofile_bytes',
          dcm=_test_dcm_with_icc_profile(
              'OpticalPathSequence/0/ICCProfile',
              'srgb',
              'sRGB_v4_ICC_preference.icc',
          ),
          param_to_return=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_BYTES,
          expected_result=color_conversion_util._read_internal_icc_profile(
              'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
      ),
      dict(
          testcase_name='iccprofile_path',
          dcm=_test_dcm_with_icc_profile(
              'ICCProfile', 'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
          param_to_return=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_TAG_DICOM_PATH,
          expected_result='ICCProfile',
      ),
      dict(
          testcase_name='opticalpathsq_iccprofile_path',
          dcm=_test_dcm_with_icc_profile(
              'OpticalPathSequence/0/ICCProfile',
              'srgb',
              'sRGB_v4_ICC_preference.icc',
          ),
          param_to_return=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_TAG_DICOM_PATH,
          expected_result='OpticalPathSequence/0/ICCProfile',
      ),
  ])
  def test_get_series_icc_profile_from_store_not_using_bulk_data(
      self,
      dcm: pydicom.FileDataset,
      param_to_return: color_conversion_util._GetIccProfileReturnValue,
      expected_result: Union[bytes, str],
  ):
    baseurl = dicom_url_util.DicomWebBaseURL(
        MOCK_TEST_API_VERSION,
        'mock_project',
        'earth',
        'mock_dateset',
        'mock_store',
    )
    with dicom_store_mock.MockDicomStores(
        baseurl.full_url
    ) as mocked_dicom_stores:
      mocked_dicom_stores[baseurl.full_url].add_instance(dcm)
      redis_mk = shared_test_util.RedisMock()
      with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
        result = color_conversion_util._get_series_icc_profile(
            user_auth_util.AuthSession({}),
            dicom_url_util.base_dicom_series_url(
                baseurl,
                dicom_url_util.StudyInstanceUID(dcm.StudyInstanceUID),
                dicom_url_util.SeriesInstanceUID(dcm.SeriesInstanceUID),
            ),
            _UNDEFINED_SOP_INSTANCE_UID,
            cache_enabled_type.CachingEnabled(True),
            param_to_return,
        )
        self.assertLen(redis_mk, 17)
      self.assertEqual(result, expected_result)

  @parameterized.named_parameters([
      dict(
          testcase_name='iccprofile_bytes',
          dcm=_test_dcm_with_icc_profile(
              'ICCProfile', 'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
          param_to_return=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_BYTES,
          expected_result=color_conversion_util._read_internal_icc_profile(
              'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
      ),
      dict(
          testcase_name='opticalpathsq_iccprofile_bytes',
          dcm=_test_dcm_with_icc_profile(
              'OpticalPathSequence/0/ICCProfile',
              'srgb',
              'sRGB_v4_ICC_preference.icc',
          ),
          param_to_return=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_BYTES,
          expected_result=color_conversion_util._read_internal_icc_profile(
              'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
      ),
      dict(
          testcase_name='no_icc_profile_bytes',
          dcm=shared_test_util.jpeg_encoded_dicom_instance(),
          param_to_return=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_BYTES,
          expected_result=None,
      ),
      dict(
          testcase_name='iccprofile_path',
          dcm=_test_dcm_with_icc_profile(
              'ICCProfile', 'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
          param_to_return=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_TAG_DICOM_PATH,
          expected_result='ICCProfile',
      ),
      dict(
          testcase_name='opticalpathsq_iccprofile_path',
          dcm=_test_dcm_with_icc_profile(
              'OpticalPathSequence/0/ICCProfile',
              'srgb',
              'sRGB_v4_ICC_preference.icc',
          ),
          param_to_return=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_TAG_DICOM_PATH,
          expected_result='OpticalPathSequence/0/ICCProfile',
      ),
  ])
  @mock.patch.object(
      bulkdata_util,
      'does_dicom_store_support_bulkdata',
      autospec=True,
      return_value=True,
  )
  def test_get_series_icc_profile_from_store_using_bulk_data(
      self,
      unused_mock,
      dcm: pydicom.FileDataset,
      param_to_return: color_conversion_util._GetIccProfileReturnValue,
      expected_result: Union[bytes, str],
  ):
    baseurl = dicom_url_util.DicomWebBaseURL(
        MOCK_TEST_API_VERSION,
        'mock_project',
        'earth',
        'mock_dateset',
        'mock_store',
    )
    with dicom_store_mock.MockDicomStores(
        baseurl.full_url
    ) as mocked_dicom_stores:
      mocked_dicom_stores[baseurl.full_url].add_instance(dcm)
      redis_mk = shared_test_util.RedisMock()
      with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
        result = color_conversion_util._get_series_icc_profile(
            user_auth_util.AuthSession({}),
            dicom_url_util.base_dicom_series_url(
                baseurl,
                dicom_url_util.StudyInstanceUID(dcm.StudyInstanceUID),
                dicom_url_util.SeriesInstanceUID(dcm.SeriesInstanceUID),
            ),
            dicom_url_util.SOPInstanceUID(dcm.SOPInstanceUID),
            cache_enabled_type.CachingEnabled(True),
            param_to_return,
        )
      self.assertEqual(result, expected_result)

  @parameterized.parameters([
      color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_TAG_DICOM_PATH,
      color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_BYTES,
  ])
  def test_get_series_icc_profile_invalid_ur_returns_none(
      self, param_to_return
  ):
    self.assertIsNone(
        color_conversion_util._get_series_icc_profile(
            user_auth_util.AuthSession({}),
            'invalid_url',
            dicom_url_util.SOPInstanceUID('123'),
            cache_enabled_type.CachingEnabled(True),
            param_to_return,
        )
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='profile_bytes_loads_cache_returned_cached_path',
          dcm=_test_dcm_with_icc_profile(
              'ICCProfile', 'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
          param_to_load_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_BYTES,
          param_to_query_from_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_TAG_DICOM_PATH,
          expected_result='ICCProfile',
      ),
      dict(
          testcase_name='profile_bytes_loads_cache_returned_cached_bytes',
          dcm=_test_dcm_with_icc_profile(
              'ICCProfile', 'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
          param_to_load_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_BYTES,
          param_to_query_from_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_BYTES,
          expected_result=color_conversion_util._read_internal_icc_profile(
              'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
      ),
      dict(
          testcase_name='no_returned_cached_bytes',
          dcm=_test_dcm_with_icc_profile(
              'ICCProfile', 'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
          param_to_load_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_BYTES,
          param_to_query_from_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_BYTES,
          expected_result=color_conversion_util._read_internal_icc_profile(
              'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
      ),
      dict(
          testcase_name='profile_path_loads_cache_returned_cached_bytes',
          dcm=_test_dcm_with_icc_profile(
              'OpticalPathSequence/0/ICCProfile',
              'srgb',
              'sRGB_v4_ICC_preference.icc',
          ),
          param_to_load_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_TAG_DICOM_PATH,
          param_to_query_from_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_BYTES,
          expected_result=color_conversion_util._read_internal_icc_profile(
              'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
      ),
      dict(
          testcase_name='profile_path_loads_cache_returned_cached_path',
          dcm=_test_dcm_with_icc_profile(
              'OpticalPathSequence/0/ICCProfile',
              'srgb',
              'sRGB_v4_ICC_preference.icc',
          ),
          param_to_load_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_TAG_DICOM_PATH,
          param_to_query_from_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_TAG_DICOM_PATH,
          expected_result='OpticalPathSequence/0/ICCProfile',
      ),
      dict(
          testcase_name='return_cached_colorspace',
          dcm=_test_dcm_with_icc_profile(
              'OpticalPathSequence/0/ICCProfile',
              'srgb',
              'sRGB_v4_ICC_preference.icc',
              'sRGB',
          ),
          param_to_load_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_COLOR_SPACE,
          param_to_query_from_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_COLOR_SPACE,
          expected_result='sRGB',
      ),
  ])
  def test_get_series_icc_profile_from_redis_cache(
      self,
      dcm: pydicom.FileDataset,
      param_to_load_cache: color_conversion_util._GetIccProfileReturnValue,
      param_to_query_from_cache: color_conversion_util._GetIccProfileReturnValue,
      expected_result: Union[bytes, str],
  ):
    baseurl = dicom_url_util.DicomWebBaseURL(
        MOCK_TEST_API_VERSION,
        'mock_project',
        'earth',
        'mock_dateset',
        'mock_store',
    )
    with dicom_store_mock.MockDicomStores(
        baseurl.full_url
    ) as mocked_dicom_stores:
      mocked_dicom_stores[baseurl.full_url].add_instance(dcm)
      redis_mk = shared_test_util.RedisMock()
      with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
        color_conversion_util._get_series_icc_profile(
            user_auth_util.AuthSession({}),
            dicom_url_util.base_dicom_series_url(
                baseurl,
                dicom_url_util.StudyInstanceUID(dcm.StudyInstanceUID),
                dicom_url_util.SeriesInstanceUID(dcm.SeriesInstanceUID),
            ),
            _UNDEFINED_SOP_INSTANCE_UID,
            cache_enabled_type.CachingEnabled(True),
            param_to_load_cache,
        )
        self.assertLen(redis_mk, 17)
        self.assertEqual(
            color_conversion_util._get_series_icc_profile(
                user_auth_util.AuthSession({}),
                dicom_url_util.base_dicom_series_url(
                    baseurl,
                    dicom_url_util.StudyInstanceUID(dcm.StudyInstanceUID),
                    dicom_url_util.SeriesInstanceUID(dcm.SeriesInstanceUID),
                ),
                _UNDEFINED_SOP_INSTANCE_UID,
                cache_enabled_type.CachingEnabled(True),
                param_to_query_from_cache,
            ),
            expected_result,
        )
        self.assertLen(redis_mk, 17)

  @parameterized.named_parameters([
      dict(
          testcase_name='profile_bytes_loads_cache_returned_cached_path',
          dcm=_test_dcm_with_icc_profile(
              'ICCProfile', 'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
          param_to_load_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_BYTES,
          param_to_query_from_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_TAG_DICOM_PATH,
          expected_missing_value='',
      ),
      dict(
          testcase_name='profile_bytes_loads_cache_returned_cached_bytes',
          dcm=_test_dcm_with_icc_profile(
              'ICCProfile', 'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
          param_to_load_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_BYTES,
          param_to_query_from_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_BYTES,
          expected_missing_value=None,
      ),
      dict(
          testcase_name='profile_path_loads_cache_returned_cached_bytes',
          dcm=_test_dcm_with_icc_profile(
              'OpticalPathSequence/0/ICCProfile',
              'srgb',
              'sRGB_v4_ICC_preference.icc',
          ),
          param_to_load_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_TAG_DICOM_PATH,
          param_to_query_from_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_BYTES,
          expected_missing_value=None,
      ),
      dict(
          testcase_name='profile_path_loads_cache_returned_cached_path',
          dcm=_test_dcm_with_icc_profile(
              'OpticalPathSequence/0/ICCProfile',
              'srgb',
              'sRGB_v4_ICC_preference.icc',
          ),
          param_to_load_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_TAG_DICOM_PATH,
          param_to_query_from_cache=color_conversion_util._GetIccProfileReturnValue.ICC_PROFILE_TAG_DICOM_PATH,
          expected_missing_value='',
      ),
  ])
  def test_get_series_icc_profile_from_missing_instance_returns_none(
      self,
      dcm: pydicom.FileDataset,
      param_to_load_cache: color_conversion_util._GetIccProfileReturnValue,
      param_to_query_from_cache: color_conversion_util._GetIccProfileReturnValue,
      expected_missing_value: Optional[str],
  ):
    baseurl = dicom_url_util.DicomWebBaseURL(
        MOCK_TEST_API_VERSION,
        'mock_project',
        'earth',
        'mock_dateset',
        'mock_store',
    )
    with dicom_store_mock.MockDicomStores(baseurl.full_url):
      redis_mk = shared_test_util.RedisMock()
      with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
        self.assertIsNone(
            color_conversion_util._get_series_icc_profile(
                user_auth_util.AuthSession({}),
                dicom_url_util.base_dicom_series_url(
                    baseurl,
                    dicom_url_util.StudyInstanceUID(dcm.StudyInstanceUID),
                    dicom_url_util.SeriesInstanceUID(dcm.SeriesInstanceUID),
                ),
                _UNDEFINED_SOP_INSTANCE_UID,
                cache_enabled_type.CachingEnabled(True),
                param_to_load_cache,
            )
        )
        self.assertLen(redis_mk, 1)
        self.assertEqual(
            color_conversion_util._get_series_icc_profile(
                user_auth_util.AuthSession({}),
                dicom_url_util.base_dicom_series_url(
                    baseurl,
                    dicom_url_util.StudyInstanceUID(dcm.StudyInstanceUID),
                    dicom_url_util.SeriesInstanceUID(dcm.SeriesInstanceUID),
                ),
                _UNDEFINED_SOP_INSTANCE_UID,
                cache_enabled_type.CachingEnabled(True),
                param_to_query_from_cache,
            ),
            expected_missing_value,
        )
        self.assertLen(redis_mk, 1)

  @parameterized.named_parameters([
      dict(
          testcase_name='no_icc_profile',
          dcm=pydicom.dcmread(
              shared_test_util.jpeg_encoded_dicom_instance_test_path()
          ),
          expected_path='',
      ),
      dict(
          testcase_name='icc_profile',
          dcm=_test_dcm_with_icc_profile(
              'ICCProfile', 'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
          expected_path='ICCProfile',
      ),
      dict(
          testcase_name='optical_path_sequence_icc_profile',
          dcm=_test_dcm_with_icc_profile(
              'OpticalPathSequence/0/ICCProfile',
              'srgb',
              'sRGB_v4_ICC_preference.icc',
          ),
          expected_path='OpticalPathSequence/0/ICCProfile',
      ),
  ])
  def test_get_series_icc_profile_path(
      self, dcm: pydicom.FileDataset, expected_path: str
  ):
    baseurl = dicom_url_util.DicomWebBaseURL(
        MOCK_TEST_API_VERSION,
        'mock_project',
        'earth',
        'mock_dateset',
        'mock_store',
    )
    with dicom_store_mock.MockDicomStores(
        baseurl.full_url
    ) as mocked_dicom_stores:
      mocked_dicom_stores[baseurl.full_url].add_instance(dcm)
      redis_mk = shared_test_util.RedisMock()
      with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
        self.assertEqual(
            color_conversion_util.get_series_icc_profile_path(
                user_auth_util.AuthSession({}),
                dicom_url_util.base_dicom_series_url(
                    baseurl,
                    dicom_url_util.StudyInstanceUID(dcm.StudyInstanceUID),
                    dicom_url_util.SeriesInstanceUID(dcm.SeriesInstanceUID),
                ),
                _UNDEFINED_SOP_INSTANCE_UID,
                cache_enabled_type.CachingEnabled(True),
            ),
            expected_path,
        )

  @parameterized.named_parameters([
      dict(
          testcase_name='no_icc_profile',
          dcm=pydicom.dcmread(
              shared_test_util.jpeg_encoded_dicom_instance_test_path()
          ),
          expected_bytes=None,
      ),
      dict(
          testcase_name='icc_profile',
          dcm=_test_dcm_with_icc_profile(
              'ICCProfile', 'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
          expected_bytes=color_conversion_util._read_internal_icc_profile(
              'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
      ),
      dict(
          testcase_name='optical_path_sequence_icc_profile',
          dcm=_test_dcm_with_icc_profile(
              'OpticalPathSequence/0/ICCProfile',
              'srgb',
              'sRGB_v4_ICC_preference.icc',
          ),
          expected_bytes=color_conversion_util._read_internal_icc_profile(
              'srgb', 'sRGB_v4_ICC_preference.icc'
          ),
      ),
  ])
  def test_get_series_icc_profile_bytes(
      self, dcm: pydicom.FileDataset, expected_bytes: bytes
  ):
    baseurl = dicom_url_util.DicomWebBaseURL(
        MOCK_TEST_API_VERSION,
        'mock_project',
        'earth',
        'mock_dateset',
        'mock_store',
    )
    with dicom_store_mock.MockDicomStores(
        baseurl.full_url
    ) as mocked_dicom_stores:
      mocked_dicom_stores[baseurl.full_url].add_instance(dcm)
      redis_mk = shared_test_util.RedisMock()
      with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
        self.assertEqual(
            color_conversion_util.get_series_icc_profile_bytes(
                user_auth_util.AuthSession({}),
                dicom_url_util.base_dicom_series_url(
                    baseurl,
                    dicom_url_util.StudyInstanceUID(dcm.StudyInstanceUID),
                    dicom_url_util.SeriesInstanceUID(dcm.SeriesInstanceUID),
                ),
                _UNDEFINED_SOP_INSTANCE_UID,
                cache_enabled_type.CachingEnabled(True),
            ),
            expected_bytes,
        )

  def test_get_srgb_iccprofile(self):
    self.assertLen(color_conversion_util._get_srgb_iccprofile(), 60960)

  @mock.patch.object(
      color_conversion_util,
      '_read_internal_icc_profile',
      autospec=True,
      side_effect=FileNotFoundError('file not found'),
  )
  def test_get_srgb_iccprofile_cannot_load_embedded_profile_fail_over_to_pil(
      self, _
  ):
    self.assertLen(color_conversion_util._get_srgb_iccprofile(), 588)

  def test_get_rommrgb_iccprofile(self):
    self.assertLen(color_conversion_util._get_rommrgb_iccprofile(), 864)

  def test_get_srgb_iccprofile_from_plugin(self):
    expected = b'test'
    tdir = self.create_tempdir()
    with open(os.path.join(tdir, 'srgb.icc'), 'wb') as tfile:
      tfile.write(expected)
    with flagsaver.flagsaver(third_party_icc_profile_directory=tdir.full_path):
      self.assertEqual(color_conversion_util._get_srgb_iccprofile(), expected)

  @parameterized.parameters(['adobergb', 'adobergb1998'])
  def test_get_adobergb_iccprofile_from_plugin(self, name):
    expected = b'test'
    tdir = self.create_tempdir()
    with open(os.path.join(tdir, f'{name}.icc'), 'wb') as tfile:
      tfile.write(expected)
    with flagsaver.flagsaver(third_party_icc_profile_directory=tdir.full_path):
      self.assertEqual(
          color_conversion_util._get_adobergb_iccprofile(), expected
      )

  def test_get_rommrgb_iccprofile_from_plugin(self):
    expected = b'test'
    tdir = self.create_tempdir()
    with open(os.path.join(tdir, 'rommrgb.icc'), 'wb') as tfile:
      tfile.write(expected)
    with flagsaver.flagsaver(third_party_icc_profile_directory=tdir.full_path):
      self.assertEqual(
          color_conversion_util._get_rommrgb_iccprofile(), expected
      )

  def test_get_srgb_iccprofile_from_plugin_directory(self):
    expected = b'test'
    tdir = self.create_tempdir()
    path = os.path.join(tdir, 'srgb')
    os.mkdir(path)
    with open(os.path.join(path, 'name.icc'), 'wb') as tfile:
      tfile.write(expected)
    with flagsaver.flagsaver(third_party_icc_profile_directory=tdir.full_path):
      self.assertEqual(color_conversion_util._get_srgb_iccprofile(), expected)

  @parameterized.parameters(['adobergb', 'adobergb1998'])
  def test_get_adobergb_iccprofile_from_plugin_directory(self, name):
    expected = b'test'
    tdir = self.create_tempdir()
    path = os.path.join(tdir, name)
    os.mkdir(path)
    with open(os.path.join(path, 'name.icc'), 'wb') as tfile:
      tfile.write(expected)
    with flagsaver.flagsaver(third_party_icc_profile_directory=tdir.full_path):
      self.assertEqual(
          color_conversion_util._get_adobergb_iccprofile(), expected
      )

  def test_get_rommrgb_iccprofile_from_plugin_directory(self):
    expected = b'test'
    tdir = self.create_tempdir()
    path = os.path.join(tdir, 'rommrgb')
    os.mkdir(path)
    with open(os.path.join(path, 'name.icc'), 'wb') as tfile:
      tfile.write(expected)
    with flagsaver.flagsaver(third_party_icc_profile_directory=tdir.full_path):
      self.assertEqual(
          color_conversion_util._get_rommrgb_iccprofile(), expected
      )

  def test_init_fork_module_state(self):
    color_conversion_util._cache_lock = 'mock'
    color_conversion_util._icc_transform_cache = 'mock'
    color_conversion_util._init_fork_module_state()
    self.assertIsNotNone(color_conversion_util._cache_lock)
    self.assertNotEqual(color_conversion_util._cache_lock, 'mock')
    self.assertIsInstance(
        color_conversion_util._icc_transform_cache, cachetools.LRUCache
    )

  def test_clear_cache_before_fork(self) -> None:
    color_conversion_util._icc_transform_cache['foo'] = 'bar'
    color_conversion_util._clear_cache_before_fork()
    self.assertEmpty(color_conversion_util._icc_transform_cache)

  @parameterized.named_parameters([
      dict(
          testcase_name='has_icc_profile',
          md=icc_profile_metadata_cache.ICCProfileMetadata(
              'ICCProfile', 'sRGB', 'http://bulkdata', '1234'
          ),
          expected='sRGB',
      ),
      dict(
          testcase_name='missing_icc_profile',
          md=icc_profile_metadata_cache.INSTANCE_MISSING_ICC_PROFILE_METADATA,
          expected='',
      ),
  ])
  def test_get_cached_icc_profile_colorspace(self, md, expected):
    self.assertEqual(
        color_conversion_util._get_cached_icc_profile_colorspace(md), expected
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='has_icc_profile',
          md=icc_profile_metadata_cache.ICCProfileMetadata(
              'ICCProfile', 'sRGB', 'http://bulkdata', '1234'
          ),
          expected='ICCProfile',
      ),
      dict(
          testcase_name='missing_icc_profile',
          md=icc_profile_metadata_cache.INSTANCE_MISSING_ICC_PROFILE_METADATA,
          expected='',
      ),
  ])
  def test_get_cached_icc_profile_path(self, md, expected):
    self.assertEqual(
        color_conversion_util._get_cached_icc_profile_path(md), expected
    )

  def test_get_cached_icc_profile_bytes_missing_value(self):
    redis_mk = shared_test_util.RedisMock()
    with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
      redis_result = color_conversion_util._get_cached_icc_profile_bytes(
          icc_profile_metadata_cache.INSTANCE_MISSING_ICC_PROFILE_METADATA,
          redis_cache.RedisCache(),
      )
    self.assertIsNone(redis_result.value)  # pytype: disable=attribute-error

  def test_get_cached_icc_profile_bytes_has_value(self):
    mk_profile_bytes = b'ICC_PROFILE_BYTES'
    mk_icc_profile_hash_value = '1234'
    redis_mk = shared_test_util.RedisMock()
    redis_mk.set(
        color_conversion_util._icc_profile_cache_key(mk_icc_profile_hash_value),
        mk_profile_bytes,
        nx=False,
        ex=24,
    )
    with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
      redis_result = color_conversion_util._get_cached_icc_profile_bytes(
          icc_profile_metadata_cache.ICCProfileMetadata(
              'ICCProfile',
              'sRGB',
              'http://bulkdata',
              mk_icc_profile_hash_value,
          ),
          redis_cache.RedisCache(),
      )
      self.assertEqual(redis_result.value, mk_profile_bytes)  # pytype: disable=attribute-error

  def test_get_cached_icc_profile_bytes_not_found(self):
    mk_icc_profile_hash_value = '1234'
    redis_mk = shared_test_util.RedisMock()
    with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
      redis_result = color_conversion_util._get_cached_icc_profile_bytes(
          icc_profile_metadata_cache.ICCProfileMetadata(
              'ICCProfile',
              'sRGB',
              'http://bulkdata',
              mk_icc_profile_hash_value,
          ),
          redis_cache.RedisCache(),
      )
      self.assertIsNone(redis_result)

  def test_icc_profile_cache_key(self):
    self.assertEqual(
        color_conversion_util._icc_profile_cache_key('abc'),
        'abc_ICC_PROFILE_BYTES',
    )

  def test_set_cached_icc_profile_none(self):
    redis_mk = shared_test_util.RedisMock()
    with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
      series_url = dicom_url_util.DicomSeriesUrl('http://series_url')
      instance = dicom_url_util.SOPInstanceUID('1.2.3')
      does_dicom_store_support_bulkdata = True
      color_conversion_util._set_cached_icc_profile(
          redis_cache.RedisCache(),
          series_url,
          instance,
          does_dicom_store_support_bulkdata,
          None,
          icc_profile_metadata_cache.INSTANCE_MISSING_ICC_PROFILE_METADATA,
      )
    self.assertLen(redis_mk._redis, 1)
    self.assertEqual(
        redis_mk.get(
            icc_profile_metadata_cache._cache_key(
                series_url, instance, does_dicom_store_support_bulkdata
            )
        ),
        icc_profile_metadata_cache.INSTANCE_MISSING_ICC_PROFILE_METADATA.to_json(
            sort_keys=True
        ).encode(
            'utf-8'
        ),
    )

  def test_set_cached_icc_profile_bytes(self):
    redis_mk = shared_test_util.RedisMock()
    with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
      series_url = dicom_url_util.DicomSeriesUrl('http://series_url')
      instance = dicom_url_util.SOPInstanceUID('1.2.3')
      does_dicom_store_support_bulkdata = True
      icc_profile_hash_value = 'hash_value'
      icc_profile_bytes = b'1234'
      profile_metadata = icc_profile_metadata_cache.ICCProfileMetadata(
          'ICCProfile', 'sRGB', 'http://bulkdata', icc_profile_hash_value
      )

      color_conversion_util._set_cached_icc_profile(
          redis_cache.RedisCache(),
          series_url,
          instance,
          does_dicom_store_support_bulkdata,
          icc_profile_bytes,
          profile_metadata,
      )
    self.assertLen(redis_mk._redis, 2)
    self.assertEqual(
        redis_mk.get(
            icc_profile_metadata_cache._cache_key(
                series_url, instance, does_dicom_store_support_bulkdata
            )
        ),
        profile_metadata.to_json(sort_keys=True).encode('utf-8'),
    )
    self.assertEqual(
        redis_mk.get(
            color_conversion_util._icc_profile_cache_key(icc_profile_hash_value)
        ),
        icc_profile_bytes,
    )


if __name__ == '__main__':
  absltest.main()
