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
"""Tests for icc profile metadata cache."""

from unittest import mock
import uuid

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import cachetools

from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import icc_profile_metadata_cache
from pathology.dicom_proxy import redis_cache
from pathology.dicom_proxy import shared_test_util
from pathology.dicom_proxy import user_auth_util


class ColorConversionUtilTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    icc_profile_metadata_cache._init_fork_module_state()
    self.enter_context(
        mock.patch.object(
            user_auth_util.AuthSession,
            'email',
            new_callable=mock.PropertyMock,
            return_value='mock@email.com',
        )
    )
    icc_profile_metadata_cache._metadata_cache = cachetools.LRUCache(
        icc_profile_metadata_cache._LOCALMETADATA_CACHESIZE
    )
    icc_profile_metadata_cache._is_debugging = True

  def test_init_fork_module_state(self):
    icc_profile_metadata_cache._metadata_cache_lock = 'mock'
    icc_profile_metadata_cache._metadata_cache = 'mock'
    icc_profile_metadata_cache._init_fork_module_state()
    self.assertIsNotNone(icc_profile_metadata_cache._metadata_cache_lock)
    self.assertNotEqual(icc_profile_metadata_cache._metadata_cache_lock, 'mock')
    self.assertIsInstance(
        icc_profile_metadata_cache._metadata_cache, cachetools.LRUCache
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='bulkdata_supported',
          does_dicom_store_support_bulkdata=True,
          expected='icc_profile_metadata url:series_url/instances/instance_uid',
      ),
      dict(
          testcase_name='bulkdata_not_supported',
          does_dicom_store_support_bulkdata=False,
          expected='icc_profile_metadata url:series_url',
      ),
  ])
  def test_cache_key(self, does_dicom_store_support_bulkdata, expected):
    self.assertEqual(
        icc_profile_metadata_cache._cache_key(
            dicom_url_util.DicomSeriesUrl('series_url'),
            dicom_url_util.SOPInstanceUID('instance_uid'),
            does_dicom_store_support_bulkdata,
        ),
        expected,
    )

  def test_cache_tools_hash_key_debugging(self):
    self.assertStartsWith(
        icc_profile_metadata_cache._cache_tools_hash_key('key'), 'key_'
    )

  def test_cache_tools_hash_key_not_debugging(self):
    icc_profile_metadata_cache._is_debugging = False
    self.assertEqual(
        icc_profile_metadata_cache._cache_tools_hash_key('key'), 'key'
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='ttl_disabled',
          flag_value=-1,
          expected=None,
      ),
      dict(
          testcase_name='ttl_zero',
          flag_value=0,
          expected=0,
      ),
      dict(
          testcase_name='ttl_enabled',
          flag_value=10,
          expected=10,
      ),
  ])
  def test_icc_profile_metadata_redis_cache_ttl(self, flag_value, expected):
    with flagsaver.flagsaver(icc_profile_redis_cache_ttl=flag_value):
      self.assertEqual(
          icc_profile_metadata_cache.icc_profile_metadata_redis_cache_ttl(),
          expected,
      )

  @mock.patch.object(uuid, 'uuid1', autospec=True, return_value='')
  def test_set_cached_instance_icc_profile_metadata(self, _):
    metadata = icc_profile_metadata_cache.ICCProfileMetadata(
        'path', 'srgb', 'bulkdata_uri', 'hash'
    )
    redis_mk = shared_test_util.RedisMock()
    with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
      icc_profile_metadata_cache.set_cached_instance_icc_profile_metadata(
          redis_cache.RedisCache(),
          dicom_url_util.DicomSeriesUrl('series_url'),
          dicom_url_util.SOPInstanceUID('instance_uid'),
          False,
          metadata,
      )
    cache_key = icc_profile_metadata_cache._cache_key(
        dicom_url_util.DicomSeriesUrl('series_url'),
        dicom_url_util.SOPInstanceUID('instance_uid'),
        False,
    )
    self.assertLen(redis_mk._redis, 1)
    self.assertEqual(
        redis_mk.get(cache_key),
        metadata.to_json(sort_keys=True).encode('utf-8'),
    )
    self.assertEqual(
        icc_profile_metadata_cache._metadata_cache[
            icc_profile_metadata_cache._cache_tools_hash_key(cache_key)
        ],
        metadata,
    )

  def test_get_cached_instance_icc_profile_metadata_not_cached(self):
    redis_mk = shared_test_util.RedisMock()
    with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
      self.assertIsNone(
          icc_profile_metadata_cache.get_cached_instance_icc_profile_metadata(
              redis_cache.RedisCache(),
              dicom_url_util.DicomSeriesUrl('series_url'),
              dicom_url_util.SOPInstanceUID('instance_uid'),
              False,
          )
      )

  def test_get_cached_instance_icc_profile_metadata_from_cache_tools(self):
    icc_profile_metadata_cache._is_debugging = False
    metadata = icc_profile_metadata_cache.ICCProfileMetadata(
        'path', 'srgb', 'bulkdata_uri', 'hash'
    )
    redis_mk = shared_test_util.RedisMock()
    with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
      redis_mock = redis_cache.RedisCache()
      icc_profile_metadata_cache.set_cached_instance_icc_profile_metadata(
          redis_mock,
          dicom_url_util.DicomSeriesUrl('series_url'),
          dicom_url_util.SOPInstanceUID('instance_uid'),
          False,
          metadata,
      )
      redis_mk.clear()
      self.assertEqual(
          icc_profile_metadata_cache.get_cached_instance_icc_profile_metadata(
              redis_mock,
              dicom_url_util.DicomSeriesUrl('series_url'),
              dicom_url_util.SOPInstanceUID('instance_uid'),
              False,
          ),
          metadata,
      )

  def test_get_cached_instance_icc_profile_metadata_from_redis(self):
    metadata = icc_profile_metadata_cache.ICCProfileMetadata(
        'path', 'srgb', 'bulkdata_uri', 'hash'
    )
    cache_key = icc_profile_metadata_cache._cache_key(
        dicom_url_util.DicomSeriesUrl('series_url'),
        dicom_url_util.SOPInstanceUID('instance_uid'),
        False,
    )
    redis_mk = shared_test_util.RedisMock()
    with mock.patch('redis.Redis', autospec=True, return_value=redis_mk):
      redis_mk.set(
          cache_key,
          metadata.to_json(sort_keys=True).encode('utf-8'),
          nx=True,
          ex=24,
      )
      self.assertEqual(
          icc_profile_metadata_cache.get_cached_instance_icc_profile_metadata(
              redis_cache.RedisCache(),
              dicom_url_util.DicomSeriesUrl('series_url'),
              dicom_url_util.SOPInstanceUID('instance_uid'),
              False,
          ),
          metadata,
      )


if __name__ == '__main__':
  absltest.main()
