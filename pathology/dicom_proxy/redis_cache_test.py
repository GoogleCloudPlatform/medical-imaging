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
"""Tests for redis cache."""

import dataclasses
import os
import subprocess
import time
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import psutil
import redis

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import redis_cache
from pathology.dicom_proxy import shared_test_util
from pathology.shared_libs.logging_lib import cloud_logging_client_instance
from pathology.shared_libs.test_utils.gcs_mock import gcs_mock


_TEST_KEY = 'test_key'
_TEST_VALUE_STR = 'test_value'
_TEST_VALUE_BYTES = _TEST_VALUE_STR.encode('utf-8')


class _RedisMockLongValue:

  def __len__(self) -> int:
    return redis_cache._VALUE_REDIS_CACHE_SIZE_LIMIT_LIMIT + 1


@dataclasses.dataclass
class _MockedAvailMem:
  """Mocks psutils.virtual_memory() function response."""

  available: int


class RedisCacheTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    redis_cache._redis_host_instance = None

  def tearDown(self):
    redis_cache._redis_host_instance = None
    super().tearDown()

  @mock.patch('redis.Redis', autospec=True)
  def test_get_missing_key(self, redis_mock):
    redis_mock.return_value = dict()
    cache = redis_cache.RedisCache()

    self.assertIsNone(cache.get(_TEST_KEY))
    self.assertTrue(cache.is_enabled)

  @flagsaver.flagsaver(
      redis_cache_host_ip='123',
      redis_cache_host_port=45,
      redis_cache_host_db=6,
  )
  def test_redis_user_config(self):
    cache = redis_cache.RedisCache()
    host = cache._redis.connection_pool.connection_kwargs['host']
    port = cache._redis.connection_pool.connection_kwargs['port']
    db = cache._redis.connection_pool.connection_kwargs['db']
    self.assertFalse(cache.is_localhost)
    self.assertTrue(cache.is_enabled)
    self.assertTrue(cache.server_defined)
    self.assertEqual(host, '123')
    self.assertEqual(port, 45)
    self.assertEqual(db, 6)

  @flagsaver.flagsaver(redis_cache_maxmemory=0)
  def test_setting_maxmemory_to_zero_disables_redis_cache(self):
    with flagsaver.flagsaver(
        redis_cache_host_ip='localhost', redis_cache_host_port=6379
    ):
      cache = redis_cache.RedisCache()
      self.assertFalse(cache.is_enabled)

  @flagsaver.flagsaver(
      redis_cache_host_ip='123',
      redis_cache_host_port=45,
      redis_cache_host_db=6,
      redis_tls_certificate_authority_gcs_uri='gs://foo/bar.pem',
  )
  def test_redis_remote_server_tls(self):
    cache = redis_cache.RedisCache()
    host = cache._redis.connection_pool.connection_kwargs['host']
    port = cache._redis.connection_pool.connection_kwargs['port']
    db = cache._redis.connection_pool.connection_kwargs['db']
    self.assertFalse(cache.is_localhost)
    self.assertTrue(cache.is_enabled)
    self.assertTrue(cache.server_defined)
    self.assertEqual(host, '127.0.0.1')
    self.assertEqual(port, 6378)
    self.assertEqual(db, 6)

  def test_redis_default_config(self):
    cache = redis_cache.RedisCache()
    host = cache._redis.connection_pool.connection_kwargs['host']
    port = cache._redis.connection_pool.connection_kwargs['port']
    db = cache._redis.connection_pool.connection_kwargs['db']
    self.assertTrue(cache.is_localhost)
    self.assertTrue(cache.is_enabled)
    self.assertTrue(cache.server_defined)
    self.assertEqual(host, '127.0.0.1')
    self.assertEqual(port, 6379)
    self.assertEqual(db, 0)

  @parameterized.parameters([
      cache_enabled_type.CachingEnabled(True),
      cache_enabled_type.CachingEnabled(False),
  ])
  @flagsaver.flagsaver(
      redis_cache_host_ip=' ',
      redis_cache_host_port=45,
      redis_cache_host_db=6,
  )
  def test_redis_user_config_disabled(self, cache_enabled):
    cache = redis_cache.RedisCache(cache_enabled)
    self.assertFalse(cache.is_localhost)
    self.assertFalse(cache.server_defined)
    self.assertFalse(cache.is_enabled)

  @parameterized.parameters([
      (None, None),
      (redis_cache._REDIS_NONE_VALUE, None),
      (_TEST_VALUE_BYTES, _TEST_VALUE_BYTES),
      (_TEST_VALUE_STR, _TEST_VALUE_BYTES),
  ])
  @mock.patch('redis.Redis', autospec=True)
  def test_set_get_key_with_value(self, value, expected_value, redis_mock):
    redis_mock.return_value = shared_test_util.RedisMock()
    cache = redis_cache.RedisCache()

    self.assertTrue(cache.set(_TEST_KEY, value))
    self.assertEqual(cache.get(_TEST_KEY).value, expected_value)  # type: ignore
    self.assertTrue(cache.is_enabled)

  @mock.patch.object(redis.Redis, 'get', autospec=True)
  @mock.patch.object(cloud_logging_client_instance, '_absl_log', autospec=True)
  def test_set_get_key_redis_connection_disabled(
      self, mock_log, redis_get_mock
  ):
    redis_get_mock.side_effect = redis.exceptions.ConnectionError()
    cache = redis_cache.RedisCache()
    self.assertIsNone(cache.get(_TEST_KEY))  # type: ignore
    self.assertTrue(cache.is_enabled)
    mock_log.assert_called_once()

  @mock.patch.object(redis.Redis, 'set', autospec=True)
  @mock.patch.object(cloud_logging_client_instance, '_absl_log', autospec=True)
  def test_set_redis_connection_disabled(self, mock_log, redis_set_mock):
    redis_set_mock.side_effect = redis.exceptions.ConnectionError()
    cache = redis_cache.RedisCache()
    self.assertFalse(cache.set(_TEST_KEY, _TEST_VALUE_STR))
    self.assertTrue(cache.is_enabled)
    mock_log.assert_called_once()

  @mock.patch('redis.Redis', autospec=True)
  def test_set_redis_connection_value_to_long(self, redis_mock):
    redis_mock.return_value = shared_test_util.RedisMock()
    cache = redis_cache.RedisCache()
    test_val = _RedisMockLongValue()
    self.assertFalse(cache.set(_TEST_KEY, test_val))  # type: ignore
    self.assertTrue(cache.is_enabled)
    self.assertEmpty(redis_mock.return_value._redis)

  @mock.patch('redis.Redis', autospec=True)
  def test_set_redis_nx_true(self, redis_mock):
    key_value_1 = '1234'
    key_value_2 = '1235'
    redis_mock.return_value = shared_test_util.RedisMock()
    cache = redis_cache.RedisCache()
    self.assertTrue(cache.set(_TEST_KEY, key_value_1))
    self.assertFalse(cache.set(_TEST_KEY, key_value_2, allow_overwrite=False))

  @mock.patch('redis.Redis', autospec=True)
  def test_set_redis_none_nx_true(self, redis_mock):
    key_value_1 = '1234'
    redis_mock.return_value = shared_test_util.RedisMock()
    cache = redis_cache.RedisCache()
    self.assertTrue(cache.set(_TEST_KEY, key_value_1))
    self.assertFalse(cache.set(_TEST_KEY, None, allow_overwrite=False))
    self.assertEqual(
        cache.get(_TEST_KEY),
        redis_cache.RedisResult(key_value_1.encode('utf-8')),
    )

  @mock.patch('redis.Redis', autospec=True)
  def test_set_redis_nx_false(self, redis_mock):
    key_value_1 = '1234'
    key_value_2 = '1235'
    redis_mock.return_value = shared_test_util.RedisMock()
    cache = redis_cache.RedisCache()
    self.assertTrue(cache.set(_TEST_KEY, key_value_1))
    self.assertTrue(cache.set(_TEST_KEY, key_value_2, allow_overwrite=True))

  @mock.patch('redis.Redis', autospec=True)
  def test_delete_success(self, redis_mock):
    key_value_1 = '1234'
    redis_mock.return_value = shared_test_util.RedisMock()
    cache = redis_cache.RedisCache()
    cache.set(_TEST_KEY, key_value_1)
    self.assertEqual(cache.delete(_TEST_KEY), 1)

  @mock.patch('redis.Redis', autospec=True)
  def test_delete_not_found(self, redis_mock):
    key_value_1 = '1234'
    redis_mock.return_value = shared_test_util.RedisMock()
    cache = redis_cache.RedisCache()
    self.assertEqual(cache.delete(key_value_1), 0)

  @mock.patch.object(redis.Redis, 'delete', autospec=True)
  def test_delete_connection_error(self, mock_delete):
    key_value_1 = '1234'
    mock_delete.side_effect = redis.exceptions.ConnectionError()
    cache = redis_cache.RedisCache()
    cache.set(_TEST_KEY, key_value_1)
    self.assertEqual(cache.delete(key_value_1), 0)

  @parameterized.parameters([
      cache_enabled_type.CachingEnabled(True),
      cache_enabled_type.CachingEnabled(False),
  ])
  def test_set_is_enabled(self, enabled):
    cache = redis_cache.RedisCache(enabled)
    self.assertEqual(cache.is_enabled, enabled)

  @parameterized.parameters([None, _TEST_VALUE_BYTES, _TEST_VALUE_STR])
  def test_disabled_set_get_key_with_value(self, value):
    cache = redis_cache.RedisCache(cache_enabled_type.CachingEnabled(False))

    self.assertFalse(cache.set(_TEST_KEY, value))
    self.assertIsNone(cache.get(_TEST_KEY))
    self.assertFalse(cache.is_enabled)

  def test_ping_disabled(self):
    cache = redis_cache.RedisCache(cache_enabled_type.CachingEnabled(False))
    self.assertFalse(cache.ping())
    self.assertFalse(cache.is_enabled)

  @mock.patch.object(redis.Redis, 'ping', autospec=True)
  def test_ping_pass(self, mock_ping):
    mock_ping.return_value = None
    cache = redis_cache.RedisCache()
    self.assertTrue(cache.ping())
    self.assertTrue(cache.is_enabled)

  @mock.patch.object(redis.Redis, 'ping', autospec=True)
  @mock.patch.object(cloud_logging_client_instance, '_absl_log', autospec=True)
  def test_ping_pass_fail(self, mock_log, mock_ping):
    mock_ping.side_effect = redis.exceptions.ConnectionError()

    cache = redis_cache.RedisCache()
    self.assertFalse(cache.ping())
    self.assertTrue(cache.is_enabled)
    mock_log.assert_called_once()

  @parameterized.parameters([(-1, 1), (1, 1)])
  @mock.patch.object(redis.Redis, 'config_set', autospec=True)
  @mock.patch.object(redis.Redis, 'config_get', autospec=True)
  @mock.patch.object(psutil, 'virtual_memory', autospec=True)
  @mock.patch.object(cloud_logging_client_instance, '_absl_log', autospec=True)
  def test_init_config_maxmemory_user_set_value(
      self,
      redis_cache_max_memory,
      expected_mem,
      mock_log,
      virtual_memory_mock,
      config_get_mock,
      config_set_mock,
  ):
    virtual_memory_mock.return_value = _MockedAvailMem(4000000000)
    config_set_mock.return_value = True
    config_get_mock.side_effect = ['500GB', f'{expected_mem}GB']
    with flagsaver.flagsaver(redis_cache_maxmemory=redis_cache_max_memory):
      redis_cache._init_config_maxmemory(redis_cache.RedisCache())
    self.assertStartsWith(
        mock_log.call_args[0][0],
        redis_cache._SET_REDIS_CACHE_MAXMEMORY_CONFIG,
    )

  @flagsaver.flagsaver(redis_cache_maxmemory=5)
  @mock.patch.object(psutil, 'virtual_memory', autospec=True)
  def test_init_config_maxmemory_set_to_value_greater_than_avail(
      self, virtual_memory_mock
  ):
    virtual_memory_mock.return_value = _MockedAvailMem(4000000000)
    with self.assertRaisesRegex(
        redis_cache.RedisConfigError,
        redis_cache._REDIS_CACHE_MAXMEMORY_GREATER_THAN_MEM_AVAIL,
    ):
      redis_cache._init_config_maxmemory(redis_cache.RedisCache())

  @flagsaver.flagsaver(redis_cache_host_ip='1.2.3', redis_cache_maxmemory=1)
  @mock.patch.object(psutil, 'virtual_memory', autospec=True)
  def test_init_config_maxmemory_external_server_warning(
      self, virtual_memory_mock
  ):
    virtual_memory_mock.return_value = _MockedAvailMem(4000000000)
    with self.assertRaisesRegex(
        redis_cache.RedisConfigError,
        redis_cache._SET_MAXMEMORY_NON_LOCAL_REDIS_ERROR,
    ):
      redis_cache._init_config_maxmemory(redis_cache.RedisCache())

  @flagsaver.flagsaver(redis_cache_host_ip='', redis_cache_maxmemory=1)
  @mock.patch.object(psutil, 'virtual_memory', autospec=True)
  def test_init_config_maxmemory_redis_disabled(self, virtual_memory_mock):
    virtual_memory_mock.return_value = _MockedAvailMem(4000000000)
    with self.assertRaisesRegex(
        redis_cache.RedisConfigError,
        redis_cache._SET_MAXMEMORY_NON_LOCAL_REDIS_ERROR,
    ):
      redis_cache._init_config_maxmemory(redis_cache.RedisCache())

  @parameterized.named_parameters([
      dict(testcase_name='username_set', username='username', password=None),
      dict(testcase_name='password_set', username=None, password='password'),
      dict(testcase_name='both_set', username='username', password='password'),
  ])
  def test_validate_flags_raises_if_user_name_or_password_set_in_local_host(
      self, username, password
  ):
    with flagsaver.flagsaver(
        redis_cache_host_ip='127.0.0.1',
        redis_auth_username=username,
        redis_auth_password=password,
    ):
      with self.assertRaisesRegex(
          redis_cache.RedisConfigError,
          redis_cache._REDIS_USERNAME_AND_PASSWORD_MUST_BE_NONE_RUNNING_FROM_LOCALHOST,
      ):
        redis_cache._validate_flags(redis_cache.RedisCache())

  @flagsaver.flagsaver(
      redis_cache_host_ip='localhost',
      redis_tls_certificate_authority_gcs_uri='gs://foo/bar.pem',
  )
  def test_validate_flags_raises_if_non_localhost_is_proxy(self):
    with self.assertRaisesRegex(
        redis_cache.RedisConfigError,
        redis_cache._REDIS_CACHE_CANNOT_BE_TLS_PROXY_WITH_LOCALHOST_IP,
    ):
      redis_cache._validate_flags(redis_cache.RedisCache())

  @flagsaver.flagsaver(
      redis_cache_host_ip='localhost',
      redis_cache_host_port=6378,
  )
  def test_validate_flags_raises_if_non_localhost_is_on_invalid_port(self):
    with self.assertRaisesRegex(
        redis_cache.RedisConfigError,
        redis_cache._INVALID_REDIS_LOCALHOST_PORT,
    ):
      redis_cache._validate_flags(redis_cache.RedisCache())

  @parameterized.named_parameters([
      dict(
          testcase_name='localhost',
          host='localhost',
          username=None,
          password=None,
          gcs_uri='',
          expected_popencall_count=1,
          expected_config_set_call_count=2,
      ),
      dict(
          testcase_name='remote_host_proxy_no_auth',
          host='45.45.24.2',
          username=None,
          password=None,
          gcs_uri='gs://foo/bar.pem',
          expected_popencall_count=1,
          expected_config_set_call_count=0,
      ),
      dict(
          testcase_name='remote_host_proxy_auth',
          host='45.45.24.2',
          username='bob',
          password='123',
          gcs_uri='gs://foo/bar.pem',
          expected_popencall_count=1,
          expected_config_set_call_count=0,
      ),
      dict(
          testcase_name='remote_host_auth',
          host='55.77.0.1',
          username='bob',
          password='123',
          gcs_uri='',
          expected_popencall_count=0,
          expected_config_set_call_count=0,
      ),
      dict(
          testcase_name='remote_host_no_auth',
          host='55.77.0.1',
          username=None,
          password=None,
          gcs_uri='',
          expected_popencall_count=0,
          expected_config_set_call_count=0,
      ),
  ])
  def test_setup_succeeds(
      self,
      host,
      username,
      password,
      gcs_uri,
      expected_popencall_count,
      expected_config_set_call_count,
  ):
    """Tests redis_cache.setup.

    Args:
      host: Test Redis cache host IP.
      username: User name used to connect to Redis instance.
      password: Password used to connect to Redis instance.
      gcs_uri: GCS URI where Redis certificate authority is stored.
      expected_popencall_count: Number of times processes are started. Possible
        processes that can be started, 1) local redis instance, 2) stunnel
        (encrypted communication between server and client).
      expected_config_set_call_count: Number of times which we are expecting the
        redis configuration to be set. If the proxy is configured to use the
        local internal redis then we expect two properties will be set: 1)
        enable autodefrag; 2) set maxmemory.

    Returns:
      None
    """
    mock_dir = self.create_tempdir().full_path
    testfile_path = os.path.join(mock_dir, 'bar.pem')
    with open(testfile_path, 'wt') as infile:
      infile.write('test')
    with flagsaver.flagsaver(
        redis_cache_host_ip=host,
        redis_auth_username=username,
        redis_auth_password=password,
        redis_tls_certificate_authority_gcs_uri=gcs_uri,
    ):
      with mock.patch.object(
          redis_cache.RedisCache, 'config_set', autospec=True
      ) as mock_config_set:
        with gcs_mock.GcsMock({'foo': mock_dir}):
          with mock.patch.object(
              subprocess, 'Popen', autospec=True
          ) as mock_popen:
            self.assertIsNone(redis_cache.setup())
          self.assertEqual(mock_popen.call_count, expected_popencall_count)
          self.assertEqual(
              mock_config_set.call_count, expected_config_set_call_count
          )

  @parameterized.named_parameters([
      dict(
          testcase_name='localhost_ip',
          flag_setting='127.0.0.1 ',
          expected='127.0.0.1',
      ),
      dict(
          testcase_name='localhost_name',
          flag_setting=' Localhost',
          expected='127.0.0.1',
      ),
      dict(
          testcase_name='localhost_name_lowercase',
          flag_setting=' localhost ',
          expected='127.0.0.1',
      ),
      dict(
          testcase_name='not_localhost_ip',
          flag_setting=' 555.77.0.1 ',
          expected='555.77.0.1',
      ),
  ])
  def test_redis_host_ip(self, flag_setting, expected):
    with flagsaver.flagsaver(redis_cache_host_ip=flag_setting):
      self.assertEqual(redis_cache._redis_host_ip(), expected)

  def test_get_redis_db_last_flushed(self):
    pi = 3.14159
    redis_cache._write_redis_db_last_flushed(pi)
    self.assertEqual(redis_cache._get_redis_db_last_flushed(), pi)

  @parameterized.named_parameters([
      dict(
          testcase_name='called_within_flush_window',
          time_last_flushed=time.time(),
          redis_flushall=0,
          redis_info_return={},
      ),
      dict(
          testcase_name='called_outside_of_flush_window',
          time_last_flushed=0.0,
          redis_flushall=1,
          redis_info_return={'lazyfree_pending_objects': 0},
      ),
      dict(
          testcase_name='pending_flush_blocks_additional_flush',
          time_last_flushed=0.0,
          redis_flushall=0,
          redis_info_return={'lazyfree_pending_objects': 1},
      ),
  ])
  @mock.patch.object(redis.Redis, 'info', autospec=True)
  @mock.patch.object(redis.Redis, 'flushall', autospec=True)
  @mock.patch.object(
      redis.Redis,
      'set',
      autospec=True,
      side_effect=redis.exceptions.ResponseError(
          'command not allowed under OOM prevention'
      ),
  )
  def test_redis_set_oom_error(
      self,
      unused_mk_redis_set,
      mk_redis_flushall,
      mk_redis_info,
      time_last_flushed,
      redis_flushall,
      redis_info_return,
  ):
    mk_redis_info.return_value = redis_info_return
    redis_cache._write_redis_db_last_flushed(time_last_flushed)
    cache = redis_cache.RedisCache()
    self.assertFalse(cache.set('test', b'foo'))
    self.assertEqual(mk_redis_flushall.call_count, redis_flushall)
    if redis_flushall > 0:
      self.assertLess(time.time() - redis_cache._get_redis_db_last_flushed(), 2)

  @mock.patch.object(redis.Redis, 'flushall', autospec=True)
  @mock.patch.object(
      redis.Redis,
      'set',
      autospec=True,
      side_effect=redis.exceptions.ResponseError(''),
  )
  def test_redis_set_other_error_does_nothing(
      self,
      unused_mk_redis_set,
      mk_redis_flushall,
  ):
    redis_cache._write_redis_db_last_flushed(5)
    cache = redis_cache.RedisCache()
    self.assertFalse(cache.set('test', b'foo'))
    mk_redis_flushall.assert_not_called()
    self.assertEqual(redis_cache._get_redis_db_last_flushed(), 5)

  def test_init_fork_module_state(self) -> None:
    redis_cache._redis_host_instance_lock = 'mock'
    redis_cache._redis_host_instance = 'mock'
    redis_cache._init_fork_module_state()
    self.assertNotEqual(redis_cache._redis_host_instance_lock, 'mock')
    self.assertIsNotNone(redis_cache._redis_host_instance_lock)
    self.assertIsNone(redis_cache._redis_host_instance)


if __name__ == '__main__':
  absltest.main()
