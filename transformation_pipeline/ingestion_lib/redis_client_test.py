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
"""Tests for redis_client."""
import contextlib
import time
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import redis

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import mock_redis_client
from transformation_pipeline.ingestion_lib import redis_client

_REDIS_MOCK_IP = '1.3.4.5'
_MOCK_LOCK_NAME = 'mock_lock'
_MOCK_LOCK2_NAME = 'mock_lock2'
_MOCK_TOKEN_VALUE = 'token'
_MOCK_TOKEN2_VALUE = 'token2'


class RedisClientTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(mock_redis_client.MockRedisClient(_REDIS_MOCK_IP))
    self._context_block = self.enter_context(contextlib.ExitStack())

  def test_redis_client_singleton(self):
    self.assertIs(redis_client.redis_client(), redis_client.redis_client())

  def test_redis_client_class_twice_raises(self):
    redis_client.redis_client()
    with self.assertRaises(ValueError):
      redis_client.RedisClient()

  def test_fork_init(self):
    redis_client.RedisClient._instance = 'mock'
    redis_client.RedisClient._instance_creation_lock = 'mock'
    redis_client.RedisClient.init_fork_module_state()
    self.assertIsNone(redis_client.RedisClient._instance)
    self.assertNotIsInstance(
        redis_client.RedisClient._instance_creation_lock, str
    )

  def test_ping(self):
    self.assertTrue(redis_client.redis_client().ping())

  def test_redis_ip(self):
    self.assertEqual(redis_client.redis_client().redis_ip, _REDIS_MOCK_IP)

  def test_client(self):
    self.assertIsNotNone(redis_client.redis_client().client)

  def test_acquire_redis_lock(self):
    cl = redis_client.redis_client()
    self.assertIsNone(
        cl.acquire_non_blocking_lock(
            _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 10, self._context_block
        )
    )

  def test_redis_lock_owned_true(self):
    cl = redis_client.redis_client()
    cl.acquire_non_blocking_lock(
        _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 10, self._context_block
    )
    self.assertTrue(cl.is_lock_owned(_MOCK_LOCK_NAME))

  def test_redis_lock_owned_false(self):
    cl = redis_client.redis_client()
    cl.acquire_non_blocking_lock(
        _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 10, self._context_block
    )
    self.assertFalse(cl.is_lock_owned(_MOCK_LOCK2_NAME))

  def test_redis_lock_expire_owned_false(self):
    cl = redis_client.redis_client()
    cl.acquire_non_blocking_lock(
        _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 0.1, self._context_block
    )  # pytype: disable=wrong-arg-types
    time.sleep(0.3)
    self.assertFalse(cl.is_lock_owned(_MOCK_LOCK_NAME))

  def test_redis_lock_release_not_owned(self):
    cl = redis_client.redis_client()
    cl.acquire_non_blocking_lock(
        _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 10, self._context_block
    )
    cl.release_lock(_MOCK_LOCK_NAME, ignore_redis_exception=False)
    self.assertFalse(cl.is_lock_owned(_MOCK_LOCK_NAME))

  def test_expired_redis_lock_release_raises(self):
    cl = redis_client.redis_client()
    cl.acquire_non_blocking_lock(
        _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 0.1, self._context_block
    )  # pytype: disable=wrong-arg-types
    time.sleep(0.3)
    with self.assertRaises(redis.exceptions.LockError):
      cl.release_lock(_MOCK_LOCK_NAME, ignore_redis_exception=False)

  def test_reacquireing_overwrites_ttl(self):
    cl = redis_client.redis_client()
    cl.acquire_non_blocking_lock(
        _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 0.1, self._context_block
    )  # pytype: disable=wrong-arg-types
    cl.acquire_non_blocking_lock(
        _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 10, self._context_block
    )
    time.sleep(0.3)
    self.assertTrue(cl.is_lock_owned(_MOCK_LOCK_NAME))

  def test_expired_redis_lock_release_ignore_exception_does_not_raise(self):
    cl = redis_client.redis_client()
    cl.acquire_non_blocking_lock(
        _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 0.1, self._context_block
    )  # pytype: disable=wrong-arg-types
    time.sleep(0.3)
    cl.release_lock(_MOCK_LOCK_NAME, ignore_redis_exception=True)

  def test_undefined_redis_server_ip_raises(self):
    with self.assertRaises(redis_client.RedisServerIPUndefinedError):
      with flagsaver.flagsaver(redis_server_ip=''):
        redis_client.redis_client()

  def test_init_redis_server_disables_locks(self):
    with flagsaver.flagsaver(redis_server_ip=None):
      self.assertFalse(redis_client.redis_client().has_redis_client())

  def test_cannot_acquire_lock_taken(self):
    cl = redis_client.redis_client()
    cl.acquire_non_blocking_lock(
        _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 10, self._context_block
    )
    with self.assertRaises(redis_client.CouldNotAcquireNonBlockingLockError):
      cl.acquire_non_blocking_lock(
          _MOCK_LOCK_NAME, _MOCK_TOKEN2_VALUE, 10, self._context_block
      )

  def test_extend_all_locks(self):
    cl = redis_client.redis_client()
    cl.acquire_non_blocking_lock(
        _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 0.5, self._context_block
    )  # pytype: disable=wrong-arg-types
    cl.extend_lock_timeouts(10)
    time.sleep(0.7)
    self.assertTrue(cl.is_lock_owned(_MOCK_LOCK_NAME))

  def test_extend_all_locks_still_timeout(self):
    cl = redis_client.redis_client()
    cl.acquire_non_blocking_lock(
        _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 0.5, self._context_block
    )  # pytype: disable=wrong-arg-types
    cl.extend_lock_timeouts(0.1)  # pytype: disable=wrong-arg-types
    time.sleep(2)
    self.assertFalse(cl.is_lock_owned(_MOCK_LOCK_NAME))

  def test_redis_lost_connection_acquire_lock_raises(self):
    cl = redis_client.redis_client()
    cl.client.redis_server_connected = False
    with self.assertRaises(redis.exceptions.ConnectionError):
      cl.acquire_non_blocking_lock(
          _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 10, self._context_block
      )

  def test_redis_lost_connection_owner_lock_raises(self):
    cl = redis_client.redis_client()
    cl.acquire_non_blocking_lock(
        _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 10, self._context_block
    )
    cl.client.redis_server_connected = False
    with self.assertRaises(redis.exceptions.ConnectionError):
      cl.is_lock_owned(_MOCK_LOCK_NAME)

  def test_redis_lost_connection_release_lock_raises(self):
    cl = redis_client.redis_client()
    cl.acquire_non_blocking_lock(
        _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 10, self._context_block
    )
    cl.client.redis_server_connected = False
    with self.assertRaises(redis.exceptions.ConnectionError):
      cl.release_lock(_MOCK_LOCK_NAME, False)

  def test_redis_lost_connection_release_lock_eat_exceptions_not_raise(self):
    cl = redis_client.redis_client()
    cl.acquire_non_blocking_lock(
        _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 10, self._context_block
    )
    cl.client.redis_server_connected = False
    self.assertIsNone(cl.release_lock(_MOCK_LOCK_NAME, True))

  def test_redis_lost_connection_extend_locks_raise(self):
    cl = redis_client.redis_client()
    cl.acquire_non_blocking_lock(
        _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 10, self._context_block
    )
    cl.client.redis_server_connected = False
    with self.assertRaises(redis.exceptions.ConnectionError):
      cl.extend_lock_timeouts(10)

  def test_redis_acquire_lock_empty_token_raise(self):
    cl = redis_client.redis_client()
    with self.assertRaises(ValueError):
      cl.acquire_non_blocking_lock(_MOCK_LOCK_NAME, '', 10, self._context_block)

  def test_redis_extend_locks_if_timeout_less_than_zero_raise(self):
    cl = redis_client.redis_client()
    cl.acquire_non_blocking_lock(
        _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 10, self._context_block
    )
    with self.assertRaises(ValueError):
      cl.extend_lock_timeouts(-100)

  def test_redis_closes_exiting_context(self):
    cl = redis_client.redis_client()
    with contextlib.ExitStack() as context_block:
      cl.acquire_non_blocking_lock(
          _MOCK_LOCK_NAME, _MOCK_TOKEN_VALUE, 10, context_block
      )
      self.assertTrue(cl.is_lock_owned(_MOCK_LOCK_NAME))
    self.assertFalse(cl.is_lock_owned(_MOCK_LOCK_NAME))

  @parameterized.named_parameters([
      dict(
          testcase_name='invalid_lock_name',
          lock_name='',
          timeout=10,
          thread_local_storage=False,
      ),
      dict(
          testcase_name='invalid_timeout',
          lock_name='foo',
          timeout=-10,
          thread_local_storage=False,
      ),
      dict(
          testcase_name='invalid_thread_local_storage',
          lock_name='foo',
          timeout=10,
          thread_local_storage=True,
      ),
  ])
  def test_mock_redis_lock_constructor_raises(
      self, lock_name, timeout, thread_local_storage
  ):
    mk = mock_redis_client._MockRedis('1.2.3', 10)
    with self.assertRaises(ValueError):
      mock_redis_client._MockRedisLock(
          mk, lock_name, timeout, thread_local_storage
      )

  def test_mock_redis_lock_acquire_blocking_raises(self):
    mk = mock_redis_client._MockRedis('1.2.3', 10)
    lock = mock_redis_client._MockRedisLock(mk, 'foo', 10, False)
    with self.assertRaises(ValueError):
      lock.acquire(True, 'foo')

  def test_mock_redis_lock_extend_non_acquired_lock_raises(self):
    mk = mock_redis_client._MockRedis('1.2.3', 10)
    lock = mock_redis_client._MockRedisLock(mk, 'foo', 10, False)
    with self.assertRaises(ValueError):
      lock.extend(10, True)

  def test_mock_redis_lock_extend_existing_ttl_on_acquired_lock_raises(self):
    mk = mock_redis_client._MockRedis('1.2.3', 10)
    lock = mock_redis_client._MockRedisLock(mk, 'foo', 10, False)
    lock.acquire(False, 'foo')
    with self.assertRaises(ValueError):
      lock.extend(10, False)

  def test_redis_port_default(self):
    cl = redis_client.redis_client()
    self.assertEqual(cl.redis_port, 6379)

  @flagsaver.flagsaver(redis_server_port=7169)
  def test_redis_port_flag(self):
    cl = redis_client.redis_client()
    self.assertEqual(cl.redis_port, 7169)

  @mock.patch.object(
      redis_client.RedisClient,
      'get_singleton',
      autospec=True,
      return_value=mock.create_autospec(
          redis_client.RedisClient, instance=True
      ),
  )
  @mock.patch.object(cloud_logging_client, 'info', autospec=True)
  def test_auto_unlocker_log(self, mock_log, unused_mock):
    with redis_client._AutoLockUnlocker('test'):
      time.sleep(1)
    self.assertGreater(mock_log.call_args.args[1]['lock_held_sec'], 1)


if __name__ == '__main__':
  absltest.main()
