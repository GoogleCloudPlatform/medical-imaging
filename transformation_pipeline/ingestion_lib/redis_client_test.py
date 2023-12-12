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
"""Tests for redis_client using local redis server.

Test verifies redis client using local redis instance and must be run on local
machine. Test takes about 12s to run.
To run test:
sudo apt-get install redis
sudo service redis start
blaze test
//transformation_pipeline/ingestion_lib:redis_client_test
--noforge
"""
import time

from absl.testing import absltest
from absl.testing import flagsaver
import redis

from transformation_pipeline.ingestion_lib import redis_client

REDIS_IP_ADDRESS = 'localhost'


class RedisClientTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    self._redis_instance = redis_client.redis_client(REDIS_IP_ADDRESS)
    self._redis_instance.flushdb()

  def tearDown(self):
    super().tearDown()
    self._redis_instance.flushdb()

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testPing(self):
    self.assertTrue(self._redis_instance.ping())

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testSet(self):
    self.assertTrue(self._redis_instance.set('hello', 'world'))
    self.assertEqual(self._redis_instance.get('hello'), 'world')
    self.assertTrue(self._redis_instance.set('hello', 'world2'))
    self.assertEqual(self._redis_instance.get('hello'), 'world2')

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testSetLock(self):
    self.assertTrue(self._redis_instance.acquire_lock('hello', 'world'))
    self.assertEqual(self._redis_instance.get('hello'), 'world')
    self.assertFalse(self._redis_instance.acquire_lock('hello', 'world2'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testSetIfUnset(self):
    self.assertTrue(self._redis_instance.set_if_unset('hello', 'world'))
    self.assertFalse(self._redis_instance.set_if_unset('hello', 'world'))
    self.assertFalse(self._redis_instance.set_if_unset('hello', 'world2'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testSetIfUnsetBlocking(self):
    self.assertTrue(
        self._redis_instance.set_if_unset('hello', 'world', expiry_seconds=2)
    )
    self.assertTrue(
        self._redis_instance.set_if_unset('hello', 'world', blocking=True)
    )

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testSetRecursiveLockNoTimeOut(self):
    self.assertTrue(self._redis_instance.acquire_lock('hello', 'world'))
    self.assertEqual(self._redis_instance.get('hello'), 'world')
    self.assertTrue(self._redis_instance.acquire_lock('hello', 'world'))
    self.assertEqual(self._redis_instance.get('hello'), 'world')
    self.assertFalse(self._redis_instance.delete('hello'))
    self.assertEqual(self._redis_instance.get('hello'), 'world')
    self.assertTrue(self._redis_instance.delete('hello'))
    self.assertFalse(self._redis_instance.exists('hello'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testSetRecursiveLockTimeOut(self):
    self.assertTrue(self._redis_instance.acquire_lock('hello', 'world', 5))
    self.assertEqual(self._redis_instance.get('hello'), 'world')
    self.assertTrue(self._redis_instance.acquire_lock('hello', 'world', 6))
    self.assertEqual(self._redis_instance.get('hello'), 'world')
    self.assertFalse(self._redis_instance.delete('hello'))
    self.assertEqual(self._redis_instance.get('hello'), 'world')
    self.assertTrue(self._redis_instance.delete('hello'))
    self.assertFalse(self._redis_instance.exists('hello'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testMixingLocks(self):
    self._redis_instance.acquire_lock('hello', 'world')
    with self.assertRaises(ValueError):
      self._redis_instance.set('hello', 'world2')
    self._redis_instance.set('hello2', 'world')
    with self.assertRaises(ValueError):
      self._redis_instance.acquire_lock('hello2', 'world2')

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testSetWithExpiry(self):
    self._redis_instance.set('hello', 'world', 2)
    self.assertEqual(self._redis_instance.get('hello'), 'world')
    time.sleep(2)
    self.assertIsNone(self._redis_instance.get('hello'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testSetLockWithExpiry(self):
    self._redis_instance.acquire_lock('hello', 'world', 2)
    self.assertEqual(self._redis_instance.get('hello'), 'world')
    time.sleep(2)
    self.assertIsNone(self._redis_instance.get('hello'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testSetLockDuplicateKeyFails(self):
    self.assertTrue(self._redis_instance.acquire_lock('hello', 'world'))
    self.assertFalse(
        self._redis_instance.acquire_lock('hello', 'again', blocking=False)
    )

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testSetDuplicateKeySucceeds(self):
    self.assertTrue(self._redis_instance.set('hello', 'world'))
    self.assertTrue(self._redis_instance.set('hello', 'again'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testDelete(self):
    self._redis_instance.set('hello', 'world')
    self.assertEqual(self._redis_instance.get('hello'), 'world')
    self._redis_instance.delete('hello')
    self.assertIsNone(self._redis_instance.get('hello'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testExists(self):
    self._redis_instance.set('hello', 'world')
    self.assertTrue(self._redis_instance.exists('hello'))
    self.assertFalse(self._redis_instance.exists('test'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testExtendTtl(self):
    self._redis_instance.set('hello', 'world', 2)
    self.assertEqual(self._redis_instance.get('hello'), 'world')
    self._redis_instance.extend_ttl('hello', 2)
    self.assertLessEqual(self._redis_instance.get_ttl('hello'), 4)

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testExtendTtlExpiration(self):
    self._redis_instance.set('hello', 'world', 2)
    self.assertEqual(self._redis_instance.get('hello'), 'world')
    self._redis_instance.extend_ttl('hello', 2)
    time.sleep(self._redis_instance.get_ttl('hello') + 1)
    self.assertFalse(self._redis_instance.exists('hello'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testLock(self):
    with self._redis_instance.lock('hello', 'world', 2):
      self.assertTrue(self._redis_instance.exists('hello'))
      with self.assertRaises(ValueError):
        self._redis_instance.set('hello', 'again')
    self.assertFalse(self._redis_instance.exists('hello'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testLockAutorelease(self):
    """Tests that lock will autorelease after the expiry time passes."""
    with self._redis_instance.lock('hello', 'world', 2):
      self.assertTrue(self._redis_instance.exists('hello'))
      self.assertFalse(self._redis_instance.acquire_lock('hello', 'again'))
      time.sleep(2)
      self.assertIsNone(self._redis_instance.get('hello'))
      self.assertFalse(self._redis_instance.exists('hello'))
    self.assertTrue(self._redis_instance.set('hello', 'again'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testNonBlockingRecursiveLock(self):
    """Tests that if lock can be called with same key-value."""
    with self._redis_instance.lock('hello', 'world'):
      with self._redis_instance.non_blocking_lock('hello', 'world'):
        self.assertTrue(self._redis_instance.exists('hello'))
      self.assertTrue(self._redis_instance.exists('hello'))
    self.assertFalse(self._redis_instance.exists('hello'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testBlockingRecursiveLock(self):
    """Tests that if lock can be called with same key-value."""
    with self._redis_instance.lock('hello', 'world'):
      with self._redis_instance.lock('hello', 'world'):
        self.assertTrue(self._redis_instance.exists('hello'))
      self.assertTrue(self._redis_instance.exists('hello'))
    self.assertFalse(self._redis_instance.exists('hello'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testNonBlockingLockThrows(self):
    """Tests that if lock is called with 'blocking' a CouldNotAcquireNonBlockingLock exception is raised."""
    with self._redis_instance.lock('hello', 'world'):
      self.assertTrue(self._redis_instance.exists('hello'))
      with self.assertRaises(redis_client.CouldNotAcquireNonBlockingLockError):
        with self._redis_instance.non_blocking_lock('hello', 'again'):
          pass
      self.assertTrue(self._redis_instance.exists('hello'))
    self.assertFalse(self._redis_instance.exists('hello'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testLockWaits(self):
    """Tests that if lock is called with 'blocking' set to True and lock cannot be acquired, process will wait for lock to be acquired or time out.

    In this test case we wait for the lock to expire (5 sec)
    """
    self._redis_instance.acquire_lock('hello', 'world', 5)
    start_time = time.time()
    self.assertEqual(self._redis_instance.get('hello'), 'world')
    with self._redis_instance.lock('hello', 'again', 5):
      end_time = time.time()
      # Verify process waited to create lock
      self.assertEqual(int(end_time - start_time), 5)
      self.assertEqual(self._redis_instance.get('hello'), 'again')
    self.assertFalse(self._redis_instance.exists('hello'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def testFlushdb(self):
    self._redis_instance.set('hello', 'world')
    self._redis_instance.set('test', 'value')
    self.assertEqual(self._redis_instance.get('hello'), 'world')
    self.assertEqual(self._redis_instance.get('test'), 'value')
    self._redis_instance.flushdb()

    self.assertIsNone(self._redis_instance.get('hello'))
    self.assertIsNone(self._redis_instance.get('test'))

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def test_get_keys(self):
    self._redis_instance.set('hello', 'world')
    self._redis_instance.set('test', 'value')
    keys = sorted(self._redis_instance.get_keys())
    self.assertListEqual(keys, ['hello', 'test'])

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def test_has_expected_value(self):
    self._redis_instance.set('hello', 'world')
    self.assertTrue(self._redis_instance.has_expected_value('hello'))
    self.assertEqual(self._redis_instance.get('hello'), 'world')

    self._redis_instance.delete('hello')

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def test_time_extension(self):
    self._redis_instance.set('hello', 'world', 2)
    self._redis_instance.set('test', 'value', 2)
    self._redis_instance.set('for', 'ever')
    self._redis_instance.extend_lock_timeouts(5)
    time.sleep(5)
    self.assertEqual(self._redis_instance.get('hello'), 'world')
    self.assertEqual(self._redis_instance.get('test'), 'value')
    time.sleep(5)
    self.assertEqual(self._redis_instance.get('for'), 'ever')

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def test_get_key_ttl(self):
    self._redis_instance.set('hello', 'world', 5)
    self.assertLessEqual(self._redis_instance.get_ttl('hello'), 5)
    self._redis_instance.set('hello', 'world')
    self.assertEqual(self._redis_instance.get_ttl('hello'), -1)

  #
  # Start tests of transaction protected race conditions.
  #

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def test_redis_watch_value_set(self):
    client = self._redis_instance.client
    with self.assertRaises(redis.WatchError):
      with client.pipeline() as pipe:
        pipe.watch('foo')
        client.set('foo', 'bar', 1)
        key_ttl = pipe.ttl('foo')
        pipe.multi()
        pipe.expire('foo', key_ttl + 4)
        pipe.execute()

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def test_redis_watch_value_changed(self):
    client = self._redis_instance.client
    client.set('foo', 'bar')
    with self.assertRaises(redis.WatchError):
      with client.pipeline() as pipe:
        pipe.watch('foo')
        client.set('foo', 'man', 1)
        key_ttl = pipe.ttl('foo')
        pipe.multi()
        pipe.expire('foo', key_ttl + 4)
        pipe.execute()

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def test_redis_watch_timeout_changed(self):
    client = self._redis_instance.client
    client.set('foo', 'bar', 5)
    with self.assertRaises(redis.WatchError):
      with client.pipeline() as pipe:
        pipe.watch('foo')
        client.expire('foo', 20)
        key_ttl = pipe.ttl('foo')
        pipe.multi()
        pipe.expire('foo', key_ttl + 4)
        pipe.execute()

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def test_redis_watch_succeed(self):
    client = self._redis_instance.client
    client.set('foo', 'bar', 5)
    with client.pipeline() as pipe:
      pipe.watch('foo')
      key_ttl = pipe.ttl('foo')
      self.assertLessEqual(key_ttl, 5)
      pipe.multi()
      pipe.expire('foo', key_ttl + 4)
      pipe.execute()
    self.assertLessEqual(client.ttl('foo'), 9)

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def test_redis_watch_persist(self):
    client = self._redis_instance.client
    client.set('foo', 'bar', 5)
    with client.pipeline() as pipe:
      pipe.watch('foo')
      key_ttl = pipe.ttl('foo')
      self.assertLessEqual(key_ttl, 5)
      pipe.multi()
      pipe.persist('foo')
      pipe.execute()
    self.assertEqual(client.ttl('foo'), -1)

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def test_redis_watch_donothing_succeed(self):
    client = self._redis_instance.client
    client.set('foo', 'bar', 5)
    with client.pipeline() as pipe:
      pipe.watch('foo')
      key_ttl = pipe.ttl('foo')
      pipe.multi()
      pipe.execute()
    self.assertLessEqual(key_ttl, 5)

  @flagsaver.flagsaver(pod_hostname='test_pod')
  @flagsaver.flagsaver(pod_uid='123')
  def test_redis_watch_donothing_fail(self):
    client = self._redis_instance.client
    client.set('foo', 'bar', 5)
    with self.assertRaises(redis.WatchError):
      with client.pipeline() as pipe:
        pipe.watch('foo')
        client.expire('foo', 20)
        pipe.multi()
        pipe.execute()


if __name__ == '__main__':
  absltest.main()
