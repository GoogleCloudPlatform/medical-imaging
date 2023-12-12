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
"""Wrapper for cloud memorystore redis operations."""
import contextlib
import dataclasses
import os
import threading
import time
from typing import Dict, Generator, List, Optional

import redis

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import ingest_const


@dataclasses.dataclass
class LocalKeyRef:
  """Stores state of GKE keys set locally. Enables validation."""

  value: str
  ref_count: int
  lock: bool


class CouldNotAcquireNonBlockingLockError(Exception):
  """Exception raised from lock context manager if called in non-blocking context and cannot acquire lock."""

  def __init__(self, name: str, value: str):
    super().__init__('Could not acquire lock')
    self.name = name
    self.value = value


class RedisClient:
  """Wrapper for cloud memorystore redis operations."""

  _instance = None

  def __init__(self, host_ip: Optional[str] = None):
    if RedisClient._instance is not None:
      raise ValueError('Singleton already initialized.')
    self._host_ip = os.environ.get('REDISHOST', host_ip)
    if self._host_ip is not None:
      self._host_ip = self._host_ip.strip()
    redis_port = int(os.environ.get('REDISPORT', 6379))
    self._client_instance = None
    if self._host_ip:
      self._client_instance = redis.StrictRedis(
          host=self._host_ip, port=redis_port
      )

    # protects _key_value_pairs from async access
    # internal copy of pods GKE redis state.
    # used to test if redis lock is held and ref count
    # to enable recursive locks
    self._key_value_pairs_lock = threading.RLock()
    self._key_value_pairs = {}
    RedisClient._instance = self

  def has_redis_client(self) -> bool:
    return self._client_instance is not None

  @classmethod
  def get_singleton(cls, host_ip: Optional[str] = None):
    """Returns RedisClient instance.

    Args:
      host_ip: ip address of redis server to connect to.
    """
    if RedisClient._instance is None:
      RedisClient(host_ip)
    if host_ip is not None:
      host_ip = host_ip.strip()
    if (
        host_ip is not None
        and host_ip
        and RedisClient._instance.host_ip != host_ip
    ):
      raise ValueError(
          (
              'Error redis Client already initialized to another host ip.'
              f' Singleton init to host: {RedisClient._instance.host_ip}'
              f' Requested host: {host_ip}'
          )
      )
    return RedisClient._instance

  @property
  def host_ip(self) -> str:
    return self._host_ip

  @property
  def client(self) -> redis.StrictRedis:
    return self._client_instance

  @property
  def rlock(self) -> threading.RLock:
    return self._key_value_pairs_lock

  def get_local_key_values(self) -> Dict[str, str]:
    ret = {}
    with self._key_value_pairs_lock:
      for key, val in self._key_value_pairs.items():
        ret[str(key)] = str(val.value)
    return ret

  def get_keys(self) -> List[str]:
    """Returns list of current redis keys."""
    with self._key_value_pairs_lock:
      return list(self._key_value_pairs)

  def _del_local_key_value_pair(self, name: str):
    try:
      del self._key_value_pairs[name]
    except KeyError:
      pass

  def extend_lock_timeouts(self, amount: int = ingest_const.MESSAGE_TTL_S):
    """Interface for thread to call to extend TTL on Redis keys.

    Args:
      amount: time in seconds to extend timeout.
    """
    with self._key_value_pairs_lock:
      if not self.has_redis_client() or not self._key_value_pairs:
        return

      remove_name = []
      for name in self._key_value_pairs:
        # only if extends TTL on key if key's value remains unchanged for
        # duration of watch.
        with self._client_instance.pipeline() as pipe:
          try:
            pipe.watch(name)
            if not self._has_expected_value(name, pipe=pipe):
              pipe.unwatch()
              remove_name.append(name)
            else:
              key_ttl = pipe.ttl(name)
              pipe.multi()
              if key_ttl != -1:
                pipe.expire(name, key_ttl + amount)
              pipe.execute()
              cloud_logging_client.logger().info(
                  'Extended redis key ttl.',
                  {
                      'key': name,
                      'current_value': str(self._key_value_pairs[name].value),
                      'ttl': str(key_ttl + amount),
                  },
              )
          except redis.WatchError:
            remove_name.append(name)
      for name in remove_name:
        cloud_logging_client.logger().warning(
            'Could not extend redis key ttl. Expected value changed.',
            {
                'key': name,
                'expected_value': str(self._key_value_pairs[name].value),
                'current_value': str(self.get(name)),
            },
        )
        self._del_local_key_value_pair(name)

  def ping(self) -> bool:
    """Pings the redis server.

    Returns:
      True if success
    """
    return self._client_instance.ping()

  def _set(
      self,
      name: str,
      value: str,
      expiry_seconds: Optional[int] = None,
      lock: bool = True,
      recursive_lock: bool = False,
  ) -> bool:
    """Sets the value at key with an expiry time.

    Args:
      name: Name of key
      value: Value to be stored for key
      expiry_seconds: Expiry time in seconds
      lock: Only set key if key doesn't already exist
      recursive_lock: True set supports recursive lock inside GKE process.

    Returns:
      True if key was created or ref_counted
    """
    with self._key_value_pairs_lock:
      # if key defined and defined as a lock
      if name in self._key_value_pairs and self.exists(name):
        if self._key_value_pairs[name].lock != lock:
          raise ValueError('Cannot mix locked and unlocked keys.')
      if self._client_instance.set(name, value, ex=expiry_seconds, nx=lock):
        self._key_value_pairs[name] = LocalKeyRef(value, 1, lock)
        msg = 'Set redis key'
        if lock:
          msg = f'{msg} lock'
        msg = f'{msg}.'
        cloud_logging_client.logger().info(
            msg,
            {
                'key': name,
                'expiry_seconds': str(expiry_seconds),
                'current_value': value,
            },
        )
        return True
      elif lock and recursive_lock:
        local_key_ref = self._key_value_pairs.get(name)
        if local_key_ref is not None:
          if local_key_ref.value == value:  # if local state = has set value
            with self._client_instance.pipeline() as pipe:
              try:
                pipe.watch(name)
                if not self._has_expected_value(
                    name, pipe=pipe
                ):  # if redis has
                  self._del_local_key_value_pair(name)
                  pipe.unwatch()
                else:
                  key_expiration = pipe.ttl(name)
                  pipe.multi()
                  if key_expiration != -1 and key_expiration < expiry_seconds:
                    pipe.expire(name, expiry_seconds)
                  elif key_expiration != -1 and expiry_seconds is None:
                    pipe.persist(name)
                  pipe.execute()
                  # increment counter after transaction to require transaction
                  # to succeed for counter to inc.
                  local_key_ref.ref_count += 1
                  cloud_logging_client.logger().info(
                      'Incremented redis key lock ref_count.',
                      {
                          'key': name,
                          'ref_count': str(local_key_ref.ref_count),
                          'current_value': value,
                      },
                  )
                  return True
              except redis.WatchError:
                pass
    # failures logged in calling methods
    return False

  def acquire_lock(
      self,
      name: str,
      value: str,
      expiry_seconds: Optional[int] = None,
      blocking: bool = False,
  ) -> bool:
    """Sets the value at key with an expiry time only if a key with name does not already exist or the current key value pair already exists.

    Args:
      name: Name of key
      value: Value to be stored for key
      expiry_seconds: Expiry time in seconds
      blocking: If true, thread blocks and waits till key can be acquired.

    Returns:
      True if key was created or ref_counted
    """
    while True:
      lock = self._set(
          name, value, expiry_seconds, lock=True, recursive_lock=True
      )
      if lock or not blocking:
        if not lock:
          cloud_logging_client.logger().info(
              'Could not acquire non-blocking redis key lock.',
              {
                  'key': name,
                  'current_value': str(self.get(name)),
                  'value_tried_to_set': value,
              },
          )
        return lock
      cloud_logging_client.logger().info(
          'Blocking for redis key acquire_lock.',
          {
              'key': name,
              'current_key_value': str(self.get(name)),
              'value_trying_to_set': value,
          },
      )
      time.sleep(0.1)

  def set_if_unset(
      self,
      name: str,
      value: str,
      expiry_seconds: Optional[int] = None,
      blocking: bool = False,
  ) -> bool:
    """Sets the value at key with an expiry time only if a key with name does not already exist.

    Function will return

    Args:
      name: Name of key
      value: Value to be stored for key
      expiry_seconds: Expiry time in seconds
      blocking: If true, thread blocks and waits till key can be acquired.

    Returns:
      True if key was created
    """
    while True:
      lock = self._set(
          name, value, expiry_seconds, lock=True, recursive_lock=False
      )
      if lock or not blocking:
        if not lock:
          cloud_logging_client.logger().info(
              'Could not set_if_unset redis key value.',
              {
                  'key': name,
                  'current_value': str(self.get(name)),
                  'value_tried_to_set': value,
              },
          )
        return lock
      cloud_logging_client.logger().info(
          'Blocking for redis key set_if_unset.',
          {
              'key': name,
              'current_key_value': str(self.get(name)),
              'value_trying_to_set': value,
          },
      )
      time.sleep(0.1)

  def set(
      self, name: str, value: str, expiry_seconds: Optional[int] = None
  ) -> bool:
    """Sets the value at key with an expiry time.

    Will overwrite existing keys.

    Args:
      name: Name of key
      value: Value to be stored for key
      expiry_seconds: Expiry time in seconds

    Returns:
      True if key was created
    """
    result = self._set(
        name, value, expiry_seconds, lock=False, recursive_lock=False
    )
    if not result:
      cloud_logging_client.logger().warning(
          'Could not set redis key value.',
          {
              'key': name,
              'current_value': str(self.get(name)),
              'value_tried_to_set': value,
          },
      )
    return result

  def get(self, name: str) -> Optional[str]:
    """Gets the value at key name.

    Args:
      name: Name of key

    Returns:
      value if key exists or None
    """
    if self._client_instance.exists(name):
      return self._client_instance.get(name).decode()
    else:
      return None

  def _has_expected_value(
      self, name: str, pipe: Optional[redis.client.Pipeline]
  ) -> bool:
    """Returns true if redis key value matches local copy.

       Purpose: tests if lock is still held. If lock expired then
       another pods instance could have taken it.

       Must be called  inside a _key_value_pairs_lock.

    Args:
      name: redis key to test
      pipe: redis pipeline to execute key get from (None = Non-pipeline exec)

    Returns:
       True if values match.
    """
    try:
      if pipe is None:
        redis_key_value = self.get(name)
      else:
        redis_key_value = pipe.get(name)
        if redis_key_value is not None:
          redis_key_value = redis_key_value.decode()
      return redis_key_value == self._key_value_pairs[name].value
    except KeyError:
      return False

  def has_expected_value(self, name: str) -> bool:
    with self._key_value_pairs_lock:
      return self._has_expected_value(name, pipe=None)

  def get_ttl(self, name: str) -> int:
    """Returns ttl for a key (sec) or -1 if inf.

    Args:
      name: key name

    Returns:
      Returns ttl for a key (sec) or -1 if inf.
    """
    return self._client_instance.ttl(name)

  def delete(self, name: str, force: bool = False) -> bool:
    """Deletes the key name.

    Args:
      name: Name of key
      force: bool (true deletes key ignoring of ref count.)

    Returns:
      True if key was deleted from redis.
    """
    with self._key_value_pairs_lock:
      try:
        # Checks that name value matches expectation.
        ref_count_value = self._key_value_pairs[name]
        ref_count_value.ref_count -= 1
        if ref_count_value.ref_count <= 0 or force:
          with self._client_instance.pipeline() as pipe:
            try:
              # makes sure key value pair cannot change between test and delete.
              pipe.watch(name)
              if not self._has_expected_value(name, pipe=pipe):
                pipe.unwatch()
              else:
                pipe.multi()
                pipe.delete(name)
                pipe.execute()
                cloud_logging_client.logger().info(
                    'Deleted redis key.',
                    {
                        'key': name,
                        'ref_count': ref_count_value.ref_count,
                        'current_value': str(self._key_value_pairs[name].value),
                    },
                )
                self._del_local_key_value_pair(name)
                return True
            except redis.WatchError:
              pass
            cloud_logging_client.logger().warning(
                'Could not delete redis key. Expected value changed.',
                {
                    'key': name,
                    'ref_count': ref_count_value.ref_count,
                    'current_value': str(self.get(name)),
                    'expected_value': str(self._key_value_pairs[name].value),
                },
            )
          # regardless of external state delete internal value store
          # value is not needed.
          self._del_local_key_value_pair(name)
        else:
          cloud_logging_client.logger().info(
              'Local redis key ref_count decremented.',
              {
                  'key': name,
                  'ref_count': str(ref_count_value.ref_count),
                  'current_value': str(self._key_value_pairs[name].value),
              },
          )
      except KeyError as exp:
        cloud_logging_client.logger().error(
            'Expected local redis key not found.',
            {
                'key': name,
            },
            exp,
        )
    return False

  def exists(self, name: str) -> bool:
    """Checks if key name exists.

    Args:
     name: Name of key

    Returns:
     True if key exists
    """
    return bool(self._client_instance.exists(name) == 1)

  def extend_ttl(
      self, name: str, amount: int = ingest_const.MESSAGE_TTL_S
  ) -> bool:
    """Extends the time to live of the key name.

    Args:
     name: Name of key
     amount: Time in seconds to extend by

    Returns:
     True if ttl extended.
    """
    with self._key_value_pairs_lock:
      if name in self._key_value_pairs:
        with self._client_instance.pipeline() as pipe:
          try:
            pipe.watch(name)
            if not self._has_expected_value(name, pipe=pipe):
              pipe.unwatch()
            else:
              key_ttl = pipe.ttl(name)
              pipe.multi()
              if key_ttl != -1:
                pipe.expire(name, key_ttl + amount)
              pipe.execute()
              cloud_logging_client.logger().info(
                  'Extended redis key ttl.',
                  {
                      'key': name,
                      'current_value': str(self._key_value_pairs[name].value),
                      'ttl': str(key_ttl + amount),
                  },
              )
              return True
          except redis.WatchError:
            pass
          self._del_local_key_value_pair(name)
          cloud_logging_client.logger().warning(
              'Could not extend redis key ttl. Expected value changed.',
              {
                  'key': name,
                  'current_value': str(self.get(name)),
                  'expected_value': str(self._key_value_pairs[name].value),
              },
          )
    return False

  def flushdb(self):
    """Flushes database of all keys.

    Returns:
      None
    """
    with self._key_value_pairs_lock:
      self._key_value_pairs = {}
      return self._client_instance.flushdb()

  @contextlib.contextmanager
  def lock(
      self,
      name: str,
      value: str,
      expiry_seconds: Optional[int] = None,
  ) -> Generator[bool, None, None]:
    """Attempts to acquire a lock for the given key, value, expiry.

    If the lock cannot be acquired it will wait till a
    lock can be acquired and check in 100ms increments.

    Pseudeo code examples:


        with redis.lock('key', 'value'):
          # print('always eventually acquires lock')

    Args:
     name: Name of key
     value: Value to be stored for key
     expiry_seconds: Expiry time in seconds

    Yields:
      True : Lock acquired
    """
    lock = self.acquire_lock(name, value, expiry_seconds, blocking=True)
    try:
      yield lock
    finally:
      self.delete(name)

  @contextlib.contextmanager
  def non_blocking_lock(
      self,
      name: str,
      value: str,
      expiry_seconds: Optional[int] = None,
  ) -> Generator[bool, None, None]:
    """Attempts to acquire a lock for the given key, value, expiry.

    If the lock cannot be acquired
    an exception CouldNotAcquireNonBlockingLock is raised.

    Pseudeo code examples:

    try:
       with redis.non_blocking_lock('key', 'value') :
         # print('acquired lock')
    except CouldNotAcquireNonBlockingLock:
      # print('could not acquire lock')

    Args:
     name: Name of key
     value: Value to be stored for key
     expiry_seconds: Expiry time in seconds

    Yields:
      True: lock taken

    Raises:
      CouldNotAcquireNonBlockingLockError: raised if cannot acquire lock and not
      blocking.
    """
    lock = self.acquire_lock(name, value, expiry_seconds, blocking=False)
    try:
      if not lock:
        raise CouldNotAcquireNonBlockingLockError(name, value)
      yield lock
    finally:
      # only release the lock if one was created
      if lock:
        self.delete(name)


def redis_client(host_ip: Optional[str] = None) -> RedisClient:
  return RedisClient.get_singleton(host_ip)
