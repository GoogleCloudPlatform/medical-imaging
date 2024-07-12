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
"""Mock Redis Client."""
import contextlib
import dataclasses
import threading
import time
from typing import Dict, Optional
from unittest import mock
import uuid

from absl.testing import flagsaver
import redis

from transformation_pipeline.ingestion_lib import redis_client


@dataclasses.dataclass
class _MockRedisLockState:
  """Mock Redis Lock State."""

  token: str = ''
  owned: bool = False
  expire_time: float = 0.0


class _MockRedis:
  """Mock Redis Client."""

  def __init__(
      self,
      host: str,
      port: int,
      db: int = 0,
      username: Optional[str] = None,
      password: Optional[str] = None,
  ):
    del host, port, db, username, password
    self._connected = True
    self._redis_locks: Dict[str, _MockRedisLockState] = {}
    self.lock = threading.Lock()

  def ping(self) -> bool:
    return self.redis_server_connected

  def clean_expired_locks(self) -> None:
    """Removes expired locks."""
    current_time = time.time()
    expired_locks = [
        name
        for name, state in self._redis_locks.items()
        if state.expire_time < current_time
    ]
    for name in expired_locks:
      del self._redis_locks[name]

  @property
  def redis_server_connected(self) -> bool:
    return self._connected

  @redis_server_connected.setter
  def redis_server_connected(self, val: bool) -> None:
    with self.lock:
      self._connected = val

  def get_lock(self, name: str) -> Optional[_MockRedisLockState]:
    return self._redis_locks.get(name)

  def set_lock(self, name: str, state: _MockRedisLockState) -> None:
    self._redis_locks[name] = state

  def get_lock_expire_time(self, name: str) -> float:
    return self._redis_locks[name].expire_time


class _MockRedisLock:
  """Mock Redis Lock."""

  def __init__(
      self, instance: _MockRedis, name: str, timeout: int, thread_local: bool
  ):
    if not name:
      raise ValueError('Undefined name')
    if timeout <= 0:
      raise ValueError('Invalid timeout')
    if thread_local:
      raise ValueError(
          'Redis instance running with thread local storage not supported by'
          ' mock.'
      )
    self._name = name
    self._token = str(uuid.uuid4())
    self._redis_instance = instance
    self._ttl = timeout
    with self._redis_instance.lock:
      self._redis_instance.clean_expired_locks()

  def extend(self, additional_time: int, replace_ttl: bool = False) -> None:
    """Extends the Redis Lock TTL."""
    with self._redis_instance.lock:
      if not self._redis_instance.redis_server_connected:
        raise redis.exceptions.ConnectionError()
      if additional_time < 0:
        raise ValueError('Invalid lock extend amount.')
      self._redis_instance.clean_expired_locks()
      lock_state = self._redis_instance.get_lock(self._name)
      if (
          lock_state is not None
          and lock_state.owned
          and lock_state.token == self._token
      ):
        if replace_ttl:
          lock_state.expire_time = time.time() + additional_time
          return
        raise ValueError(
            'Cannot extend existing TTL is not supported by the Redis mock'
        )
      raise ValueError('Cannot extend lock is not owned.', lock_state)

  def acquire(self, blocking: bool, token: str) -> bool:
    """Returns True if Redis Lock is acquired."""
    with self._redis_instance.lock:
      if not self._redis_instance.redis_server_connected:
        raise redis.exceptions.ConnectionError()
      if blocking:
        raise ValueError('Acquired blocking lock not supported by mock.')
      if not token:
        raise ValueError('Undefined token.')
      self._redis_instance.clean_expired_locks()
      lock_state = self._redis_instance.get_lock(self._name)
      if (
          lock_state is None
          or not lock_state.owned
          or lock_state.token == token
      ):
        self._token = token
        self._redis_instance.set_lock(
            self._name,
            _MockRedisLockState(token, True, self._ttl + time.time()),
        )
        return True
      return False

  def release(self) -> None:
    """Releases the Redis Lock."""
    with self._redis_instance.lock:
      if not self._redis_instance.redis_server_connected:
        raise redis.exceptions.ConnectionError()
      self._redis_instance.clean_expired_locks()
      lock_state = self._redis_instance.get_lock(self._name)
      if (
          lock_state is not None
          and lock_state.owned
          and lock_state.token == self._token
      ):
        lock_state.owned = False
        return
      raise redis.exceptions.LockError('Cannot release an unlocked lock')

  def owned(self) -> bool:
    """Returns True if Redis Lock is ownend."""
    with self._redis_instance.lock:
      if not self._redis_instance.redis_server_connected:
        raise redis.exceptions.ConnectionError()
      self._redis_instance.clean_expired_locks()
      lock_state = self._redis_instance.get_lock(self._name)
      return bool(
          lock_state is not None
          and lock_state.owned
          and lock_state.token == self._token
      )


class MockRedisClient(contextlib.ExitStack):
  """Creates Context manged block to init Redis Client Mock."""

  def __init__(
      self, redis_server_ip: Optional[str] = None, pod_uid: str = 'MOCK_POD_UID'
  ):
    super().__init__()
    self.enter_context(
        mock.patch('redis.Redis', autospec=True, side_effect=_MockRedis)
    )
    self.enter_context(
        mock.patch('redis.lock.Lock', autospec=True, side_effect=_MockRedisLock)
    )
    self.enter_context(
        flagsaver.flagsaver(
            redis_server_ip=redis_server_ip, transform_pod_uid=pod_uid
        )
    )
    redis_client.RedisClient.init_fork_module_state()
