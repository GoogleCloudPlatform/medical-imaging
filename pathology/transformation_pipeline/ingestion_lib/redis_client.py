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

from __future__ import annotations

import contextlib
import os
import threading
import time
from typing import Dict, Mapping, Optional

import redis

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import ingest_const


class RedisServerIPUndefinedError(Exception):
  """Redis server IP is undefined or empty."""


class CouldNotAcquireNonBlockingLockError(Exception):
  """Exception raised when nonblocking lock cannot be acquired."""

  def __init__(self, lock_name: str, log: Optional[Mapping[str, str]] = None):
    super().__init__(
        f'Could not acquire lock: {lock_name}.',
        ingest_const.ErrorMsgs.COULD_NOT_ACQUIRE_LOCK,
    )
    self._lock_name = lock_name
    self._log = log

  @property
  def log(self) -> Mapping[str, str]:
    return self._log if self._log is not None else {}

  @property
  def lock_name(self) -> str:
    return self._lock_name


class _AutoLockUnlocker(contextlib.AbstractContextManager):
  """Automatically unlocks redis lock when lock exits a context block."""

  def __init__(self, name: str, log: Optional[Mapping[str, str]] = None):
    super().__init__()
    self._lock_name = name
    self._start_time = time.time()
    self._unlock_log = dict(log) if log is not None else {}

  def __exit__(self, exc_type, exc_value, traceback):
    super().__exit__(exc_type, exc_value, traceback)
    r_client = RedisClient.get_singleton()
    if not r_client.has_redis_client():
      return
    r_client.release_lock(self._lock_name, ignore_redis_exception=True)
    self._unlock_log[ingest_const.LogKeywords.LOCK_HELD_SEC] = (
        time.time() - self._start_time
    )
    cloud_logging_client.info(
        f'Released transformation lock: {self._lock_name}', self._unlock_log
    )


def _extend_rlock(redis_lock: redis.lock.Lock, ttl: int) -> None:
  try:
    redis_lock.extend(ttl, replace_ttl=True)
  except redis.exceptions.LockError as exp:
    cloud_logging_client.error('Error occured extending redis lock TTL.', exp)


class RedisClient:
  """Wrapper for cloud memorystore redis operations."""

  _instance_creation_lock = threading.Lock()
  _instance: Optional[RedisClient] = None

  def __init__(self):
    if RedisClient._instance is not None:
      raise ValueError('Singleton already initialized.')
    self._lock = threading.Lock()
    self._redis_lock_dict: Dict[str, redis.lock.Lock] = {}
    self._redis_ip = ingest_flags.REDIS_SERVER_IP_FLG.value
    if self._redis_ip is None:
      self._client_instance = None
    else:
      self._redis_ip = self._redis_ip.strip()
      if not self._redis_ip:
        raise RedisServerIPUndefinedError()
      self._redis_port = ingest_flags.REDIS_SERVER_PORT_FLG.value
      self._client_instance = redis.Redis(
          host=self._redis_ip,
          port=self._redis_port,
          db=ingest_flags.REDIS_DB_FLG.value,
          username=ingest_flags.REDIS_USERNAME_FLG.value,
          password=ingest_flags.REDIS_AUTH_PASSWORD_FLG.value,
      )
    RedisClient._instance = self

  @classmethod
  def init_fork_module_state(cls) -> None:
    cls._instance_creation_lock = threading.Lock()
    cls._instance = None

  def has_redis_client(self) -> bool:
    return self._client_instance is not None

  @classmethod
  def get_singleton(cls) -> RedisClient:
    """Returns RedisClient instance."""
    if cls._instance is None:
      with cls._instance_creation_lock:
        if cls._instance is None:
          RedisClient()
    return cls._instance

  @property
  def redis_ip(self) -> str:
    return self._redis_ip

  @property
  def redis_port(self) -> int:
    return self._redis_port

  @property
  def client(self) -> redis.Redis:
    return self._client_instance

  def extend_lock_timeouts(self, amount: int):
    """Interface for thread to call to extend TTL on Redis keys.

    Args:
      amount: time in seconds to extend timeout.
    """
    with self._lock:
      if not self.has_redis_client() or not self._redis_lock_dict:
        return
      for redis_lock in self._redis_lock_dict.values():
        _extend_rlock(redis_lock, amount)

  def ping(self) -> bool:
    """Pings the redis server.

    Returns:
      True if success
    """
    return self._client_instance.ping()

  def acquire_non_blocking_lock(
      self,
      name: str,
      value: str,
      expiry_seconds: int,
      context_block: contextlib.ExitStack,
      unlock_log: Optional[Mapping[str, str]] = None,
  ):
    """Sets the value at key with an expiry time only if a key with name does not already exist or the current key value pair already exists.

    Args:
      name: Name of key
      value: Value to be stored for key
      expiry_seconds: Expiry time in seconds
      context_block: Context block to auto-unlock lock.
      unlock_log: Optional log to report when lock is released, only applies to
        newly acquired locks.

    Raises:
      CouldNotAcquireNonBlockingLockError: Lock cannot be acquired.
    """
    with self._lock:
      rlock = self._redis_lock_dict.get(name)
      if rlock is not None:
        new_lock = False
      else:
        # Disable thread local storage, lock will be extended in background
        # thread. Within process thread safty handled by this class.
        new_lock = True
        rlock = redis.lock.Lock(
            self._client_instance,
            name,
            timeout=expiry_seconds,
            thread_local=False,
        )
      acquired = rlock.acquire(blocking=False, token=value)
      if not acquired:
        raise CouldNotAcquireNonBlockingLockError(name, unlock_log)
      self._redis_lock_dict[name] = rlock
      if new_lock:
        context_block.enter_context(_AutoLockUnlocker(name, unlock_log))
      else:
        _extend_rlock(rlock, expiry_seconds)

  def is_lock_owned(self, name: str) -> bool:
    with self._lock:
      rlock = self._redis_lock_dict.get(name)
    if rlock is None:
      return False
    return rlock.owned()

  def release_lock(self, name: str, ignore_redis_exception: bool) -> None:
    """Releases lock associated with key."""
    with self._lock:
      rlock = self._redis_lock_dict.get(name)
      if rlock is not None:
        try:
          rlock.release()
        except (
            redis.exceptions.LockError,
            redis.exceptions.ConnectionError,
        ) as _:
          if not ignore_redis_exception:
            raise
        del self._redis_lock_dict[name]


def redis_client() -> RedisClient:
  return RedisClient.get_singleton()


os.register_at_fork(after_in_child=RedisClient.init_fork_module_state)
