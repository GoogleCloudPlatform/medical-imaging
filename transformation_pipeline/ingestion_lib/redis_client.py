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

import os
import threading
from typing import Dict, Optional

import redis

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags


class RedisServerIPUndefinedError(Exception):
  """Redis server IP is undefined or empty."""


def _extend_rlock(redis_lock: redis.lock.Lock, ttl: int) -> None:
  try:
    redis_lock.extend(ttl, replace_ttl=True)
  except redis.exceptions.LockError as exp:
    cloud_logging_client.logger().error(
        'Error occured extending redis lock TTL.', exp
    )


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
          host=self._redis_ip, port=self._redis_port
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
  ) -> bool:
    """Sets the value at key with an expiry time only if a key with name does not already exist or the current key value pair already exists.

    Args:
      name: Name of key
      value: Value to be stored for key
      expiry_seconds: Expiry time in seconds

    Returns:
      True if key was created or ref_counted
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
        return False
      self._redis_lock_dict[name] = rlock
      if not new_lock:
        _extend_rlock(rlock, expiry_seconds)
      return True

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
