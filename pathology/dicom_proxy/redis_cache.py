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
"""Interface to communicate with local in memory Redis cache."""
import dataclasses
import os
import subprocess
import tempfile
import threading
import time
from typing import Any, Mapping, Optional, Union

import google.cloud.storage
import psutil
import redis

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import dicom_proxy_flags
from pathology.shared_libs.logging_lib import cloud_logging_client


_REDIS_NONE_VALUE = b'NONE'
_VALUE_REDIS_CACHE_SIZE_LIMIT_LIMIT = 512000000

_SET_REDIS_CACHE_MAXMEMORY_CONFIG = 'Set Redis cache maxmemory config'
_REDIS_CACHE_MAXMEMORY_GREATER_THAN_MEM_AVAIL = (
    'REDIS_CACHE_MAXMEMORY set to value > available memory.'
)
_ERROR_SETTING_REDIS_CACHE_MAXMEMORY = 'Error setting Redis cache maxmemory.'

_SET_MAXMEMORY_NON_LOCAL_REDIS_ERROR = (
    'Tile-server cache configured to use non-local redis instance. Configure '
    'non-local redis instance max memory settings directly on the REDIS server.'
)
_REDIS_USERNAME_AND_PASSWORD_MUST_BE_NONE_RUNNING_FROM_LOCALHOST = (
    'Username and password must be None when running in from localhost.'
)
_REDIS_CACHE_CANNOT_BE_TLS_PROXY_WITH_LOCALHOST_IP = (
    'Redis cache cannot be configured to use TLS encryption with localhost.'
)
_INVALID_REDIS_LOCALHOST_PORT = (
    'Redis cache running on Localhost must be configured to run on port 6379.'
)
_LOCALHOST_IP = '127.0.0.1'
_STUNNEL_PORT = 6378


class RedisConfigError(Exception):
  """Redis configuration error."""


@dataclasses.dataclass
class RedisResult:
  """Result from RedisCache.get holding value returned for key."""

  value: Optional[bytes]


def _redis_tls_certificate_authority_gcs_uri() -> str:
  val = dicom_proxy_flags.REDIS_TLS_CERTIFICATE_AUTHORITY_GCS_URI_FLG.value
  return val.strip() if val is not None else ''


def _redis_host_ip_config() -> str:
  host_ip = dicom_proxy_flags.REDIS_CACHE_HOST_IP_FLG.value
  host_ip = host_ip.strip() if host_ip is not None else ''
  if host_ip.lower() in (_LOCALHOST_IP, 'localhost'):
    return _LOCALHOST_IP
  return host_ip


def _redis_host_ip() -> str:
  """Returns Redis Host IP."""
  host_ip = _redis_host_ip_config()
  if _redis_tls_certificate_authority_gcs_uri() and host_ip:
    return _LOCALHOST_IP
  return host_ip


def _redis_host_port() -> int:
  if _redis_tls_certificate_authority_gcs_uri():
    return _STUNNEL_PORT
  return dicom_proxy_flags.REDIS_CACHE_HOST_PORT_FLG.value


def _log_and_raise_critical_error(
    msg: str, *args: Union[Mapping[str, Any], Exception, None]
) -> None:
  """Logs and raises critical error.

  Args:
    msg: Error message.
    *args: Additional arguments to log.

  Raises:
    RedisConfigError: Proxy server Redis configuration error.
  """
  cloud_logging_client.critical(msg, *args)
  raise RedisConfigError(msg)


# Path to file which records time instance last perfomed redis cache flush.
_redis_db_last_flushed_filepath = os.path.join(
    tempfile.gettempdir(), 'redis_db_last_flushed.txt'
)
_redis_host_instance_lock = threading.Lock()
_redis_host_instance: Optional[redis.Redis] = None


def _init_fork_module_state() -> None:
  global _redis_host_instance_lock
  global _redis_host_instance
  _redis_host_instance_lock = threading.Lock()
  _redis_host_instance = None


def _min_redis_cache_flush_interval() -> int:
  """Returns minium time between instances Redis cache flush operations."""
  return max(dicom_proxy_flags.REDIS_MIN_CACHE_FLUSH_INTERVAL_FLG.value, 0)


def _get_redis_db_last_flushed() -> float:
  """Returns time Redis was last flushed by the instance."""
  try:
    with open(_redis_db_last_flushed_filepath, 'rt') as infile:
      return float(infile.read())
  except OSError as os_exp:
    cloud_logging_client.warning(
        f'A OSError occured reading: {_redis_db_last_flushed_filepath}.',
        os_exp,
    )
    # If read error occures return current time. At most this will delay
    # cache reset. Server will continue to run.
    return time.time()


def _write_redis_db_last_flushed(update: float) -> None:
  """Writes the time the instance last flushed the Redis DB."""
  try:
    with open(_redis_db_last_flushed_filepath, 'wt') as outfile:
      outfile.write(str(update))
  except OSError as os_exp:
    cloud_logging_client.warning(
        f'A OSError occured writing: {_redis_db_last_flushed_filepath}.',
        os_exp,
    )
    # If write error occures ingore. At most this will cause result in
    #  additional cache reset. Server will continue to run.


class RedisCache:
  """Interface to communicate with local in memory Redis cache."""

  def __init__(
      self,
      cache_enabled: cache_enabled_type.CachingEnabled = cache_enabled_type.CachingEnabled(
          True
      ),
  ):
    if not self.server_defined or (
        dicom_proxy_flags.REDIS_CACHE_MAXMEMORY_FLG.value == 0
        and self.is_localhost
    ):
      cloud_logging_client.warning(
          'Redis cache disabled.',
          {
              'server defined': self.server_defined,
              'max_memory': dicom_proxy_flags.REDIS_CACHE_MAXMEMORY_FLG.value,
              'localhost': self.is_localhost,
          },
      )
      cache_enabled = cache_enabled_type.CachingEnabled(False)
    self._cache_enabled = cache_enabled
    if not self.is_enabled:
      return
    disable_redis_host_cache = (
        dicom_proxy_flags.DISABLE_REDIS_INSTANCE_PROCESS_CACHE_IN_DEBUG_FLG.value
    )
    global _redis_host_instance
    if _redis_host_instance is not None and not disable_redis_host_cache:
      self._redis = _redis_host_instance
      return
    with _redis_host_instance_lock:
      if _redis_host_instance is not None and not disable_redis_host_cache:
        self._redis = _redis_host_instance
        return
      self._redis = redis.Redis(
          host=_redis_host_ip(),
          port=_redis_host_port(),
          db=dicom_proxy_flags.REDIS_CACHE_DB_FLG.value,
          username=dicom_proxy_flags.REDIS_USERNAME_FLG.value,
          password=dicom_proxy_flags.REDIS_AUTH_PASSWORD_FLG.value,
      )
      _redis_host_instance = self._redis
      return

  def config_set(self, name: str, value: str) -> bool:
    """Sets redis configuration.

    Args:
      name: String name of config to set.
      value: Configuration value.

    Returns:
      True if configuration set.
    """
    if not self.is_enabled or not self.is_localhost:
      return False
    try:
      old_value = self._redis.config_get(name)
      result = self._redis.config_set(name, value)
      new_value = self._redis.config_get(name)
      # in unit tests config set was not throw connection errors when no
      # redis store was a avaliable log change to verify that config
      # parameter changes.
      cloud_logging_client.info(
          'Redis config changed.',
          {
              'config': name,
              'old_value': old_value,
              'value_set': value,
              'new_value': new_value,
          },
      )
      return result
    except redis.exceptions.ConnectionError:
      self._cache_enabled = cache_enabled_type.CachingEnabled(False)
    return False

  @property
  def server_defined(self) -> bool:
    return bool(_redis_host_ip_config())

  @property
  def is_localhost(self) -> bool:
    return bool(_redis_host_ip_config() == _LOCALHOST_IP)

  @property
  def is_enabled(self) -> bool:
    """Returns True if Redis cache is enabled."""
    return self._cache_enabled

  def ping(self) -> bool:
    if not self.is_enabled:
      return False
    try:
      self._redis.ping()
      return True
    except redis.exceptions.ConnectionError as exp:
      cloud_logging_client.warning('Error connecting to Redis Cache.', exp)
    return False

  def get(self, cache_key: str) -> Optional[RedisResult]:
    """Returns value stored for key in redis cache.

    Args:
      cache_key: Key to look up in Redis store.

    Returns:
      Value associated with key or None if value is not found.
      If RedisResult.value == None then None was stored for key's value.
    """
    if not self.is_enabled:
      return None
    try:
      result = self._redis.get(cache_key)
      if result is None:
        return result
      if result == _REDIS_NONE_VALUE:
        return RedisResult(None)
      return RedisResult(result)
    except redis.exceptions.ConnectionError as exp:
      cloud_logging_client.warning('Error getting value from Redis Cache.', exp)
      return None

  def set(
      self,
      cache_key: str,
      value: Optional[Union[bytes, str]],
      allow_overwrite: bool = True,
      ttl_sec: Optional[int] = None,
  ) -> bool:
    """Sets key value in redis cache.

    Args:
      cache_key: Key to look up in Redis store.
      value: Key's value.
      allow_overwrite: Allow set to overwrite pre-existing cache value
      ttl_sec: TTL (sec) for key: value pair to expire

    Returns:
      True if key:value set in redis cache.
    """
    if not self.is_enabled:
      return False
    nx = not allow_overwrite  # Redis param: set the key if it does not exist.
    ex = ttl_sec  # Redis Parameter TTL sec
    try:
      if value is None:
        if self._redis.set(cache_key, _REDIS_NONE_VALUE, nx=nx, ex=ex) is None:
          return False
        return True
      if isinstance(value, str):
        set_value = value.encode('utf-8')
      else:
        set_value = value
      if len(set_value) > _VALUE_REDIS_CACHE_SIZE_LIMIT_LIMIT:
        return False
      if self._redis.set(cache_key, set_value, nx=nx, ex=ex) is None:
        return False
      return True
    except redis.exceptions.ConnectionError as exp:
      cloud_logging_client.warning(
          f'Error setting value in Redis Cache. {exp}', exp
      )
      return False
    except redis.exceptions.ResponseError as exp:
      if 'command not allowed under OOM prevention' not in str(exp):
        cloud_logging_client.warning('Unexpected Redis cache error.', exp)
        return False

      # Redis cache should be configured to avoid OOM conditions.
      # https://cloud.google.com/memorystore/docs/redis/memory-management-best-practices
      # If Redis cache returns OOM then attempt to recover by erasing
      # current cached data. Proxy server runs on multiple threads, processes,
      # and GKE instances. As a result a race condition exists here where
      # multiple OOM events could simultationusly try to empty the cache. Fully
      # protecting against this is not necessary. Current execution will
      # eventually result in a usable cache. Two mechanisms
      # have been added to reduce the likelyhood of the race condition.
      # 1) To protect against the race condition within a server instance
      # each proxy instance writes and reads a temporay file which tracks the
      # time the instance last performed a redis.flushall operation. The min
      # time delta between cache flush operations is configurable using the
      # REDIS_MIN_CACHE_FLUSH_INTERVAL enviromental variable.
      # 2) To protect against cross instance execution Redis are checked
      # lazyfree_pending_objects if the Redis database reports pending objects.
      # the Redis instance ins not flushed.

      cloud_logging_client.warning(
          'Redis cache OOM; See:'
          ' https://cloud.google.com/memorystore/docs/redis/memory-management-best-practices',
          exp,
      )
      redis_last_flushed = _get_redis_db_last_flushed()
      if time.time() - redis_last_flushed < _min_redis_cache_flush_interval():
        return False
      try:
        try:
          lazyfree_pending_objects = self._redis.info('memory').get(
              'lazyfree_pending_objects', 0
          )
          if lazyfree_pending_objects > 0:
            cloud_logging_client.info(
                f'Redis cache has N={lazyfree_pending_objects} '
                'lazyfree_pending_objects. Redis not flushed.'
            )
            return False
        except AttributeError as attribute_exp:
          cloud_logging_client.warning(
              'Redis info returned unexpected result.', attribute_exp
          )
        _write_redis_db_last_flushed(time.time())
        self._redis.flushall(asynchronous=True)
        cloud_logging_client.info('Flushed Redis cache.')
      except (
          redis.exceptions.ConnectionError,
          redis.exceptions.ResponseError,
      ) as redis_exp:
        _write_redis_db_last_flushed(redis_last_flushed)
        cloud_logging_client.warning(
            'Unexpected error flushing Redis cache.', redis_exp
        )
      return False

  def delete(self, cache_key: str) -> int:
    try:
      return self._redis.delete(cache_key)
    except redis.exceptions.ConnectionError:
      return 0


def _init_config_defrag(cache: RedisCache) -> None:
  if not cache.is_enabled or not cache.is_localhost:
    return
  try:
    if cache.config_set('activedefrag', 'yes'):
      return
    raised_exception = None
  except redis.exceptions.ResponseError as exp:
    raised_exception = exp
  cloud_logging_client.warning(
      'Could not enable activedefrag on localhost redis.', raised_exception
  )


def _init_config_maxmemory(cache: RedisCache) -> None:
  """Called at program initialization to init redis LRU maxmemory setting.

     If no redis server -> Do nothing
     If remote redis server log warning that config should be performed on
       remote server.
     Redis maxmemory read from REDIS_CACHE_MAXMEMORY env.
       If not defined set to 1/2 of available memory in Gigabytes.
     If local validate that memory setting is valid >= 1 and within container
      memory limits.

     Then attempt to set the memory config

  Args:
    cache: Redis cache to init.

  Returns:
    None

  Raises:
    RedisConfigError: Proxy server Redis configuration error.
  """
  if not cache.is_enabled or not cache.is_localhost:
    if dicom_proxy_flags.REDIS_CACHE_MAXMEMORY_FLG.value > 0:
      _log_and_raise_critical_error(
          _SET_MAXMEMORY_NON_LOCAL_REDIS_ERROR,
          {
              'REDIS_CACHE_MAXMEMORY': (
                  dicom_proxy_flags.REDIS_CACHE_MAXMEMORY_FLG.value
              )
          },
      )
    return
  gigabyte = pow(1024, 3)
  vm_available = psutil.virtual_memory().available
  if dicom_proxy_flags.REDIS_CACHE_MAXMEMORY_FLG.value < 0:
    memory_config = int(vm_available / (2 * gigabyte))
  else:
    memory_config = dicom_proxy_flags.REDIS_CACHE_MAXMEMORY_FLG.value
  free_memory = int(vm_available / gigabyte)
  if memory_config > free_memory:
    _log_and_raise_critical_error(
        _REDIS_CACHE_MAXMEMORY_GREATER_THAN_MEM_AVAIL,
        {
            'redis set_config maxmemory': f'{memory_config}GB',
            'total_available_memory': f'{free_memory}GB',
        },
    )

  memory_config_str = f'{memory_config}GB'
  if not cache.config_set('maxmemory', memory_config_str):
    _log_and_raise_critical_error(
        _ERROR_SETTING_REDIS_CACHE_MAXMEMORY,
        {
            'redis set_config maxmemory': memory_config_str,
            'total_available_memory': f'{free_memory}GB',
        },
    )
  cloud_logging_client.info(
      _SET_REDIS_CACHE_MAXMEMORY_CONFIG,
      {'redis set_config maxmemory': memory_config_str},
  )


def _validate_flags(cache: RedisCache) -> None:
  """Validates proxy server redis configuration flags.

  Args:
    cache: Redis cache to validate with flags.

  Raises:
    RedisConfigError: Proxy server Redis configuration error.
  """
  if cache.is_localhost:
    username = dicom_proxy_flags.REDIS_USERNAME_FLG.value
    password = dicom_proxy_flags.REDIS_AUTH_PASSWORD_FLG.value
    if username is not None or password is not None:
      _log_and_raise_critical_error(
          _REDIS_USERNAME_AND_PASSWORD_MUST_BE_NONE_RUNNING_FROM_LOCALHOST,
          {'username': username, 'password': password},
      )
    if _redis_tls_certificate_authority_gcs_uri():
      _log_and_raise_critical_error(
          _REDIS_CACHE_CANNOT_BE_TLS_PROXY_WITH_LOCALHOST_IP,
          {
              'redis_host_ip': dicom_proxy_flags.REDIS_CACHE_HOST_IP_FLG.value,
              'redis_tls_certificate': (
                  _redis_tls_certificate_authority_gcs_uri()
              ),
          },
      )
    if _redis_host_port() != 6379:
      _log_and_raise_critical_error(
          _INVALID_REDIS_LOCALHOST_PORT, {'redis_port': _redis_host_port()}
      )


def _start_stunnel_proxy() -> None:
  """Init stunnnel local proxy if external Redis configured with TLS CA."""
  temp_dir = tempfile.TemporaryDirectory().name
  os.mkdir(temp_dir)
  config_file = os.path.join(temp_dir, 'stunnel.config')
  ca_file = os.path.join(temp_dir, 'server_ca.pem')
  stunnel_pid = os.path.join(temp_dir, 'stunnel.pid')
  try:
    client = google.cloud.storage.Client()
    blob = google.cloud.storage.Blob.from_string(
        _redis_tls_certificate_authority_gcs_uri(), client
    )
    with open(ca_file, 'wb') as outfile:
      outfile.write(blob.download_as_bytes())
  except Exception as exp:
    cloud_logging_client.critical(
        'Error downloading Redis TLS certifcate.', exp
    )
    raise
  try:
    server_ip = dicom_proxy_flags.REDIS_CACHE_HOST_IP_FLG.value
    server_ip = server_ip.strip() if server_ip is not None else ''
    server_port = dicom_proxy_flags.REDIS_CACHE_HOST_PORT_FLG.value
    with open(config_file, 'wt') as outfile:
      lines = [
          f'CAfile={ca_file}',
          'client=yes',
          f'pid={stunnel_pid}',
          'verifyChain=yes',
          'sslVersion=TLSv1.2',
          '[redis]',
          f'accept={_LOCALHOST_IP}:{_STUNNEL_PORT}',
          f'connect={server_ip}:{server_port}',
      ]
      outfile.write('\n'.join(lines))
  except Exception as exp:
    cloud_logging_client.critical('Error saving Redis CA.', exp)
    raise
  try:
    cloud_logging_client.info('Starting stunnel.')
    subprocess.Popen(['/usr/bin/stunnel', config_file])
    cloud_logging_client.info('stunnel started.')
  except Exception as exp:
    cloud_logging_client.critical('Error starting stunnel.', exp)
    raise


def setup() -> None:
  """Initializes and Validates Redis cache; call once at startup.

  Returns:
    None

  Raises:
    RedisConfigError: Proxy server Redis configuration error.
  """
  _write_redis_db_last_flushed(time.time() - _min_redis_cache_flush_interval())
  cache = RedisCache()
  _validate_flags(cache)
  if cache.is_localhost:
    subprocess.Popen(['/usr/bin/redis-server', '/redis.conf'])
    time.sleep(30)  # Give time for redis to start before configuring.
  _init_config_defrag(cache)
  _init_config_maxmemory(cache)
  if not cache.is_enabled:
    cloud_logging_client.warning('Redis cache disabled.')
    return
  if cache.is_localhost:
    cache_status = {'running': 'locally'}
  elif _redis_tls_certificate_authority_gcs_uri():
    _start_stunnel_proxy()
    cache_status = {'running': 'remote_instance_tls'}
  else:
    cache_status = {
        'running': 'remote_instance',
        'redis_host_ip': dicom_proxy_flags.REDIS_CACHE_HOST_IP_FLG.value,
    }
  cloud_logging_client.info('Redis cache setup.', cache_status)


os.register_at_fork(after_in_child=_init_fork_module_state)
