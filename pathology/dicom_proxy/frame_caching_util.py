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
"""Util for preemptive DICOM frame caching.

Pods should be set up in GKE to run with user affinity. Without this users won't
preferentially hit the same pod in GKE and the effect of caching will be
reduced. Without a cross pod shared redis memory store the effectiveness of
front-end queuing would be indeterminate. In the current design it is possible
even with GKE user affinity that the front-end conducts transactions across
multiple pods. In the current design each pod has its own cache, feasible with
C2N30 VM, given its relatively large amount of RAM (~120 GB) these machines
have. Each pod controls its own cache and pre-buffers itself by the traffic it
is experiencing.
"""

from __future__ import annotations

import concurrent.futures
import dataclasses
import functools
import itertools
import math
import multiprocessing
import os
import sys
import tempfile
import threading
import time
from typing import Iterator, List, MutableMapping, Optional, Sequence, Set, Tuple, Union

from absl import flags
import psutil
import pydicom

from pathology.dicom_proxy import dicom_instance_request
from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import frame_retrieval_util
from pathology.dicom_proxy import metadata_util
from pathology.dicom_proxy import redis_cache
from pathology.dicom_proxy import render_frame_params
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.logging_lib import cloud_logging_client
from pathology.shared_libs.pydicom_version_util import pydicom_version_util


# Types
_DicomInstanceRequest = dicom_instance_request.DicomInstanceRequest


@dataclasses.dataclass(frozen=True)
class _PremptiveCacheLoadingJob:
  thread_or_process: Union[threading.Thread, multiprocessing.Process]
  operation: Union[_CacheWholeInstance, _CacheInstanceFrameBlock]


_ACTIVE_CACHE_LOADING_CLEANUP_TIMEOUT = 60.0
_cache_instance_thread_lock = threading.Lock()
_active_cache_loading_instances: Set[_PremptiveCacheLoadingJob] = set()
_last_cleanup_preemptive_cache_loading = 0.0


def _init_fork_module_state() -> None:
  global _cache_instance_thread_lock
  global _active_cache_loading_instances
  global _last_cleanup_preemptive_cache_loading
  _cache_instance_thread_lock = threading.Lock()
  _active_cache_loading_instances = set()
  _last_cleanup_preemptive_cache_loading = 0.0


# Structured Log Keys
CACHE_WHOLE_INSTANCE = 'cache_whole_instance'
CPU_LOAD = 'cpu_load'
DICOM_INSTANCE = 'DICOM_SOPInstanceUID'
DICOM_TRANSFER_SYNTAX = 'transfer_syntax'
ELAPSED_TIME = 'elapsed_time(sec)'
NUMBER_OF_FRAMES_CACHED = 'number_of_frame_cached'
FIRST_FRAME = 'first_frame'
FRAMES_REQUESTED = 'DICOM_frame_numbers_requested'
LAST_FRAME = 'last_frame'
MAX_WHOLE_INSTANCE_CACHING_CPU_LOAD = 'MAX_WHOLE_INSTANCE_CACHING_CPU_LOAD'
NUMBER_OF_FRAMES_IN_DICOM_INSTANCE = 'number_of_frames_in_dicom_instance'
PREEMPTIVE_CACHE_FRAME_BLOCK_SIZE = 'PREEMPTIVE_CACHE_FRAME_BLOCK_SIZE'
PREEMPTIVE_INSTANCE_CACHE_MAX = 'PREEMPTIVE_INSTANCE_CACHE_MAX'
PREEMPTIVE_INSTANCE_CACHE_MAX_INSTANCE_FRAME_NUMBER = (
    'PREEMPTIVE_INSTANCE_CACHE_MAX_INSTANCE_FRAME_NUMBER'
)


class UnexpectedDicomTransferSyntaxError(Exception):
  pass


def get_cache_render_params(
    dicom_sop_instance_url: dicom_url_util.DicomSopInstanceUrl,
    metadata: metadata_util.DicomInstanceMetadata,
) -> render_frame_params.RenderFrameParams:
  """Returns rendered parameters to use in caching DICOM instance.

  Args:
    dicom_sop_instance_url: URL to DICOM instance being cached.
    metadata: Metadata for DICOM instance being cached.

  Returns:
    Caching Render Frame Parameters.

  Raises:
    UnexpectedDicomTransferSyntaxError: Transfer syntax of DICOM instance is
      not somthing caching module supports.
  """
  render_params = render_frame_params.RenderFrameParams()
  render_params.downsample = 1.0
  if metadata.is_baseline_jpeg:
    render_params.compression = enum_types.Compression.JPEG
  elif metadata.is_jpeg2000:
    render_params.compression = enum_types.Compression.JPEG2000
  elif metadata.is_jpg_transcoded_to_jpegxl:
    render_params.compression = enum_types.Compression.JPEG_TRANSCODED_TO_JPEGXL
  elif metadata.is_jpegxl:
    render_params.compression = enum_types.Compression.JPEGXL
  else:
    msg = 'Unexpected DICOM pixel encoding (Transfer Syntax).'
    cloud_logging_client.error(
        msg,
        {
            DICOM_INSTANCE: dicom_sop_instance_url,
            DICOM_TRANSFER_SYNTAX: metadata.dicom_transfer_syntax,
        },
    )
    raise UnexpectedDicomTransferSyntaxError(msg)
  return render_params


def set_cached_frame(
    redis: redis_cache.RedisCache,
    uauth: user_auth_util.AuthSession,
    dicom_instance_url: dicom_url_util.DicomSopInstanceUrl,
    render_params: render_frame_params.RenderFrameParams,
    ttl_sec: Optional[int],
    allow_overwrite: bool,
    frame_data: bytes,
    frame_number: int,
) -> bool:
  return redis.set(
      frame_retrieval_util.frame_lru_cache_key(
          dicom_url_util.download_dicom_raw_frame(
              uauth, dicom_instance_url, [frame_number], render_params
          )
      ),
      frame_data,
      allow_overwrite=allow_overwrite,
      ttl_sec=ttl_sec,
  )


def _set_cached_frame_from_gen(
    redis: redis_cache.RedisCache,
    uauth: user_auth_util.AuthSession,
    dicom_instance_url: dicom_url_util.DicomSopInstanceUrl,
    render_params: render_frame_params.RenderFrameParams,
    ttl_sec: Optional[int],
    frame_data_tpl: Tuple[bytes, int],
) -> bool:
  return set_cached_frame(
      redis,
      uauth,
      dicom_instance_url,
      render_params,
      ttl_sec,
      True,
      *frame_data_tpl,
  )


def _get_encapsulated_data_frame(
    encaps_data: Iterator[bytes],
) -> Iterator[Tuple[bytes, int]]:
  for index, data in enumerate(encaps_data):
    yield (data, index + 1)


def _get_pixel_data_frame(
    pixel_data: bytes, number_of_frames: int
) -> Iterator[Tuple[bytes, int]]:
  frame_size = int(len(pixel_data) / number_of_frames)
  for index, offset in enumerate(range(0, len(pixel_data), frame_size)):
    yield (pixel_data[offset : offset + frame_size], index + 1)


def _cache_entire_instance(dicom_instance: _DicomInstanceRequest) -> int:
  """Caches frames from entire DICOM instance.

  Args:
    dicom_instance: DICOM Store instance to downsample.

  Returns:
    Number of frames cached.
  """
  with tempfile.TemporaryDirectory() as temp_dir:
    path = os.path.join(temp_dir, 'instance.dcm')
    dicom_instance.download_instance(path)
    try:
      render_params = get_cache_render_params(
          dicom_instance.dicom_sop_instance_url, dicom_instance.metadata
      )
    except UnexpectedDicomTransferSyntaxError as exp:
      cloud_logging_client.warning(
          'Can not cache DICOM instance, instance encoded in unsupported'
          ' transfer syntax.',
          {
              'dicomWebInstance': dicom_instance.dicom_sop_instance_url,
              'transfer_syntax_uid': (
                  dicom_instance.metadata.dicom_transfer_syntax
              ),
          },
          exp,
      )
      return 0
    uauth = dicom_instance.user_auth
    dicom_instance_url = dicom_instance.dicom_sop_instance_url
    with pydicom.dcmread(path) as local_pydicom_instance:
      number_of_frames = int(local_pydicom_instance.NumberOfFrames)
      transfer_syntax_uid = str(
          local_pydicom_instance.file_meta.TransferSyntaxUID
      )
      pixel_data = local_pydicom_instance.PixelData
  redis = redis_cache.RedisCache()
  frame_ttl = frame_retrieval_util.frame_cache_ttl()
  set_frame_partial = functools.partial(
      _set_cached_frame_from_gen,
      redis,
      uauth,
      dicom_instance_url,
      render_params,
      frame_ttl,
  )
  if metadata_util.is_transfer_syntax_encapsulated(transfer_syntax_uid):
    frame_data_generator = _get_encapsulated_data_frame(
        pydicom_version_util.generate_frames(pixel_data, number_of_frames)
    )
  else:
    frame_data_generator = _get_pixel_data_frame(pixel_data, number_of_frames)

  # If running locally or processing small number of frames just iterate over
  # frames and set cache from main in the thread.
  if redis.is_localhost or number_of_frames < 100:
    for frame_data in frame_data_generator:
      set_frame_partial(frame_data)
  else:
    # If the number of frames is large create a thread pool and use pool to set
    # Redis cache.
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as th_pool:
      th_pool.map(set_frame_partial, frame_data_generator)
  return number_of_frames


def _background_cache_key(dicom_instance: _DicomInstanceRequest) -> str:
  return f'PreemptiveFrameCaching: {dicom_instance.dicom_sop_instance_url}'


class _CacheWholeInstance:
  """Downloads DICOM instance and loads frame cache in background."""

  def __init__(
      self,
      dicom_instance: _DicomInstanceRequest,
      running_in_process: bool,
      load_display_thread: Optional[_CacheInstanceFrameBlock],
  ):
    """Constructor.

    Args:
      dicom_instance: DICOM instance to load into cache.
      running_in_process: True if executed in process, False if executed in a
        thread.
      load_display_thread: Batch request to quickly load frames that likely
        overlap in the display. Only pass if running in thread. If running in
        process this parameter should be None.
    """
    self._instance_request = dicom_instance
    cloud_logging_client.info(
        'Preemptive whole instance caching started.',
        {
            DICOM_INSTANCE: self._instance_request.dicom_sop_instance_url,
            CACHE_WHOLE_INSTANCE: True,
        },
    )
    self._running_in_process = running_in_process
    self._load_display_thread = load_display_thread

  def is_caching_whole_slide_instance(
      self, check: _DicomInstanceRequest
  ) -> bool:
    return str(self._instance_request.dicom_sop_instance_url) == str(
        check.dicom_sop_instance_url
    )

  def _get_completion_msg_and_log(
      self, start_time: float
  ) -> Tuple[str, MutableMapping[str, str]]:
    """Returns logging message and structured log."""
    elapsed_time = time.time() - start_time
    msg = (
        'Preemptive whole instance caching stopped; elapsed_time:'
        f' {elapsed_time:.3f}(sec)'
    )
    struct_log = {
        DICOM_INSTANCE: str(self._instance_request.dicom_sop_instance_url),
        ELAPSED_TIME: str(elapsed_time),
        CACHE_WHOLE_INSTANCE: str(True),
        NUMBER_OF_FRAMES_IN_DICOM_INSTANCE: str(
            self._instance_request.metadata.number_of_frames
        ),
    }
    return (msg, struct_log)

  def run(self) -> None:
    """Entry point for process that loads a DICOM instance into the cache."""
    start_time = time.time()
    if not self._running_in_process:
      # runing in thread
      if (
          self._load_display_thread is not None
          and self._load_display_thread.total_frames_in_block()
          < self._instance_request.metadata.number_of_frames
      ):
        # Only run the load display thread if it has less frames than the
        # whole instance which will be loaded.
        self._load_display_thread.run()  # pytype: disable=attribute-error
    else:
      # running in process init flags.
      flags.FLAGS(sys.argv, known_only=True)
      # do not re-duplicate logging startup messaging.
      cloud_logging_client.do_not_log_startup_msg()
    instance_request = self._instance_request
    try:
      _cache_entire_instance(instance_request)
      msg, struct_log = self._get_completion_msg_and_log(start_time)
      number_of_frames = instance_request.metadata.number_of_frames
      msg = f'{msg}; NumberOfFrames: {number_of_frames}.'
      struct_log['frames_cached'] = number_of_frames
      cloud_logging_client.info(msg, struct_log)
    except Exception as exp:
      redis = redis_cache.RedisCache()
      redis.delete(_background_cache_key(instance_request))
      msg, struct_log = self._get_completion_msg_and_log(start_time)
      cloud_logging_client.error(msg, struct_log, exp)
      raise


def _get_frame_request_region(
    request_height_px: int,
    request_width_px: int,
    first_frame: int,
    frames_per_row: int,
    frames_per_column: int,
    frame_row_dim: int,
    frame_col_dim: int,
) -> Tuple[int, int, int, int]:
  """Returns inclusive frame coordinates (row, column) for first lst frame.

  Args:
    request_height_px: Hight in pixels to load.
    request_width_px: Width in pixels to load.
    first_frame: Frame number to base returned coordinates on.
    frames_per_row: Number of frames per-row.
    frames_per_column: Number of frames per-column.
    frame_row_dim: Pixel dimensions of a frame height.
    frame_col_dim: Pixel dimensions of frame width.

  Returns:
    First frame (row, col), Last Frame (row, col)
  """
  f_row = int(first_frame / frames_per_row)
  f_col = int(first_frame % frames_per_row)
  request_rows = int(math.ceil(request_height_px / frame_row_dim))
  request_columns = int(math.ceil(request_width_px / frame_col_dim))
  monitor_resolution = int(
      dicom_proxy_flags.PREEMPTIVE_DISPLAY_CACHE_PIXEL_DIM_FLG.value / 2
  )
  column_bias = int(math.ceil(monitor_resolution / frame_col_dim))
  row_bias = int(math.ceil(monitor_resolution / frame_row_dim))
  if request_rows > row_bias:
    f_row = max(f_row - int((request_rows - row_bias) / 2), 0)
  if request_columns > column_bias:
    f_col = max(f_col - int((request_columns - column_bias) / 2), 0)
  l_row = f_row + request_rows - 1
  l_col = f_col + request_columns - 1
  if l_col >= frames_per_row:
    pad = l_col - (frames_per_row - 1)
    f_col = max(0, f_col - pad)
    l_col = frames_per_row - 1
  if l_row >= frames_per_column:
    pad = l_row - (frames_per_column - 1)
    f_row = max(0, f_row - pad)
    l_row = frames_per_column - 1
  return f_row, f_col, l_row, l_col


class _CacheInstanceFrameBlock:
  """Downloads DICOM instance and loads frame cache in background."""

  def __init__(
      self,
      dicom_instance: _DicomInstanceRequest,
      requested_frames: List[int],
      request_height: int,
      request_width: int,
      load_display_thread: Optional[_CacheInstanceFrameBlock] = None,
      require_first_frame: bool = False,
  ):
    super().__init__()
    self._require_first_frame = require_first_frame
    self._load_display_thread = load_display_thread
    self._instance_request = dicom_instance
    frames_per_row = int(
        math.ceil(
            dicom_instance.metadata.total_pixel_matrix_columns
            / dicom_instance.metadata.columns
        )
    )
    frames_per_column = int(
        math.ceil(
            dicom_instance.metadata.total_pixel_matrix_rows
            / dicom_instance.metadata.rows
        )
    )
    # Convert request in pixel dimensions to frames
    first_frame = min(requested_frames) - 1
    f_row, f_col, l_row, l_col = _get_frame_request_region(
        request_height,
        request_width,
        first_frame,
        frames_per_row,
        frames_per_column,
        dicom_instance.metadata.rows,
        dicom_instance.metadata.columns,
    )
    self._first_frame_row = f_row
    self._first_frame_col = f_col + 1  # Add 1 to make 1 first index
    self._width_frame_count = l_col - f_col + 1
    self._height_frame_count = l_row - f_row + 1
    self._frames_per_row = frames_per_row
    if (
        self._load_display_thread is not None
        and self.total_frames_in_block()
        <= self._load_display_thread.total_frames_in_block()
    ):
      # Only run the load display thread if it has less frames than the
      # frame block which will be loaded.
      self._load_display_thread = None

  def total_frames_in_block(self) -> int:
    return self._width_frame_count * self._height_frame_count  # pytype: disable=attribute-error

  def is_caching_whole_slide_instance(
      self, unused_check: _DicomInstanceRequest
  ) -> bool:
    return False

  def get_start_end_frame_numbers_in_row(
      self, frame_row_offset: int
  ) -> Optional[Tuple[int, int]]:
    """Return first and last frame numbers in row of frames.

    Args:
      frame_row_offset: Starting frame number offset for row of frames.

    Returns:
      None if no frames in row or Tuple[start, end (not inclusive)]
    """
    if frame_row_offset < self._first_frame_row * self._frames_per_row:
      return None
    if (
        frame_row_offset
        >= (self._first_frame_row + self._height_frame_count)
        * self._frames_per_row
    ):
      return None
    first_frame_number = max(self._first_frame_col + frame_row_offset, 1)
    last_frame_number = min(
        first_frame_number + self._width_frame_count,
        self._instance_request.metadata.number_of_frames + 1,
    )
    return (first_frame_number, last_frame_number)

  def _get_frame_number_range_in_row(
      self, frame_row_offset: int
  ) -> Union[Sequence[int], Iterator[int]]:
    """Sequence of frame numbers in row, if defined clips inner regions.

    Args:
      frame_row_offset: Starting frame number offset for row of frames.

    Returns:
      Sequence of frame numbers in row, if defined excludes inner regions loaded
      by load_display_thread_start_end_frames sub-thread.
    """
    start_end_frame_numbers = self.get_start_end_frame_numbers_in_row(
        frame_row_offset
    )
    if start_end_frame_numbers is None:
      return range(1, -1)
    if self._load_display_thread is not None:
      load_display_thread_start_end_frames = (
          self._load_display_thread.get_start_end_frame_numbers_in_row(  # pytype: disable=attribute-error
              frame_row_offset
          )
      )
      if load_display_thread_start_end_frames is not None:
        return itertools.chain(
            range(
                start_end_frame_numbers[0],
                load_display_thread_start_end_frames[0],
            ),
            range(
                load_display_thread_start_end_frames[1],
                start_end_frame_numbers[1],
            ),
        )
    return range(start_end_frame_numbers[0], start_end_frame_numbers[1])

  def run(self) -> None:
    """Entry point for thread that loads a DICOM frames into the cache."""
    if self._load_display_thread is not None:
      self._load_display_thread.run()  # pytype: disable=attribute-error
    log_struct = {
        DICOM_INSTANCE: self._instance_request.dicom_sop_instance_url,
        CACHE_WHOLE_INSTANCE: False,
        NUMBER_OF_FRAMES_IN_DICOM_INSTANCE: (
            self._instance_request.metadata.number_of_frames
        ),
        'frames_height': self._height_frame_count,
        'frames_width': self._width_frame_count,
        'frames_area': self._height_frame_count * self._width_frame_count,
    }
    try:
      start_time = time.time()
      instance_request = self._instance_request
      uauth = instance_request.user_auth
      try:
        render_params = get_cache_render_params(
            instance_request.dicom_sop_instance_url, instance_request.metadata
        )
      except UnexpectedDicomTransferSyntaxError as exp:
        cloud_logging_client.warning(
            'Can not cache DICOM instance, instance encoded in unsupported'
            ' transfer syntax.',
            {
                'dicomWebInstance': instance_request.dicom_sop_instance_url,
                'transfer_syntax_uid': (
                    instance_request.metadata.dicom_transfer_syntax
                ),
            },
            exp,
        )
        return
      redis = redis_cache.RedisCache()
      # Clip range to edge of region not in block
      frame_list = []
      frame_counter = 0
      for frame_row_index_offset in range(
          self._first_frame_row * self._frames_per_row,
          (self._first_frame_row + self._height_frame_count)
          * self._frames_per_row,
          self._frames_per_row,
      ):
        for frame_number in self._get_frame_number_range_in_row(
            frame_row_index_offset
        ):
          frame_counter += 1
          if not set_cached_frame(
              redis,
              uauth,
              instance_request.dicom_sop_instance_url,
              render_params,
              5,  # Tests if frame defined in cache. If not
              False,  # Try to set cache loading placeholder
              frame_retrieval_util.CACHE_LOADING_FRAME_BYTES,
              frame_number,
          ):  # defined marks frame with temp place holder
            if self._require_first_frame and frame_counter == 1:
              return
            continue  # avoid but not prevent duplicate cache load.
          frame_list.append(frame_number)
      if len(frame_list) < 2:
        return
      log_struct[NUMBER_OF_FRAMES_CACHED] = len(frame_list)
      instance_request.get_raw_dicom_frames(render_params, frame_list)
      elapsed_time = time.time() - start_time
      log_struct[ELAPSED_TIME] = elapsed_time
      cloud_logging_client.info(
          f'Done Frame Block Cache; Loaded: {len(frame_list)}(# of frames);'
          f' Elapsed time: {elapsed_time:.3f}(sec)',
          log_struct,
      )
    except Exception as exp:
      cloud_logging_client.info(
          'Unexpected exception occurred loading frame block cache.',
          log_struct,
          exp,
      )
      raise


def _remove_dead_cache_threads() -> None:
  """Removes no long running threads from processes thread set.

  Not thread safe must be called within _cache_instance_thread_lock.
  """
  for job in [
      job
      for job in _active_cache_loading_instances
      if not job.thread_or_process.is_alive()
  ]:
    _active_cache_loading_instances.remove(job)


def wait_for_cache_loading_threads() -> None:
  """Waits for all cache loading threads to complete."""
  for job in _active_cache_loading_instances:
    if job.thread_or_process.is_alive():
      job.thread_or_process.join()
  _remove_dead_cache_threads()


def _is_another_thread_caching_whole_slide_instance(
    dicom_instance: _DicomInstanceRequest,
) -> bool:
  """Checks if instance is being processed by another process thread.

  Not thread safe must be called within _cache_instance_thread_lock.

  Highly unlikely, but possible redis cache could be lost due to LRU purge.
  Redis set is primary protection and protects all processes on the machine.
  This check is cheap and validates again that the process is not currently
  caching the dicom instance in anothe thread.

  Args:
    dicom_instance: DICOM instance to check.

  Returns:
    True if instance is being ingested in another thread.
  """
  for loading_cache in _active_cache_loading_instances:
    if loading_cache.operation.is_caching_whole_slide_instance(dicom_instance):
      return True
  return False


def _create_threaded_cache_loader(
    frame_loader: Union[_CacheWholeInstance, _CacheInstanceFrameBlock],
) -> threading.Thread:
  return threading.Thread(target=frame_loader.run, daemon=True)


def _start_display_thread(
    load_display_thread: Optional[_CacheInstanceFrameBlock],
) -> None:
  if load_display_thread is not None:
    _create_threaded_cache_loader(load_display_thread).start()


def cleanup_preemptive_cache_loading_set() -> None:
  """Cleansup completed preemptive cache loading jobs.

  The purpose of this method is to enable the main loop to cleanup and log
  messages returned from Processes loading whole DICOM instances into the cache.
  """
  if not _cache_instance_thread_lock.acquire(blocking=False):
    return
  try:
    if not _active_cache_loading_instances:
      return
    current_time = time.time()
    global _last_cleanup_preemptive_cache_loading
    if (
        current_time - _last_cleanup_preemptive_cache_loading
        > _ACTIVE_CACHE_LOADING_CLEANUP_TIMEOUT
    ):
      _last_cleanup_preemptive_cache_loading = current_time
      _remove_dead_cache_threads()
  finally:
    _cache_instance_thread_lock.release()


def _get_instance_cache_mp_class():
  ctx = multiprocessing.get_context('spawn')
  return ctx.Process


def _get_max_precent_memory_used_for_whole_slide_caching() -> float:
  max_percent_memory = float(
      dicom_proxy_flags.MAX_WHOLE_INSTANCE_CACHING_MEMORY_PRECENT_LOAD_FLG.value
  )
  return max(0.0, min(1.0, max_percent_memory / 100.0))


def _force_cache_whole_instance_in_memory(
    dicom_instance: _DicomInstanceRequest,
    frames_retrieved_in_block_request: int,
) -> bool:
  # returns True if whole instance is small and should be cached at once using
  # threaded in memory whole instance caching.
  return (
      dicom_instance.metadata.number_of_frames
      <= frames_retrieved_in_block_request
  )


def cache_instance_frames_in_background(
    dicom_instance: _DicomInstanceRequest,
    frame_indexes_requested: List[int],
) -> bool:
  """Launches thread to cache all instance frames.

  Args:
    dicom_instance: DICOM instance to cache.
    frame_indexes_requested: Requested frames which triggered caching.

  Returns:
    True if background frame caching thread started started.
  """
  if not frame_indexes_requested:
    return False
  if dicom_proxy_flags.DISABLE_PREEMPTIVE_INSTANCE_FRAME_CACHE_FLG.value:
    return False
  if not (
      dicom_instance.metadata.is_baseline_jpeg
      or dicom_instance.metadata.is_jpeg2000
      or dicom_instance.metadata.is_jpegxl
      or dicom_instance.metadata.is_jpg_transcoded_to_jpegxl
  ):
    return False
  if (
      dicom_instance.metadata.number_of_frames
      < dicom_proxy_flags.PREEMPTIVE_INSTANCE_CACHE_MIN_INSTANCE_FRAME_NUMBER_FLG.value
  ):
    return False
  cache_whole_instance = (
      dicom_proxy_flags.ENABLE_PREEMPTIVE_WHOLEINSTANCE_CACHING_FLG.value
  )

  premptive_block_px_dim = (
      dicom_proxy_flags.PREEMPTIVE_INSTANCE_CACHE_BLOCK_PIXEL_DIM_FLG.value
  )
  frames_retrieved_in_block_request = math.ceil(
      premptive_block_px_dim / dicom_instance.metadata.rows
  ) * math.ceil(premptive_block_px_dim / dicom_instance.metadata.columns)
  if (
      cache_whole_instance
      and dicom_instance.metadata.number_of_frames
      > dicom_proxy_flags.PREEMPTIVE_INSTANCE_CACHE_MAX_INSTANCE_FRAME_NUMBER_FLG.value
  ):
    # if caching whole instance and number of frames in instance exceeds
    # a threshold then download frame batch.
    cache_whole_instance = False
    cloud_logging_client.debug(
        'Performance optimization: Number of frames in DICOM instance is > '
        ' PREEMPTIVE_INSTANCE_CACHE_MAX_INSTANCE_FRAME_NUMBER; enabling frame'
        ' batch caching.',
        {
            DICOM_INSTANCE: dicom_instance.dicom_sop_instance_url,
            NUMBER_OF_FRAMES_IN_DICOM_INSTANCE: (
                dicom_instance.metadata.number_of_frames
            ),
            PREEMPTIVE_INSTANCE_CACHE_MAX_INSTANCE_FRAME_NUMBER: (
                dicom_proxy_flags.PREEMPTIVE_INSTANCE_CACHE_MAX_INSTANCE_FRAME_NUMBER_FLG.value
            ),
        },
    )
  # If size of the instance equal or smaller than batch request.
  # Use threaded whole instance retrieval. Greatest performance for small
  # instances.
  force_cache_whole_instance_in_memory = _force_cache_whole_instance_in_memory(
      dicom_instance, frames_retrieved_in_block_request
  )
  if cache_whole_instance and not force_cache_whole_instance_in_memory:
    # If caching whole instance and CPU load on DICOM Proxy is high then cache
    # frame block to ensure a faster response when instances with many frames
    # are being cached.
    max_cpu_load = max(
        0,
        min(
            dicom_proxy_flags.MAX_WHOLE_INSTANCE_CACHING_CPU_LOAD_FLG.value,
            100,
        ),
    )
    if max_cpu_load < 100:
      try:
        cpu_count = float(psutil.cpu_count())
        cpu_load = [load / cpu_count * 100 for load in psutil.getloadavg()[:2]]
        if max(cpu_load) > max_cpu_load:
          cache_whole_instance = False
          cloud_logging_client.debug(
              'Performance optimization: CPU load exceeds '
              ' MAX_WHOLE_INSTANCE_CACHING_CPU_LOAD; enabling frame batch'
              ' caching.',
              {
                  DICOM_INSTANCE: dicom_instance.dicom_sop_instance_url,
                  CPU_LOAD: cpu_load,
                  MAX_WHOLE_INSTANCE_CACHING_CPU_LOAD: (
                      dicom_proxy_flags.MAX_WHOLE_INSTANCE_CACHING_CPU_LOAD_FLG.value
                  ),
              },
          )
        else:
          percent_memory_used = psutil.Process().memory_percent()
          max_percent_memory = (
              _get_max_precent_memory_used_for_whole_slide_caching()
          )
          if percent_memory_used > max_percent_memory:
            cache_whole_instance = False
            precent_threshold = int(100.0 * max_percent_memory)
            cloud_logging_client.debug(
                f'Memory utilization exceeds {precent_threshold}%; enabling'
                ' frame batch caching.',
                {'percent_system_memory_used': percent_memory_used},
            )
      except OSError:
        pass
  if (
      dicom_instance.metadata.number_of_frames
      < dicom_proxy_flags.PREEMPTIVE_DISPLAY_CACHE_MIN_INSTANCE_FRAME_NUMBER_FLG.value
  ):
    load_display_thread = None
  else:
    load_display_thread = _CacheInstanceFrameBlock(
        dicom_instance,
        frame_indexes_requested,
        dicom_proxy_flags.PREEMPTIVE_DISPLAY_CACHE_PIXEL_DIM_FLG.value,
        dicom_proxy_flags.PREEMPTIVE_DISPLAY_CACHE_PIXEL_DIM_FLG.value,
        require_first_frame=True,
    )
  redis = redis_cache.RedisCache()
  redis_cache_key = _background_cache_key(dicom_instance)
  # Try to set key value pair. Only succeeds if key value pair doesn't exist to
  # avoid re-caching the same instance if under high usage.
  # Key is deleted on error or after timeout or on compeltion for batch frame
  # caching.
  if not redis.set(
      redis_cache_key,
      'Running',
      allow_overwrite=False,
      ttl_sec=dicom_proxy_flags.PREEMPTIVE_WHOLEINSTANCE_RECACHING_TTL_FLG.value
      if cache_whole_instance or force_cache_whole_instance_in_memory
      else 3,
  ):
    _start_display_thread(load_display_thread)
    return False

  with _cache_instance_thread_lock:
    try:
      _remove_dead_cache_threads()
      if _is_another_thread_caching_whole_slide_instance(dicom_instance):
        # Test is a bit redundant with redis check.
        # Validates that whole instance is not being cached on currently.
        # Possible redis check was removed from cache as a result of timeout
        # or pseudo LRU ejection.
        # Keep redis_cache_key lock in place.
        _start_display_thread(load_display_thread)
        return False
      if (
          len(_active_cache_loading_instances)
          >= dicom_proxy_flags.PREEMPTIVE_INSTANCE_CACHE_MAX_THREADS_FLG.value
      ):
        cloud_logging_client.info(
            (
                'Concurrent DICOM instance read threshold hit; preemptive'
                ' caching not started for instance.'
            ),
            {
                DICOM_INSTANCE: dicom_instance.dicom_sop_instance_url,
                PREEMPTIVE_INSTANCE_CACHE_MAX: (
                    dicom_proxy_flags.PREEMPTIVE_INSTANCE_CACHE_MAX_THREADS_FLG.value
                ),
            },
        )
        # delete redis_cache_key to enable instance caching to be retried
        # without delay.
        redis.delete(redis_cache_key)
        _start_display_thread(load_display_thread)
        return False

      if force_cache_whole_instance_in_memory:
        # If size of the instance equal or smaller than batch request.
        # Use threaded whole instance retrieval.  Greatest Performance.
        ch_instance = _CacheWholeInstance(
            dicom_instance,
            running_in_process=False,
            load_display_thread=load_display_thread,
        )
        cache_instance_thread = _create_threaded_cache_loader(ch_instance)
      elif cache_whole_instance:
        ch_instance = _CacheWholeInstance(
            dicom_instance, running_in_process=True, load_display_thread=None
        )
        cache_mp_obj = _get_instance_cache_mp_class()
        cache_instance_thread = cache_mp_obj(
            target=ch_instance.run, daemon=True
        )
        _start_display_thread(load_display_thread)
      else:
        ch_instance = _CacheInstanceFrameBlock(
            dicom_instance,
            frame_indexes_requested,
            dicom_proxy_flags.PREEMPTIVE_INSTANCE_CACHE_BLOCK_PIXEL_DIM_FLG.value,
            dicom_proxy_flags.PREEMPTIVE_INSTANCE_CACHE_BLOCK_PIXEL_DIM_FLG.value,
            load_display_thread=load_display_thread,
        )
        cache_instance_thread = _create_threaded_cache_loader(ch_instance)
      cache_instance_thread.start()
      _active_cache_loading_instances.add(
          _PremptiveCacheLoadingJob(cache_instance_thread, ch_instance)
      )
      return True
    except Exception as exp:
      redis.delete(redis_cache_key)  # Unblock instance and frame block cache.
      cloud_logging_client.error(
          'Unexpected error.',
          {
              DICOM_INSTANCE: dicom_instance.dicom_sop_instance_url,
              CACHE_WHOLE_INSTANCE: cache_whole_instance,
              FRAMES_REQUESTED: frame_indexes_requested,
          },
          exp,
      )
      raise
    finally:
      if not cache_whole_instance and not force_cache_whole_instance_in_memory:
        redis.delete(redis_cache_key)  # Unblock caching another frame block.


os.register_at_fork(after_in_child=_init_fork_module_state)
