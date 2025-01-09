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
"""Function decorator for logging execution time."""
import dataclasses
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import flask_util
from pathology.shared_libs.logging_lib import cloud_logging_client

# String Constants
_THREAD_ID = 'thread_id'
_PROCESS_ID = 'process_id'
_TOTAL_ELAPSED_TIME = 'total_elapsed_time_(sec)'
_REQUEST = 'request'
_LOG_TRACE = 'log_trace'


@dataclasses.dataclass
class _ThreadExecutionTiming:
  """Holds states for execution timing within a thread."""

  stack_depth: int
  function_call_index: int
  thread_lock: threading.Lock
  logged_trace: List[str]
  logged_trace_index: List[int]
  flask_request_path: str

  def pre_function_method_call(self) -> int:
    """Increment class prior to function call.

    Returns:
      Stack position passed to post_function_method_call.
    """
    with self.thread_lock:
      stack_position = self.function_call_index
      self.stack_depth += 1
      self.function_call_index += 1
    return stack_position

  def post_function_method_call(
      self,
      process_id: int,
      thread_id: int,
      stack_position: int,
      func_name: str,
      elapsed_time: float,
  ) -> None:
    """Adds function/method call to trace with timing following its execution.

    Args:
      process_id: Id of calling process.
      thread_id: Id of thread function/method executed in.
      stack_position: Stack position returned from pre_function_method_call.
      func_name: Name of function executed.
      elapsed_time: Execution time.
    """
    with self.thread_lock:
      self.stack_depth -= 1
      self.logged_trace_index.append(stack_position)
      self.logged_trace.append(
          f'{func_name} execution time {elapsed_time} (sec)'
      )
      if self.stack_depth == 0:
        ordered_logs = sorted(
            zip(self.logged_trace_index, self.logged_trace), key=lambda x: x[0]
        )
        log_trace = '\n'.join([log[1] for log in ordered_logs])
        structured_log = {
            _LOG_TRACE: log_trace,
            _THREAD_ID: thread_id,
            _PROCESS_ID: process_id,
            _TOTAL_ELAPSED_TIME: elapsed_time,
        }
        structured_log[_REQUEST] = self.flask_request_path
        cloud_logging_client.info(
            (
                f'{func_name} ({_THREAD_ID}: {thread_id}, '
                f'{_PROCESS_ID}: {process_id}) total execution time '
                f'{elapsed_time} (sec)'
            ),
            structured_log,
        )
        self.logged_trace = []
        self.logged_trace_index = []


_global_lock = threading.Lock()
_global_execution_timer: Dict[int, _ThreadExecutionTiming] = {}


def _create_thread_execution_timing(
    request_path: str,
) -> _ThreadExecutionTiming:
  return _ThreadExecutionTiming(0, 0, threading.Lock(), [], [], request_path)


def _is_running(thread_id: int) -> bool:
  for th in threading.enumerate():
    if th.native_id == thread_id:
      return True
  return False


def log_message_to_execution_time_stack(log: str) -> bool:
  """Log message to execution time stack."""
  if not dicom_proxy_flags.ENABLE_DEBUG_FUNCTION_TIMING_FLG.value:
    return False
  thread_id = threading.get_native_id()
  thread_execution_timing = _global_execution_timer.get(thread_id)
  if thread_execution_timing is None:
    return False

  with thread_execution_timing.thread_lock:
    stack_position = thread_execution_timing.function_call_index
    thread_execution_timing.function_call_index += 1
    thread_execution_timing.logged_trace_index.append(stack_position)
    thread_execution_timing.logged_trace.append(log)
  return True


def _remove_stopped_threads() -> None:
  """Removes stopped threads from _global_execution_timer.

  Not thread safe must be called within _global_lock
  """
  for th_id in list(_global_execution_timer):
    if not _is_running(th_id):
      del _global_execution_timer[th_id]


def _race_condition_test_shim(
    thread_id: int,
) -> Optional[_ThreadExecutionTiming]:
  """Enables mocking for testing race condition in _get_thread_execution_timing."""
  return _global_execution_timer.get(thread_id)


def _get_thread_execution_timing(thread_id: int) -> _ThreadExecutionTiming:
  """Returns _ThreadExecutionTiming for thread_id."""
  thread_execution_timing = _race_condition_test_shim(thread_id)
  if thread_execution_timing is not None:
    return thread_execution_timing
  with _global_lock:
    _remove_stopped_threads()

    # Check if thread_execution_timing was created between first test and lock
    thread_execution_timing = _global_execution_timer.get(thread_id)
    if thread_execution_timing is not None:
      return thread_execution_timing
    # Create and return new _ThreadExecutionTiming class
    thread_execution_timing = _create_thread_execution_timing(
        flask_util.get_full_request_url()
    )
    _global_execution_timer[thread_id] = thread_execution_timing
    return thread_execution_timing


def log_execution_time(func_name: str) -> Callable[..., Any]:
  """Logs function execution time.

  Args:
    func_name: Name of decorated function.

  Returns:
    Decorated function.
  """

  def inner_decorator(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator for logging execution time.

    Args:
      func: Function to decorate.

    Returns:
      Decorated function.
    """

    def inner1(*args, **kwargs) -> Any:
      if not dicom_proxy_flags.ENABLE_DEBUG_FUNCTION_TIMING_FLG.value:
        return func(*args, **kwargs)

      thread_id = threading.get_native_id()
      thread_execution_timing = _get_thread_execution_timing(thread_id)

      stack_position = thread_execution_timing.pre_function_method_call()
      start_time = time.time()
      try:
        return func(*args, **kwargs)
      finally:
        elapsed_time = time.time() - start_time
        thread_execution_timing.post_function_method_call(
            os.getpid(), thread_id, stack_position, func_name, elapsed_time
        )

    return inner1

  return inner_decorator
