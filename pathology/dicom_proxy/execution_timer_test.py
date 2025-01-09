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
"""Tests for execution timer."""
import dataclasses
import os
import re
import threading
import time
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver

from pathology.dicom_proxy import execution_timer
from pathology.dicom_proxy import flask_util
from pathology.shared_libs.logging_lib import cloud_logging_client

_MOCK_FLASK_REQUEST_PATH = '/mock/dicom/url'


class _MockThread(threading.Thread):

  def __init__(self):
    super().__init__()
    self.daemon = True
    self._running = True

  def stop(self) -> None:
    self._running = False

  def run(self):
    while self._running:
      time.sleep(1)


class ThreadExecutionTimingDataclassTest(absltest.TestCase):

  def test_thread_execution_timing_dataclass(self):
    stack_depth = 4
    function_call_index = 5
    thread_lock = None
    logged_trace = ['trace', 'trace2', 'trace3']
    logged_trace_index = [1, 2, 3]
    expected = dict(
        stack_depth=stack_depth,
        function_call_index=function_call_index,
        thread_lock=thread_lock,
        logged_trace=logged_trace,
        logged_trace_index=logged_trace_index,
        flask_request_path=_MOCK_FLASK_REQUEST_PATH,
    )
    dc = execution_timer._ThreadExecutionTiming(
        stack_depth,
        function_call_index,
        thread_lock,  # pytype: disable=wrong-arg-types
        logged_trace,
        logged_trace_index,
        _MOCK_FLASK_REQUEST_PATH,
    )
    dc = dataclasses.asdict(dc)

    self.assertEqual(dc, expected)

  def test_pre_function_method_call(self):
    thread_mon = execution_timer._create_thread_execution_timing(
        _MOCK_FLASK_REQUEST_PATH
    )
    thread_mon.function_call_index = 1

    self.assertEqual(thread_mon.stack_depth, 0)
    self.assertEqual(thread_mon.pre_function_method_call(), 1)
    self.assertEqual(thread_mon.stack_depth, 1)
    self.assertEqual(thread_mon.function_call_index, 2)

  @mock.patch.object(cloud_logging_client, 'info')
  def test_post_function_method_call(self, mock_logging_client):
    process_id = 123
    thread_id = 456
    func_name_1 = 'foo'
    func_name_2 = 'bar'
    elapsed_time_1 = 9.56
    elapsed_time_2 = 7.56
    thread_mon = execution_timer._create_thread_execution_timing(
        _MOCK_FLASK_REQUEST_PATH
    )

    stack_position_1 = thread_mon.pre_function_method_call()
    stack_position_2 = thread_mon.pre_function_method_call()
    thread_mon.post_function_method_call(
        process_id, thread_id, stack_position_2, func_name_2, elapsed_time_2
    )
    thread_mon.post_function_method_call(
        process_id, thread_id, stack_position_1, func_name_1, elapsed_time_1
    )

    mock_logging_client.assert_called_once_with(
        (
            f'{func_name_1} ({execution_timer._THREAD_ID}: {thread_id}, '
            f'{execution_timer._PROCESS_ID}: {process_id}) '
            f'total execution time {elapsed_time_1} (sec)'
        ),
        {
            execution_timer._LOG_TRACE: (
                f'{func_name_1} execution time {elapsed_time_1} (sec)\n'
                f'{func_name_2} execution time {elapsed_time_2} (sec)'
            ),
            execution_timer._THREAD_ID: thread_id,
            execution_timer._PROCESS_ID: process_id,
            execution_timer._TOTAL_ELAPSED_TIME: elapsed_time_1,
            execution_timer._REQUEST: _MOCK_FLASK_REQUEST_PATH,
        },
    )


@execution_timer.log_execution_time('_test_innerfunc_2')
def _test_innerfunc_2():
  time.sleep(0.25)
  execution_timer.log_message_to_execution_time_stack('msg2')


@execution_timer.log_execution_time('_test_innerfunc_1')
def _test_innerfunc_1():
  time.sleep(0.25)
  execution_timer.log_message_to_execution_time_stack('msg1')
  _test_innerfunc_2()


@execution_timer.log_execution_time('_test_decorated_function')
def _test_decorated_function():
  time.sleep(0.25)
  _test_innerfunc_1()
  execution_timer.log_message_to_execution_time_stack('msg0')
  _test_innerfunc_1()


class ExecutionTimerTest(absltest.TestCase):

  def test_is_running(self):
    test_thread = _MockThread()
    try:
      test_thread.start()
      time.sleep(1)
      self.assertTrue(execution_timer._is_running(test_thread.native_id))
    finally:
      test_thread.stop()

  @flagsaver.flagsaver(enable_debug_function_timing=False)
  @mock.patch.object(threading, 'get_native_id', autospec=True)
  def test_log_message_to_execution_time_stack_disabled(self, get_native_id):
    self.assertFalse(execution_timer.log_message_to_execution_time_stack('msg'))
    get_native_id.assert_not_called()

  @flagsaver.flagsaver(enable_debug_function_timing=True)
  @mock.patch.object(
      threading, 'get_native_id', autospec=True, return_value='fake_thread_id'
  )
  def test_log_message_to_execution_time_thread_not_running(
      self, get_native_id
  ):
    self.assertFalse(execution_timer.log_message_to_execution_time_stack('msg'))
    get_native_id.assert_called_once()

  @flagsaver.flagsaver(enable_debug_function_timing=True)
  @mock.patch.object(
      threading, 'get_native_id', return_value='MOCKVAL1', autospec=True
  )
  def test_log_message_to_execution_time_succeed(self, _):
    t_id = threading.get_native_id()
    with execution_timer._global_lock:
      thread_exec_timing = execution_timer._create_thread_execution_timing(
          _MOCK_FLASK_REQUEST_PATH
      )
      execution_timer._global_execution_timer[t_id] = thread_exec_timing

    self.assertTrue(execution_timer.log_message_to_execution_time_stack('msg1'))
    self.assertTrue(execution_timer.log_message_to_execution_time_stack('msg2'))

    with execution_timer._global_lock:
      self.assertEqual(thread_exec_timing.function_call_index, 2)
      self.assertEqual(thread_exec_timing.logged_trace_index, [0, 1])
      self.assertEqual(thread_exec_timing.logged_trace, ['msg1', 'msg2'])
      del execution_timer._global_execution_timer[t_id]

  def test_remove_stopped_threads(self):
    with execution_timer._global_lock:
      execution_timer._global_execution_timer[-1] = (
          execution_timer._create_thread_execution_timing(
              _MOCK_FLASK_REQUEST_PATH
          )
      )
      execution_timer._global_execution_timer[-2] = (
          execution_timer._create_thread_execution_timing(
              _MOCK_FLASK_REQUEST_PATH
          )
      )

      execution_timer._remove_stopped_threads()

      self.assertNotIn(-1, execution_timer._global_execution_timer)
      self.assertNotIn(-2, execution_timer._global_execution_timer)

  @flagsaver.flagsaver(enable_debug_function_timing=True)
  @mock.patch.object(
      threading, 'get_native_id', return_value='MOCKVAL2', autospec=True
  )
  @mock.patch.object(execution_timer, '_remove_stopped_threads', autospec=True)
  def test_get_thread_execution_timing_pre_existing_thread(
      self, mock_remove_stopped_threads, _
  ):
    thread_id = threading.get_native_id()
    with execution_timer._global_lock:
      expected_instance = execution_timer._create_thread_execution_timing(
          _MOCK_FLASK_REQUEST_PATH
      )
      execution_timer._global_execution_timer[thread_id] = expected_instance

    result = execution_timer._get_thread_execution_timing(thread_id)

    self.assertIs(result, expected_instance)
    mock_remove_stopped_threads.assert_not_called()

  @flagsaver.flagsaver(enable_debug_function_timing=True)
  @mock.patch.object(
      threading, 'get_native_id', return_value='MOCKVAL3', autospec=True
  )
  @mock.patch.object(execution_timer, '_remove_stopped_threads', autospec=True)
  @mock.patch.object(flask_util, 'get_full_request_url', autospec=True)
  def test_get_thread_execution_timing_create_new_thread(
      self, mock_get_url, mock_remove_stopped_threads, _
  ):
    mock_get_url.return_value = 'https://digital_pathology_test.org'
    thread_id = threading.get_native_id()
    with execution_timer._global_lock:
      self.assertNotIn(thread_id, execution_timer._global_execution_timer)

    result = execution_timer._get_thread_execution_timing(thread_id)

    with execution_timer._global_lock:
      self.assertIsNotNone(result)
      self.assertIn(result, execution_timer._global_execution_timer.values())
    mock_remove_stopped_threads.assert_called_once()

  @flagsaver.flagsaver(enable_debug_function_timing=True)
  @mock.patch.object(
      threading, 'get_native_id', return_value='MOCKVAL2', autospec=True
  )
  @mock.patch.object(
      execution_timer,
      '_race_condition_test_shim',
      autospec=True,
      return_value=None,
  )
  @mock.patch.object(execution_timer, '_remove_stopped_threads', autospec=True)
  def test_get_thread_execution_timing_create_new_thread_race_condition(
      self, mock_remove_stopped_threads, mock_race_condition_test_shim, _
  ):
    thread_id = threading.get_native_id()
    with execution_timer._global_lock:
      expected_instance = execution_timer._create_thread_execution_timing(
          _MOCK_FLASK_REQUEST_PATH
      )
      execution_timer._global_execution_timer[thread_id] = expected_instance

    result = execution_timer._get_thread_execution_timing(thread_id)

    with execution_timer._global_lock:
      self.assertIs(expected_instance, result)
      self.assertIs(
          expected_instance, execution_timer._global_execution_timer[thread_id]
      )
    mock_remove_stopped_threads.assert_called_once()
    mock_race_condition_test_shim.assert_called_once()

  @mock.patch.object(threading, 'get_native_id', autospec=True)
  @mock.patch.object(
      cloud_logging_client.CloudLoggingClient, 'debug', autospec=True
  )
  @flagsaver.flagsaver(enable_debug_function_timing=False)
  def test_function_decorator_func_timing_disabled(
      self, mock_logging_client, mock_get_native_id
  ):
    _test_decorated_function()
    mock_get_native_id.assert_not_called()
    mock_logging_client.assert_not_called()

  @mock.patch.object(
      threading, 'get_native_id', return_value='MOCKVAL4', autospec=True
  )
  @mock.patch.object(
      flask_util,
      'get_full_request_url',
      autospec=True,
      return_value=_MOCK_FLASK_REQUEST_PATH,
  )
  @mock.patch.object(cloud_logging_client.CloudLoggingClient, 'info')
  @flagsaver.flagsaver(enable_debug_function_timing=True)
  def test_function_decorator_func(
      self, mock_logging_client, mock_get_flask_request_path, _
  ):
    url = f'https://dpas.cloudflyer.info{_MOCK_FLASK_REQUEST_PATH}'
    mock_get_flask_request_path.return_value = url
    process_id = os.getpid()
    thread_id = threading.get_native_id()

    _test_decorated_function()
    mock_logging_client.assert_called_once()
    arg_list = mock_logging_client.call_args[0]
    self.assertLen(arg_list, 2)
    logged_message = arg_list[0]
    result = re.fullmatch(
        (
            r'_test_decorated_function \('
            f'{execution_timer._THREAD_ID}: '
            f'{thread_id}, {execution_timer._PROCESS_ID}: {process_id}'
            r'\) total execution time (.+) \(sec\)'
        ),
        logged_message,
    )
    self.assertIsNotNone(result)
    self.assertGreaterEqual(float(result.groups()[0]), 1.25)  # pytype: disable=attribute-error

    structed_log = arg_list[1]
    self.assertEqual(
        set(structed_log.keys()),
        {
            execution_timer._THREAD_ID,
            execution_timer._PROCESS_ID,
            execution_timer._TOTAL_ELAPSED_TIME,
            execution_timer._REQUEST,
            execution_timer._LOG_TRACE,
        },
    )
    self.assertEqual(structed_log[execution_timer._THREAD_ID], thread_id)
    self.assertEqual(structed_log[execution_timer._PROCESS_ID], process_id)
    self.assertGreaterEqual(
        structed_log[execution_timer._TOTAL_ELAPSED_TIME], 1.25
    )
    self.assertEqual(structed_log[execution_timer._REQUEST], url)
    log_trace = structed_log[execution_timer._LOG_TRACE]

    regex = [
        r'_test_decorated_function execution time (.+) \(sec\)',
        r'_test_innerfunc_1 execution time (.+) \(sec\)',
        'msg1',
        r'_test_innerfunc_2 execution time (.+) \(sec\)',
        'msg2',
        'msg0',
        r'_test_innerfunc_1 execution time (.+) \(sec\)',
        'msg1',
        r'_test_innerfunc_2 execution time (.+) \(sec\)',
        'msg2',
    ]

    result = re.fullmatch('\n'.join(regex), log_trace)
    self.assertIsNotNone(result)
    exec_time = list(result.groups())  # pytype: disable=attribute-error
    self.assertLen(exec_time, 5)
    self.assertGreaterEqual(float(exec_time[0]), 1.25)
    self.assertGreaterEqual(float(exec_time[1]), 0.50)
    self.assertGreaterEqual(float(exec_time[2]), 0.25)
    self.assertGreaterEqual(float(exec_time[3]), 0.50)
    self.assertGreaterEqual(float(exec_time[4]), 0.25)


if __name__ == '__main__':
  absltest.main()
