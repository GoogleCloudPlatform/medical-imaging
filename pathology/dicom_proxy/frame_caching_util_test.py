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
"""Tests for downsample util."""

import threading
import time
from typing import Any, List, Mapping, Optional, Tuple
from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import redis

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import dicom_instance_request
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import frame_caching_util
from pathology.dicom_proxy import frame_retrieval_util
from pathology.dicom_proxy import metadata_util
from pathology.dicom_proxy import parameters_exceptions_and_return_types
from pathology.dicom_proxy import redis_cache
from pathology.dicom_proxy import render_frame_params
from pathology.dicom_proxy import shared_test_util
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.logging_lib import cloud_logging_client


# Mark flags as parsed to avoid UnparsedFlagAccessError for tests using
# --test_srcdir flag in parameters.
flags.FLAGS.mark_as_parsed()

# Types
_AuthSession = user_auth_util.AuthSession
_LocalDicomInstance = parameters_exceptions_and_return_types.LocalDicomInstance
_RenderFrameParams = render_frame_params.RenderFrameParams
_DicomInstanceWebRequest = (
    parameters_exceptions_and_return_types.DicomInstanceWebRequest
)
_DicomInstanceRequest = dicom_instance_request.DicomInstanceRequest


def _test_dicom_instance_web_request(
    sop_instance_uid: Optional[str] = None,
) -> Tuple[_DicomInstanceWebRequest, Mapping[str, Any]]:
  params = dict(
      api_version='v1',
      project='test-project',
      location='us-west1',
      dataset='bigdata',
      dicom_store='bigdicomstore',
      study='1.2.3',
      series='1.2.3.4',
      instance='1.2.3.4.5' if sop_instance_uid is None else sop_instance_uid,
      authorization='test_auth',
      authority='test_authority',
      enable_caching=True,
  )
  web_request = _DicomInstanceWebRequest(
      _AuthSession({
          'authorization': params['authorization'],
          'authority': params['authority'],
      }),
      dicom_url_util.DicomWebBaseURL(
          params['api_version'],
          params['project'],
          params['location'],
          params['dataset'],
          params['dicom_store'],
      ),
      dicom_url_util.StudyInstanceUID(params['study']),
      dicom_url_util.SeriesInstanceUID(params['series']),
      dicom_url_util.SOPInstanceUID(params['instance']),
      cache_enabled_type.CachingEnabled(params['enable_caching']),
      {},
      {},
  )
  return (web_request, params)


class MockCacheInstance(frame_caching_util._CacheWholeInstance):

  def __init__(self, dicom_instance: _DicomInstanceRequest):
    super().__init__(dicom_instance, True, None)
    self._stopsignaled = False

  def run(self):
    while not self._stopsignaled:
      time.sleep(0.25)

  def signal_stop(self):
    self._stopsignaled = True


class FrameCachingUtilTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    # init frame caching globals
    redis_cache._redis_host_instance = None
    frame_caching_util._cache_instance_thread_lock = threading.Lock()
    frame_caching_util._active_cache_loading_instances = set()
    self.enter_context(
        mock.patch.object(
            frame_caching_util,
            '_get_instance_cache_mp_class',
            return_value=threading.Thread,
        )
    )

  def tearDown(self):
    # ensure any started background thread / process are complete
    frame_caching_util.wait_for_cache_loading_threads()
    redis_cache._redis_host_instance = None
    super().tearDown()

  @mock.patch.object(redis.Redis, 'set')
  def test_cache_entire_instance_jpeg(self, mock_redis_set):
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    frames_processed = frame_caching_util._cache_entire_instance(local_instance)
    self.assertTrue(local_instance.metadata.is_baseline_jpeg)
    self.assertEqual(frames_processed, 15)
    self.assertEqual(frames_processed, mock_redis_set.call_count)

  @mock.patch.object(redis.Redis, 'set')
  def test_cache_entire_instance_jpeg2000(self, mock_redis_set):
    local_instance = shared_test_util.jpeg2000_encoded_dicom_local_instance()
    frames_processed = frame_caching_util._cache_entire_instance(local_instance)
    self.assertTrue(local_instance.metadata.is_jpeg2000)
    self.assertEqual(frames_processed, 28)
    self.assertEqual(frames_processed, mock_redis_set.call_count)

  def test_background_cache_key(self):
    instance_webrequest, params = _test_dicom_instance_web_request()
    self.assertEqual(
        frame_caching_util._background_cache_key(instance_webrequest),
        (
            'PreemptiveFrameCaching: https://healthcare.googleapis.com/v1/'
            f'projects/{params["project"]}/locations/{params["location"]}/datasets'
            f'/{params["dataset"]}/dicomStores/{params["dicom_store"]}/dicomWeb/'
            f'studies/{params["study"]}/series/{params["series"]}/instances/'
            f'{params["instance"]}'
        ),
    )

  @mock.patch.object(time, 'time', autospec=True, return_value=10)
  def test_get_completion_msg_and_log(self, unused_mock_time):
    instance_webrequest, _ = _test_dicom_instance_web_request()
    thread_instance = frame_caching_util._CacheWholeInstance(
        instance_webrequest, True, None
    )
    instance_webrequest._source_instance_metadata = mock.create_autospec(
        metadata_util.DicomInstanceMetadata
    )
    instance_webrequest._source_instance_metadata.number_of_frames = 4
    msg, log = thread_instance._get_completion_msg_and_log(0)
    self.assertEqual(
        msg,
        'Preemptive whole instance caching stopped; elapsed_time: 10.000(sec)',
    )
    self.assertEqual(
        log,
        {
            'DICOM_SOPInstanceUID': (
                'https://healthcare.googleapis.com/v1/projects/test-project/'
                'locations/us-west1/datasets/bigdata/dicomStores/bigdicomstore'
                '/dicomWeb/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5'
            ),
            'elapsed_time(sec)': '10',
            'cache_whole_instance': 'True',
            'number_of_frames_in_dicom_instance': '4',
        },
    )

  @mock.patch.object(redis.Redis, 'delete')
  @mock.patch.object(redis.Redis, 'set')
  def test_cache_instance_thread(self, mock_redis_set, mock_redis_delete):
    expected_time = 0.102
    time_scale_factor = 100
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    thread_instance = frame_caching_util._create_threaded_cache_loader(
        frame_caching_util._CacheWholeInstance(local_instance, False, None)
    )
    thread_instance.start()
    thread_instance.join(expected_time * time_scale_factor)
    self.assertFalse(thread_instance.is_alive())
    frames_processed = local_instance.metadata.number_of_frames

    mock_redis_delete.assert_not_called()
    self.assertEqual(frames_processed, 15)
    self.assertEqual(frames_processed, mock_redis_set.call_count)

  @mock.patch.object(
      frame_caching_util, '_cache_entire_instance', side_effect=ValueError
  )
  @mock.patch.object(redis.Redis, 'delete')
  @mock.patch.object(redis.Redis, 'set')
  def test_cache_instance_thread_throws(
      self, mock_redis_set, mock_redis_delete, unused_mock_error
  ):
    expected_time = 0.102
    time_scale_factor = 100
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    thread_instance = frame_caching_util._create_threaded_cache_loader(
        frame_caching_util._CacheWholeInstance(local_instance, True, None)
    )
    thread_instance.start()
    thread_instance.join(expected_time * time_scale_factor)
    self.assertFalse(thread_instance.is_alive())
    mock_redis_delete.assert_called()
    self.assertEqual(mock_redis_set.call_count, 0)

  @parameterized.parameters([True, False])
  @mock.patch.object(redis.Redis, 'set')
  @flagsaver.flagsaver(disable_preemptive_instance_frame_cache=True)
  def test_cache_instance_frames_in_background_flag_disabled(
      self, cache_whole_instance, mock_redis_set
  ):
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    with flagsaver.flagsaver(
        enable_preemptive_wholeinstance_caching=cache_whole_instance
    ):
      self.assertFalse(
          frame_caching_util.cache_instance_frames_in_background(
              local_instance, [1]
          )
      )
    mock_redis_set.assert_not_called()

  @parameterized.parameters([True, False])
  @mock.patch.object(redis.Redis, 'set')
  @flagsaver.flagsaver(disable_preemptive_instance_frame_cache=False)
  def test_cache_instance_frames_in_background_not_valid_transfer_syntax(
      self, cache_whole_instance, mock_redis_set
  ):
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance(
        {'dicom_transfer_syntax': 'abcdefg'}
    )
    with flagsaver.flagsaver(
        enable_preemptive_wholeinstance_caching=cache_whole_instance
    ):
      self.assertFalse(
          frame_caching_util.cache_instance_frames_in_background(
              local_instance, [1]
          )
      )
    mock_redis_set.assert_not_called()

  @parameterized.parameters([True, False])
  @flagsaver.flagsaver(disable_preemptive_instance_frame_cache=False)
  @mock.patch.object(cloud_logging_client.CloudLoggingClient, 'info')
  @mock.patch.object(redis.Redis, 'set')
  def test_cache_instance_frames_in_background_exceeds_max_frame_number(
      self, cache_whole_instance, unused_mock_redis_set, unused_mock_logger
  ):
    url = 'test_url'
    max_frames = 5
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    local_instance.dicom_sop_instance_url = url
    with flagsaver.flagsaver(
        preemptive_instance_cache_max_instance_frame_number=max_frames,
        enable_preemptive_wholeinstance_caching=cache_whole_instance,
    ):
      self.assertTrue(
          frame_caching_util.cache_instance_frames_in_background(
              local_instance, [1]
          )
      )
      self.assertLen(frame_caching_util._active_cache_loading_instances, 1)

  def test_remove_dead_cache_threads(self):
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    mock_cache_instance = MockCacheInstance(local_instance)
    th = frame_caching_util._create_threaded_cache_loader(mock_cache_instance)
    frame_caching_util._active_cache_loading_instances.add(
        frame_caching_util._PremptiveCacheLoadingJob(th, mock_cache_instance)
    )
    th.start()
    self.assertLen(frame_caching_util._active_cache_loading_instances, 1)
    frame_caching_util._remove_dead_cache_threads()
    mock_cache_instance.signal_stop()
    th.join()
    frame_caching_util._remove_dead_cache_threads()
    self.assertEmpty(frame_caching_util._active_cache_loading_instances)

  def test_is_another_thread_caching_whole_slide_instance_true(self):
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    mock_cache_instance = MockCacheInstance(local_instance)
    frame_caching_util._active_cache_loading_instances.add(
        frame_caching_util._PremptiveCacheLoadingJob(
            threading.Thread(target=lambda: None),
            mock_cache_instance,
        )
    )
    self.assertTrue(
        frame_caching_util._is_another_thread_caching_whole_slide_instance(
            local_instance
        )
    )

  def test_is_another_thread_caching_whole_slide_instance_false(self):
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    self.assertFalse(
        frame_caching_util._is_another_thread_caching_whole_slide_instance(
            local_instance
        )
    )

  @mock.patch.object(redis.Redis, 'delete')
  @mock.patch.object(redis.Redis, 'set')
  def test_process_running_another_thread_second_thread_not_started(
      self, unused_mock_redis_set, mock_redis_delete
  ):
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    mock_cache_instance = MockCacheInstance(local_instance)
    th = frame_caching_util._create_threaded_cache_loader(mock_cache_instance)
    try:
      th.start()
      frame_caching_util._active_cache_loading_instances.add(
          frame_caching_util._PremptiveCacheLoadingJob(th, mock_cache_instance)
      )
      with mock.patch.object(
          threading.Thread, 'start', autospec=True
      ) as mock_th:
        with flagsaver.flagsaver(
            max_whole_instance_caching_cpu_load=100,
            enable_preemptive_wholeinstance_caching=True,
        ):
          mock_redis_delete.assert_not_called()
          self.assertFalse(
              frame_caching_util.cache_instance_frames_in_background(
                  local_instance, [1]
              )
          )
        mock_th.assert_not_called()
        mock_redis_delete.assert_not_called()
    finally:
      mock_cache_instance.signal_stop()

  @mock.patch.object(redis.Redis, 'delete')
  @mock.patch.object(redis.Redis, 'set')
  @flagsaver.flagsaver(
      preemptive_instance_cache_max_threads=1,
      enable_preemptive_wholeinstance_caching=True,
  )
  def test_exceeding_preemptive_instance_cache_max_threads_requeues(
      self, unused_mock_redis_set, mock_redis_delete
  ):
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    another_local_instance = (
        shared_test_util.jpeg_encoded_dicom_local_instance()
    )
    another_local_instance.dicom_sop_instance_url = 'bogus'
    mock_cache_instance = MockCacheInstance(local_instance)
    th = frame_caching_util._create_threaded_cache_loader(mock_cache_instance)
    try:
      th.start()
      frame_caching_util._active_cache_loading_instances.add(
          frame_caching_util._PremptiveCacheLoadingJob(th, mock_cache_instance)
      )
      with mock.patch.object(
          threading.Thread, 'start', autospec=True
      ) as mock_th:
        self.assertFalse(
            frame_caching_util.cache_instance_frames_in_background(
                another_local_instance, [1]
            )
        )
        mock_th.assert_not_called()
        # Test that redis key deleted to enable retrying later
        mock_redis_delete.assert_called()
    finally:
      mock_cache_instance.signal_stop()

  @parameterized.named_parameters([
      dict(
          testcase_name='jpeg_encoded_force_cache_whole_instance_true',
          local_instance=shared_test_util.jpeg_encoded_dicom_local_instance(),
          cache_whole_instance=True,
          force_whole_instance_caching=True,
      ),
      dict(
          testcase_name='jpeg2000_encoded_force_cache_whole_instance_true',
          local_instance=shared_test_util.jpeg2000_encoded_dicom_local_instance(),
          cache_whole_instance=True,
          force_whole_instance_caching=True,
      ),
      dict(
          testcase_name='jpeg_encoded_cache_whole_instance_true',
          local_instance=shared_test_util.jpeg_encoded_dicom_local_instance(),
          cache_whole_instance=True,
          force_whole_instance_caching=False,
      ),
      dict(
          testcase_name='jpeg2000_encoded_cache_whole_instance_true',
          local_instance=shared_test_util.jpeg2000_encoded_dicom_local_instance(),
          cache_whole_instance=True,
          force_whole_instance_caching=False,
      ),
      dict(
          testcase_name='jpeg_encoded_cache_whole_instance_false',
          local_instance=shared_test_util.jpeg_encoded_dicom_local_instance(),
          cache_whole_instance=False,
          force_whole_instance_caching=False,
      ),
      dict(
          testcase_name='jpeg2000_encoded_cache_whole_instance_false',
          local_instance=shared_test_util.jpeg2000_encoded_dicom_local_instance(),
          cache_whole_instance=False,
          force_whole_instance_caching=False,
      ),
  ])
  @mock.patch.object(redis.Redis, 'delete')
  @mock.patch.object(redis.Redis, 'set')
  def test_cache_instance_frames_in_background_happy_path(
      self,
      unused_mock_redis_set,
      mock_redis_delete,
      local_instance,
      cache_whole_instance,
      force_whole_instance_caching,
  ):
    try:
      with flagsaver.flagsaver(
          preemptive_instance_cache_min_instance_frame_number=1,
          max_whole_instance_caching_cpu_load=100,
          enable_preemptive_wholeinstance_caching=cache_whole_instance,
      ):
        with mock.patch.object(
            frame_caching_util,
            '_force_cache_whole_instance_in_memory',
            autospec=True,
            return_value=force_whole_instance_caching,
        ):
          self.assertTrue(
              frame_caching_util.cache_instance_frames_in_background(
                  local_instance, [1]
              )
          )
      self.assertLen(frame_caching_util._active_cache_loading_instances, 1)
      th = list(frame_caching_util._active_cache_loading_instances)[
          0
      ].thread_or_process
      start_time = time.time()
      while th.is_alive():
        time.sleep(1)
        if time.time() - start_time > 120:
          raise ValueError('Test should have finished. Timeout')
      # Test that redis key never deleted makes sure instance not re-cached
      # before time key ttl.
      self.assertEqual(
          mock_redis_delete.called,
          not cache_whole_instance and not force_whole_instance_caching,
      )
    finally:
      frame_caching_util._active_cache_loading_instances = set()

  @mock.patch.object(threading.Thread, 'start', side_effect=RuntimeError)
  @mock.patch.object(redis.Redis, 'delete')
  @mock.patch.object(redis.Redis, 'set')
  @flagsaver.flagsaver(enable_preemptive_wholeinstance_caching=True)
  def test_cache_instance_frames_in_background_raises_error_starting_th(
      self, unused_mock_redis_set, mock_redis_delete, unused_th_start
  ):
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    with self.assertRaises(RuntimeError):
      try:
        frame_caching_util.cache_instance_frames_in_background(
            local_instance, [1]
        )
      finally:
        frame_caching_util._active_cache_loading_instances = set()
    # Test that redis key deleted to enable retrying later
    mock_redis_delete.assert_called()

  @mock.patch.object(redis.Redis, 'delete')
  @mock.patch.object(redis.Redis, 'set', return_value=None)
  def test_cache_collision_blocks_start(
      self, mock_redis_set, mock_redis_delete
  ):
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    with flagsaver.flagsaver(
        max_whole_instance_caching_cpu_load=100,
        enable_preemptive_wholeinstance_caching=True,
    ):
      self.assertFalse(
          frame_caching_util.cache_instance_frames_in_background(
              local_instance, [1]
          )
      )
    mock_redis_set.assert_called_once_with(
        f'PreemptiveFrameCaching: {local_instance.dicom_sop_instance_url}',
        b'Running',
        nx=True,
        ex=300,
    )
    mock_redis_delete.assert_not_called()

  @parameterized.parameters([True, False])
  def test_cache_instance_frames_less_than_frame_minimium_not_preemptive_cached(
      self, cache_whole_instance
  ):
    with flagsaver.flagsaver(
        preemptive_instance_cache_min_instance_frame_number=90,
        enable_preemptive_wholeinstance_caching=cache_whole_instance,
    ):
      self.assertFalse(
          frame_caching_util.cache_instance_frames_in_background(
              shared_test_util.jpeg2000_encoded_dicom_local_instance(), [1]
          )
      )

  @parameterized.parameters([True, False])
  def test_cache_instance_frames_in_background_no_frames_nop(
      self, cache_whole_instance: bool
  ):
    with flagsaver.flagsaver(
        enable_preemptive_wholeinstance_caching=cache_whole_instance
    ):
      self.assertFalse(
          frame_caching_util.cache_instance_frames_in_background(
              shared_test_util.jpeg2000_encoded_dicom_local_instance(), []
          )
      )

  @parameterized.named_parameters([
      dict(
          testcase_name='cache_filled_with_prexisting_values_nop',
          indices_with_initialized_cache_values=[9, 10, 14, 15],
          expected_cache_values=[
              b'badfood_9',
              b'badfood_10',
              b'badfood_14',
              b'badfood_15',
          ],
      ),
      dict(
          testcase_name='cache_range_clipped_due_to_prexisting_val_middle_value_initialized',
          indices_with_initialized_cache_values=[9, 15],
          expected_cache_values=[
              b'badfood_9',
              b'set_value_10',
              b'set_value_14',
              b'badfood_15',
          ],
      ),
      dict(
          testcase_name='cache_range_empty_all_values_initialized',
          indices_with_initialized_cache_values=[],
          expected_cache_values=[
              b'set_value_9',
              b'set_value_10',
              b'set_value_14',
              b'set_value_15',
          ],
      ),
      dict(
          testcase_name=(
              'cache_range_starts_with_prexisting_value_remainder_initialized'
          ),
          indices_with_initialized_cache_values=[9],
          expected_cache_values=[
              b'badfood_9',
              b'set_value_10',
              b'set_value_14',
              b'set_value_15',
          ],
      ),
      dict(
          testcase_name='cache_range_ends_with_prexisting_value_starting_portion_initialized',
          indices_with_initialized_cache_values=[15],
          expected_cache_values=[
              b'set_value_9',
              b'set_value_10',
              b'set_value_14',
              b'badfood_15',
          ],
      ),
      dict(
          testcase_name=(
              'cache_range_starts_and_end_not_initialized_middle_initialized_all'
              '_values_initialized'
          ),
          indices_with_initialized_cache_values=[10, 14],
          expected_cache_values=[
              b'set_value_9',
              b'badfood_10',
              b'badfood_14',
              b'set_value_15',
          ],
      ),
  ])
  @mock.patch.object(
      frame_retrieval_util, 'get_local_frame_list', autospec=True
  )
  @mock.patch('redis.Redis', autospec=True)
  def test_cache_frame_block_loading_success_load_none(
      self,
      mock_redis,
      mock_get_raw_frames,
      indices_with_initialized_cache_values,
      expected_cache_values,
  ):
    def _mock_get_raw_frames(
        unused_py_dicom_file,
        frame_number_list: List[int],
        frame_numbers_start_at_index_0: bool = True,
    ) -> List[frame_retrieval_util.FrameData]:
      frame_list = []
      for index in frame_number_list:
        frame_data = f'set_value_{index}'.encode('utf-8')
        if frame_numbers_start_at_index_0:
          index += 1
        frame_caching_util.set_cached_frame(
            cache,
            _AuthSession(None),
            instance_url,
            params,
            None,
            True,
            frame_data,
            index,
        )
        frame_list.append(frame_retrieval_util.FrameData(frame_data, False))
      return frame_list

    user_auth = _AuthSession(None)
    redis_mock = shared_test_util.RedisMock()
    mock_redis.return_value = redis_mock
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    instance_url = dicom_url_util.DicomSopInstanceUrl(
        local_instance.dicom_sop_instance_url
    )
    params = frame_caching_util.get_cache_render_params(
        instance_url, local_instance.metadata
    )
    cache = redis_cache.RedisCache()
    mock_get_raw_frames.side_effect = _mock_get_raw_frames
    for index in indices_with_initialized_cache_values:
      frame_caching_util.set_cached_frame(
          cache,
          user_auth,
          instance_url,
          params,
          None,
          True,
          f'badfood_{index}'.encode('utf-8'),
          index,
      )
    with flagsaver.flagsaver(
        enable_preemptive_wholeinstance_caching=False,
        preemptive_instance_cache_block_pixel_dim=500,
    ):
      self.assertTrue(
          frame_caching_util.cache_instance_frames_in_background(
              local_instance, [10]
          )
      )
    th = list(frame_caching_util._active_cache_loading_instances)[
        0
    ].thread_or_process
    start_time = time.time()
    while th.is_alive():
      time.sleep(1)
      if time.time() - start_time > 120:
        raise ValueError('Test should have finished. Timeout')

    expected_cache_values.insert(0, None)
    indexes = [8, 9, 10, 14, 15]
    for index, frame_data in zip(indexes, expected_cache_values):
      cache_key = frame_retrieval_util.frame_lru_cache_key(
          dicom_url_util.download_dicom_raw_frame(
              user_auth,
              instance_url,
              [index],
              params,
          )
      )
      self.assertEqual(redis_mock.get(cache_key), frame_data)

  @parameterized.named_parameters([
      dict(
          testcase_name='jpeg',
          dicom_instance=shared_test_util.jpeg_encoded_dicom_local_instance(),
          expected_compression=enum_types.Compression.JPEG,
      ),
      dict(
          testcase_name='jpeg2000',
          dicom_instance=shared_test_util.jpeg2000_encoded_dicom_local_instance(),
          expected_compression=enum_types.Compression.JPEG2000,
      ),
  ])
  def test_get_cache_render_params_succeeds(
      self, dicom_instance, expected_compression
  ):
    params = frame_caching_util.get_cache_render_params(
        dicom_instance.dicom_sop_instance_url, dicom_instance.metadata
    )
    self.assertEqual(params.downsample, 1.0)
    self.assertEqual(params.compression, expected_compression)

  def test_get_cache_render_params_invalid_transfer_syntax_raises(self):
    instance = shared_test_util.jpeg_encoded_dicom_local_instance(
        {'dicom_transfer_syntax': '123'}
    )
    with self.assertRaises(
        frame_caching_util.UnexpectedDicomTransferSyntaxError
    ):
      frame_caching_util.get_cache_render_params(
          instance.dicom_sop_instance_url, instance.metadata
      )

  @parameterized.named_parameters([
      dict(
          testcase_name='unclipped_request',
          request_height_px=4,
          request_width_px=4,
          first_frame=8,
          frames_per_row=5,
          frames_per_column=4,
          frame_row_dim=2,
          frame_col_dim=3,
          expected=(1, 3, 2, 4),
      ),
      dict(
          testcase_name='unclipped_request_expanded_dim',
          request_height_px=8,
          request_width_px=8,
          first_frame=7,
          frames_per_row=5,
          frames_per_column=5,
          frame_row_dim=2,
          frame_col_dim=2,
          expected=(0, 1, 3, 4),
      ),
      dict(
          testcase_name='upperleft_expanded_dim',
          request_height_px=8,
          request_width_px=8,
          first_frame=0,
          frames_per_row=5,
          frames_per_column=5,
          frame_row_dim=2,
          frame_col_dim=2,
          expected=(0, 0, 3, 3),
      ),
      dict(
          testcase_name='lowerright_expanded_dim',
          request_height_px=8,
          request_width_px=8,
          first_frame=24,
          frames_per_row=5,
          frames_per_column=5,
          frame_row_dim=2,
          frame_col_dim=2,
          expected=(1, 1, 4, 4),
      ),
  ])
  @flagsaver.flagsaver(preemptive_display_cache_pixel_dim=8)
  def test_get_frame_request_region(
      self,
      request_height_px,
      request_width_px,
      first_frame,
      frames_per_row,
      frames_per_column,
      frame_row_dim,
      frame_col_dim,
      expected,
  ):
    self.assertEqual(
        frame_caching_util._get_frame_request_region(
            request_height_px,
            request_width_px,
            first_frame,
            frames_per_row,
            frames_per_column,
            frame_row_dim,
            frame_col_dim,
        ),
        expected,
    )

  @parameterized.named_parameters([
      dict(testcase_name='above_batch_region', row_index=0, expected=None),
      dict(
          testcase_name='first_row_batch_region',
          row_index=5,
          expected=(7, 9),
      ),
      dict(
          testcase_name='second_row_batch_region',
          row_index=10,
          expected=(12, 14),
      ),
      dict(testcase_name='below_batch_region', row_index=15, expected=None),
  ])
  def test_get_start_end_frame_numbers_in_row(self, row_index, expected):
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    batch_cache_instance = frame_caching_util._CacheInstanceFrameBlock(
        local_instance, [12], 500, 500
    )
    self.assertEqual(
        batch_cache_instance.get_start_end_frame_numbers_in_row(row_index),
        expected,
    )

  @parameterized.named_parameters([
      dict(testcase_name='above_batch_region', row_index=0, expected=[]),
      dict(
          testcase_name='first_row_batch_region',
          row_index=5,
          expected=[7, 8],
      ),
      dict(
          testcase_name='second_row_batch_region',
          row_index=10,
          expected=[12, 13],
      ),
      dict(testcase_name='below_batch_region', row_index=15, expected=[]),
  ])
  def test_get_frame_number_range_in_row(self, row_index, expected):
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    batch_cache_instance = frame_caching_util._CacheInstanceFrameBlock(
        local_instance, [12], 500, 500
    )
    self.assertEqual(
        list(batch_cache_instance._get_frame_number_range_in_row(row_index)),
        expected,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='above_batch_region',
          row_index=0,
          expected=[1, 2, 3, 4, 5],
      ),
      dict(
          testcase_name='first_row_batch_region',
          row_index=5,
          expected=[6, 9, 10],
      ),
      dict(
          testcase_name='second_row_batch_region',
          row_index=10,
          expected=[11, 14, 15],
      ),
      dict(testcase_name='below_batch_region', row_index=15, expected=[]),
  ])
  def test_get_frame_number_clipped_range_in_row(self, row_index, expected):
    local_instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    clip_block = frame_caching_util._CacheInstanceFrameBlock(
        local_instance, [12], 500, 500
    )
    batch_cache_instance = frame_caching_util._CacheInstanceFrameBlock(
        local_instance, [12], 1250, 1250, load_display_thread=clip_block
    )
    self.assertEqual(
        list(batch_cache_instance._get_frame_number_range_in_row(row_index)),
        expected,
    )

  def test_init_fork_module_state(self) -> None:
    frame_caching_util._cache_instance_thread_lock = 'mock'
    frame_caching_util._active_cache_loading_instances = 'mock'
    frame_caching_util._last_cleanup_preemptive_cache_loading = 'mock'
    frame_caching_util._init_fork_module_state()
    self.assertIsNotNone(frame_caching_util._cache_instance_thread_lock)
    self.assertNotEqual(frame_caching_util._cache_instance_thread_lock, 'mock')
    self.assertIsInstance(
        frame_caching_util._active_cache_loading_instances, set
    )
    self.assertEqual(
        frame_caching_util._last_cleanup_preemptive_cache_loading, 0.0
    )

  @parameterized.named_parameters([
      dict(testcase_name='below_zero', val=-1, expected=0.0),
      dict(testcase_name='zero', val=0, expected=0.0),
      dict(testcase_name='one', val=1, expected=0.01),
      dict(testcase_name='seventy', val=70, expected=0.7),
      dict(testcase_name='max', val=100, expected=1.0),
      dict(testcase_name='above_max', val=101, expected=1.0),
  ])
  def test_get_max_precent_memory_used_for_whole_slide_caching(
      self, val, expected
  ):
    with flagsaver.flagsaver(
        max_whole_instance_caching_memory_precent_load=val
    ):
      self.assertEqual(
          frame_caching_util._get_max_precent_memory_used_for_whole_slide_caching(),
          expected,
      )

  @parameterized.named_parameters([
      dict(testcase_name='below', size=99, expected=True),
      dict(testcase_name='equal', size=15, expected=True),
      dict(testcase_name='above', size=14, expected=False),
  ])
  def test_force_cache_whole_instance_in_memory(self, size, expected):
    instance = shared_test_util.jpeg_encoded_dicom_local_instance()
    self.assertEqual(
        frame_caching_util._force_cache_whole_instance_in_memory(
            instance, size
        ),
        expected,
    )


if __name__ == '__main__':
  absltest.main()
