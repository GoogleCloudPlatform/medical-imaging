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
#
# ==============================================================================
"""Tests for pipeline_util."""

import dataclasses
import time

from absl.testing import absltest
from absl.testing import parameterized

import ilm_config
import ilm_types
from ilm_lib import pipeline_util
from test_utils import test_const
from shared_libs.test_utils.gcs_mock import gcs_mock

_TEST_BUCKET = 'test_bucket'
_TEST_DIR = 'ilm/testdata/'

_INSTANCE_METADATA = ilm_types.InstanceMetadata(
    instance=test_const.INSTANCE_0,
    modality=None,
    num_frames=10,
    pixel_spacing=None,
    sop_class_uid='1.2.300',
    acquisition_date=None,
    content_date=None,
    series_date=None,
    study_date=None,
    image_type=None,
    size_bytes=1024,
    storage_class=ilm_config.StorageClass.STANDARD,
    num_days_in_current_storage_class=20,
)
_INSTANCE_METADATA_ZERO_FRAMES = dataclasses.replace(
    _INSTANCE_METADATA, num_frames=0
)
_LOG_ACCESS_METADATA = ilm_types.LogAccessMetadata(
    frames_access_counts=[],
    instance_access_counts=[ilm_config.AccessCount(1, 2)],
)
_LOG_ACCESS_METADATA_WITH_FRAME_COUNTS = ilm_types.LogAccessMetadata(
    frames_access_counts=[ilm_config.AccessCount(10, 2)],
    instance_access_counts=[ilm_config.AccessCount(5, 4)],
)
_ACCESS_METADATA = ilm_types.AccessMetadata([ilm_config.AccessCount(1, 2)])


class PipelineUtilTest(parameterized.TestCase):

  def test_config_missing(self):
    with self.assertRaises(Exception):
      with gcs_mock.GcsMock() as _:
        pipeline_util.read_ilm_config('gs://bucket/config.json')

  def test_read_invalid_config(self):
    with self.assertRaises(ilm_config.InvalidConfigError):
      with gcs_mock.GcsMock({_TEST_BUCKET: _TEST_DIR}) as _:
        pipeline_util.read_ilm_config(
            f'gs://{_TEST_BUCKET}/invalid_config.json'
        )

  def test_read_config(self):
    with gcs_mock.GcsMock({_TEST_BUCKET: _TEST_DIR}) as _:
      pipeline_util.read_ilm_config(f'gs://{_TEST_BUCKET}/config.json')

  def test_should_keep_instance(self):
    self.assertTrue(
        pipeline_util.should_keep_instance(
            (test_const.INSTANCE_0, None), test_const.ILM_CONFIG
        )
    )

  def test_should_not_keep_instance(self):
    self.assertFalse(
        pipeline_util.should_keep_instance(
            (test_const.INSTANCE_DISALLOW_LIST, True), test_const.ILM_CONFIG
        )
    )

  @parameterized.named_parameters(
      (
          'multiple_access_metadata',
          [_LOG_ACCESS_METADATA, _LOG_ACCESS_METADATA],
          [_INSTANCE_METADATA],
      ),
      (
          'multiple_instance_metadata',
          [_LOG_ACCESS_METADATA],
          [_INSTANCE_METADATA, _INSTANCE_METADATA],
      ),
  )
  def test_include_access_count_in_metadata_fails(
      self, access_metadata_iter, instance_metadata_iter
  ):
    with self.assertRaises(ValueError):
      _ = list(
          pipeline_util.include_access_count_in_metadata(
              (
                  test_const.INSTANCE_0,
                  (access_metadata_iter, instance_metadata_iter),
              ),
          )
      )

  def test_include_access_count_in_metadata_missing_metadata_succeeds(self):
    metadata_missing_result = list(
        pipeline_util.include_access_count_in_metadata(
            (
                test_const.INSTANCE_0,
                ([_LOG_ACCESS_METADATA], []),
            ),
        )
    )
    self.assertEmpty(metadata_missing_result)

  def test_include_access_count_in_metadata_missing_num_frames_succeeds(self):
    instance_metadata_with_access_metadata = list(
        pipeline_util.include_access_count_in_metadata(
            (
                test_const.INSTANCE_0,
                (
                    [_LOG_ACCESS_METADATA_WITH_FRAME_COUNTS],
                    [_INSTANCE_METADATA_ZERO_FRAMES],
                ),
            ),
        )
    )
    expected_instance_metadata = ilm_types.InstanceMetadata(
        instance=test_const.INSTANCE_0,
        modality=None,
        # Number of frames assumed to be 1 if missing.
        num_frames=1,
        pixel_spacing=None,
        sop_class_uid='1.2.300',
        acquisition_date=None,
        content_date=None,
        series_date=None,
        study_date=None,
        image_type=None,
        size_bytes=1024,
        storage_class=ilm_config.StorageClass.STANDARD,
        num_days_in_current_storage_class=20,
        access_metadata=ilm_types.AccessMetadata(
            cumulative_access_counts=[
                ilm_config.AccessCount(count=10.0, num_days=2),
                ilm_config.AccessCount(count=15.0, num_days=4),
            ]
        ),
    )
    self.assertEqual(
        instance_metadata_with_access_metadata, [expected_instance_metadata]
    )

  @parameterized.named_parameters(
      (
          dict(
              testcase_name='access_counts_empty',
              frame_counts=[],
              instance_counts=[],
              expected_cumulative_counts=[],
          )
      ),
      (
          dict(
              testcase_name='single_instance_access',
              frame_counts=[],
              instance_counts=[ilm_config.AccessCount(count=1.0, num_days=1)],
              expected_cumulative_counts=[
                  ilm_config.AccessCount(count=1.0, num_days=1)
              ],
          )
      ),
      (
          dict(
              testcase_name='single_frame_access',
              frame_counts=[ilm_config.AccessCount(count=1.0, num_days=1)],
              instance_counts=[],
              expected_cumulative_counts=[
                  ilm_config.AccessCount(count=0.1, num_days=1)
              ],
          )
      ),
      (
          dict(
              testcase_name='multiple_instance_accesses',
              frame_counts=[],
              instance_counts=[
                  ilm_config.AccessCount(count=3.0, num_days=4),
                  ilm_config.AccessCount(count=7.0, num_days=8),
                  ilm_config.AccessCount(count=5.0, num_days=6),
                  ilm_config.AccessCount(count=1.0, num_days=2),
              ],
              expected_cumulative_counts=[
                  ilm_config.AccessCount(count=1.0, num_days=2),
                  ilm_config.AccessCount(count=4.0, num_days=4),
                  ilm_config.AccessCount(count=9.0, num_days=6),
                  ilm_config.AccessCount(count=16.0, num_days=8),
              ],
          )
      ),
      (
          dict(
              testcase_name='multiple_frame_accesses',
              frame_counts=[
                  ilm_config.AccessCount(count=3.0, num_days=4),
                  ilm_config.AccessCount(count=7.0, num_days=8),
                  ilm_config.AccessCount(count=5.0, num_days=6),
                  ilm_config.AccessCount(count=1.0, num_days=2),
              ],
              instance_counts=[],
              expected_cumulative_counts=[
                  ilm_config.AccessCount(count=0.1, num_days=2),
                  ilm_config.AccessCount(count=0.4, num_days=4),
                  ilm_config.AccessCount(count=0.9, num_days=6),
                  ilm_config.AccessCount(count=1.6, num_days=8),
              ],
          )
      ),
      (
          dict(
              testcase_name='multiple_frame_and_instance_accesses',
              frame_counts=[
                  ilm_config.AccessCount(count=3.0, num_days=4),
                  ilm_config.AccessCount(count=7.0, num_days=8),
                  ilm_config.AccessCount(count=5.0, num_days=6),
                  ilm_config.AccessCount(count=1.0, num_days=2),
                  ilm_config.AccessCount(count=20.0, num_days=10),
              ],
              instance_counts=[
                  ilm_config.AccessCount(count=3.0, num_days=4),
                  ilm_config.AccessCount(count=7.0, num_days=8),
                  ilm_config.AccessCount(count=5.0, num_days=6),
                  ilm_config.AccessCount(count=1.0, num_days=2),
                  ilm_config.AccessCount(count=20.0, num_days=12),
              ],
              expected_cumulative_counts=[
                  ilm_config.AccessCount(count=1.1, num_days=2),
                  ilm_config.AccessCount(count=4.4, num_days=4),
                  ilm_config.AccessCount(count=9.9, num_days=6),
                  ilm_config.AccessCount(count=17.6, num_days=8),
                  ilm_config.AccessCount(count=19.6, num_days=10),
                  ilm_config.AccessCount(count=39.6, num_days=12),
              ],
          )
      ),
  )
  def test_compute_cumulative_access_count(
      self, frame_counts, instance_counts, expected_cumulative_counts
  ):
    access_metadata = pipeline_util._compute_cumulative_access_count(
        log_access_metadata=ilm_types.LogAccessMetadata(
            frames_access_counts=frame_counts,
            instance_access_counts=instance_counts,
        ),
        num_frames=10,
    )
    self.assertEqual(
        access_metadata,
        ilm_types.AccessMetadata(
            cumulative_access_counts=expected_cumulative_counts
        ),
    )

  @parameterized.named_parameters(
      ('single_access_metadata', [_LOG_ACCESS_METADATA], _ACCESS_METADATA),
      (
          'single_access_metadata_with_frames',
          [
              ilm_types.LogAccessMetadata(
                  frames_access_counts=[ilm_config.AccessCount(1, 2)],
                  instance_access_counts=[ilm_config.AccessCount(1, 2)],
              )
          ],
          ilm_types.AccessMetadata([ilm_config.AccessCount(1.1, 2)]),
      ),
      ('no_access_metadata', [], ilm_types.AccessMetadata([])),
  )
  def test_include_access_count_in_metadata_succeeds(
      self, log_access_metadata_iter, expected_access_metadata
  ):
    instance_metadata_with_access_metadata = list(
        pipeline_util.include_access_count_in_metadata(
            (
                test_const.INSTANCE_0,
                (log_access_metadata_iter, [_INSTANCE_METADATA]),
            ),
        )
    )

    expected_instance_metadata = ilm_types.InstanceMetadata(
        instance=test_const.INSTANCE_0,
        modality=None,
        num_frames=10,
        pixel_spacing=None,
        sop_class_uid='1.2.300',
        size_bytes=1024,
        acquisition_date=None,
        content_date=None,
        series_date=None,
        study_date=None,
        image_type=None,
        storage_class=ilm_config.StorageClass.STANDARD,
        access_metadata=expected_access_metadata,
        num_days_in_current_storage_class=20,
    )
    self.assertEqual(
        instance_metadata_with_access_metadata, [expected_instance_metadata]
    )

  @parameterized.named_parameters(
      ('negative', -0.5),
      ('zero', 0),
  )
  def test_throttler_init_fails(self, target_qps):
    with self.assertRaises(ValueError):
      pipeline_util.Throttler(target_qps)

  def test_throttler_wait(self):
    throttler = pipeline_util.Throttler(target_qps=4)
    for _ in range(5):
      start_time = time.time()
      throttler.wait()
      end_time = time.time()
      self.assertBetween(end_time - start_time, 0.25, 0.251)


if __name__ == '__main__':
  absltest.main()
