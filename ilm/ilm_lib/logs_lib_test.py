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
"""Tests for logs_lib."""

import dataclasses
import datetime

from absl.testing import absltest
from absl.testing import parameterized
import mock

import ilm_config
import ilm_types
from ilm_lib import dicom_store_lib
from ilm_lib import logs_lib
from ilm_lib import pipeline_util
from test_utils import test_const


_RETRIEVE_INSTANCE_REQUEST_JSON = (
    '{{"@type":"type.googleapis.com/google.cloud.healthcare.{}.dicomweb.RetrieveInstanceRequest",'
    '"dicomWebPath":"studies/1.22.333/series/1.44.555/instances/6.77.888",'
    '"parent":"projects/my-proj/locations/my-location/datasets/my-ds-dataset/dicomStores/my-dicom-store"}}'
)
_RETRIEVE_RENDERED_INSTANCE_REQUEST_JSON = (
    '{{"@type":"type.googleapis.com/google.cloud.healthcare.{}.dicomweb.RetrieveRenderedInstanceRequest",'
    '"dicomWebPath":"studies/1.22.333/series/1.44.555/instances/6.77.888/rendered",'
    '"parent":"projects/my-proj/locations/my-location/datasets/my-ds-dataset/dicomStores/my-dicom-store"}}'
)
_RETRIEVE_FRAMES_REQUEST_JSON = (
    '{{"@type":"type.googleapis.com/google.cloud.healthcare.{}.dicomweb.RetrieveFramesRequest",'
    ' "dicomWebPath":"studies/1.22.333/series/1.44.555/instances/6.77.888/frames/15,16,21,22",'
    ' "parent":"projects/my-proj/locations/my-location/datasets/my-ds-dataset/dicomStores/my-dicom-store"}}'
)
_RETRIEVE_RENDERED_FRAMES_REQUEST_JSON = (
    '{{"@type":"type.googleapis.com/google.cloud.healthcare.{}.dicomweb.RetrieveRenderedFramesRequest",'
    '"dicomWebPath":"studies/1.22.333/series/1.44.555/instances/6.77.888/frames/1/rendered",'
    '"parent":"projects/my-proj/locations/my-location/datasets/my-ds-dataset/dicomStores/my-dicom-store"}}'
)
_RETRIEVE_STUDY_REQUEST_JSON = (
    '{{"@type":"type.googleapis.com/google.cloud.healthcare.{}.dicomweb.RetrieveStudyRequest",'
    '"dicomWebPath":"studies/1.22.333",'
    '"parent":"projects/my-proj/locations/my-location/datasets/my-ds-dataset/dicomStores/my-dicom-store"}}'
)
_RETRIEVE_SERIES_REQUEST_JSON = (
    '{{"@type":"type.googleapis.com/google.cloud.healthcare.{}.dicomweb.RetrieveSeriesRequest",'
    '"dicomWebPath":"studies/1.22.333/series/1.44.555",'
    '"parent":"projects/my-proj/locations/my-location/datasets/my-ds-dataset/dicomStores/my-dicom-store"}}'
)

_LOG_ENTRY_TIMESTAMP = datetime.datetime(2023, 11, 9, 5, 48, 23, 417355)


def _create_raw_log_entry(
    dicomweb_service_method: str = 'RetrieveInstance',
    request_json: str = _RETRIEVE_INSTANCE_REQUEST_JSON,
    api_version: str = 'v1',
):
  request_json = request_json.format(api_version)
  return {
      'methodName': f'google.cloud.healthcare.{api_version}.dicomweb.DicomWebService.{dicomweb_service_method}',
      'resourceName': 'projects/my-proj/locations/my-location/datasets/my-ds-dataset/dicomStores/my-dicom-store',
      'time': _LOG_ENTRY_TIMESTAMP,
      'requestJson': request_json,
  }


class LogsLibTest(parameterized.TestCase):

  def test_get_data_access_logs_query(self):
    query = logs_lib.get_data_access_logs_query(test_const.ILM_CONFIG)
    self.assertIn(test_const.ILM_CONFIG.logs_config.logs_bigquery_table, query)
    for method in [
        'RetrieveInstance',
        'RetrieveRenderedInstance',
        'RetrieveFrames',
        'RetrieveRenderedFrames',
        'RetrieveStudy',
        'RetrieveSeries',
    ]:
      self.assertIn(method, query)
    for api_version in ['v1', 'v1beta1']:
      self.assertIn(api_version, query)
    self.assertIn('time > PARSE_TIMESTAMP("%Y%m%d", "20231120")', query)

  def test_get_data_access_logs_query_no_date_filter(self):
    logs_bigquery_table = test_const.ILM_CONFIG.logs_config.logs_bigquery_table
    ilm_cfg = dataclasses.replace(
        test_const.ILM_CONFIG,
        logs_config=ilm_config.DataAccessLogsConfiguration(
            logs_bigquery_table=logs_bigquery_table,
        ),
    )
    query = logs_lib.get_data_access_logs_query(ilm_cfg)
    self.assertIn(logs_bigquery_table, query)
    self.assertNotIn('time > PARSE_TIMESTAMP', query)

  def test_parse_dofn_process_fails_missing_request_json(self):
    invalid_log_entry = _create_raw_log_entry()
    del invalid_log_entry['requestJson']
    logs_dofn = logs_lib.ParseDataAccessLogsDoFn(test_const.ILM_CONFIG)
    self.assertEmpty(list(logs_dofn.process(invalid_log_entry)))

  def test_parse_dofn_process_fails_invalid_request_json(self):
    invalid_log_entry = _create_raw_log_entry()
    invalid_log_entry['requestJson'] = '{}'
    logs_dofn = logs_lib.ParseDataAccessLogsDoFn(test_const.ILM_CONFIG)
    self.assertEmpty(list(logs_dofn.process(invalid_log_entry)))

  def test_parse_dofn_process_skips_disallow_list_instance(self):
    raw_log_entry = _create_raw_log_entry()
    raw_log_entry['requestJson'] = raw_log_entry['requestJson'].replace(
        test_const.INSTANCE_0, test_const.INSTANCE_DISALLOW_LIST
    )
    logs_dofn = logs_lib.ParseDataAccessLogsDoFn(test_const.ILM_CONFIG)
    self.assertEmpty(list(logs_dofn.process(raw_log_entry)))

  def test_parse_dofn_process_fails_unsupported_request_json(self):
    logs_dofn = logs_lib.ParseDataAccessLogsDoFn(test_const.ILM_CONFIG)
    log_entry = _create_raw_log_entry(
        dicomweb_service_method='RetrieveInstanceMetadata'
    )
    self.assertEmpty(list(logs_dofn.process(log_entry)))

  @parameterized.named_parameters(
      (
          'retrieve_instance_request_v1',
          'RetrieveInstance',
          _RETRIEVE_INSTANCE_REQUEST_JSON,
          'v1',
      ),
      (
          'retrieve_instance_request_v1beta1',
          'RetrieveInstance',
          _RETRIEVE_INSTANCE_REQUEST_JSON,
          'v1beta1',
      ),
      (
          'retrieve_rendered_instance_request',
          'RetrieveRenderedInstance',
          _RETRIEVE_RENDERED_INSTANCE_REQUEST_JSON,
          'v1',
      ),
  )
  def test_parse_dofn_process_single_instance_access_succeeds(
      self, method, request_json, api_version
  ):
    raw_log_entry = _create_raw_log_entry(method, request_json, api_version)
    logs_dofn = logs_lib.ParseDataAccessLogsDoFn(test_const.ILM_CONFIG)
    logs_dofn._today = datetime.datetime(
        2023, 11, 15, tzinfo=datetime.timezone.utc
    )
    self.assertEqual(
        list(logs_dofn.process(raw_log_entry)),
        [(
            test_const.INSTANCE_0,
            ilm_types.LogAccessCount(
                instance_access_count=ilm_config.AccessCount(
                    count=1.0, num_days=5
                )
            ),
        )],
    )

  @parameterized.named_parameters(
      (
          'retrieve_study_request_v1',
          'RetrieveStudy',
          _RETRIEVE_STUDY_REQUEST_JSON,
          'v1',
          'studies/1.22.333',
      ),
      (
          'retrieve_study_request_v1beta1',
          'RetrieveStudy',
          _RETRIEVE_STUDY_REQUEST_JSON,
          'v1beta1',
          'studies/1.22.333',
      ),
      (
          'retrieve_series_request',
          'RetrieveSeries',
          _RETRIEVE_SERIES_REQUEST_JSON,
          'v1',
          'studies/1.22.333/series/1.44.555',
      ),
  )
  @mock.patch.object(
      dicom_store_lib.DicomStoreClient,
      'fetch_instances',
      return_value=[
          test_const.INSTANCE_0,
          test_const.INSTANCE_DISALLOW_LIST,
          test_const.INSTANCE_2,
      ],
      autospec=True,
  )
  @mock.patch.object(pipeline_util.Throttler, 'wait', autospec=True)
  def test_parse_dofn_process_multiple_instances_access_succeeds(
      self,
      method,
      request_json,
      api_version,
      expected_dicomweb_path,
      mk_throttler,
      mk_fetch_instances,
  ):
    raw_log_entry = _create_raw_log_entry(method, request_json, api_version)
    logs_dofn = logs_lib.ParseDataAccessLogsDoFn(test_const.ILM_CONFIG)
    logs_dofn._today = datetime.datetime(
        2023, 11, 15, tzinfo=datetime.timezone.utc
    )
    self.assertEqual(
        list(logs_dofn.process(raw_log_entry)),
        [
            (
                test_const.INSTANCE_0,
                ilm_types.LogAccessCount(
                    instance_access_count=ilm_config.AccessCount(
                        count=1.0, num_days=5
                    )
                ),
            ),
            (
                test_const.INSTANCE_2,
                ilm_types.LogAccessCount(
                    instance_access_count=ilm_config.AccessCount(
                        count=1.0, num_days=5
                    )
                ),
            ),
        ],
    )
    mk_fetch_instances.assert_called_once_with(mock.ANY, expected_dicomweb_path)
    mk_throttler.assert_called_once()

  @parameterized.named_parameters(
      (
          'retrieve_rendered_frames_request',
          'RetrieveRenderedFrames',
          _RETRIEVE_RENDERED_FRAMES_REQUEST_JSON,
          'v1',
          1,
      ),
      (
          'retrieve_frames_request',
          'RetrieveRenderedFrames',
          _RETRIEVE_FRAMES_REQUEST_JSON,
          'v1',
          4,
      ),
      (
          'retrieve_frames_request_v1beta1',
          'RetrieveRenderedFrames',
          _RETRIEVE_FRAMES_REQUEST_JSON,
          'v1beta1',
          4,
      ),
  )
  def test_parse_dofn_process_frames_access_succeeds(
      self, method, request_json, api_version, expected_count
  ):
    raw_log_entry = _create_raw_log_entry(method, request_json, api_version)
    logs_dofn = logs_lib.ParseDataAccessLogsDoFn(test_const.ILM_CONFIG)
    logs_dofn._today = datetime.datetime(
        2023, 11, 15, tzinfo=datetime.timezone.utc
    )
    self.assertEqual(
        list(logs_dofn.process(raw_log_entry)),
        [
            (
                test_const.INSTANCE_0,
                ilm_types.LogAccessCount(
                    frames_access_count=ilm_config.AccessCount(
                        count=expected_count, num_days=5
                    )
                ),
            ),
        ],
    )

  @parameterized.named_parameters(
      (
          'single_instance_and_frames_access',
          [
              ilm_types.LogAccessCount(
                  frames_access_count=ilm_config.AccessCount(
                      count=1.0, num_days=5
                  )
              ),
              ilm_types.LogAccessCount(
                  instance_access_count=ilm_config.AccessCount(
                      count=2.0, num_days=5
                  )
              ),
          ],
          ilm_types.LogAccessMetadata(
              frames_access_counts=[
                  ilm_config.AccessCount(count=1.0, num_days=5)
              ],
              instance_access_counts=[
                  ilm_config.AccessCount(count=2.0, num_days=5)
              ],
          ),
      ),
      (
          'multiple_instance_and_frames_access',
          [
              ilm_types.LogAccessCount(
                  frames_access_count=ilm_config.AccessCount(
                      count=1.0, num_days=5
                  )
              ),
              ilm_types.LogAccessCount(
                  frames_access_count=ilm_config.AccessCount(
                      count=2.0, num_days=5
                  )
              ),
              ilm_types.LogAccessCount(
                  instance_access_count=ilm_config.AccessCount(
                      count=3.0, num_days=5
                  )
              ),
              ilm_types.LogAccessCount(
                  instance_access_count=ilm_config.AccessCount(
                      count=4.0, num_days=5
                  )
              ),
              ilm_types.LogAccessCount(
                  frames_access_count=ilm_config.AccessCount(
                      count=5.0, num_days=10
                  )
              ),
              ilm_types.LogAccessCount(
                  instance_access_count=ilm_config.AccessCount(
                      count=6.0, num_days=10
                  )
              ),
          ],
          ilm_types.LogAccessMetadata(
              frames_access_counts=[
                  ilm_config.AccessCount(count=3.0, num_days=5),
                  ilm_config.AccessCount(count=5.0, num_days=10),
              ],
              instance_access_counts=[
                  ilm_config.AccessCount(count=7.0, num_days=5),
                  ilm_config.AccessCount(count=6.0, num_days=10),
              ],
          ),
      ),
      (
          'only_frame_access',
          [
              ilm_types.LogAccessCount(
                  frames_access_count=ilm_config.AccessCount(
                      count=1.0, num_days=5
                  )
              ),
              ilm_types.LogAccessCount(
                  frames_access_count=ilm_config.AccessCount(
                      count=2.0, num_days=5
                  )
              ),
              ilm_types.LogAccessCount(
                  frames_access_count=ilm_config.AccessCount(
                      count=5.0, num_days=10
                  )
              ),
          ],
          ilm_types.LogAccessMetadata(
              frames_access_counts=[
                  ilm_config.AccessCount(count=3.0, num_days=5),
                  ilm_config.AccessCount(count=5.0, num_days=10),
              ],
              instance_access_counts=[],
          ),
      ),
      (
          'only_instance_access',
          [
              ilm_types.LogAccessCount(
                  instance_access_count=ilm_config.AccessCount(
                      count=3.0, num_days=5
                  )
              ),
              ilm_types.LogAccessCount(
                  instance_access_count=ilm_config.AccessCount(
                      count=4.0, num_days=5
                  )
              ),
              ilm_types.LogAccessCount(
                  instance_access_count=ilm_config.AccessCount(
                      count=6.0, num_days=10
                  )
              ),
          ],
          ilm_types.LogAccessMetadata(
              frames_access_counts=[],
              instance_access_counts=[
                  ilm_config.AccessCount(count=7.0, num_days=5),
                  ilm_config.AccessCount(count=6.0, num_days=10),
              ],
          ),
      ),
      ('no_instance_or_frames_access', [], ilm_types.LogAccessMetadata([], [])),
  )
  def test_compute_log_access_metadata_succeeds(
      self, log_access_counts, expected_log_access_metadata
  ):
    instance, log_access_metadata = logs_lib.compute_log_access_metadata(
        (test_const.INSTANCE_0, log_access_counts)
    )
    self.assertEqual(instance, test_const.INSTANCE_0)
    self.assertEqual(log_access_metadata, expected_log_access_metadata)

  @parameterized.named_parameters(
      (
          'retrieve_instance_request_negative_num_days_access_count',
          'RetrieveInstance',
          _RETRIEVE_INSTANCE_REQUEST_JSON,
          ilm_types.LogAccessCount(
              instance_access_count=ilm_config.AccessCount(
                  count=1.0, num_days=0
              )
          ),
      ),
      (
          'retrieve_rendered_frames_request_negative_num_days_access_count',
          'RetrieveRenderedFrames',
          _RETRIEVE_RENDERED_FRAMES_REQUEST_JSON,
          ilm_types.LogAccessCount(
              frames_access_count=ilm_config.AccessCount(count=1.0, num_days=0)
          ),
      ),
  )
  def test_parse_dofn_process_negative_num_days_access_count_access(
      self,
      method,
      request_json,
      expected_access_count,
  ):
    raw_log_entry = _create_raw_log_entry(method, request_json, 'v1')
    logs_dofn = logs_lib.ParseDataAccessLogsDoFn(test_const.ILM_CONFIG)
    logs_dofn._today = (
        _LOG_ENTRY_TIMESTAMP - datetime.timedelta(days=1)
    ).astimezone(datetime.timezone.utc)
    self.assertEqual(
        list(logs_dofn.process(raw_log_entry)),
        [(test_const.INSTANCE_0, expected_access_count)],
    )


if __name__ == '__main__':
  absltest.main()
