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
"""Tests for gke_main."""

from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver

from transformation_pipeline import gke_main
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import polling_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator


class GkeMainTest(absltest.TestCase):

  def test_run_oof_ingest_fails_missing_oof_subscription(self):
    with self.assertRaisesRegex(ValueError, 'Missing OOF subscription'):
      gke_main._run_oof_ingest()

  @flagsaver.flagsaver(
      oof_subscription='oof_sub',
      gcs_file_to_ingest_list=['/path/to/file.svs'],
  )
  @mock.patch.object(polling_client.PollingClient, '__enter__', autospec=True)
  @mock.patch.object(polling_client.PollingClient, '__exit__', autospec=True)
  @mock.patch.object(
      polling_client.PollingClient, '__init__', return_value=None, autospec=True
  )
  def test_run_oof_ingest_sync_error(self, _, mck_exit, mck_enter):
    mck_enter.return_value.run.side_effect = RuntimeError()
    gke_main._run_oof_ingest()
    mck_exit.assert_called_once()

  @flagsaver.flagsaver(
      oof_subscription='oof_sub',
      gcs_file_to_ingest_list=['/path/to/file.svs'],
  )
  @mock.patch.object(polling_client.PollingClient, '__enter__', autospec=True)
  @mock.patch.object(polling_client.PollingClient, '__exit__', autospec=True)
  @mock.patch.object(
      polling_client.PollingClient, '__init__', return_value=None, autospec=True
  )
  def test_run_oof_ingest_succeeds(self, mck_constructor, mck_exit, mck_enter):
    mck_enter.return_value.run.return_value = True
    gke_main._run_oof_ingest()
    mck_constructor.assert_called_once_with(
        mock.ANY, 'proj', {'oof_sub': mock.ANY}
    )
    mck_enter.return_value.run.assert_called_once()
    mck_exit.assert_called_once()

  def test_run_default_ingest_fails_missing_gcs_subscription(self):
    with self.assertRaisesRegex(ValueError, 'Missing GCS subscription'):
      gke_main._run_default_ingest()

  @flagsaver.flagsaver(
      gcs_subscription='gcs_sub',
      gcs_file_to_ingest_list=['/path/to/file.svs'],
      gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.METADATA,
      ingest_succeeded_uri='gs://mybucket/success',
      ingest_failed_uri='gs://mybucket/failed',
      metadata_bucket='test',
  )
  @mock.patch.object(polling_client.PollingClient, '__enter__', autospec=True)
  @mock.patch.object(polling_client.PollingClient, '__exit__', autospec=True)
  @mock.patch.object(
      polling_client.PollingClient, '__init__', return_value=None, autospec=True
  )
  def test_run_default_ingest_gcs_only_succeeds(
      self, mck_constructor, mck_exit, mck_enter
  ):
    mck_enter.return_value.run.return_value = True
    gke_main._run_default_ingest()
    mck_constructor.assert_called_once_with(
        mock.ANY, 'proj', {'gcs_sub': mock.ANY}
    )
    mck_enter.return_value.run.assert_called_once()
    mck_exit.assert_called_once()

  @flagsaver.flagsaver(
      project_id='proj',
      gcs_subscription='gcs_sub',
      gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.METADATA,
      ingest_succeeded_uri='gs://mybucket/success',
      ingest_failed_uri='gs://mybucket/failed',
      dicom_store_subscription='dcm_sub',
      dicom_store_ingest_gcs_uri='dcm-store-recovery-bucket',
      metadata_bucket='test',
  )
  @mock.patch.object(polling_client.PollingClient, '__enter__', autospec=True)
  @mock.patch.object(polling_client.PollingClient, '__exit__', autospec=True)
  @mock.patch.object(
      polling_client.PollingClient, '__init__', return_value=None, autospec=True
  )
  def test_run_default_ingest_gcs_and_dicom_store_succeeds(
      self, mck_constructor, mck_exit, mck_enter
  ):
    mck_enter.return_value.run.return_value = True
    gke_main._run_default_ingest()
    mck_constructor.assert_called_once_with(
        mock.ANY, 'proj', {'gcs_sub': mock.ANY, 'dcm_sub': mock.ANY}
    )
    mck_enter.return_value.run.assert_called_once()
    mck_exit.assert_called_once()

  @flagsaver.flagsaver(oof_dicomweb_base_url='')
  def test_get_oof_trigger_config_disabled(self):
    self.assertIsNone(gke_main._get_oof_trigger_config())

  def test_get_oof_trigger_config_enabled(self):
    dicom_store_web_path = 'http://healthcare.dicomstore.mock/'
    with flagsaver.flagsaver(oof_dicomweb_base_url=dicom_store_web_path):
      self.assertEqual(
          gke_main._get_oof_trigger_config().dicom_store_web_path,  # pytype: disable=attribute-error
          dicom_store_web_path,
      )


if __name__ == '__main__':
  with flagsaver.flagsaver(
      project_id='proj',
      dicomweb_url='http://healthcare.mock/',
      dicom_guid_prefix=uid_generator.TEST_UID_PREFIX,
  ):
    absltest.main()
