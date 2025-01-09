# Copyright 2024 Google LLC
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

"""Test for reading in keep list of DeId tags from GCS.

Test must be run as a binary as it uses GCP credentials.
Writes and reads file from cloud storage.

To run:
gcloud auth login
gcloud config set project orchestrator-dev-endpoints
blaze run //pathology.orchestrator:read_deid_list_test
"""
import os
from typing import List

from absl import logging
from absl.testing import absltest
from absl.testing import flagsaver
from google.cloud import storage as gcs
import grpc

from pathology.orchestrator import pathology_cohorts_handler
from pathology.orchestrator import rpc_status
from pathology.shared_libs.test_utils.gcs_mock import gcs_mock

_BUCKET_NAME = 'read_deid_list_test_tmp'
_BLOB_NAME = 'deid_keep_list_tmp'
_BLOB_FULL_PATH = f'gs://{_BUCKET_NAME}/{_BLOB_NAME}'
_SOURCE_FILE = os.path.join(
    os.path.dirname(__file__), 'test_deid_tag_keep_list.txt'
)


def _read_expected_deid_keep_list() -> List[str]:
  """Reads and processes test_deid_tag_keep_list.txt."""
  with open(_SOURCE_FILE) as infile:
    return [
        tag_keyword.strip() for tag_keyword in infile if tag_keyword.strip()
    ]


class ReadDeidListTest(absltest.TestCase):
  """Test for reading in keep list of DeId tags from GCS."""

  def setUp(self):
    super().setUp()
    self.enter_context(gcs_mock.GcsMock())
    self._gcs_client = gcs.Client()

    # Write deid file to cloud storage.
    self._bucket = self._gcs_client.create_bucket(_BUCKET_NAME)
    logging.info('Created bucket %s.', _BUCKET_NAME)
    self._blob = self._gcs_client.bucket(_BUCKET_NAME).blob(_BLOB_NAME)
    self._blob.upload_from_filename(_SOURCE_FILE)
    logging.info('Uploaded deid tag keep list as blob %s.', _BLOB_NAME)

  @flagsaver.flagsaver(deid_tag_keep_list=_BLOB_FULL_PATH)
  def test_read_deid_tag_keep_list_succeeds(self):
    deid_tag_list_gcs = pathology_cohorts_handler._read_deid_tag_keep_list()

    self.assertEqual(deid_tag_list_gcs, _read_expected_deid_keep_list())

  @flagsaver.flagsaver(deid_tag_keep_list=_BLOB_FULL_PATH)
  def test_read_deid_tag_keep_list_no_file_fails(self):
    self._blob.delete()
    with self.assertRaises(rpc_status.RpcFailureError) as exc:
      pathology_cohorts_handler._read_deid_tag_keep_list()
    self.assertIs(exc.exception.status.code, grpc.StatusCode.NOT_FOUND)
    self.assertEqual(
        exc.exception.status.error_msg,
        f'Could not find GCS file {_BLOB_FULL_PATH}.',
    )

  @flagsaver.flagsaver(deid_tag_keep_list='')
  def test_read_deid_tag_keep_list_flag_not_set_fails(self):
    with self.assertRaises(
        rpc_status.RpcFailureError,
    ) as exc:
      pathology_cohorts_handler._read_deid_tag_keep_list()
    self.assertIs(exc.exception.status.code, grpc.StatusCode.INVALID_ARGUMENT)
    self.assertEqual(
        exc.exception.status.error_msg,
        'DEID_TAG_KEEP_LIST environmental variable is undefined; specify a '
        'path to GCS (e.g., gs://bucket/file.txt) or a path to GKE container '
        'volume mounted configmap (e.g., /config/my_config).',
    )


if __name__ == '__main__':
  absltest.main()
