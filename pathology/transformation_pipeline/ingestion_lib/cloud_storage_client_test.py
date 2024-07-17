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
"""Tests for cloud_storage_client."""
from typing import Optional
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
import google.api_core
from google.cloud import storage

from transformation_pipeline.ingestion_lib import cloud_storage_client


class CloudStorageClientTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    cloud_storage_client.reset_storage_client('PROJECT_ID')
    self._mocked_classes = []
    self._mocks = [
        mock.patch(
            'google.auth.default',
            autospec=True,
            return_value=(
                mock.MagicMock(universe_domain='googleapis.com'),
                mock.MagicMock(),
            ),
        )
    ]
    for mk in self._mocks:
      self._mocked_classes.append(mk.start())
      self.addCleanup(mk.stop)

  @parameterized.parameters([
      ('gs://bucket/endpoint', 'google/test.foo'),
      ('gs://bucket/endpoint/', 'google/test.foo'),
      ('gs://bucket/endpoint/google', 'test.foo'),
      ('gs://bucket/endpoint/google/', 'test.foo'),
      ('gs://bucket/endpoint/google/test.foo', ''),
      ('gs://bucket/endpoint/google/test.foo', None),
  ])
  def test_get_blob_succeeds(self, gs_uri: str, filename: Optional[str]):
    result = cloud_storage_client._get_blob(gs_uri, filename)
    self.assertEqual(result.bucket.name, 'bucket')
    self.assertEqual(result.name, 'endpoint/google/test.foo')

  def test_get_blob_fails_with_invalid_uri(self):
    with self.assertRaises(ValueError):
      _ = cloud_storage_client._get_blob('invalid bucket', 'test.foo')

  def test_copy_blob_to_uri_none_input1(self):
    """Tests copy_blob_to_uri.

    source_uri = None; Returns True; operation succeeded. nothing to do.
    """
    self.assertTrue(
        cloud_storage_client.copy_blob_to_uri(source_uri=None, dst_uri='test')
    )

  def test_copy_blob_to_uri_none_input2(self):
    """Tests copy_blob_to_uri.

    dst_uri = None; Returns True; operation succeeded. nothing to do.
    """
    self.assertTrue(
        cloud_storage_client.copy_blob_to_uri(source_uri='test', dst_uri=None)
    )

  def test_copy_blob_to_uri_fails_with_invalid_uri(self):
    self.assertFalse(
        cloud_storage_client.copy_blob_to_uri(
            source_uri='gs://source/bucket/test.svs', dst_uri='invalid'
        )
    )

  @mock.patch.object(storage.Bucket, 'exists', return_value=True, autospec=True)
  @mock.patch.object(storage.Blob, 'exists', return_value=True, autospec=True)
  @mock.patch.object(storage.Blob, 'reload', return_value=True, autospec=True)
  @mock.patch(
      'google.cloud.storage.Blob.md5_hash',
      new_callable=mock.PropertyMock,
      side_effect=[1, 2],
  )
  def test_copy_blob_to_uri_source_and_dest_exist_md5_dont_match(
      self, unused_mk1, unused_mk2, unused_mk3, unused_mk4
  ):
    with mock.patch.object(
        storage.Blob, 'rewrite', autospec=True, return_value=(False, 1, 1)
    ) as rewrite_blob:
      self.assertTrue(
          cloud_storage_client.copy_blob_to_uri(
              'gs://source/bucket/test.svs', 'gs://outbucket/converted'
          )
      )
      self.assertTrue(rewrite_blob.called)

  @mock.patch.object(storage.Bucket, 'exists', return_value=True, autospec=True)
  @mock.patch.object(storage.Blob, 'exists', return_value=True, autospec=True)
  @mock.patch.object(storage.Blob, 'reload', return_value=True, autospec=True)
  @mock.patch(
      'google.cloud.storage.Blob.md5_hash',
      new_callable=mock.PropertyMock,
      side_effect=[1, 1],
  )
  def test_copy_blob_to_uri_source_and_dest_exist_md5_match(
      self, unused_mk1, unused_mk2, unused_mk3, unused_mk4
  ):
    with mock.patch.object(
        storage.Blob, 'rewrite', autospec=True, return_value=(False, 1, 1)
    ) as rewrite_blob:
      self.assertTrue(
          cloud_storage_client.copy_blob_to_uri(
              'gs://source/bucket/test.svs', 'gs://outbucket/converted'
          )
      )
      self.assertFalse(rewrite_blob.called)

  @mock.patch.object(storage.Bucket, 'exists', return_value=True, autospec=True)
  @mock.patch.object(
      storage.Blob, 'exists', autospec=True, side_effect=[True, False]
  )
  @mock.patch.object(storage.Blob, 'reload', autospec=True, return_value=True)
  @mock.patch(
      'google.cloud.storage.Blob.md5_hash',
      new_callable=mock.PropertyMock,
      side_effect=[1, None],
  )
  def test_copy_blob_to_uri_source_and_not_dest_exist(
      self, unused_mk1, unused_mk2, unused_mk3, unused_mk4
  ):
    with mock.patch.object(
        storage.Blob, 'rewrite', autospec=True, return_value=(False, 1, 1)
    ) as rewrite_blob:
      self.assertTrue(
          cloud_storage_client.copy_blob_to_uri(
              'gs://source/bucket/test.svs', 'gs://outbucket/converted'
          )
      )
      self.assertTrue(rewrite_blob.called)

  @mock.patch.object(storage.Bucket, 'exists', return_value=True, autospec=True)
  @mock.patch.object(
      storage.Blob, 'exists', autospec=True, side_effect=[False, False]
  )
  def test_copy_blob_to_uri_source_and_dest_not_exist(
      self, unused_mk1, unused_mk2
  ):
    self.assertFalse(
        cloud_storage_client.copy_blob_to_uri(
            'gs://source/bucket/test.svs', 'gs://outbucket/converted'
        )
    )

  @mock.patch.object(storage.Bucket, 'exists', return_value=True, autospec=True)
  @mock.patch.object(
      storage.Blob,
      'exists',
      autospec=True,
      side_effect=[False, False, False, False],
  )
  def test_copy_blob_to_uri_dest_bucket_does_exist_local_file(
      self, unused_mk1, unused_mk2
  ):
    with mock.patch.object(
        storage.Blob, 'upload_from_filename', return_value=True, autospec=True
    ) as upload_from_filename:
      self.assertTrue(
          cloud_storage_client.copy_blob_to_uri(
              'gs://source/bucket/test.svs',
              'gs://outbucket/converted',
              'foo.svs',
          )
      )
      self.assertTrue(upload_from_filename.called)

  @mock.patch.object(
      storage.Bucket, 'exists', return_value=False, autospec=True
  )
  def test_copy_blob_to_uri_dest_bucket_does_not_exist(self, _):
    self.assertFalse(
        cloud_storage_client.copy_blob_to_uri(
            'gs://source/bucket/test.svs', 'gs://outbucket/converted'
        )
    )

  def test_upload_blob_to_uri_no_local_file(self):
    self.assertTrue(
        cloud_storage_client.upload_blob_to_uri('', 'dest', 'foo.svs')
    )

  def test_upload_blob_to_uri_no_dest_uri(self):
    self.assertTrue(
        cloud_storage_client.upload_blob_to_uri('local.svs', '', 'foo.svs')
    )

  def test_upload_blob_to_uri_invalid_uri(self):
    self.assertFalse(
        cloud_storage_client.upload_blob_to_uri(
            'local.svs', 'invalid', 'foo.svs'
        )
    )

  @parameterized.parameters([
      ('gs://bucket/endpoint', 'google/test.foo'),
      ('gs://bucket/endpoint/', 'google/test.foo'),
      ('gs://bucket/endpoint/google', 'test.foo'),
      ('gs://bucket/endpoint/google/', 'test.foo'),
      ('gs://bucket/endpoint/google/test.foo', ''),
      ('gs://bucket/endpoint/google/test.foo', None),
  ])
  @mock.patch.object(storage.Bucket, 'exists', return_value=True, autospec=True)
  @mock.patch.object(storage.Blob, 'exists', return_value=False, autospec=True)
  @mock.patch.object(storage.Blob, 'upload_from_filename', autospec=True)
  def test_upload_blob_to_uri_succeeds(self, bucket, out_file, mk1, mk2, mk3):
    self.assertTrue(
        cloud_storage_client.upload_blob_to_uri('local.foo', bucket, out_file)
    )
    mk1.assert_called_once()
    mk2.assert_called_once()
    mk3.assert_called_once()

  @mock.patch.object(
      storage.Bucket, 'exists', return_value=False, autospec=True
  )
  def test_upload_blob_to_uri_bucket_missing_fails(self, mk):
    self.assertFalse(
        cloud_storage_client.upload_blob_to_uri(
            'local.foo', 'gs://outbucket', 'test.foo'
        )
    )
    mk.assert_called_once()

  @mock.patch.object(storage.Bucket, 'exists', return_value=True, autospec=True)
  @mock.patch.object(storage.Blob, 'exists', return_value=False, autospec=True)
  @mock.patch.object(
      storage.Blob,
      'upload_from_filename',
      side_effect=google.api_core.exceptions.NotFound('error'),
      autospec=True,
  )
  def test_upload_blob_to_uri_upload_fails(self, mk1, mk2, mk3):
    self.assertFalse(
        cloud_storage_client.upload_blob_to_uri(
            'local.foo', 'gs://outbucket', 'test.foo'
        )
    )
    mk1.assert_called_once()
    mk2.assert_called_once()
    mk3.assert_called_once()


if __name__ == '__main__':
  absltest.main()
