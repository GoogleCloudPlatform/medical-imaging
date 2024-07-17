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
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized

from transformation_pipeline.ingestion_lib.dicom_gen import ingestion_dicom_store_urls

_BASE_TEST_URL = (
    'https://healthcare.googleapis.com/v1/projects/foo/locations'
    '/bar/datasets/shark/dicomStores/fin'
)


class IngestionDicomStoreUrlsTest(parameterized.TestCase):

  @flagsaver.flagsaver(dicomweb_url='test_url')
  def test_get_main_dicom_web_url_defined(self):
    self.assertEqual(
        ingestion_dicom_store_urls.get_main_dicom_web_url(), 'test_url'
    )

  @flagsaver.flagsaver(dicomweb_url='')
  def test_get_main_dicom_web_url_not_defined(self):
    with self.assertRaises(ValueError):
      ingestion_dicom_store_urls.get_main_dicom_web_url()

  @parameterized.parameters([
      _BASE_TEST_URL,
      f' {_BASE_TEST_URL} ',
      f' {_BASE_TEST_URL}/ ',
      f' {_BASE_TEST_URL}/dicomWeb ',
      f' {_BASE_TEST_URL}/dicomWeb/ ',
  ])
  def test_normalize_dicom_store_url_append_dicomweb(self, test_url):
    expectation = f'{_BASE_TEST_URL}/dicomWeb'
    self.assertEqual(
        ingestion_dicom_store_urls.normalize_dicom_store_url(test_url, True),
        expectation,
    )

  @parameterized.parameters([
      _BASE_TEST_URL,
      f' {_BASE_TEST_URL} ',
      f' {_BASE_TEST_URL}/ ',
      f' {_BASE_TEST_URL}/dicomWeb ',
      f' {_BASE_TEST_URL}/dicomWeb/ ',
  ])
  def test_normalize_dicom_store_url(self, test_url):
    expectation = _BASE_TEST_URL
    self.assertEqual(
        ingestion_dicom_store_urls.normalize_dicom_store_url(test_url, False),
        expectation,
    )

  @parameterized.parameters([
      (
          'http://healthcare.googleapis.com/v1/projects/foo/locations/bar/datasets'
          '/shark/dicomStores/fin'
      ),
      'https://healthcare.googleapis.com/v1/unmatched',
  ])
  def test_unexpected_dicom_store_url(self, test_url):
    self.assertEqual(
        ingestion_dicom_store_urls.normalize_dicom_store_url(test_url, False),
        test_url,
    )


if __name__ == '__main__':
  absltest.main()
