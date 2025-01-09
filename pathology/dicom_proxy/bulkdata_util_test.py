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
# ==============================================================================
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import flask

from pathology.dicom_proxy import bulkdata_util
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import flask_util

_DICOMSTORE_BASEURL = dicom_url_util.DicomWebBaseURL(
    'V1', 'GCP_PROJECTID', 'GCP_LOCATION', 'FOO_DATASET', 'BAR_DICOMSTORE'
)
_SERIES_INSTANCE_UID = dicom_url_util.SeriesInstanceUID('123.456.789')
_STUDY_INSTANCE_UID = dicom_url_util.StudyInstanceUID('123.456')

_TEST_METADATA_WITHOUT_BULKDATA_URI = [
    dict(testcase_name='Empty', metadata={}),
    dict(
        testcase_name='one_level_one_item',
        metadata={
            '123': {bulkdata_util._VR: 'LO', bulkdata_util._VALUE: ['abc']}
        },
    ),
    dict(
        testcase_name='one_level_two_items',
        metadata={
            '123': {bulkdata_util._VR: 'LO', bulkdata_util._VALUE: ['abc']},
            '456': {bulkdata_util._VR: 'LO', bulkdata_util._VALUE: ['abc']},
        },
    ),
    dict(
        testcase_name='empty_sq',
        metadata={
            '123': {
                bulkdata_util._VR: bulkdata_util._SQ,
                bulkdata_util._VALUE: [],
            }
        },
    ),
    dict(
        testcase_name='sq_with_item',
        metadata={
            '123': {
                bulkdata_util._VR: bulkdata_util._SQ,
                bulkdata_util._VALUE: [{
                    '123': {
                        bulkdata_util._VR: 'LO',
                        bulkdata_util._VALUE: ['abc'],
                    }
                }],
            }
        },
    ),
    dict(
        testcase_name='sq_with_sq_wth_item',
        metadata={
            '123': {
                bulkdata_util._VR: bulkdata_util._SQ,
                bulkdata_util._VALUE: [{
                    '123': {
                        bulkdata_util._VR: bulkdata_util._SQ,
                        bulkdata_util._VALUE: [{
                            '123': {
                                bulkdata_util._VR: 'LO',
                                bulkdata_util._VALUE: ['abc'],
                            }
                        }],
                    }
                }],
            }
        },
    ),
]

_TEST_METADATA_WITH_BULKDATA_URI = [
    dict(
        testcase_name='one_level_one_item',
        metadata={
            '123': {
                bulkdata_util._VR: 'OB',
                bulkdata_util.BULK_DATA_URI_KEY: 'abc',
            }
        },
    ),
    dict(
        testcase_name='one_level_two_items',
        metadata={
            '123': {bulkdata_util._VR: 'LO', bulkdata_util._VALUE: ['abc']},
            '456': {
                bulkdata_util._VR: 'OB',
                bulkdata_util.BULK_DATA_URI_KEY: 'abc',
            },
        },
    ),
    dict(
        testcase_name='sq_with_item',
        metadata={
            '123': {
                bulkdata_util._VR: bulkdata_util._SQ,
                bulkdata_util._VALUE: [{
                    '123': {
                        bulkdata_util._VR: 'OB',
                        bulkdata_util.BULK_DATA_URI_KEY: 'abc',
                    }
                }],
            }
        },
    ),
    dict(
        testcase_name='sq_with_sq_wth_item',
        metadata={
            '123': {
                bulkdata_util._VR: bulkdata_util._SQ,
                bulkdata_util._VALUE: [{
                    '123': {
                        bulkdata_util._VR: bulkdata_util._SQ,
                        bulkdata_util._VALUE: [{
                            '123': {
                                bulkdata_util._VR: 'OB',
                                bulkdata_util.BULK_DATA_URI_KEY: 'abc',
                            }
                        }],
                    }
                }],
            }
        },
    ),
]


class BulkdataUtilTest(parameterized.TestCase):

  def test_init_fork_module_state(self):
    bulkdata_util._dicom_store_versions_supporting_bulkdata = None
    bulkdata_util._dicom_store_versions_supporting_bulkdata_lock = None
    bulkdata_util._init_fork_module_state()
    self.assertIsInstance(
        bulkdata_util._dicom_store_versions_supporting_bulkdata, set
    )
    self.assertEmpty(bulkdata_util._dicom_store_versions_supporting_bulkdata)
    self.assertIsNotNone(
        bulkdata_util._dicom_store_versions_supporting_bulkdata_lock
    )

  @parameterized.named_parameters(_TEST_METADATA_WITHOUT_BULKDATA_URI)
  def test_does_json_have_bulkdata_uri_key_false(self, metadata):
    self.assertFalse(bulkdata_util._does_json_have_bulkdata_uri_key(metadata))

  @parameterized.named_parameters(_TEST_METADATA_WITH_BULKDATA_URI)
  def test_does_json_have_bulkdata_uri_key_true(self, metadata):
    self.assertTrue(bulkdata_util._does_json_have_bulkdata_uri_key(metadata))

  @parameterized.named_parameters([
      dict(
          testcase_name='list_of_items',
          metadata=[{
              '123': {
                  bulkdata_util._VR: 'OB',
                  bulkdata_util.BULK_DATA_URI_KEY: 'abc',
              }
          }],
      ),
      dict(
          testcase_name='string',
          metadata='abc',
      ),
  ])
  def test_does_json_have_bulkdata_uri_key_raises(self, metadata):
    with self.assertRaises(bulkdata_util._UnexpectedDicomJsonMetadataError):
      bulkdata_util._does_json_have_bulkdata_uri_key(metadata)

  @parameterized.named_parameters([
      dict(
          testcase_name='minimum',
          store_url_version='https://healthcare.googleapis.com/v2',
          expected='v2',
      ),
      dict(
          testcase_name='normal',
          store_url_version=(
              'HTTPS://healthcare.googleapis.com/v3/studies/123.12/series/12.12'
          ),
          expected='v3',
      ),
  ])
  def test_parse_store_url_version(self, store_url_version, expected):
    self.assertEqual(
        bulkdata_util._parse_store_url_version(store_url_version), expected
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='no_version',
          store_url_version='https://healthcare.googleapis.com/',
      ),
      dict(
          testcase_name='missing_version',
          store_url_version=(
              'HTTPS://healthcare.googleapis.com//studies/123.12/series/12.12'
          ),
      ),
      dict(
          testcase_name='api_version_is_space',
          store_url_version=(
              'HTTPS://healthcare.googleapis.com/ /studies/123.12/series/12.12'
          ),
      ),
  ])
  def test_parse_store_url_version_raises(self, store_url_version):
    with self.assertRaises(bulkdata_util.InvalidDicomStoreUrlError):
      bulkdata_util._parse_store_url_version(store_url_version)

  @parameterized.named_parameters([
      dict(
          testcase_name='v1',
          store_url_version='v1',
          expected=False,
      ),
      dict(
          testcase_name='v1beta1',
          store_url_version='v1beta1',
          expected=True,
      ),
  ])
  def test_does_store_version_support_bulkdata(
      self, store_url_version, expected
  ):
    self.assertEqual(
        bulkdata_util._does_store_version_support_bulkdata(store_url_version),
        expected,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='_store_version_supports_bulkdata',
          store_base_url='HTTPS://healthcare.googleapis.com/v1beta1',
          expected=True,
      ),
      dict(
          testcase_name='_store_version_detected_supporting_bulkdata',
          store_base_url='HTTPS://healthcare.googleapis.com/v1',
          expected=True,
      ),
      dict(
          testcase_name='_store_version_detected_not_supporting_bulkdata',
          store_base_url='HTTPS://healthcare.googleapis.com/v2',
          expected=False,
      ),
  ])
  def test_does_dicom_store_support_bulkdata(self, store_base_url, expected):
    default = bulkdata_util._is_debugging
    try:
      bulkdata_util._dicom_store_versions_supporting_bulkdata.add('v1')
      bulkdata_util._is_debugging = False
      self.assertEqual(
          bulkdata_util.does_dicom_store_support_bulkdata(store_base_url),
          expected,
      )
    finally:
      bulkdata_util._dicom_store_versions_supporting_bulkdata.clear()
      bulkdata_util._is_debugging = default

  @parameterized.parameters(
      ['localhost', '', 'HTTPS://healthcare.googleapis.com/v1beta1']
  )
  def test_set_dicom_store_supports_bulkdata_does_not_set(self, url):
    bulkdata_util._dicom_store_versions_supporting_bulkdata.clear()
    bulkdata_util.set_dicom_store_supports_bulkdata(url)
    self.assertEmpty(bulkdata_util._dicom_store_versions_supporting_bulkdata)

  def test_set_dicom_store_supports_bulkdata(self):
    bulkdata_util._dicom_store_versions_supporting_bulkdata.clear()
    bulkdata_util.set_dicom_store_supports_bulkdata(
        'HTTPS://healthcare.googleapis.com/v1'
    )
    self.assertIn('v1', bulkdata_util._dicom_store_versions_supporting_bulkdata)
    bulkdata_util._dicom_store_versions_supporting_bulkdata.clear()

  def test_get_url_str(self):
    self.assertEqual(
        bulkdata_util._get_url('HTTPS://healthcare.googleapis.com/v1beta1'),
        'HTTPS://healthcare.googleapis.com/v1beta1',
    )

  def test_get_url_dicomwebbaseurl(self):
    self.assertEqual(
        bulkdata_util._get_url(
            dicom_url_util.DicomWebBaseURL('v1beta1', 'p', 'l', 'd', 'di')
        ),
        'https://healthcare.googleapis.com/v1beta1/projects/p/locations/l/datasets/d/dicomStores/di/dicomWeb',
    )

  @parameterized.named_parameters(_TEST_METADATA_WITH_BULKDATA_URI)
  def test_dicom_store_metadata_for_identifies_support_in_metadata(
      self, metadata
  ):
    try:
      bulkdata_util._dicom_store_versions_supporting_bulkdata.clear()
      url = dicom_url_util.DicomWebBaseURL('v1', 'p', 'l', 'd', 'di')
      bulkdata_util.test_dicom_store_metadata_for_bulkdata_uri_support(
          url, metadata
      )
      self.assertIn(
          'v1', bulkdata_util._dicom_store_versions_supporting_bulkdata
      )
    finally:
      bulkdata_util._dicom_store_versions_supporting_bulkdata.clear()

  @parameterized.named_parameters(_TEST_METADATA_WITHOUT_BULKDATA_URI)
  def test_dicom_store_metadata_for_does_not_identify_support_in_metadata(
      self, metadata
  ):
    try:
      bulkdata_util._dicom_store_versions_supporting_bulkdata.clear()
      url = dicom_url_util.DicomWebBaseURL('v1', 'p', 'l', 'd', 'di')
      bulkdata_util.test_dicom_store_metadata_for_bulkdata_uri_support(
          url, metadata
      )
      self.assertEmpty(bulkdata_util._dicom_store_versions_supporting_bulkdata)
    finally:
      bulkdata_util._dicom_store_versions_supporting_bulkdata.clear()

  @parameterized.parameters([
      '',
      'localhost',
      dicom_url_util.DicomWebBaseURL('v1beta1', 'p', 'l', 'd', 'di'),
  ])
  def test_dicom_store_metadata_does_not_set_flag_when_metadata_defines_bulkdata(
      self, url
  ):
    try:
      bulkdata_util._dicom_store_versions_supporting_bulkdata.clear()
      metadata = {
          '123': {
              bulkdata_util._VR: 'OB',
              bulkdata_util.BULK_DATA_URI_KEY: 'abc',
          }
      }
      bulkdata_util.test_dicom_store_metadata_for_bulkdata_uri_support(
          url, metadata
      )
      self.assertEmpty(bulkdata_util._dicom_store_versions_supporting_bulkdata)
    finally:
      bulkdata_util._dicom_store_versions_supporting_bulkdata.clear()

  def test_get_dicom_proxy_project_regex(self):
    result = bulkdata_util._GET_TILE_SERVER_PROJECT_REGEX.fullmatch(
        f'https://dpas.cloudflyer.info/tile/{_DICOMSTORE_BASEURL}/'
        f'{_STUDY_INSTANCE_UID}/{_SERIES_INSTANCE_UID}/instances'
    )
    self.assertEqual(
        result.groups(), ('https', f'{str(_DICOMSTORE_BASEURL)[3:]}')  # pytype: disable=attribute-error
    )

  @flagsaver.flagsaver(bulk_data_proxy_url='https://www.test.com/')
  def test_get_bulk_data_base_url_returns_proxy_url(self):
    self.assertEqual(
        bulkdata_util.get_bulk_data_base_url(_DICOMSTORE_BASEURL),
        'https://www.test.com',
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='success',
          root='https://www.test.com',
          path='/projects/proj/locations/loc/datasets/ds/dicomStores/store/dicomWeb/foo',
          expected='http://www.test.com/tile/v2/projects/proj/locations/loc/datasets/ds/dicomStores/store/dicomWeb',
      ),
      dict(
          testcase_name='bad_protocal',
          root='www.test.com',
          path='/projects/proj/locations/loc/datasets/ds/dicomStores/store/dicomWeb/foo',
          expected='',
      ),
      dict(
          testcase_name='bad_path',
          root='https://www.test.com',
          path='/projects/proj/loc/datasets/ds/dicomStores/store/dicomWeb/foo',
          expected='',
      ),
  ])
  @mock.patch.object(flask_util, 'get_base_url', autospec=True)
  @mock.patch.object(flask_util, 'get_url_root', autospec=True)
  @flagsaver.flagsaver(bulkdata_uri_protocol='http')
  def test_get_bulk_data_base_url(
      self, mk_get_root, mk_get_base_url, root, path, expected
  ):
    mk_get_root.return_value = root
    mk_get_base_url.return_value = f'{root}{path}'
    self.assertEqual(
        bulkdata_util.get_bulk_data_base_url(
            dicom_url_util.DicomWebBaseURL(
                'v2',
                'GCP_PROJECTID',
                'GCP_LOCATION',
                'FOO_DATASET',
                'BAR_DICOMSTORE',
            )
        ),
        expected,
    )

  @mock.patch.object(flask_util, 'get_base_url', autospec=True)
  @mock.patch.object(flask_util, 'get_url_root', autospec=True)
  @flagsaver.flagsaver(bulkdata_uri_protocol='https')
  def test_proxy_dicom_store_bulkdata_response_changed(
      self, mk_get_root, mk_get_base_url
  ):
    root = 'https://www.test.com'
    path = '/projects/proj/locations/loc/datasets/ds/dicomStores/store/dicomWeb/foo'
    mk_get_root.return_value = root
    mk_get_base_url.return_value = f'{root}{path}'
    response = mock.create_autospec(flask.Response, instance=True)
    url = b'"https://healthcare.googleapis.com/v1beta1/projects/prj/locations/uswest/datasets/dataset/dicomStores/ds/dicomWeb/studies/1.2/series/1.2.3/instances/1.2.3/bulkdata/data"'
    response.data = b''.join([url, url])
    bulkdata_util.proxy_dicom_store_bulkdata_response(
        _DICOMSTORE_BASEURL, response
    )
    expected_url = b'"https://www.test.com/tile/V1/projects/proj/locations/loc/datasets/ds/dicomStores/store/dicomWeb/studies/1.2/series/1.2.3/instances/1.2.3/bulkdata/data"'
    self.assertEqual(response.data, b''.join([expected_url, expected_url]))

  @mock.patch.object(flask_util, 'get_base_url', autospec=True)
  @mock.patch.object(flask_util, 'get_url_root', autospec=True)
  @flagsaver.flagsaver(bulkdata_uri_protocol='https')
  def test_proxy_dicom_store_bulkdata_response_cannot_determine_bulkuri_nochange(
      self, mk_get_root, mk_get_base_url
  ):
    root = 'httpswww.test.com'
    path = '/projects/proj/locations/loc/datasets/ds/dicomStores/store/dicomWeb/foo'
    mk_get_root.return_value = root
    mk_get_base_url.return_value = f'{root}{path}'
    response = mock.create_autospec(flask.Response, instance=True)
    url = b'"https://healthcare.googleapis.com/v1beta1/projects/prj/locations/uswest/datasets/dataset/dicomStores/ds/dicomWeb/studies/1.2/series/1.2.3/instances/1.2.3/bulkdata/data"'
    response.data = b''.join([url, url])
    bulkdata_util.proxy_dicom_store_bulkdata_response(
        _DICOMSTORE_BASEURL, response
    )
    self.assertEqual(response.data, b''.join([url, url]))


if __name__ == '__main__':
  absltest.main()
