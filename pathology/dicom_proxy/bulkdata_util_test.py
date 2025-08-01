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
_VR = 'vr'
_VALUE = 'Value'
_SQ = 'SQ'

_TEST_METADATA_WITHOUT_BULKDATA_URI = [
    dict(testcase_name='Empty', metadata={}),
    dict(
        testcase_name='one_level_one_item',
        metadata={'123': {_VR: 'LO', _VALUE: ['abc']}},
    ),
    dict(
        testcase_name='one_level_two_items',
        metadata={
            '123': {_VR: 'LO', _VALUE: ['abc']},
            '456': {_VR: 'LO', _VALUE: ['abc']},
        },
    ),
    dict(
        testcase_name='empty_sq',
        metadata={
            '123': {
                _VR: _SQ,
                _VALUE: [],
            }
        },
    ),
    dict(
        testcase_name='sq_with_item',
        metadata={
            '123': {
                _VR: _SQ,
                _VALUE: [
                    {
                        '123': {
                            _VR: 'LO',
                            _VALUE: ['abc'],
                        }
                    }
                ],
            }
        },
    ),
    dict(
        testcase_name='sq_with_sq_wth_item',
        metadata={
            '123': {
                _VR: _SQ,
                _VALUE: [{
                    '123': {
                        _VR: _SQ,
                        _VALUE: [
                            {
                                '123': {
                                    _VR: 'LO',
                                    _VALUE: ['abc'],
                                }
                            }
                        ],
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
                _VR: 'OB',
                bulkdata_util.BULK_DATA_URI_KEY: 'abc',
            }
        },
    ),
    dict(
        testcase_name='one_level_two_items',
        metadata={
            '123': {_VR: 'LO', _VALUE: ['abc']},
            '456': {
                _VR: 'OB',
                bulkdata_util.BULK_DATA_URI_KEY: 'abc',
            },
        },
    ),
    dict(
        testcase_name='sq_with_item',
        metadata={
            '123': {
                _VR: _SQ,
                _VALUE: [{
                    '123': {
                        _VR: 'OB',
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
                _VR: _SQ,
                _VALUE: [{
                    '123': {
                        _VR: _SQ,
                        _VALUE: [{
                            '123': {
                                _VR: 'OB',
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
