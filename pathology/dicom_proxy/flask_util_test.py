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
"""Tests for flask util."""
import http
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized

from pathology.dicom_proxy import flask_util


class FlaskUtilTest(parameterized.TestCase):

  @mock.patch.object(flask_util, 'get_first_key_args', autospec=True)
  def test_get_dicom_proxy_request_args(self, mock_get_flask_args):
    mock_get_flask_args.return_value = {
        ' IcCpRoFiLe ': 'abcd',
        ' disable_caching ': 'TRUE',
        ' DownSample ': '2.0',
        ' interpolation ': 'AREA',
        'quality': '100',
        'Not_in_list': 'bogus',
        '': '',
    }
    self.assertEqual(
        flask_util.get_dicom_proxy_request_args(),
        {
            'disable_caching': 'TRUE',
            'downsample': '2.0',
            'iccprofile': 'abcd',
            'interpolation': 'AREA',
            'quality': '100',
        },
    )

  def test_parse_downsample(self):
    value = 5.0
    self.assertEqual(
        flask_util.parse_downsample({'downsample': str(value)}),
        value,
    )

  def test_parse_downsample_no_value(self):
    self.assertEqual(flask_util.parse_downsample({}), 1.0)

  def test_parse_downsample_throws(self):
    with self.assertRaises(ValueError):
      flask_util.parse_downsample({'downsample': '0.5'})

  @parameterized.parameters(['Yes', 'True', 'On', '1'])
  def test_parse_cache_enabled_false(self, val):
    self.assertFalse(flask_util.parse_cache_enabled({'disable_caching': val}))

  @parameterized.parameters(['No', 'False', 'Off', '0'])
  def test_parse_cache_disabled_true(self, val):
    self.assertTrue(flask_util.parse_cache_enabled({'disable_caching': val}))

  def test_parse_cache_disabled_raises(self):
    with self.assertRaises(ValueError):
      flask_util.parse_cache_enabled({'disable_caching': 'ABC'})

  @parameterized.parameters([Exception('bad request'), 'bad request'])
  def test_exception_flask_response(self, exp):
    response = flask_util.exception_flask_response(exp)

    self.assertEqual(response.data, b'bad request')
    self.assertEqual(response.status_code, http.HTTPStatus.BAD_REQUEST)
    self.assertEqual(response.content_type, 'text/plain')

  @parameterized.parameters([
      (['B', 'c'], {'b': 2, 'c': 3}),
      (['b', 'C'], {'b': 2, 'c': 3}),
      ({'b', 'C'}, {'b': 2, 'c': 3}),
      (['Z'], {}),
      (['B', 'B'], {'b': 2}),
      ([], {}),
  ])
  def test_norm_dict_keys(self, request, expected):
    input_dict = {' A ': 1, ' b ': 2, ' C ': 3, ' d ': 4}
    self.assertEqual(flask_util.norm_dict_keys(input_dict, request), expected)

  @parameterized.named_parameters([
      dict(testcase_name='none', keylist={}, expected=set()),
      dict(testcase_name='empty', keylist={'includefield': []}, expected=set()),
      dict(
          testcase_name='one',
          keylist={'includefield': ['all']},
          expected=set(['all']),
      ),
      dict(
          testcase_name='three',
          keylist={'includefield': ['all', 'one', 'two']},
          expected=set(['all', 'one', 'two']),
      ),
      dict(
          testcase_name='other',
          keylist={'other': ['all', 'one', 'two']},
          expected=set(),
      ),
      dict(
          testcase_name='mixed',
          keylist={
              'includefield': ['four', 'five'],
              'other': ['all', 'one', 'two'],
          },
          expected=set(['four', 'five']),
      ),
      dict(
          testcase_name='mixed_with_list',
          keylist={
              'includefield': ['four', 'five', 'six, seven, eight'],
              'other': ['all', 'one', 'two'],
          },
          expected=set(['four', 'five', 'six', 'seven', 'eight']),
      ),
  ])
  @mock.patch.object(flask_util, 'get_key_args_list', autospec=True)
  def test_get_includefields(self, mock_get_key_args_list, keylist, expected):
    mock_get_key_args_list.return_value = keylist
    self.assertEqual(flask_util.get_includefields(), expected)

  @parameterized.named_parameters([
      dict(testcase_name='none', keylist={}, expected=False),
      dict(testcase_name='empty', keylist={'includefield': []}, expected=False),
      dict(
          testcase_name='all',
          keylist={'includefield': ['all']},
          expected=True,
      ),
      dict(
          testcase_name='keyword',
          keylist={'includefield': ['PixelData']},
          expected=True,
      ),
      dict(
          testcase_name='address',
          keylist={'includefield': ['7FE00010']},
          expected=True,
      ),
      dict(
          testcase_name='address2',
          keylist={'includefield': ['0x7FE00010']},
          expected=True,
      ),
      dict(
          testcase_name='non-binary_tag',
          keylist={'includefield': ['PatientName']},
          expected=False,
      ),
      dict(
          testcase_name='mixed',
          keylist={'includefield': ['PatientName', '7FE00010']},
          expected=True,
      ),
      dict(
          testcase_name='badvalue',
          keylist={'includefield': ['BadValue']},
          expected=False,
      ),
  ])
  @mock.patch.object(flask_util, 'get_key_args_list', autospec=True)
  def test_includefield_binary_tags(
      self, mock_get_key_args_list, keylist, expected
  ):
    """Return true if dicomWeb includefield param requests all tags or binary tag."""
    mock_get_key_args_list.return_value = keylist
    self.assertEqual(flask_util.includefield_binary_tags(), expected)

  @parameterized.named_parameters([
      dict(testcase_name='none', url='https://test.com/abc/efg', expected=''),
      dict(
          testcase_name='param',
          url='https://test.com/abc/efg?param=1',
          expected='?param=1',
      ),
  ])
  @mock.patch.object(flask_util, 'get_full_request_url', autospec=True)
  def test_get_parameters(self, mk_full_request, url, expected):
    mk_full_request.return_value = url
    self.assertEqual(flask_util.get_parameters(), expected)


if __name__ == '__main__':
  absltest.main()
