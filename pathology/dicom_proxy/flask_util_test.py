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
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized

from pathology.dicom_proxy import flask_util


class FlaskUtilTest(parameterized.TestCase):

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
