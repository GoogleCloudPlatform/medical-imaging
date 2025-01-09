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
"""Tests for logging util test."""
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized

from pathology.dicom_proxy import logging_util
from pathology.dicom_proxy import proxy_const


class LoggingUtilTest(parameterized.TestCase):

  @parameterized.named_parameters([
      dict(testcase_name='empty', header={}, expected={}),
      dict(
          testcase_name='unchanged',
          header={'ABC': '123'},
          expected={'ABC': '123'},
      ),
      dict(
          testcase_name='direct_match',
          header={
              'ABC': '123',
              proxy_const.HeaderKeywords.AUTH_HEADER_KEY: 'abc',
              proxy_const.HeaderKeywords.IAP_JWT_HEADER: '123',
              proxy_const.HeaderKeywords.COOKIE: 'efg',
          },
          expected={
              'ABC': '123',
              proxy_const.HeaderKeywords.AUTH_HEADER_KEY: (
                  logging_util._REMOVED_FROM_LOG
              ),
              proxy_const.HeaderKeywords.IAP_JWT_HEADER: (
                  logging_util._REMOVED_FROM_LOG
              ),
              proxy_const.HeaderKeywords.COOKIE: logging_util._REMOVED_FROM_LOG,
          },
      ),
      dict(
          testcase_name='norm_match',
          header={
              'ABC': '123',
              proxy_const.HeaderKeywords.AUTH_HEADER_KEY.lower(): '123',
              proxy_const.HeaderKeywords.IAP_JWT_HEADER.lower(): 'efg',
              proxy_const.HeaderKeywords.COOKIE: 'cfbg',
          },
          expected={
              'ABC': '123',
              proxy_const.HeaderKeywords.AUTH_HEADER_KEY.lower(): (
                  logging_util._REMOVED_FROM_LOG
              ),
              proxy_const.HeaderKeywords.IAP_JWT_HEADER.lower(): (
                  logging_util._REMOVED_FROM_LOG
              ),
              proxy_const.HeaderKeywords.COOKIE: logging_util._REMOVED_FROM_LOG,
          },
      ),
  ])
  def test_mask_privileged_header_values(self, header, expected):
    self.assertEqual(
        logging_util.mask_privileged_header_values(header), expected
    )

  @flagsaver.flagsaver(
      keywords_to_mask_from_connection_header_log=[
          'ABC',
          proxy_const.HeaderKeywords.COOKIE,
      ]
  )
  def test_mask_privileged_header_values_flags(self):
    self.assertEqual(
        logging_util.mask_privileged_header_values({
            'ABC': '123',
            proxy_const.HeaderKeywords.AUTH_HEADER_KEY: 'abc',
            proxy_const.HeaderKeywords.IAP_JWT_HEADER: '123',
            proxy_const.HeaderKeywords.COOKIE: 'efg',
        }),
        {
            'ABC': logging_util._REMOVED_FROM_LOG,
            proxy_const.HeaderKeywords.AUTH_HEADER_KEY: 'abc',
            proxy_const.HeaderKeywords.IAP_JWT_HEADER: '123',
            proxy_const.HeaderKeywords.COOKIE: logging_util._REMOVED_FROM_LOG,
        },
    )


if __name__ == '__main__':
  absltest.main()
