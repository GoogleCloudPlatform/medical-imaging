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
from absl.testing import parameterized

from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard_util


class DICOMStandardUtilTest(parameterized.TestCase):

  def test_all_tag_vr_types_are_recognized(self):
    """Test that VR types for all tags defined default tag json are recognized."""
    standard_util = dicom_standard_util.DicomStandardIODUtil()
    vr_types = set()
    for tag in standard_util._tags['main'].values():
      vr_types |= set(tag['vr'].split(' or '))
    for tag in standard_util._tags['mask'].values():
      vr_types |= set(tag['vr'].split(' or '))
    # remove retired VR Types which show up in retired tag def and are not
    # used / defined
    vr_types -= set(['OV', 'SV', 'UV', 'NONE'])
    for vr in vr_types:
      # Assert Remediation.
      # VR Code is undefined, VR code has been added to dicom standard
      # VR code needs to be added to: standard_util
      # Add VR code to vr type set and set character or byte length if
      # applicable.
      self.assertTrue(
          standard_util.is_recognized_vr(set([vr])),
          msg=f'Error vr code {vr} is not defined.',
      )

  def test_bogus_vr_is_not_recognized(self):
    """Test a undefined VR type is not recognized."""
    standard_util = dicom_standard_util.DicomStandardIODUtil()
    self.assertFalse(standard_util.is_recognized_vr(set(['BG'])))

  @parameterized.parameters([
      ({'FD'}, '12', '12.00'),
      ({'FL'}, '15', '15.00'),
      ({'SL'}, '7', '7'),
      ({'SS'}, '700', '700'),
      ({'UL'}, '800', '800'),
      ({'US'}, '1900', '1900'),
      ({'PN'}, 'Bob', 'Bob'),
      ({'CS'}, 'NO', 'NO'),
      ({'FD'}, 12, 12.00),
      ({'FL'}, 15, 15.00),
      ({'SL'}, 7, 7.000),
      ({'SS'}, 700, 700),
      ({'UL'}, 800, 800),
      ({'US'}, 1900, 1900),
  ])
  def test_get_normalized_tag_str_equal(self, keyword, val1, val2):
    standard_util = dicom_standard_util.DicomStandardIODUtil()

    val1 = standard_util.get_normalized_tag_str(keyword, val1)
    val2 = standard_util.get_normalized_tag_str(keyword, val2)

    self.assertEqual(val1, val2)

  @parameterized.parameters([
      ({'FD'}, '12', '12.001'),
      ({'FL'}, '15', '13.00'),
      ({'SL'}, '7', '71'),
      ({'SS'}, '700', '7001'),
      ({'UL'}, '800', '801'),
      ({'US'}, '1900', '1901'),
      ({'PN'}, 'Bob', 'Sarah'),
      ({'CS'}, 'NO', 'YES'),
      ({'FD'}, 13, 13.01),
      ({'FL'}, 15, 15.01),
      ({'SL'}, 7, 8),
      ({'SS'}, 700, 701),
      ({'UL'}, 800, 801),
      ({'US'}, 1900, 1901),
  ])
  def test_get_normalized_tag_str_not_equal(self, keyword, val1, val2):
    standard_util = dicom_standard_util.DicomStandardIODUtil()

    val1 = standard_util.get_normalized_tag_str(keyword, val1)
    val2 = standard_util.get_normalized_tag_str(keyword, val2)

    self.assertNotEqual(val1, val2)


if __name__ == '__main__':
  absltest.main()
