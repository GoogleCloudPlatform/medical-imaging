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

_FUNCTIONAL_GROUP_MACRO_IODS = [
    'Confocal Microscopy Tiled Pyramidal Image IOD Modules',
    'Confocal Microscopy Image IOD Modules',
    'Photoacoustic Image IOD Modules',
    'Enhanced Continuous RT Image IOD Modules',
    'Enhanced RT Image IOD Modules',
    (
        'Ophthalmic Optical Coherence Tomography B-scan Volume Analysis IOD'
        ' Modules'
    ),
    'Breast Projection X-Ray Image IOD Modules',
    'Legacy Converted Enhanced PET Image IOD Modules',
    'Legacy Converted Enhanced MR Image IOD Modules',
    'Legacy Converted Enhanced CT Image IOD Modules',
    'Intravascular Optical Coherence Tomography Image IOD Modules',
    'Enhanced US Volume IOD Modules',
    'Enhanced PET Image IOD Modules',
    'Breast Tomosynthesis Image IOD Modules',
    'X-Ray 3D Craniofacial Image IOD Modules',
    'X-Ray 3D Angiographic Image IOD Modules',
    'Ophthalmic Tomography Image IOD Modules',
    'Multi-frame Grayscale Byte Secondary Capture Image IOD Modules',
    'Multi-frame Grayscale Word Secondary Capture Image IOD Modules',
    'Multi-frame True Color Secondary Capture Image IOD Modules',
    'VL Whole Slide Microscopy Image IOD Modules',
    'Enhanced MR Image IOD Modules',
    'MR Spectroscopy IOD Modules',
    'Enhanced CT Image IOD Modules',
    'Enhanced XA Image IOD Modules',
    'Enhanced XRF Image IOD Modules',
    'Real-Time Video Endoscopic Image IOD Modules',
    'Real-Time Video Photographic Image IOD Modules',
    'Real-Time Audio Waveform IOD Modules',
    'Segmentation IOD Modules',
    'Parametric Map IOD Modules',
]

_ModuleDef = dicom_standard_util.ModuleDef
_ModuleName = dicom_standard_util.ModuleName
_ModuleRef = dicom_standard_util.ModuleRef
_ModuleUsage = dicom_standard_util.ModuleUsage


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

  @parameterized.parameters(_FUNCTIONAL_GROUP_MACRO_IODS)
  def test_get_iods_with_functional_group_macro(self, iod_name):
    standard_util = dicom_standard_util.DicomStandardIODUtil()
    self.assertTrue(standard_util.has_iod_functional_groups(iod_name))

  @parameterized.parameters(
      set(dicom_standard_util.DicomStandardIODUtil().list_dicom_iods())
      - set(_FUNCTIONAL_GROUP_MACRO_IODS)
  )
  def test_get_iod_no_functional_group_macro(self, iod_name):
    standard_util = dicom_standard_util.DicomStandardIODUtil()
    self.assertFalse(standard_util.has_iod_functional_groups(iod_name))

  def test_get_iod_functional_group_modules(self):
    standard_util = dicom_standard_util.DicomStandardIODUtil()
    result = standard_util.get_iod_functional_group_modules(
        dicom_standard_util.IODName(
            'VL Whole Slide Microscopy Image IOD Modules'
        )
    )
    self.assertEqual(
        result,
        [
            _ModuleDef(
                name=_ModuleName('Pixel Measures'),
                ref=_ModuleRef('sect_C.7.6.16.2.1'),
                usage=_ModuleUsage('M'),
            ),
            _ModuleDef(
                name=_ModuleName('Frame Content'),
                ref=_ModuleRef('sect_C.7.6.16.2.2'),
                usage=_ModuleUsage('U'),
            ),
            _ModuleDef(
                name=_ModuleName('Referenced Image'),
                ref=_ModuleRef('sect_C.7.6.16.2.5'),
                usage=_ModuleUsage('U'),
            ),
            _ModuleDef(
                name=_ModuleName('Derivation Image'),
                ref=_ModuleRef('sect_C.7.6.16.2.6'),
                usage=_ModuleUsage('C'),
            ),
            _ModuleDef(
                name=_ModuleName('Real World Value Mapping'),
                ref=_ModuleRef('sect_C.7.6.16.2.11'),
                usage=_ModuleUsage('U'),
            ),
            _ModuleDef(
                name=_ModuleName('Plane Position (Slide)'),
                ref=_ModuleRef('sect_C.8.12.6.1'),
                usage=_ModuleUsage('C'),
            ),
            _ModuleDef(
                name=_ModuleName('Optical Path Identification'),
                ref=_ModuleRef('sect_C.8.12.6.2'),
                usage=_ModuleUsage('C'),
            ),
            _ModuleDef(
                name=_ModuleName('Specimen Reference'),
                ref=_ModuleRef('sect_C.8.12.6.3'),
                usage=_ModuleUsage('U'),
            ),
            _ModuleDef(
                name=_ModuleName('Whole Slide Microscopy Image Frame Type'),
                ref=_ModuleRef('sect_C.8.12.9'),
                usage=_ModuleUsage('M'),
            ),
        ],
    )

  @parameterized.parameters(
      dicom_standard_util.DicomStandardIODUtil._STRING_TYPE_VR_SET
      - {'OB', 'OV', 'OL', 'OW'}
  )
  def test_all_string_vr_types_have_max_char_or_byte_length(self, vr):
    vr_type = {vr}
    self.assertTrue(
        dicom_standard_util.DicomStandardIODUtil.is_vr_str_type(vr_type)
    )
    self.assertGreater(
        max(
            dicom_standard_util.DicomStandardIODUtil.get_vr_max_chars(vr_type),
            dicom_standard_util.DicomStandardIODUtil.get_vr_max_bytes(vr_type),
        ),
        0,
    )

  def test_get_iod_functional_group_modules_raises_if_bad_iod_name(self):
    standard_util = dicom_standard_util.DicomStandardIODUtil()
    with self.assertRaises(dicom_standard_util.DICOMSpecMetadataError):
      standard_util.get_iod_functional_group_modules(
          dicom_standard_util.IODName('bad')
      )

  @parameterized.parameters([
      ('60xx1100', True),
      ('7Fxx0020', True),
      ('1000xxx2', True),
      ('0x002804x2', True),
      ('0xFFFEE0DD', False),
      ('00460250', False),
  ])
  def test_is_repeated_group_element_tag(self, tag_address, expected):
    standard_util = dicom_standard_util.DicomStandardIODUtil()
    self.assertEqual(
        standard_util.is_repeated_group_element_tag(tag_address), expected
    )


if __name__ == '__main__':
  absltest.main()
