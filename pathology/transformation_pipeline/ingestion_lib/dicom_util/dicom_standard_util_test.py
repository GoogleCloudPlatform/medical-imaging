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
    'Breast Projection X-Ray Image Storage - For Presentation',
    'Breast Projection X-Ray Image Storage - For Processing',
    'Breast Tomosynthesis Image Storage',
    'Confocal Microscopy Image Storage',
    'Confocal Microscopy Tiled Pyramidal Image Storage',
    'Enhanced CT Image Storage',
    'Enhanced Continuous RT Image Storage',
    'Enhanced MR Image Storage',
    'Enhanced PET Image Storage',
    'Enhanced RT Image Storage',
    'Enhanced US Volume Storage',
    'Enhanced XA Image Storage',
    'Enhanced XRF Image Storage',
    (
        'Intravascular Optical Coherence Tomography Image Storage - For'
        ' Presentation'
    ),
    'Intravascular Optical Coherence Tomography Image Storage - For Processing',
    'Legacy Converted Enhanced CT Image Storage',
    'Legacy Converted Enhanced MR Image Storage',
    'Legacy Converted Enhanced PET Image Storage',
    'MR Spectroscopy Storage',
    'Multi-frame Grayscale Byte Secondary Capture Image Storage',
    'Multi-frame Grayscale Word Secondary Capture Image Storage',
    'Multi-frame True Color Secondary Capture Image Storage',
    'Ophthalmic Optical Coherence Tomography B-scan Volume Analysis Storage',
    'Ophthalmic Tomography Image Storage',
    'Parametric Map Storage',
    'Photoacoustic Image Storage',
    'Segmentation Storage',
    'VL Whole Slide Microscopy Image Storage',
    'X-Ray 3D Angiographic Image Storage',
    'X-Ray 3D Craniofacial Image Storage',
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

  @parameterized.named_parameters([
      dict(
          testcase_name='storage_name_found',
          name='VL Whole Slide Microscopy Image Storage',
          expected='VL Whole Slide Microscopy Image Storage',
      ),
      dict(
          testcase_name='mixed_case_no_space',
          name='VLWholeSlideMicroscopyImageiodmodules',
          expected='VL Whole Slide Microscopy Image Storage',
      ),
      dict(
          testcase_name='iod_modules_found',
          name='VL Whole Slide Microscopy Image IOD Modules',
          expected='VL Whole Slide Microscopy Image Storage',
      ),
      dict(
          testcase_name='iod_modules_found_mixed_case_no_space',
          name='VL Whole Slide Microscopy Imageiodmodules',
          expected='VL Whole Slide Microscopy Image Storage',
      ),
      dict(
          testcase_name='iod_uid_found',
          name='1.2.840.10008.5.1.4.1.1.77.1.6',
          expected='VL Whole Slide Microscopy Image Storage',
      ),
      dict(
          testcase_name='not_found_returned_as_provided',
          name='not_found',
          expected='not_found',
      ),
  ])
  def test_normalize_sop_class_name(self, name, expected):
    standard_util = dicom_standard_util.DicomStandardIODUtil()
    self.assertEqual(standard_util.normalize_sop_class_name(name), expected)

  @parameterized.named_parameters([
      dict(
          testcase_name='name_storage',
          name='VL Whole Slide Microscopy Image Storage',
          expected='1.2.840.10008.5.1.4.1.1.77.1.6',
      ),
      dict(
          testcase_name='name_storage_spacing_and_case',
          name='VL Whole Slide MicroscopyImagestorage',
          expected='1.2.840.10008.5.1.4.1.1.77.1.6',
      ),
      dict(
          testcase_name='name_modules',
          name='VL Whole Slide Microscopy Image IOD Modules',
          expected='1.2.840.10008.5.1.4.1.1.77.1.6',
      ),
      dict(
          testcase_name='mixed_case_no_space',
          name='VLWholeSlideMicroscopyImageiodmodules',
          expected='1.2.840.10008.5.1.4.1.1.77.1.6',
      ),
      dict(testcase_name='not_found', name='foo_bar', expected=None),
  ])
  def test_get_sop_classname_uid(self, name, expected):
    standard_util = dicom_standard_util.DicomStandardIODUtil()
    self.assertEqual(standard_util.get_sop_classname_uid(name), expected)

  def test_iod_function_group_have_compatiable_keys_with_iod(self):
    standard_util = dicom_standard_util.DicomStandardIODUtil()
    self.assertEmpty(
        {key for key in standard_util._iod_functional_group_modules}
        - set(standard_util._iod_modules)
    )

  def test_iod_uid_and_module_map_have_same_keys(self):
    # If this test fails after updating the DICOM standard metadata then
    # json in "dicom_iod_module_map" likely needs to be updated to define
    # missing iod module mappings.
    standard_util = dicom_standard_util.DicomStandardIODUtil()
    self.assertEqual(
        set(standard_util._iod_uid), set(standard_util._iod_to_module_map)
    )

  def test_iod_uid_module_map_values_aligns_with_module_table(self):
    # If this test fails after updating the DICOM standard metadata then
    # json in "dicom_iod_module_map" likely needs to be updated to define
    # missing iod module mappings.
    standard_util = dicom_standard_util.DicomStandardIODUtil()
    self.assertEmpty(
        set(standard_util._iod_to_module_map.values())
        - set(standard_util._iod_modules)
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='no_required_modules',
          required_modules=None,
          expected_update={},
      ),
      dict(
          testcase_name='require_slide_label',
          required_modules=['Slide Label'],
          expected_update={'Slide Label': 'M'},
      ),
  ])
  def test_get_iod_modules_required(self, required_modules, expected_update):
    standard_util = dicom_standard_util.DicomStandardIODUtil()
    expected = {
        'Acquisition Context': 'M',
        'Clinical Trial Series': 'U',
        'Clinical Trial Study': 'U',
        'Clinical Trial Subject': 'U',
        'Common Instance Reference': 'M',
        'Enhanced General Equipment': 'M',
        'Frame Extraction': (
            'C - Required if the SOP Instance was created in response to'
            ' a Frame-Level retrieve request'
        ),
        'Frame of Reference': 'M',
        'General Acquisition': 'M',
        'General Equipment': 'M',
        'General Image': 'M',
        'General Reference': 'U',
        'General Series': 'M',
        'General Study': 'M',
        'Image Pixel': 'M',
        'Microscope Slide Layer Tile Organization': 'M',
        'Multi-Resolution Pyramid': (
            'U - Shall be present only if Image Type Value 3 is VOLUME or'
            ' THUMBNAIL.'
        ),
        'Multi-frame Dimension': 'M',
        'Multi-frame Functional Groups': 'M',
        'Optical Path': 'M',
        'Patient': 'M',
        'Patient Study': 'U',
        'SOP Common': 'M',
        'Slide Label': (
            'C - Required if Image Type (0008,0008) Value 3 is LABEL; may'
            ' be present otherwise'
        ),
        'Specimen': 'M',
        'Whole Slide Microscopy Image': 'M',
        'Whole Slide Microscopy Series': 'M',
    }
    expected.update(expected_update)
    self.assertEqual(
        {
            mod.name: mod.usage
            for mod in standard_util.get_iod_modules(
                dicom_standard_util.IODName('1.2.840.10008.5.1.4.1.1.77.1.6'),
                required_modules,
            )
        },
        expected,
    )


if __name__ == '__main__':
  absltest.main()
