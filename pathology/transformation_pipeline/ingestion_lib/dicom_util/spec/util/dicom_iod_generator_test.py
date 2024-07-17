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
"""Highlevel test for dicom tag XML Parser."""
import json
import os

from absl.testing import absltest

from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib import dicom_iod_uid_xml_parser
from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib import dicom_iod_xml_parser
from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib import dicom_tag_xml_parser


class UnitTests(absltest.TestCase):
  """High level tests for dicom xml parsers."""

  def test_dicom_main_tag_parser(self):
    """Tests parser against XML which models standard tag def for typical tags.

    The tag table definitions described at:
    https://www.dicomstandard.org/current in Part 6 & Part 7
    """

    dirname, _ = os.path.split(__file__)
    part6_path = os.path.join(dirname, 'test_data', 'part6_tags.xml')
    part7_path = os.path.join(dirname, 'test_data', 'part7_tags.xml')

    # DicomTagXMLParser
    # Part 6 def
    #   Search XML for tables with tag header definitions.
    #   Extracts tables and parses into rows for tables with headers:
    #   header_tables = [('Tag', 'Name', 'Keyword', 'VR', 'VM', '')]

    # Part 7 def
    #   Search XML for tables with tag header definitions.
    #   Extracts tables and parses into rows for tables with headers:
    #   header_tables = ('Tag', 'Message Field', 'Keyword', 'VR', 'VM',
    #                    'Description of Field') or
    #                   ('Tag', 'Message Field', 'Keyword', 'VR', 'VM')
    #

    tag_parser = dicom_tag_xml_parser.DicomTagXmlParser(part6_path, part7_path)
    parsed_dicom_tags = tag_parser.parse_spec()
    expected_main = {}

    # Part 6 path tags
    expected_main['0x00100010'] = dict(
        address='0x00100010',
        comment="Patient's Name",
        keyword='PatientName',
        vr='PN',
        vm='1',
        retired='',
    )
    expected_main['0x00100020'] = dict(
        address='0x00100020',
        comment='Patient ID',
        keyword='PatientID',
        vr='LO',
        vm='1',
        retired='',
    )
    expected_main['0x00100032'] = dict(
        address='0x00100032',
        comment="Patient's Birth Time",
        keyword='PatientBirthTime',
        vr='TM',
        vm='1',
        retired='',
    )

    # Part 7 path tags
    comment = (
        'Command Group Length The even number of bytes from the end of '
        'the value field to the beginning of the next group.'
    )
    expected_main['0x00000000'] = dict(
        address='0x00000000',
        comment=comment,
        keyword='CommandGroupLength',
        vr='UL',
        vm='1',
        retired='',
    )

    expected_main['0x00000001'] = dict(
        address='0x00000001',
        comment='Command Length to End',
        keyword='CommandLengthToEnd',
        vr='UL',
        vm='1',
        retired='Retired',
    )

    comment = (
        'Command Field This field distinguishes the DIMSE operation '
        'conveyed by this Message. This field shall be set to one of the'
        ' following values:0001H C-STORE-RQ8001H C-STORE-RSP0010H '
        'C-GET-RQ8010H C-GET-RSP0020H C-FIND-RQ8020H '
        'C-FIND-RSP0021HC-MOVE-RQ8021H C-MOVE-RSP0030H C-ECHO-RQ8030H '
        'C-ECHO-RSP0100H N-EVENT-REPORT-RQ8100H N-EVENT-REPORT-RSP0110H '
        'N-GET-RQ8110H N-GET-RSP0120H N-SET-RQ8120H N-SET-RSP0130H '
        'N-ACTION-RQ8130H N-ACTION-RSP0140H N-CREATE-RQ8140H '
        'N-CREATE-RSP0150H N-DELETE-RQ8150H N-DELETE-RSP0FFFH '
        'C-CANCEL-RQ'
    )
    expected_main['0x00000100'] = dict(
        address='0x00000100',
        comment=comment,
        keyword='CommandField',
        vr='US',
        vm='1',
        retired='',
    )

    expected_main['0x12346789'] = dict(
        address='0x12346789',
        comment='Retired-blank',
        keyword='',
        vr='OB',
        vm='1',
        retired='Retired',
    )

    self.assertDictEqual(parsed_dicom_tags.main_tags, expected_main)

  def test_dicom_mask_tag_parser(self):
    """Tests parser against XML which models standard tag def for masked tags.

    The tag table definitions described at:
    https://www.dicomstandard.org/current in Part 6 & Part 7
    """

    dirname, _ = os.path.split(__file__)
    part6_path = os.path.join(dirname, 'test_data', 'part6_tags.xml')
    part7_path = os.path.join(dirname, 'test_data', 'part7_tags.xml')

    # DicomTagXMLParser
    # Part 6 def
    #   Search XML for tables with tag header definitions.
    #   Extracts tables and parses into rows for tables with headers:
    #   header_tables = [('Tag', 'Name', 'Keyword', 'VR', 'VM', '')]

    # Part 7 def
    #   Search XML for tables with tag header definitions.
    #   Extracts tables and parses into rows for tables with headers:
    #   header_tables = ('Tag', 'Message Field', 'Keyword', 'VR', 'VM',
    #                    'Description of Field') or
    #                   ('Tag', 'Message Field', 'Keyword', 'VR', 'VM')
    #

    tag_parser = dicom_tag_xml_parser.DicomTagXmlParser(part6_path, part7_path)
    parsed_dicom_tags = tag_parser.parse_spec()
    expected_mask = {}

    # Part 6 path tags

    expected_mask['7Fxx0020'] = dict(
        address='7Fxx0020',
        comment='Variable Coefficients SDVN',
        keyword='VariableCoefficientsSDVN',
        vr='OW',
        vm='1',
        retired='Retired',
    )

    # Part 7 path tags
    # None
    self.assertDictEqual(parsed_dicom_tags.mask_tags, expected_mask)

  def test_dicom_iod_uid_parser(self):
    """Tests parser against XML which models standard iod storage uid def.

    The tag table definitions described at:
    https://www.dicomstandard.org/current in Part 4
    """

    dirname, _ = os.path.split(__file__)
    part4_path = os.path.join(dirname, 'test_data', 'part4_tags.xml')

    # DicomIodUidXmlParser
    # Part 4 def
    #   Search XML for Chapter B
    #      Search Chapter B sections for section without sec.attrib == 'label'
    #          Extracts tables and parses into rows for tables with headers:
    #                    header_tables = [('SOP Class Name', 'SOP Class UID',
    #                     'IOD Specification (defined in', 'Specialization')]
    #
    tag_parser = dicom_iod_uid_xml_parser.DicomIodUidXmlParser(part4_path)
    parsed_dicom_iod_uid = tag_parser.parse_spec()
    expected = {
        'Enhanced SR Storage': '1.2.840.10008.5.1.4.1.1.88.22',
        'Extensible SR Storage': '1.2.840.10008.5.1.4.1.1.88.35',
        'Multi-frame Grayscale Byte Secondary Capture Image Storage': (
            '1.2.840.10008.5.1.4.1.1.7.2'
        ),
        'Multi-frame Grayscale Word Secondary Capture Image Storage': (
            '1.2.840.10008.5.1.4.1.1.7.3'
        ),
        'Multi-frame Single Bit Secondary Capture Image Storage': (
            '1.2.840.10008.5.1.4.1.1.7.1'
        ),
        'Multi-frame True Color Secondary Capture Image Storage': (
            '1.2.840.10008.5.1.4.1.1.7.4'
        ),
        'Secondary Capture Image Storage': '1.2.840.10008.5.1.4.1.1.7',
    }

    self.assertDictEqual(parsed_dicom_iod_uid, expected)

  @classmethod
  def get_parsed(cls, filename: str, out: str):
    """Returns Dictionary of parsed structure."""
    dirname, _ = os.path.split(__file__)
    part3_path = os.path.join(dirname, 'test_data', filename)
    tag_parser = dicom_iod_xml_parser.DicomIodXmlParser(part3_path)
    parsed_dicom_iod_uid = tag_parser.parse_spec()
    if out == 'iods':
      iod_dict = {}
      for key, iod_sec_ref_list in parsed_dicom_iod_uid.iod.items():
        iod_dict[key] = [
            json.loads(sec_ref.json()) for sec_ref in iod_sec_ref_list
        ]
      return iod_dict
    if out == 'modules':
      module_dict = {}
      for key, module_list in parsed_dicom_iod_uid.modules.items():
        module_dict[key] = [json.loads(module.json()) for module in module_list]
      return module_dict
    if out == 'tables':
      table_dict = {}
      for key, table in parsed_dicom_iod_uid.tables.items():
        table_dict[key] = json.loads(table.json())
      return table_dict

  @classmethod
  def get_parsed_tables(cls, filename: str):
    return UnitTests.get_parsed(filename, 'tables')

  @classmethod
  def get_parsed_iods(cls, filename: str):
    return UnitTests.get_parsed(filename, 'iods')

  @classmethod
  def get_parsed_modules(cls, filename: str):
    return UnitTests.get_parsed(filename, 'modules')

  def test_dicom_iod_tables_parser(self):
    """Tests parser against XML which models standard iod def for iod tables.

    The tag table definitions described at:
    https://www.dicomstandard.org/current in Part 3
    """

    # no table defs
    self.assertDictEqual(UnitTests.get_parsed_tables('part3_tags_iod.xml'), {})
    # test ('Attribute Name', 'Tag', 'Type', 'Attribute Description')
    expected = {
        'C.7.1.4-1': {
            'table_name': 'C.7.1.4-1',
            'table_caption': 'Patient Group Macro Attributes',
            'table_lines': [{
                'type': 'InlineObject',
                'name': 'Source Patient Group Identification Sequence',
                'address': '0x00100026',
                'required': '3',
                'comment': 'Only a single Item is permitted in this Sequence.',
            }],
        }
    }
    self.assertDictEqual(
        UnitTests.get_parsed_tables('part3_tags_mod1.xml'), expected
    )

    # test ('Attribute Name', 'Tag', 'Attribute Description')
    expected = {
        'C.2-1': {
            'table_name': 'C.2-1',
            'table_caption': 'Patient Relationship Module Attributes',
            'table_lines': [{
                'type': 'InlineObject',
                'name': 'Referenced Study Sequence',
                'address': '0x00081110',
                'required': '',
                'comment': (
                    'Uniquely identifies the Study SOP Instances associated '
                    'with the Patient SOP Instance. One or more Items shall be'
                    ' included in this Sequence.\nSee'
                ),
            }],
        }
    }
    self.assertDictEqual(
        UnitTests.get_parsed_tables('part3_tags_mod2.xml'), expected
    )

    # test  ('Attribute Name', 'Tag', 'Type', 'Description')
    expected = {
        '10-11': {
            'table_name': '10-11',
            'table_caption': 'SOP Instance Reference Macro Attributes',
            'table_lines': [{
                'type': 'InlineObject',
                'name': 'Referenced SOP Class UID',
                'address': '0x00081150',
                'required': '1',
                'comment': 'Uniquely identifies the referenced SOP Class.',
            }],
        }
    }
    self.assertDictEqual(
        UnitTests.get_parsed_tables('part3_tags_mod3.xml'), expected
    )

    # test  ('Attribute Name', 'Tag', 'Description')
    expected = {
        'C.31-3': {
            'table_name': 'C.31-3',
            'table_caption': 'RT Ion Machine Verification Module Attributes',
            'table_lines': [{
                'type': 'InlineObject',
                'name': 'Ion Machine Verification Sequence',
                'address': '0x00741046',
                'required': '',
                'comment': (
                    'Sequence containing ion machine verification parameters.'
                    '\nZero or one Item shall be included in this Sequence.'
                ),
            }],
        }
    }
    self.assertDictEqual(
        UnitTests.get_parsed_tables('part3_tags_mod4.xml'), expected
    )

    # test linked and inline rows
    expected = {
        'C.8.12.5-1': {
            'table_name': 'C.8.12.5-1',
            'table_caption': 'Optical Path Module Attributes',
            'table_lines': [
                {
                    'type': 'InlineObject',
                    'name': 'Number of Optical Paths',
                    'address': '0x00480302',
                    'required': '1C',
                    'comment': (
                        'Number of Items in the Optical Path Sequence'
                        ' (0048,0105).\nRequired if Dimension Organization Type'
                        ' (0020,9311) is present with a value of TILED_FULL.'
                        ' May be present otherwise.'
                    ),
                },
                {
                    'type': 'InlineObject',
                    'name': 'Optical Path Sequence',
                    'address': '0x00480105',
                    'required': '1',
                    'comment': (
                        'Describes the optical paths used during the'
                        ' acquisition of this image.\nOne or more Items shall'
                        ' be included in this Sequence.\nSee'
                    ),
                },
                {
                    'type': 'InlineObject',
                    'name': '>Optical Path Identifier',
                    'address': '0x00480106',
                    'required': '1',
                    'comment': (
                        'Identifier for the optical path specified in the'
                        ' Sequence Item. The identifier shall be unique for'
                        ' each Item within the Optical Path Sequence.'
                    ),
                },
                {
                    'type': 'InlineObject',
                    'name': '>Optical Path Description',
                    'address': '0x00480107',
                    'required': '3',
                    'comment': (
                        'Description of the optical path specified in the'
                        ' Sequence Item.'
                    ),
                },
                {
                    'type': 'InlineObject',
                    'name': '>Illuminator Type Code Sequence',
                    'address': '0x00480100',
                    'required': '3',
                    'comment': (
                        'Type of illuminator.\nOnly a single Item is permitted'
                        ' in this Sequence.'
                    ),
                },
                {
                    'type': 'LinkedObject',
                    'prefix': '>>Include',
                    'linked_resource': 'table_8.8-1',
                    'usage': 'CID may be defined in the IOD constraints.',
                },
            ],
        }
    }
    self.assertDictEqual(
        UnitTests.get_parsed_tables('part3_tags_table.xml'), expected
    )

  def test_dicom_iod_modules_parser(self):
    """Tests parser against XML which models standard iod def for iod modules.

    The tag table definitions described at:
    https://www.dicomstandard.org/current in Part 3
    """
    # no module defs
    self.assertDictEqual(UnitTests.get_parsed_modules('part3_tags_iod.xml'), {})
    # test ('Attribute Name', 'Tag', 'Type', 'Attribute Description')
    expected = {
        'C.7.1.4': [{
            'name': 'C.7.1.4-1',
            'caption': 'Patient Group Macro Attributes',
        }]
    }
    self.assertDictEqual(
        UnitTests.get_parsed_modules('part3_tags_mod1.xml'), expected
    )

    # test ('Attribute Name', 'Tag', 'Attribute Description')
    expected = {
        'C.2.1': [{
            'name': 'C.2-1',
            'caption': 'Patient Relationship Module Attributes',
        }]
    }
    self.assertDictEqual(
        UnitTests.get_parsed_modules('part3_tags_mod2.xml'), expected
    )

    # test  ('Attribute Name', 'Tag', 'Type', 'Description')
    expected = {
        '10.8': [{
            'name': '10-11',
            'caption': 'SOP Instance Reference Macro Attributes',
        }]
    }
    self.assertDictEqual(
        UnitTests.get_parsed_modules('part3_tags_mod3.xml'), expected
    )

    # test  ('Attribute Name', 'Tag', 'Description')
    expected = {
        'C.31.3': [{
            'name': 'C.31-3',
            'caption': 'RT Ion Machine Verification Module Attributes',
        }]
    }
    self.assertDictEqual(
        UnitTests.get_parsed_modules('part3_tags_mod4.xml'), expected
    )

    # no modules
    self.assertDictEqual(
        UnitTests.get_parsed_modules('part3_tags_table.xml'), {}
    )

  def test_dicom_iod_iod_parser(self):
    """Tests parser against XML which models standard iod def for iods.

    The tag table definitions described at:
    https://www.dicomstandard.org/current in Part 3
    """
    # test whole slide imaging iod
    expected = {
        'VL Whole Slide Microscopy Image IOD Modules': [
            {'name': 'Patient', 'ref': 'sect_C.7.1.1', 'usage': 'M'},
            {
                'name': 'Clinical Trial Subject',
                'ref': 'sect_C.7.1.3',
                'usage': 'U',
            },
            {'name': 'General Study', 'ref': 'sect_C.7.2.1', 'usage': 'M'},
            {'name': 'Patient Study', 'ref': 'sect_C.7.2.2', 'usage': 'U'},
            {
                'name': 'Clinical Trial Study',
                'ref': 'sect_C.7.2.3',
                'usage': 'U',
            },
            {'name': 'General Series', 'ref': 'sect_C.7.3.1', 'usage': 'M'},
            {
                'name': 'Whole Slide Microscopy Series',
                'ref': 'sect_C.8.12.3',
                'usage': 'M',
            },
            {
                'name': 'Clinical Trial Series',
                'ref': 'sect_C.7.3.2',
                'usage': 'U',
            },
            {'name': 'Frame of Reference', 'ref': 'sect_C.7.4.1', 'usage': 'M'},
            {'name': 'General Equipment', 'ref': 'sect_C.7.5.1', 'usage': 'M'},
            {
                'name': 'Enhanced General Equipment',
                'ref': 'sect_C.7.5.2',
                'usage': 'M',
            },
            {'name': 'General Image', 'ref': 'sect_C.7.6.1', 'usage': 'M'},
            {'name': 'General Reference', 'ref': 'sect_C.12.4', 'usage': 'U'},
            {'name': 'Image Pixel', 'ref': 'sect_C.7.6.3', 'usage': 'M'},
            {
                'name': 'Acquisition Context',
                'ref': 'sect_C.7.6.14',
                'usage': 'M',
            },
            {
                'name': 'Multi-frame Functional Groups',
                'ref': 'sect_C.7.6.16',
                'usage': 'M',
            },
            {
                'name': 'Multi-frame Dimension',
                'ref': 'sect_C.7.6.17',
                'usage': 'M',
            },
            {'name': 'Specimen', 'ref': 'sect_C.7.6.22', 'usage': 'M'},
            {
                'name': 'Whole Slide Microscopy Image',
                'ref': 'sect_C.8.12.4',
                'usage': 'M',
            },
            {'name': 'Optical Path', 'ref': 'sect_C.8.12.5', 'usage': 'M'},
            {
                'name': 'Multi-Resolution Navigation',
                'ref': 'sect_C.8.12.7',
                'usage': (
                    'C - Required if Image Type (0008,0008) Value 3 is'
                    ' LOCALIZER'
                ),
            },
            {
                'name': 'Slide Label',
                'ref': 'sect_C.8.12.8',
                'usage': (
                    'C - Required if Image Type (0008,0008) Value 3 is LABEL;'
                    ' may be present otherwise'
                ),
            },
            {'name': 'SOP Common', 'ref': 'sect_C.12.1', 'usage': 'M'},
            {
                'name': 'Common Instance Reference',
                'ref': 'sect_C.12.2',
                'usage': 'M',
            },
            {
                'name': 'Frame Extraction',
                'ref': 'sect_C.12.3',
                'usage': (
                    'C - Required if the SOP Instance was created in response'
                    ' to a Frame-Level retrieve request'
                ),
            },
        ]
    }
    self.assertDictEqual(
        UnitTests.get_parsed_iods('part3_tags_iod.xml'), expected
    )

    # no iod dict
    self.assertDictEqual(UnitTests.get_parsed_iods('part3_tags_mod1.xml'), {})
    self.assertDictEqual(UnitTests.get_parsed_iods('part3_tags_mod2.xml'), {})
    self.assertDictEqual(UnitTests.get_parsed_iods('part3_tags_mod3.xml'), {})
    self.assertDictEqual(UnitTests.get_parsed_iods('part3_tags_mod4.xml'), {})
    self.assertDictEqual(UnitTests.get_parsed_iods('part3_tags_table.xml'), {})


if __name__ == '__main__':
  absltest.main()
