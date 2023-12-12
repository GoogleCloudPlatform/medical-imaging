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
"""Parses DICOM Tags from XML spec."""
import collections
import dataclasses
from typing import Optional

from absl import logging

from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib.util import dicom_abstract_xml_parser
from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib.util import dicom_iod_generator_exception
from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib.util import dicom_xml_core_parser

DicomIodGeneratorError = dicom_iod_generator_exception.DicomIodGeneratorError
ParsedTableRow = dicom_xml_core_parser.ParsedTableRow
DicomXmlCoreParser = dicom_xml_core_parser.DicomXmlCoreParser


@dataclasses.dataclass
class ParsedTag(object):
  """Container for raw dicom tag table values parsed from standard."""

  address: str
  comment: str
  keyword: str
  vr: str
  vm: str
  retired: str
  _is_mask: bool = dataclasses.field(init=False, default=False)

  def __post_init__(self):
    if isinstance(self.retired, list):
      self.retired = ' '.join(self.retired)
    self._is_mask = 'x' in self.address
    if self.address.startswith('(') and self.address.endswith(')'):
      self.address = self.address[1:-1]
    self.address = ''.join([part.strip() for part in self.address.split(',')])
    if not self._is_mask:
      self.address = f'0x{self.address}'
    if self.vr.startswith('note_'):
      self.vr = 'NONE'
    if self.retired.startswith('RET'):
      self.retired = 'Retired'
    elif self.retired in ('DICOS', 'DICONDE', 'note_6_1'):
      self.retired = ''
    if not self.vr and not self.vm:
      self.vr = 'OB'
      self.vm = '1'
      self.comment = 'Retired-blank'

  @property
  def is_mask(self) -> bool:
    return self._is_mask

  def __eq__(self, other) -> bool:
    if isinstance(other, dict):
      if self.address != other.get('address'):
        return False
      if self.comment != other.get('comment'):
        return False
      if self.keyword != other.get('keyword'):
        return False
      if self.vr != other.get('vr'):
        return False
      if self.vm != other.get('vm'):
        return False
      if self.retired != other.get('retired'):
        return False
      return True
    else:
      if self.address != other.address:
        return False
      if self.comment != other.comment:
        return False
      if self.keyword != other.keyword:
        return False
      if self.vr != other.vr:
        return False
      if self.vm != other.vm:
        return False
      if self.retired != other.retired:
        return False
      return True

  def __neq__(self, other) -> bool:
    return not self.__eq__(other)


@dataclasses.dataclass
class DicomStandardTags(object):
  """Tags defined in DICOM Standard."""

  main_tags: collections.OrderedDict[str, ParsedTag]
  mask_tags: collections.OrderedDict[str, ParsedTag]

  @classmethod
  def tag_json(cls, tag: ParsedTag) -> str:
    return (
        '{'
        f'"keyword": "{tag.keyword}", "vr": "{tag.vr}", "vm": "{tag.vm}", '
        f'"name": "{tag.comment}", "retired": "{tag.retired}"'
        '}'
    )

  def json(self) -> str:
    """Returns Json representation of DICOMStandardTags."""
    str_lst = ['{', '  "main": {']
    tags_list = []
    for tag in self.main_tags.values():
      if len(tag.address) > 2:
        tags_list.append(
            f'    "{tag.address}": {DicomStandardTags.tag_json(tag)}'
        )
    str_lst.append(',\n'.join(tags_list))
    str_lst.append('  },')
    str_lst.append('  "mask": {')
    tags_list = []
    for tag in self.mask_tags.values():
      if len(tag.address) > 2:
        tags_list.append(
            f'    "{tag.address}": {DicomStandardTags.tag_json(tag)}'
        )
    str_lst.append(',\n'.join(tags_list))
    str_lst.append('  }')
    str_lst.append('}')
    return '\n'.join(str_lst)

  def __str__(self) -> str:
    return self.json()


class DicomTagXmlParser(dicom_abstract_xml_parser.DicomAbstractXmlParser):
  """Parses Dicom Tags from Dicom Standard XML."""

  def __init__(
      self,
      part6_file_path: Optional[str] = None,
      part7_file_path: Optional[str] = None,
  ):
    super().__init__()
    if part6_file_path is None:
      xml_part6 = self.download_xml('part06')
    else:
      xml_part6 = self.read_xml(part6_file_path)
    if part7_file_path is None:
      xml_part7 = self.download_xml('part07')
    else:
      xml_part7 = self.read_xml(part7_file_path)
    if xml_part6.dcm_version != xml_part7.dcm_version:
      raise DicomIodGeneratorError('Part6 and Part7 versions do not match.')
    self.set_dcm_version(xml_part6.dcm_version)  # pytype: disable=wrong-arg-types
    self._part6_parser = DicomXmlCoreParser(self.namespace, xml_part6.xml_root)
    self._part7_parser = DicomXmlCoreParser(self.namespace, xml_part7.xml_root)

  @classmethod
  def _get_index(cls, parsed_row: ParsedTableRow, index) -> str:
    try:
      txt = parsed_row.parsed_row[index]
      if isinstance(txt, list):
        return ' '.join(txt)
      return txt
    except IndexError:
      return ''

  def parse_spec(self) -> DicomStandardTags:
    """Parses Dicom Tags from Dicom Standard XML.

    Returns:
      DicomStandardTags
    """
    logging.info('Parsing DICOM IOD XML Tags')
    dicom_tags = []
    header_tables = [('Tag', 'Name', 'Keyword', 'VR', 'VM', '')]
    table_list = self._part6_parser.get_tables(
        self._part6_parser.xml_root, header_tables
    )
    for table in table_list:
      for row in table.rows:
        parsed_row = self._part6_parser.parse_table_row(row)
        address = self.unicode_check(
            DicomTagXmlParser._get_index(parsed_row, 0)
        )
        # allow unicode in comments.
        comment = DicomTagXmlParser._get_index(parsed_row, 1)
        keyword = self.unicode_check(
            DicomTagXmlParser._get_index(parsed_row, 2)
        )
        vr = self.unicode_check(DicomTagXmlParser._get_index(parsed_row, 3))
        vm = self.unicode_check(DicomTagXmlParser._get_index(parsed_row, 4))
        retired = self.unicode_check(
            DicomTagXmlParser._get_index(parsed_row, 5)
        )
        dicom_tags.append(ParsedTag(address, comment, keyword, vr, vm, retired))

    header_tables = [
        ('Tag', 'Message Field', 'Keyword', 'VR', 'VM', 'Description of Field'),
        ('Tag', 'Message Field', 'Keyword', 'VR', 'VM'),
    ]
    table_list = self._part7_parser.get_tables(
        self._part7_parser.xml_root, header_tables
    )
    for table in table_list:
      if table.caption == 'Retired Command Fields':
        retired = 'Retired'
      else:
        retired = ''
      for row in table.rows:
        parsed_row = self._part7_parser.parse_table_row(row)
        address = DicomTagXmlParser._get_index(parsed_row, 0)
        commentlst = [
            DicomTagXmlParser._get_index(parsed_row, 1).replace('\n', ''),
            DicomTagXmlParser._get_index(parsed_row, 5).replace('\n', ''),
        ]
        comment = ' '.join([txt for txt in commentlst if txt])
        comment = self.unicode_check(comment)
        keyword = self.unicode_check(
            DicomTagXmlParser._get_index(parsed_row, 2)
        )
        vr = self.unicode_check(DicomTagXmlParser._get_index(parsed_row, 3))
        vm = self.unicode_check(DicomTagXmlParser._get_index(parsed_row, 4))
        dicom_tags.append(ParsedTag(address, comment, keyword, vr, vm, retired))
    dicom_tags = sorted(dicom_tags, key=lambda x: x.address)

    main_tags = collections.OrderedDict()
    mask_tags = collections.OrderedDict()
    for tag in dicom_tags:
      if tag.is_mask:
        mask_tags[tag.address] = tag
      else:
        main_tags[tag.address] = tag
    return DicomStandardTags(main_tags, mask_tags)
