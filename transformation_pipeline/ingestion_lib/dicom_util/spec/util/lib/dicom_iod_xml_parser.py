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
"""Parses DICOM IOD from XML spec."""
import dataclasses
import json
from typing import Any, Dict, List, Optional, Set, Union

from absl import logging

from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib.util import dicom_abstract_xml_parser
from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib.util import dicom_iod_generator_exception
from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib.util import dicom_xml_core_parser

DicomIodGeneratorError = dicom_iod_generator_exception.DicomIodGeneratorError
unicode_check = dicom_abstract_xml_parser.DicomAbstractXmlParser.unicode_check


@dataclasses.dataclass
class LinkedObject(object):
  """Linked reference to table in IOD."""

  prefix: str = ''
  linked_resource: str = ''
  comment: str = ''
  _counter: int = dataclasses.field(init=False, default=0)

  def json(self) -> str:
    """Returns Json representation of LinkedObject."""
    prefix = json.dumps(self.prefix)
    linked_resource = json.dumps(self.linked_resource)
    comment = json.dumps(self.comment)
    return (
        '{"type": "LinkedObject", "prefix": '
        f'{prefix}, "linked_resource": {linked_resource}, "usage":{comment}'
        '}'
    )

  def gen_str(
      self,
      tabledict: Optional[Dict[str, Any]] = None,
      prefix: Optional[str] = None,
      print_set: Optional[Set[str]] = None,
  ) -> str:
    """Prints representation of linked object to stdio.

    Args:
      tabledict: optional dictionary of tables to expand linked tables.
      prefix: optional to append before resource
      print_set: set of tables printed (used to avoid self referencing tabl inf
        recursion.

    Returns:
      table string
    """
    if prefix is None:
      prefix = ''
    if tabledict is None:
      return (
          f'{prefix}  Link ({self.prefix}, {self.linked_resource},'
          f' {self.comment})'
      )
    else:
      line_list = []
      if self.linked_resource.startswith('table_'):
        tablename = self.linked_resource[len('table_') :]
      if print_set is not None and tablename in print_set:
        return (
            f'{prefix}  Link ({self.prefix}, {self.linked_resource},'
            f' {self.comment})'
        )
      prefix += ''.join([ch for ch in self.prefix if ch == '>'])
      ref_table = tabledict[tablename]
      if print_set is None:
        print_set = set()
      print_set.add(tablename)
      for table_line in ref_table['table_lines']:
        if isinstance(table_line, LinkedObject):
          line_list += table_line.gen_str(
              tabledict=tabledict, prefix=prefix, print_set=print_set
          )
        else:
          line_list.append(f'{prefix}{table_line}')
      print_set.remove(tablename)
      return '\n'.join(line_list)

  def __str__(self):
    """Returns string rep of LinkedObject."""
    return self.gen_str()

  def is_init(self) -> bool:
    """Returns True if the number of parameters initialized = expected."""
    return self._counter >= 2

  def set_val(self, val: str):
    """Incrementally sets object property value.

    Args:
      val: value

    Raises:
      DicomIodGeneratorError : if more parameters added than expected.
    """
    self._counter += 1
    if self._counter == 1:
      self.prefix = unicode_check(val)
    elif self._counter == 2:
      self.linked_resource = unicode_check(val)
    elif self._counter == 3:
      self.comment = val
    else:
      raise DicomIodGeneratorError('Unexpected value:' + str(self))


@dataclasses.dataclass
class InlineObject(object):
  """Inline table in IOD."""

  param_count: int
  name: str = ''
  address: str = ''
  required: str = ''
  comment: str = ''
  _counter: int = dataclasses.field(init=False, default=0)

  def json(self) -> str:
    """Returns Json representation of InlineObject."""
    group = self.address[1:5]
    element = self.address[6:-1]
    address = group + element
    if 'x' not in address:
      address = f'0x{address}'

    inline_obj_name = json.dumps(self.name)
    required = json.dumps(self.required)
    comment = json.dumps(self.comment)
    return (
        '{"type": "InlineObject", "name": '
        f'{inline_obj_name}, "address": "{address}", "required":{required},'
        f' "comment": {comment}'
        '}'
    )

  def is_init(self) -> bool:
    """Returns True if the number of parameters initialized = expected."""
    return self._counter >= self.param_count - 1

  def set_val(self, val: str):
    """Progressively sets value of obj parameter.

    Args:
      val: value to set prarameter specified by counter to.

    Raises:
      DicomIodGeneratorError : if more parameters added than expected.
    """
    self._counter += 1
    if self._counter == 1:
      self.name = unicode_check(val)
    elif self._counter == 2:
      self.address = unicode_check(val)
    elif self._counter == 3 and self.param_count == 4:
      self.required = unicode_check(val)
      if self.required not in ('1', '1C', '2', '2C', '3'):
        raise DicomIodGeneratorError('Unexpected value:' + str(self))
    elif self._counter <= 4:
      if isinstance(val, list):
        val = ''.join(val)
      self.comment = val.strip()
    else:
      raise DicomIodGeneratorError('Unexpected value: ' + str(self))

  def __str__(self) -> str:
    """Returns string rep of InlineObject."""
    inline_object_name = self.name.replace('>', ' ')
    if self.param_count == 4:
      return f' {inline_object_name}, {self.address}, {self.required}'
    elif self.param_count == 3:
      return f' {inline_object_name}, {self.address}'
    return ''


@dataclasses.dataclass
class TableRef:
  """Table reference."""

  name: str
  caption: str
  lines: List[Union[LinkedObject, InlineObject]]

  def json(self) -> str:
    """Returns Json representation of TableRef."""
    ret_lines = []
    tbl_name = json.dumps(self.name)
    tbl_caption = json.dumps(self.caption)
    ret_lines.append(f'  "table_name": {tbl_name},')
    ret_lines.append(f'  "table_caption": {tbl_caption},')
    ret_lines.append('  "table_lines": [')
    table_line_lst = []
    for tbl_line in self.lines:
      table_line_txt = tbl_line.json()
      table_line_lst.append(f'    {table_line_txt}')
    ret_lines.append(',\n'.join(table_line_lst))
    ret_lines.append('  ]')
    ret_lines.append('}')
    return '{\n' + '\n'.join(ret_lines)

  def __str__(self) -> str:
    return self.json()


@dataclasses.dataclass
class IodSectionRef(object):
  """IOD section reference."""

  name: str
  ref: str
  usage: str

  def json(self) -> str:
    """Returns Json representation of IodSectionRef."""
    iod_section_name = json.dumps(self.name)
    ref = json.dumps(self.ref)
    usage = json.dumps(self.usage)
    return f'{{"name": {iod_section_name}, "ref": {ref}, "usage":{usage}}}'

  def __str__(self) -> str:
    return self.json()


@dataclasses.dataclass
class ModuleTableRef(object):
  """Reference to a table in dicom iod module."""

  name: str
  caption: str

  def json(self) -> str:
    """Returns Json representation of ModuleTableRef."""
    tbl_name = json.dumps(self.name)
    caption = json.dumps(self.caption)
    return f'{{"name": {tbl_name}, "caption": {caption}}}'

  def __str__(self) -> str:
    return self.json()


TableDefType = Dict[str, TableRef]
IodDefType = Dict[str, List[IodSectionRef]]
ModuleDefType = Dict[str, List[ModuleTableRef]]


@dataclasses.dataclass
class ParsedSpec(object):
  iod: IodDefType
  modules: ModuleDefType
  tables: TableDefType


class DicomIodXmlParser(dicom_abstract_xml_parser.DicomAbstractXmlParser):
  """Parser for DICOM Standard IOD XML."""

  def __init__(self, part3_file_path: Optional[str] = None):
    super().__init__()
    if part3_file_path is None:
      xmlpart = self.download_xml('part03')
    else:
      xmlpart = self.read_xml(part3_file_path)
    self.set_dcm_version(xmlpart.dcm_version)  # pytype: disable=wrong-arg-types
    self._iod_parser = dicom_xml_core_parser.DicomXmlCoreParser(
        self.namespace, xmlpart.xml_root
    )

  def _get_iod_def(self, chapter_dict: Dict[str, Any]) -> IodDefType:
    """Returns select dictionary of IOD from xml in chapter a.

    Args:
      chapter_dict: dictionary of chapters in XML

    Returns:
      Dictionary of iod
    """
    chapter_a = chapter_dict['A']
    dicom_iod_def = {}
    valid_headers = [('IE', 'Module', 'Reference', 'Usage')]
    for section in chapter_a.iter(f'{self.namespace}section'):
      module_list = []
      for tbl in self._iod_parser.get_tables(section, valid_headers):
        prev_line = []
        caption = tbl.caption
        for row_lines in tbl.rows:
          tbl_row = self._iod_parser.parse_table_row(row_lines)
          parsed_row = tbl_row.parsed_row
          if not parsed_row or len(parsed_row) == 1:
            continue
          if tbl_row.starts_with_emphasis:
            raise DicomIodGeneratorError('Emphasis found unexpectedly')
          if prev_line and len(parsed_row) == 3:
            parsed_row = [prev_line[0]] + parsed_row
          prev_line = parsed_row
          if len(parsed_row) != 4:
            raise DicomIodGeneratorError('Invalid Length')
          if len(parsed_row[2]) != 1:
            raise DicomIodGeneratorError('To many tables')
          module_list.append(
              IodSectionRef(
                  self.unicode_check(parsed_row[1]),
                  self.unicode_check(parsed_row[2][0]),
                  self.unicode_check(parsed_row[3]),
              )
          )
        if module_list:
          dicom_iod_def[caption] = module_list
    return dicom_iod_def

  def _get_module_def(self, chapter_dict: Dict[str, Any]) -> ModuleDefType:
    """Parses the DICOM modules from XML in chapter C.

    Args:
      chapter_dict: dictionary of chapters in XML

    Returns:
      dict representing module def
    """
    chapter_c = chapter_dict['C']
    modules = {}
    valid_headers = [
        ('Attribute Name', 'Tag', 'Type', 'Attribute Description'),
        ('Attribute Name', 'Tag', 'Attribute Description'),
        ('Attribute Name', 'Tag', 'Type', 'Description'),
        ('Attribute Name', 'Tag', 'Description'),
    ]
    for sec in chapter_c.iter(f'{self.namespace}section'):
      if 'label' not in sec.attrib:
        continue
      table_list = []
      for tbl in self._iod_parser.get_tables(
          sec, valid_headers, limit_search_to_first_level=True
      ):
        table_list.append(ModuleTableRef(tbl.name, tbl.caption))
      if table_list:
        modules[sec.attrib['label']] = table_list
    return modules

  def read_table_dict(self) -> TableDefType:
    """Reads and returns dictionary of IOD table definitions.

    Returns:
      Dictionary of IOD table definitions
    """
    defined_tables = {}
    valid_headers = [
        ('Attribute Name', 'Tag', 'Type', 'Attribute Description'),
        ('Attribute Name', 'Tag', 'Attribute Description'),
        ('Attribute Name', 'Tag', 'Type', 'Description'),
        ('Attribute Name', 'Tag', 'Description'),
    ]
    for tbl in self._iod_parser.get_tables(
        self._iod_parser.xml_root, valid_headers
    ):
      line_block = []
      for row_lines in tbl.rows:
        tbl_row = self._iod_parser.parse_table_row(row_lines)
        parsed_row = tbl_row.parsed_row
        if not parsed_row:
          continue
        obj = None
        if tbl_row.starts_with_emphasis:
          obj = LinkedObject()
        elif len(parsed_row) == 4:
          obj = InlineObject(4)
        elif len(parsed_row) == 3:
          obj = InlineObject(3)
        if obj is not None:
          for val in parsed_row:
            obj.set_val(val)
          if obj.is_init():
            line_block.append(obj)
      if line_block:
        t_ref = TableRef(tbl.name, tbl.caption, line_block)
        defined_tables[t_ref.name] = t_ref
    return defined_tables

  def parse_spec(self) -> ParsedSpec:
    """Parses the DICOM IOD, modules, and tables from XML.

    Returns:
      Dictionary containing dicom IOD def, Module definitions, and tables
      defined
      in spec.
    """
    logging.info('Parsing DICOM IOD XML Tables')
    table_dict = self.read_table_dict()
    chapter_dict = self._iod_parser.get_chapters(['A', 'C'])
    # Extract Composit Information Object Definitions
    logging.info('Parsing DICOM IOD XML')
    iod = self._get_iod_def(chapter_dict)
    logging.info('Parsing DICOM IOD XML Modules')
    modules = self._get_module_def(chapter_dict)
    return ParsedSpec(iod, modules, table_dict)
