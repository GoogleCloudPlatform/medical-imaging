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
"""Main utility generates json files describing tables in dicom spec.

 Downloads DICOM Spec XML - Parses to generate:
   dicom_iod.json
   dicom_iod_class_uid.json
   dicom_modules.json
   dicom_tables.json
   dicom_tags.json

Generated Json used in: dicom_standard_util.py
"""
import collections
import json
import os
from typing import Any, Dict, List, Optional, Union

from absl import logging

from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib import dicom_iod_uid_xml_parser
from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib import dicom_iod_xml_parser
from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib import dicom_tag_xml_parser
from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib.util import dicom_iod_generator_exception

DicomIodGeneratorError = dicom_iod_generator_exception.DicomIodGeneratorError
DicomStandardTags = dicom_tag_xml_parser.DicomStandardTags
DicomIodUidXmlParser = dicom_iod_uid_xml_parser.DicomIodUidXmlParser
IodUidMappingType = dicom_iod_uid_xml_parser.IodUidMappingType
DicomIodXmlParser = dicom_iod_xml_parser.DicomIodXmlParser
ModuleTableRef = dicom_iod_xml_parser.ModuleTableRef
IodSectionRef = dicom_iod_xml_parser.IodSectionRef
IodDefType = dicom_iod_xml_parser.IodDefType
ModuleDefType = dicom_iod_xml_parser.ModuleDefType
TableRef = dicom_iod_xml_parser.TableRef
TableDefType = dicom_iod_xml_parser.TableDefType


class JsonWriter:
  """Writes complex data structures out as well formatted json."""

  @classmethod
  def _is_parsed_iod_obj(cls, var: Any) -> bool:
    """Returns True if var is an instance of parsed iod obj class.

    Args:
      var: variable to test

    Returns:
      True if var is instance of parsed iod obj class
    """
    if isinstance(var, ModuleTableRef):
      return True
    if isinstance(var, IodSectionRef):
      return True
    if isinstance(var, TableRef):
      return True
    if isinstance(var, DicomStandardTags):
      return True
    return False

  @classmethod
  def json_indent(cls, l1_prefix: str, l2_prefix: str, obj: str) -> str:
    inner_list = []
    for index, json_line in enumerate(obj.split('\n')):
      if index == 0:
        inner_list.append(f'{l1_prefix}{json_line}')
      else:
        inner_list.append(f'{l2_prefix}{json_line}')
    return '\n'.join(inner_list)

  @classmethod
  def _write(
      cls,
      var: Union[
          ModuleTableRef,
          IodSectionRef,
          TableRef,
          IodUidMappingType,
          DicomStandardTags,
          List[Any],
          Dict[str, Any],
          str,
          int,
          float,
      ],
      indent_count: int,
      line_prefix: str = '',
      first_line: Optional[str] = None,
  ) -> str:
    """Recursives through IOD objects to generate json versions of parsed structs.

    Args:
      var: object to generate code for
      indent_count: source code line indent
      line_prefix: string prefix for code
      first_line: starting line

    Returns:
      json string
    """

    line_out = []
    indent_str = ' ' * indent_count
    if first_line is None:
      first_line = indent_str
    if JsonWriter._is_parsed_iod_obj(var):
      return JsonWriter.json_indent(
          f'{first_line}{line_prefix}', f'{first_line}', str(var)
      )
    elif isinstance(var, list):
      line_out.append(f'{first_line}{line_prefix}[')
      list_lines = []
      for obj in var:
        if JsonWriter._is_parsed_iod_obj(obj):
          indent = f'{indent_str}   '
          list_lines.append(JsonWriter.json_indent(indent, indent, str(obj)))
        elif isinstance(obj, list) or isinstance(obj, dict):
          list_lines.append(JsonWriter._write(obj, indent_count + 2))
        else:
          list_lines.append(f'{indent_str}  {json.dumps(obj)}')
      line_out.append(',\n'.join(list_lines))
      line_out.append(f'{first_line}]')
      return '\n'.join(line_out)
    elif isinstance(var, dict):
      line_out.append(f'{first_line}{line_prefix}{{')
      list_lines = []
      for key, obj in var.items():
        keystrprefix = f'{json.dumps(key)}: '
        if JsonWriter._is_parsed_iod_obj(obj):
          key_padding = ' ' * len(keystrprefix)
          list_lines.append(
              JsonWriter.json_indent(
                  f'{indent_str}   {keystrprefix}',
                  f'{indent_str}   {key_padding}',
                  str(obj),
              )
          )
        elif isinstance(obj, list) or isinstance(obj, dict):
          list_lines.append(
              JsonWriter._write(
                  obj,
                  indent_count + 3,
                  line_prefix=keystrprefix,
                  first_line=f'{indent_str}   ',
              )
          )
        else:
          list_lines.append(f'{indent_str}   {keystrprefix}{json.dumps(obj)}')
      line_out.append(',\n'.join(list_lines))
      line_out.append(f'{first_line}}}')
      return '\n'.join(line_out)
    else:
      return f'{first_line}{line_prefix}{json.dumps(var)}'

  @classmethod
  def write_iod_json(
      cls,
      name: str,
      json_header_txt: str,
      obj: Union[
          IodDefType,
          ModuleDefType,
          TableDefType,
          IodUidMappingType,
          DicomStandardTags,
      ],
  ):
    """Writes iod structure out to formatted file json file.

    Args:
      name: name of file to write
      json_header_txt: header for JSON file
      obj: object to write
    """
    output_path = os.path.abspath(
        os.path.join(os.getcwd(), '..', '..', '..', f'{name}.json')
    )
    with open(output_path, 'wt') as outfile:
      outfile.write('{\n')
      outfile.write(json_header_txt)
      outfile.write(
          JsonWriter._write(obj, indent_count=2, line_prefix=f'"{name}": ')
      )
      outfile.write('\n}\n')
      logging.info('Generating: %s', output_path)


def split_tables(tables: TableDefType, splits: int) -> List[TableDefType]:
  """Splits tables into blocks to keep table json below checkin size req.

  Args:
     tables: Tables to split
     splits: number of blocks to split table dict into.

  Returns:
    List of TableDefType.
  """
  table_key_list = list(tables.keys())
  splitlen = int(len(table_key_list) / splits)
  ret_list = []
  last_index = 0
  for split_index in range(splits):
    first_index = last_index
    if split_index < splits - 1:
      last_index = first_index + splitlen
    else:
      last_index = len(table_key_list)
    table_subset = collections.OrderedDict()
    for key in table_key_list[first_index:last_index]:
      table_subset[key] = tables[key]
    ret_list.append(table_subset)
  return ret_list


if __name__ == '__main__':
  logging.set_verbosity(logging.INFO)
  iod_parser = DicomIodXmlParser()
  iod_def = iod_parser.parse_spec()
  JsonWriter.write_iod_json('dicom_iod', iod_parser.header_txt, iod_def.iod)
  JsonWriter.write_iod_json(
      'dicom_modules', iod_parser.header_txt, iod_def.modules
  )
  JsonWriter.write_iod_json(
      'dicom_iod_func_groups',
      iod_parser.header_txt,
      iod_def.iod_functional_groups,
  )

  split_tables = split_tables(iod_def.tables, 3)
  for f_index, table_part in enumerate(split_tables):
    JsonWriter.write_iod_json(
        f'dicom_tables_part_{f_index+1}', iod_parser.header_txt, table_part
    )

  iod_uid_parser = DicomIodUidXmlParser()
  iod_class_uid_mapping = iod_uid_parser.parse_spec()
  JsonWriter.write_iod_json(
      'dicom_iod_class_uid', iod_uid_parser.header_txt, iod_class_uid_mapping
  )
  if iod_parser.header_txt != iod_uid_parser.header_txt:
    raise DicomIodGeneratorError('Versions of DICOM Standard do not match.')
  tag_parser = dicom_tag_xml_parser.DicomTagXmlParser()
  parsed_dicom_tags = tag_parser.parse_spec()
  JsonWriter.write_iod_json(
      'dicom_tags', tag_parser.header_txt, parsed_dicom_tags
  )
  if iod_parser.header_txt != tag_parser.header_txt:
    raise DicomIodGeneratorError('Versions of DICOM Standard do not match.')
