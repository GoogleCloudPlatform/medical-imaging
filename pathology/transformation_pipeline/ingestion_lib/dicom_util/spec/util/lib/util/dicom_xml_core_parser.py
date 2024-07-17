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
"""Core parser for DICOM Spec XML."""
import dataclasses
from typing import Any, Generator, List, Union

from absl import logging

from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib.util import dicom_iod_generator_exception

DicomIodGeneratorError = dicom_iod_generator_exception.DicomIodGeneratorError


@dataclasses.dataclass
class ParsedTableRef(object):
  name: str
  caption: str
  rows: List[str]


@dataclasses.dataclass
class ParsedTableRow(object):
  starts_with_emphasis: bool
  parsed_row: List[Union[str, List[str]]]


class DicomXmlCoreParser:
  """Parser for DICOM Standard XML."""

  def __init__(self, namespace: str, xml_root: Any):
    self._namespace = namespace
    self._xml_root = xml_root

  @property
  def xml_root(self) -> Any:
    return self._xml_root

  @xml_root.setter
  def xml_root(self, val):
    self._xml_root = val

  def get_tables(
      self, xml_block, header_tables, limit_search_to_first_level=False
  ) -> Generator[ParsedTableRef, None, None]:
    """Generator yields dictionaries of tables in xml block.

    Args:
      xml_block: block to base
      header_tables: list of the column headers for tables to return. Ignores
        tables with headers which do not match.
      limit_search_to_first_level: limits xml search to only return tables in
        the first level of the xml block.

    Yields:
      Dictionary defining parsed table.
    """
    logging.debug(
        'Searching for tables with header filter: %s', str(header_tables)
    )
    namespace = self._namespace
    if limit_search_to_first_level:
      sub_tables = xml_block.findall(f'./{namespace}table')
    else:
      sub_tables = xml_block.iter(f'{namespace}table')
    for table in sub_tables:
      if 'label' not in table.attrib:
        continue
      tname = table.attrib['label'].strip()
      caption = table.find(f'./{namespace}caption')
      if caption is not None:
        caption = caption.text.strip()
      header_block = table.find(f'{namespace}thead')
      header_text = ()
      if header_block is None:
        continue

      header_lst = []
      for itm in header_block.iter(f'{namespace}para'):
        header_entry = []
        if itm.text is not None:
          txt = itm.text.strip()
          if txt:
            header_entry.append(txt)
        for emphasis in itm.iter(f'{namespace}emphasis'):
          if emphasis.text is not None:
            txt = emphasis.text.strip()
            if txt:
              header_entry.append(txt)
        if header_entry:
          header_lst.append(' '.join(header_entry))
        else:
          header_lst.append('')
      header_text = tuple(header_lst)

      if header_text not in header_tables:
        logging.debug(
            'Ignore table, header %s does not match header filter: %s',
            str(header_text),
            str(header_tables),
        )
        continue
      table_body = table.find(f'{namespace}tbody')
      if table_body is None:
        continue
      table_rows = table_body.findall(f'./{namespace}tr')
      if not table_rows:
        continue
      yield ParsedTableRef(tname, caption, table_rows)

  def _get_table_row(self, table_row) -> ParsedTableRow:
    """private method reads a single row of a table.

       Call Public method: parse_table_row
    Args:
      table_row: lines of text defining a tables row

    Returns:
      ParsedTableRow
    """
    namespace = self._namespace
    line = []
    emphasis_start = False
    for td in table_row.findall(f'./{namespace}td'):
      line_text = []
      linked = []
      para_sub_block = td.findall(f'./{namespace}para')
      if not para_sub_block:
        para_sub_block = [td]

      pre_line_length = len(line)
      for para in para_sub_block:
        emphasis = para.find(f'./{namespace}emphasis')
        if emphasis is not None:
          if not line:
            ref = emphasis.find(f'./{namespace}xref')
            if ref is not None:
              line.append(emphasis.text.strip())
              line.append(ref.attrib['linkend'])
              emphasis_start = True
              continue
          if emphasis.text:
            line_text.append(emphasis.text.strip())

        if not emphasis_start:
          if not line_text:
            linkedref = [
                xref.attrib['linkend']
                for xref in para.findall(f'./{namespace}xref')
                if 'linkend' in xref.attrib
            ]
            if linkedref:
              linked += linkedref
              continue
        if para.text:
          line_text.append(para.text.strip())
      if line_text:
        line.append('\n'.join(line_text))
      elif linked:
        line.append(linked)
      elif pre_line_length == len(line):  # if nothing was add for column
        line.append('')  # add padding space
    return ParsedTableRow(emphasis_start, line)

  @classmethod
  def clean_text(
      cls, text: Union[str, List[str]]
  ) -> Union[str, List[str], None]:
    """Removes unicode characters from text."""
    if not text:
      return text
    if isinstance(text, str):
      text = (
          text.strip()
          .replace('\u200b', '')
          .replace('µ', 'u')
          .replace('\u2019', "'")
      )
      return text
    elif isinstance(text, List):
      return [DicomXmlCoreParser.clean_text(item) for item in text]  # pytype: disable=bad-return-type  # always-use-return-annotations

  def parse_table_row(self, table_row) -> ParsedTableRow:
    """Parses a single row of a table.

       calls _get_table_row
       cleans result and checks that result contains only ascii.

    Args:
      table_row: lines of text defining a tables row

    Returns:
      ParsedTableRow
    """
    parsed_tbl_row = self._get_table_row(table_row)
    if parsed_tbl_row.parsed_row:
      parsed_tbl_row.parsed_row = [  # pytype: disable=annotation-type-mismatch  # always-use-return-annotations
          DicomXmlCoreParser.clean_text(item)
          for item in parsed_tbl_row.parsed_row
      ]
    return parsed_tbl_row

  def get_chapters(self, chapterlist):
    """Returns select dictionary of IOD chapters from spec.

    Args:
      chapterlist: list of chapeters to return

    Returns:
      Dictionary of chapters
    """
    chapter_dict = {}
    chapter_length = len(chapterlist)
    for chapter in self.xml_root.iter(f'{self._namespace}chapter'):
      label = chapter.attrib['label']
      if label in chapterlist:
        chapter_dict[label] = chapter
        if len(chapter_dict) == chapter_length:
          return chapter_dict
    raise DicomIodGeneratorError('Chapters not found')
