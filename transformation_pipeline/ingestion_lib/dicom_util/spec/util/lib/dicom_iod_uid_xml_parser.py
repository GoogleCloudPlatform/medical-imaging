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
"""Parses DICOM IOD UID from XML spec."""
from typing import Dict, Optional

from absl import logging

from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib.util import dicom_abstract_xml_parser
from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib.util import dicom_xml_core_parser

IodUidMappingType = Dict[str, str]


class DicomIodUidXmlParser(dicom_abstract_xml_parser.DicomAbstractXmlParser):
  """Parser for DICOM Standard IOD UID XML."""

  def __init__(self, part4_file_path: Optional[str] = None):
    super().__init__()
    if part4_file_path is None:
      xmlpart = self.download_xml('part04')
    else:
      xmlpart = self.read_xml(part4_file_path)
    self.set_dcm_version(xmlpart.dcm_version)  # pytype: disable=wrong-arg-types
    self._iod_parser = dicom_xml_core_parser.DicomXmlCoreParser(
        self.namespace, xmlpart.xml_root
    )

  def parse_spec(self) -> IodUidMappingType:
    """Returns Dictionary mapping iod names to iod uids."""
    logging.info('Parsing DICOM IOD UID XML')
    valid_headers = [(
        'SOP Class Name',
        'SOP Class UID',
        'IOD Specification (defined in',
        'Specialization',
    )]
    parsed_iod_class_uid = {}
    chapter_dict = self._iod_parser.get_chapters(['B'])
    for sec in chapter_dict['B'].iter(f'{self.namespace}section'):
      if 'label' not in sec.attrib:
        continue
      for tbl in self._iod_parser.get_tables(
          sec, valid_headers, limit_search_to_first_level=False
      ):
        for row_line in tbl.rows:
          line = self._iod_parser.parse_table_row(row_line).parsed_row
          if len(line) >= 2:
            iod_name = self.unicode_check(line[0])
            iod_name_guid = self.unicode_check(line[1])
            parsed_iod_class_uid[iod_name] = iod_name_guid
    return parsed_iod_class_uid
