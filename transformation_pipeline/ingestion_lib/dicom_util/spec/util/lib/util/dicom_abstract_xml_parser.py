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
"""Abstract class for dicom xml parsers.

All classes should:
   call set_dcm_version and  implement

   def parse_spec(self) -> Any:
"""
import dataclasses
import json
import re
from typing import Any
import urllib.request as urllib2
import xml.etree.ElementTree as ET

from absl import logging

from transformation_pipeline.ingestion_lib.dicom_util.spec.util.lib.util import dicom_iod_generator_exception

DicomIodGeneratorError = dicom_iod_generator_exception.DicomIodGeneratorError


@dataclasses.dataclass
class DicomXmlSpec:
  dcm_version: str
  xml_root: Any


class DicomAbstractXmlParser:
  """Abstract class for dicom xml parsers.

  All classes should:
      call set_dcm_version and  implement

      def parse_spec(self) -> Any:
  """

  def __init__(self):
    self._namespace = '{http://docbook.org/ns/docbook}'
    self._dcm_version = None

  @property
  def namespace(self) -> str:
    return self._namespace

  def download_xml(self, dicom_standard_part: str) -> DicomXmlSpec:
    """Downloads XML from DICOM Standard http server and parses xml.

    Args:
      dicom_standard_part: Spec part to download

    Returns:
      DicomXmlSpec
    """
    url = 'http://medical.nema.org/medical/dicom/current/source/docbook'
    url_part = f'{url}/{dicom_standard_part}/{dicom_standard_part}.xml'
    logging.info('Reading XML from url: %s', url_part)
    url_data = urllib2.urlopen(url_part)
    logging.info('Parsing XML.')
    tree = ET.parse(url_data)
    xml_root = tree.getroot()
    dcm_version = xml_root.find(f'{self.namespace}subtitle')
    if dcm_version is None:
      raise DicomIodGeneratorError('Find XML namespace.')
    dcm_version = dcm_version.text.split()[2]  # pytype: disable=attribute-error
    logging.info('Processing XML.')
    return DicomXmlSpec(dcm_version, xml_root)

  def read_xml(self, path: str) -> DicomXmlSpec:
    """Reads XML from file system.

    Args:
      path: path to file

    Returns:
      DicomXmlSpec
    """
    logging.info('Reading XML from path: %s', path)
    tree = ET.parse(path)
    xml_root = tree.getroot()
    dcm_version = xml_root.find(f'{self.namespace}subtitle')
    if dcm_version is None:
      raise DicomIodGeneratorError('Find XML namespace.')
    dcm_version = dcm_version.text.split()[2]  # pytype: disable=attribute-error
    logging.info('Processing XML.')
    return DicomXmlSpec(dcm_version, xml_root)

  def set_dcm_version(self, version: str):
    self._dcm_version = version

  @property
  def header_txt(self) -> str:
    """Header that identifies version of dicom standard used to generate json."""
    if self._dcm_version is None:
      raise DicomIodGeneratorError('Parsed dicom version not set.')
    header = {'Version': self._dcm_version}
    header_txt = json.dumps({'dicom_standard_version': header})
    return f'  "header" : {header_txt},\n\n'

  def parse_spec(self) -> Any:
    pass

  @classmethod
  def unicode_check(cls, text: Any) -> Any:
    """Check text for unicode.

    Args:
      text: text to check

    Returns:
      text

    Raises:
      DicomIodGeneratorError if text contains unicode
    """
    if text:
      if isinstance(text, str):
        if re.fullmatch(r'^[\x00-\x7F]+$', text) is None:
          raise DicomIodGeneratorError(
              f'Text: {text} contains unicode. Ascii encoding: {ascii(text)}'
          )
      elif isinstance(text, list):
        for item in text:
          DicomAbstractXmlParser.unicode_check(item)
      else:
        raise DicomIodGeneratorError('Unexpected type.')
    return text
