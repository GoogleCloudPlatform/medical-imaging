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
"""Implementation DicomFileRef and Factory classes.

DICOM file ref stores select metadata-attributes to dicom files.
That are needed to identify, e.g., query dicom and identify the data represented
in Dicom.

DicomFileRefFactory implements methods to create DicomFileRef from a dicom file
and DICOM Json.
"""
import collections
import dataclasses
from typing import Any, Callable, List, Mapping, MutableMapping, Optional, TypeVar, Union

import pydicom

from shared_libs.test_utils.dicom_store_mock import dicom_store_mock_types
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard


@dataclasses.dataclass(frozen=False)
class DicomRefTag:
  value: str = ''
  private_creator_base_tag_address: Optional[str] = None

  @property
  def is_private(self) -> bool:
    return self.private_creator_base_tag_address is not None


class _BaseDicomFileRef:
  """Base class for DicomFileRef forward declares tags to facilitate typing."""

  def __init__(self):
    self._tags = collections.OrderedDict()

  @property
  def tags(self) -> Mapping[str, DicomRefTag]:
    return self._tags


class DicomFileRef(
    _BaseDicomFileRef, dicom_store_mock_types.AbstractGetDicomUidTripleInterface
):
  """Reference a subset of tags in DICOM file."""

  def __init__(self):
    super().__init__()
    self._source = ''
    self.define_tag(ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID)
    self.define_tag(ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID)
    self.define_tag(ingest_const.DICOMTagKeywords.SOP_CLASS_UID)
    self.define_tag(ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID)

  def get_dicom_uid_triple(self) -> dicom_store_mock_types.DicomUidTriple:
    """Method provides interface for DICOM store mock to get UID Triple."""
    return dicom_store_mock_types.DicomUidTriple(
        self.study_instance_uid, self.series_instance_uid, self.sop_instance_uid
    )

  def tag_addresses(self) -> List[str]:
    """Returns List of DICOM tag address for tags defined in DICOMRef class."""
    tag_addresses = []
    private_creators = set()
    for keyword, tag in self._tags.items():
      if tag.is_private:
        if not isinstance(keyword, str):
          raise ValueError('DICOM tag keyword is not string')
        address = keyword
        if address:
          if not address.startswith('0x'):
            address = f'0x{address}'
          private_creators.add(
              address[:-4] + tag.private_creator_base_tag_address
          )
          tag_addresses.append(address)
      else:
        address = dicom_standard.dicom_standard_util().get_keyword_address(
            keyword, striphex=False
        )
        if address:
          tag_addresses.append(address)
    tag_addresses.extend(private_creators)
    return tag_addresses

  def set_source(self, source: str):
    self._source = source

  @property
  def source(self) -> str:
    return self._source

  def define_tag(self, keyword: str):
    self._tags[keyword] = DicomRefTag(
        value='', private_creator_base_tag_address=None
    )

  def define_private_tag(self, address: str):
    # Private Creator Tag defined:
    # https://dicom.nema.org/dicom/2013/output/chtml/part05/sect_7.8.html
    private_creator_base_tag_address = '00' + address[-4:-2]
    self._tags[address] = DicomRefTag(
        value='',
        private_creator_base_tag_address=private_creator_base_tag_address,
    )

  def get_tag_value(self, keyword: str) -> str:
    return self._tags[keyword].value

  def set_tag_value(self, keyword: str, val: str):
    self._tags[keyword].value = str(val)

  def is_tag_private(self, keyword: str) -> bool:
    return self._tags[keyword].is_private

  @property
  def study_instance_uid(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID)

  @property
  def series_instance_uid(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID)

  @property
  def sop_class_uid(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.SOP_CLASS_UID)

  @property
  def sop_instance_uid(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID)

  def dict(self) -> MutableMapping[str, Any]:
    core_dict = collections.OrderedDict()
    for keyword, dcm_ref_tag in self._tags.items():
      core_dict[keyword] = dcm_ref_tag.value
    return core_dict

  def equals(
      self,
      other: _BaseDicomFileRef,
      ignore_tags: Optional[List[str]] = None,
      test_tags: Optional[List[str]] = None,
  ) -> bool:
    """Tests if two DicomFileRef tags are equal.

       Optionally can ignore values of tags defined in ignore_tags

    Args:
      other: Instance of DicomFileRef to test equality with.
      ignore_tags: Optional List of tag keywords to ignore.
      test_tags: Optional List of tags test for equality.

    Returns:
      bool
    """
    if tuple(list(self._tags)) != tuple(list(other.tags)):
      return False
    tag_keys = set(other.tags)
    if test_tags is not None:
      test_tags = set(test_tags)
      tag_keys = tag_keys & test_tags
      if len(tag_keys) != len(test_tags):
        raise ValueError('Only tags defined in dicom_file_ref can be specified')
    if ignore_tags is not None:
      tag_keys = tag_keys - set(ignore_tags)
    if not tag_keys:
      raise ValueError('No tags to match specified.')
    for keyword in tag_keys:
      dcm_ref_tag = other.tags[keyword]
      if self.get_tag_value(keyword) != dcm_ref_tag.value:
        return False
      if self.is_tag_private(keyword) != dcm_ref_tag.is_private:
        return False
    return True

  def __str__(self) -> str:
    str_ref = [f'DICOM_FileRef_(source: {self._source})']
    for keyword, dcm_ref_tag in self._tags.items():
      str_ref.append(f'  {keyword}: {dcm_ref_tag.value}')
    return '\n'.join(str_ref)


T = TypeVar('T')
DCMJson = Mapping[str, Any]
DCMFile = pydicom.dataset.Dataset
DCMData = Union[DCMJson, DCMFile]


def _fill_ref(
    source: str,
    dicom_data: DCMData,
    get_keyword_value: Callable[[DCMData, str, bool], str],
    dicom_file_ref: T,
) -> T:
  """Core method that creats a DicomFileRef.

  Args:
    source: string representation of source that initialized class.
    dicom_data: Dicom File or Dicom Json to extra metadata from
    get_keyword_value: Method to extract keyword from value from dicom_data.
    dicom_file_ref: DicomFileRef object to fill.

  Returns:
    DicomFileRef
  """
  dicom_file_ref.set_source(source)
  for keyword in dicom_file_ref.tags:
    is_private = dicom_file_ref.is_tag_private(keyword)
    kw_val = get_keyword_value(dicom_data, keyword, is_private)
    dicom_file_ref.set_tag_value(keyword, kw_val)
  return dicom_file_ref


def _norm_tag_value_str(tag_vr_type: str, value: Union[str, int, float]) -> str:
  """Normalize tag string representation based on tag VR type.

     For undefined vr type return passed value as string.
     For string VR type return passed value as string.
     Float VR types -> explicitly converted to float and then to string.
     Int VR types -> explicitly converted to int and then to string.

     Use to fix issue where values, especially floats, in a string
     representation could define same value but have different str values.

  Args:
    tag_vr_type: DICOM tag vr type.
    value: Current representation of tag value.

  Returns:
    Normalized tag string value.
  """
  if not tag_vr_type or (isinstance(value, str) and not value):
    return str(value)
  try:
    return dicom_standard.dicom_standard_util().get_normalized_tag_str(
        {tag_vr_type}, value
    )
  except ValueError:
    # if normalization of the value fails return the value as is.
    return str(value)


def _get_dicom_keyword_value(
    dcm: DCMFile, keyword: str, is_private: bool
) -> str:
  """Returns keyword value for DICOM files.

  Args:
    dcm: pydicom DICOM file.
    keyword: DICOM Keyword to get value of.
    is_private: Is the tag a private tag?

  Returns:
    String representation of DICOM Value
  """
  if is_private:
    # retrieve the last byte of key which is unique to the private tag
    keyword = int(keyword, 16) & 0x000000FF
    try:
      tag_val = dcm.get_private_item(
          int(ingest_const.DICOMTagKeywords.GROUP_ADDRESS, 16),
          keyword,
          ingest_const.PRIVATE_TAG_CREATOR,
      )
    except KeyError:
      return ''
    if tag_val is None:
      return ''
    return _norm_tag_value_str(tag_val.VR, tag_val.value)
  hex_address = dicom_standard.dicom_standard_util().get_keyword_address(
      keyword, striphex=True
  )
  try:
    value = dcm[hex_address].value
    tag_vr = dcm[hex_address].VR
  except KeyError:
    value = ''
    tag_vr = ''
  if isinstance(value, pydicom.multival.MultiValue):
    return '\\'.join([_norm_tag_value_str(tag_vr, val) for val in value])
  return _norm_tag_value_str(tag_vr, value)


def init_from_loaded_file(path: str, dcm: DCMFile, dicom_file_ref: T) -> T:
  """Returns DicomFileRef for a DICOM file.

  Args:
    path: File path to dicom file.
    dcm: Loaded pydicom file.
    dicom_file_ref: DicomFileRef class to initialize from file.

  Returns:
    DicomFileRef

  Raises:
     pydicom.errors.InvalidDicomError : Invalid DICOM file
  """
  return _fill_ref(path, dcm, _get_dicom_keyword_value, dicom_file_ref)


def init_from_file(path: str, dicom_file_ref: T) -> T:
  """Returns DicomFileRef for a DICOM file.

  Args:
    path: File path to dicom file
    dicom_file_ref: DicomFileRef class to initialize from file

  Returns:
    DicomFileRef

  Raises:
     pydicom.errors.InvalidDicomError: Invalid DICOM file
  """
  with pydicom.dcmread(path, defer_size='512 KB') as dcm:
    return _fill_ref(path, dcm, _get_dicom_keyword_value, dicom_file_ref)


def _get_json_keyword_value(
    json_dict: DCMJson, keyword: str, is_private: bool
) -> str:
  """Returns keyword value for DICOM Json encoding.

  Args:
    json_dict: Python dict representation of Json DICOM.
    keyword: DICOM keyword to get value of.
    is_private: Is the tag a private tag?

  Returns:
    String representation of DICOM Keyword value
  """
  if is_private:
    hex_address = keyword
  else:
    #  Get the DICOM hex address of the keyword
    hex_address = dicom_standard.dicom_standard_util().get_keyword_address(
        keyword, striphex=True
    )
    #  Get the value of the hex addess
  tag_data = json_dict.get(hex_address)
  if not tag_data:
    return ''  # tag is not define. valid for some tags
  tag_value = tag_data.get('Value')  # returns a list of strings.
  if not tag_value:
    return ''  # tag is not define. valid for some tags
  vr_type = tag_data.get('vr', '')
  return '\\'.join([_norm_tag_value_str(vr_type, val) for val in tag_value])


def init_from_json(json_dict: DCMJson, dicom_file_ref: T) -> T:
  """Returns DicomFileRef for DICOM Json.

  Args:
    json_dict: Python dict representation of Json DICOM.
    dicom_file_ref: DicomFileRef class to initialize from file.

  Returns:
     DicomFileRef

  Raises:
     KeyError or IndexError: Invalid Json
  """
  return _fill_ref('JSON', json_dict, _get_json_keyword_value, dicom_file_ref)
