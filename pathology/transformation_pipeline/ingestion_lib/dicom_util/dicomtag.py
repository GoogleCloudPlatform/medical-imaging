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
"""DICOMTag definition class validates tag at definition time.

Backwards compatible with tags defined in ez_wsi_dicomweb.ml_toolkit

Raises exception if tag def address, keyword, or vr do not identify one
tag.
"""
from __future__ import annotations

import enum
from typing import Dict, Optional, Set, Union

from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard


class DicomTag:
  """DICOM tag definitions."""

  _global_dicom_tag_dict = {}

  def __init__(
      self,
      number: str,
      keyword: str,
      vr: Optional[Union[str, enum.Enum]] = None,
      add_to_global_list: Optional[bool] = None,
  ):
    if isinstance(vr, enum.Enum):
      vr = str(vr.value)
    # convert dicom address into standard representation
    number = DicomTag.standardize_address(number)
    if dicom_standard.dicom_standard_util().get_tag(number) is None:
      raise ValueError(
          f'Tag address {number} is not a valid dicom tag address.'
      )
    # if vr is not defined. attempt to define vr code from standard
    if vr is None:
      vr = dicom_standard.dicom_standard_util().get_tag_vr(number)
      if not vr:
        raise ValueError(f'vr type defined for tag address {number}')
      if len(vr) > 1:
        raise ValueError(
            f'vr type not optional tag has indeterminate vr types: {vr}'
        )
      vr = vr.pop()

    # initialize attributes
    self._keyword = keyword
    self._vr = vr
    self._number = number

    # validate that vr matches expectations
    expected_vr = dicom_standard.dicom_standard_util().get_tag_vr(number)
    if vr not in expected_vr:
      raise ValueError(
          f'Tag vr: {vr} type does not match address definitions {expected_vr}.'
      )
    # validate that keyword and address have same underlying address
    kw_address = dicom_standard.dicom_standard_util().get_keyword_address(
        keyword
    )
    kw_address = DicomTag.standardize_address(kw_address)
    if number != kw_address:
      raise ValueError(
          f'Tag address {number} and keyword {keyword}'
          f'address {kw_address} do not match.'
      )

    # add tag to global list of tags.
    if add_to_global_list in (None, True):
      DicomTag._global_dicom_tag_dict[number] = self

  @property
  def keyword(self) -> str:
    return self._keyword

  @property
  def number(self) -> str:
    return self._number

  @property
  def vr(self) -> str:
    return self._vr

  @classmethod
  def tag_address(
      cls,
      address: str,
      keyword: str,
      vr: Optional[Union[str, enum.Enum]] = None,
  ) -> str:
    """Validats parameters match single tag and return tag address.

    Args:
      address: DICOM tag address.
      keyword: DICOM tag keyword.
      vr: DICOM tag vr type.

    Returns:
      tag address

    Raises:
      Value error if parameters don't match single tag.
    """
    tag = DicomTag(address, keyword, vr, add_to_global_list=False)
    return tag.address

  @classmethod
  def tag_keyword(
      cls,
      address: str,
      keyword: str,
      vr: Optional[Union[str, enum.Enum]] = None,
  ) -> str:
    """Validats parameters match single tag and return tag keyword.

    Args:
      address: DICOM tag address.
      keyword: DICOM tag keyword.
      vr: DICOM tag vr type.

    Returns:
      tag keyword
    Raises:
      Value error if parameters don't match single tag.
    """
    tag = DicomTag(address, keyword, vr, add_to_global_list=False)
    return tag.keyword

  @classmethod
  def tag_vr(
      cls,
      address: str,
      keyword: str,
      vr: Optional[Union[str, enum.Enum]] = None,
  ) -> str:
    """Validats parameters match single tag and return tag vr.

    Args:
      address: DICOM tag address.
      keyword: DICOM tag keyword.
      vr: DICOM tag vr type.

    Returns:
      tag vr type
    Raises:
      Value error if parameters don't match single tag.
    """
    tag = DicomTag(address, keyword, vr, add_to_global_list=False)
    return tag.vr

  @classmethod
  def standardize_address(cls, address: str) -> str:
    """Standardizes DICOM tag addresses by removing (,) and leading 0x hex.

       Converts (0xffee, 0xaabb) or (ffee, aabb) or 0xffeeaabb or ffeeaabb
       to: ffeeaabb

    Args:
      address: DICOM tag address.

    Returns:
      standerdized tag address in the ffeeaabb format.
    """
    address = address.strip()
    if address.startswith('(') and address.endswith(')'):
      address = address[1:-1]
    partlist = []
    for part in address.split(','):
      part = part.strip()
      if part.startswith('0x'):
        part = part[2:]
      if part:
        partlist.append(part)
    address = ''.join(partlist)
    # prefix short tags with missing leading zeros
    zero_prefix = '0' * max(0, 8 - len(address))
    return f'{zero_prefix}{address}'

  @property
  def address(self) -> str:
    """Returns DICOM Tag Address."""
    return self.number

  @classmethod
  def defined_tag_dict(cls) -> Dict[str, DicomTag]:
    """Returns global class dictionary of allocated tags."""
    return DicomTag._global_dicom_tag_dict

  @classmethod
  def get_tag(cls, address: str) -> Optional[DicomTag]:
    """Returns dicom tag definition associated with an address."""
    address = DicomTag.standardize_address(address)
    return DicomTag._global_dicom_tag_dict.get(address)

  @classmethod
  def defined_types(cls) -> Set[str]:
    """Returns set of all dicom types defined."""
    defined_vr = set()
    for tpl in DicomTag.defined_tag_dict().values():
      for type_str in tpl.vr.split(' or '):
        defined_vr.add(type_str)
    return defined_vr

  @classmethod
  def defined_keywords(cls) -> Set[str]:
    """Returns set of all dicom types defined."""
    defined_keywords = set()
    for tpl in DicomTag.defined_tag_dict().values():
      defined_keywords.add(tpl.keyword)
    return defined_keywords

  @classmethod
  def defined_addresses(cls) -> Set[str]:
    """Returns set of all dicom types defined."""
    defined_numbers = set()
    for tpl in DicomTag.defined_tag_dict().values():
      defined_numbers.add(tpl.number)
    return defined_numbers
