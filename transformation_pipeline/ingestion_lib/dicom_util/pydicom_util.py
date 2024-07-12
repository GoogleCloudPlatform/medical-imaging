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
"""Utility functions for pydicom."""
from __future__ import annotations

import dataclasses
from typing import List, Optional, Sequence, Set, Union

import pydicom

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib.dicom_util import dicom_iod_util
from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard


class UndefinedIODError(Exception):
  """Error identifying DICOM IOD Metadata."""


@dataclasses.dataclass(frozen=True)
class DICOMTagPathElement:
  tag: dicom_iod_util.DICOMTag
  sequence_index: Optional[int]

  def __str__(self) -> str:
    return (
        self.tag.keyword
        if self.sequence_index is None
        else f'{self.tag.keyword}[{self.sequence_index}]'
    )


class DICOMTagPath:
  """Represents a path to DICOM tag in a DICOM instance."""

  def __init__(self, tag_path: Optional[List[DICOMTagPathElement]] = None):
    self._tag_path = [] if tag_path is None else tag_path

  def get_sequence_index(
      self, tag: dicom_iod_util.DICOMTag, index: int
  ) -> DICOMTagPath:
    return DICOMTagPath(self._tag_path + [DICOMTagPathElement(tag, index)])

  def get_tag(self, tag: dicom_iod_util.DICOMTag) -> DICOMTagPath:
    return DICOMTagPath(self._tag_path + [DICOMTagPathElement(tag, None)])

  def __str__(self) -> str:
    return '.'.join([str(tag) for tag in self._tag_path])

  @property
  def path(self) -> List[DICOMTagPathElement]:
    return self._tag_path

  @property
  def tag(self) -> dicom_iod_util.DICOMTag:
    return self._tag_path[-1].tag

  def get_dicom_dataset(self, ds: pydicom.Dataset) -> pydicom.Dataset:
    """Returns DICOM dataset which holds tag defined by path."""
    for path_element in self._tag_path[:-1]:
      ds = ds[path_element.tag.address][path_element.sequence_index]
    return ds


def set_dataset_tag_value(
    dicom: pydicom.Dataset, tag_keyword: str, value: Union[str, float, int]
) -> None:
  """Sets DICOM Tag value.

  Args:
    dicom: Pydicom.Dataset  to set.
    tag_keyword: DICOM tag keyword.
    value: DICOM tag value.
  """
  dicom_util = dicom_standard.dicom_standard_util()
  address = dicom_util.get_keyword_address(tag_keyword)
  vr_type = (list(dicom_util.get_tag_vr(address)))[0]
  try:
    del dicom[tag_keyword]
  except KeyError:
    pass
  vr_set = {vr_type}
  if dicom_util.is_vr_int_type(vr_set):
    dicom.add(pydicom.DataElement(address, vr_type, int(value)))
  elif dicom_util.is_vr_float_type(vr_set):
    dicom.add(pydicom.DataElement(address, vr_type, float(value)))
  else:
    dicom.add(pydicom.DataElement(address, vr_type, str(value)))


def set_dataset_tag_value_if_undefined(
    dicom: pydicom.Dataset, tag_keyword: str, value: Union[str, float, int]
) -> bool:
  """Sets DICOM Tag if undefined.

  Args:
    dicom: Pydicom.Dataset  to set.
    tag_keyword: DICOM tag keyword.
    value: DICOM tag value.

  Returns:
    True if tag value set set.
  """
  if tag_keyword not in dicom:
    set_dataset_tag_value(dicom, tag_keyword, value)
    return True
  return False


def _get_undefined_dicom_tags_by_type(
    iod_name: dicom_standard.IODName,
    ds: pydicom.Dataset,
    tag_type: Set[str],
    dicom_tag_path: DICOMTagPath,
    require_modules: Optional[Sequence[str]],
) -> List[DICOMTagPath]:
  """Returns list of root level undefined DICOM tags that are not defined.

  Args:
    iod_name: Name of DICOM IOD.
    ds: Pydicom Dataset, must be initalized with SOPClassUID.
    tag_type: Set of DICOM tag types to return.
    dicom_tag_path: Path to DICOM dataset to test for undefined tags.
    require_modules: Require listed IOD modules to be included regardless of
      Module IOD requirement level; e.g., treat listed modules with C or U usage
      requirement as having being mandatory.

  Returns:
    List of paths to DICOM tags of desired type which are not defined.
  """
  iod_path = [path_element.tag.keyword for path_element in dicom_tag_path.path]
  dataset = dicom_standard.dicom_iod_dataset_util().get_iod_dicom_dataset(
      iod_name, iod_path=iod_path, require_modules=require_modules
  )
  if dataset is None:
    return []
  tag_list = []
  dicom_util = dicom_standard.dicom_standard_util()
  for tag_address, tag in dataset.items():
    if tag.retired:
      continue
    if dicom_util.is_repeated_group_element_tag(tag_address):
      # ignore repeating group elements.
      # https://dicom.nema.org/dicom/2013/output/chtml/part05/sect_7.6.html
      continue
    if tag_address in ds:
      if tag.is_sq() and ds[tag_address].VR == 'SQ':
        for index, inner_ds in enumerate(ds[tag_address]):
          tag_list.extend(
              _get_undefined_dicom_tags_by_type(
                  iod_name,
                  inner_ds,
                  tag_type,
                  dicom_tag_path.get_sequence_index(tag, index),
                  require_modules=require_modules,
              )
          )
      continue
    # DICOM IOD are defined as a collection of modules.  Modules define tables
    # of tags. Tags can be defined by multiple modules. Each module define
    # module level requirements to tags (M, C, or U) and the module tables
    # define tag level requirements 1, 1C, 2, 2C, or 3. Tags can be defined by
    # multiple modules, this the standard parser reports all, Module and tag
    # level requirements. The tests here check the most stringent module and
    # table tag type level requirements for tags defined in a module.
    if 'M' != tag.get_highest_module_req():
      continue
    if tag.lowest_tag_type_requirement not in tag_type:
      continue
    tag_list.append(dicom_tag_path.get_tag(tag))
  return tag_list


def get_undefined_dicom_tags_by_type(
    ds: pydicom.Dataset,
    tag_type: Set[str],
    require_modules: Optional[Sequence[str]] = None,
) -> List[DICOMTagPath]:
  """Search DICOM dataset for DICOM tags with tag_type that are not defined.

  Args:
    ds: Pydicom Dataset, must be initalized with SOPClassUID.
    tag_type: Set of DICOM tag types to return.
    require_modules: Require listed IOD modules to be included regardless of
      Module IOD requirement level; e.g., treat listed modules with C or U usage
      requirement as having being mandatory.

  Returns:
    List of paths to DICOM tags of desired type which are not defined.

  Raises:
    UndefinedIODError: Could not identify IOD.
  """
  iod_name = dicom_standard.dicom_standard_util().get_sop_classid_name(
      ds.SOPClassUID
  )
  if iod_name is None:
    cloud_logging_client.warning(
        'DICOM standard metadata error could not identify DICOM IOD for UID.',
        {'SOPClassUID': ds.SOPClassUID},
    )
    raise UndefinedIODError()
  return _get_undefined_dicom_tags_by_type(
      iod_name, ds, tag_type, DICOMTagPath(), require_modules
  )
