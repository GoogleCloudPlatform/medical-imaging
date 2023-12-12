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
from typing import Union

import pydicom

from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard


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
