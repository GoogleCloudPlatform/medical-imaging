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
"""Singlton accessor for access to DicomStandardIODUtil & DicomIODDatasetUtil.

Access through:

  def dicom_standard_util(path: Optional[str] = None)
                                      -> dcm_util.DicomStandardIODUtil:

  def dicom_iod_dataset_util(path: Optional[str] = None)
                                    -> dicom_iod_util.DicomIODDatasetUtil:

  path : Optional path to json iod description.

Purpose:
  caches loading of json datasts.
"""
from __future__ import annotations

from typing import Optional

from transformation_pipeline.ingestion_lib.dicom_util import dicom_iod_util
from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard_util as dcm_util

DicomVRCode = dcm_util.DicomVRCode
IODName = dcm_util.IODName
DicomKeyword = dcm_util.DicomKeyword
DicomVMCode = dcm_util.DicomVMCode
DicomTagAddress = dcm_util.DicomTagAddress
TableName = dcm_util.TableName
ModuleName = dcm_util.ModuleName
ModuleRef = dcm_util.ModuleRef
ModuleUsage = dcm_util.ModuleUsage
DICOMSpecMetadataError = dcm_util.DICOMSpecMetadataError
DicomPathError = dicom_iod_util.DicomPathError


class DicomStandardSingleton:
  """Singleton class for accessing dicom_standard_util & dicom_iod_util."""

  _instance = None

  def __init__(self, path: Optional[str] = None):
    if DicomStandardSingleton._instance is not None:
      raise ValueError('Singleton class once.')
    self.path = path
    self.iod = dicom_iod_util.DicomIODDatasetUtil(path)

  @classmethod
  def instance(cls, path: Optional[str] = None) -> DicomStandardSingleton:
    if DicomStandardSingleton._instance is None:
      DicomStandardSingleton._instance = DicomStandardSingleton(path)
    if DicomStandardSingleton._instance.path != path:
      DicomStandardSingleton._instance.path = path
      DicomStandardSingleton._instance.iod = dicom_iod_util.DicomIODDatasetUtil(
          path
      )
    return DicomStandardSingleton._instance

  @classmethod
  def dicom_standard_util(
      cls,
      path: Optional[str] = None,
  ) -> dcm_util.DicomStandardIODUtil:
    return DicomStandardSingleton.instance(path).iod.dicom_standard

  @classmethod
  def dicom_iod_dataset_util(
      cls,
      path: Optional[str] = None,
  ) -> dicom_iod_util.DicomIODDatasetUtil:
    return DicomStandardSingleton.instance(path).iod


def dicom_standard_util(
    path: Optional[str] = None,
) -> dcm_util.DicomStandardIODUtil:
  """Preferred accessor method for accessing DicomStandardIODUtil.

  Args:
    path: filepath to json datasets describing dicom standard

  Returns:
      DicomStandardIODUtil
  """
  return DicomStandardSingleton.dicom_standard_util(path=path)


def dicom_iod_dataset_util(
    path: Optional[str] = None,
) -> dicom_iod_util.DicomIODDatasetUtil:
  """Preferred accessor method for accessing DicomIODDatasetUtil.

  Args:
    path: filepath to json datasets describing dicom standard

  Returns:
      DicomIODDatasetUtil
  """
  return DicomStandardSingleton.dicom_iod_dataset_util(path=path)
