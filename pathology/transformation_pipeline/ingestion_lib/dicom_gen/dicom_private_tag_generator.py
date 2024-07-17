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
"""DICOM private tag generator to create custom dicom tags."""

import dataclasses
from typing import Any, List

import pydicom

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import ingest_const


@dataclasses.dataclass(frozen=True)
class DicomPrivateTag:
  """Defines a DICOM private tag by address, vr, and value."""

  address: str
  vr: str
  value: Any

  def __str__(self) -> str:
    return (
        f'PrivateTag(address={self.address}, vr={self.vr}, value={self.value})'
    )


class DicomPrivateTagGenerator(object):
  """Dicom private tag generator."""

  @classmethod
  def _get_address(cls, tag_name: str) -> int:
    return int(tag_name, 16)

  @classmethod
  def add_dicom_private_tags(
      cls,
      private_tags: List[DicomPrivateTag],
      dcm: pydicom.dataset.Dataset,
      log_action: bool = True,
  ):
    """Defines a DICOM private tag block with the given tag name and value and adds it to the dataset.

    Args:
      private_tags: Definitions for a DICOM private tags to add.
      dcm: DICOM dataset to add private block to
      log_action: Log tag addition to cloud ops.
    """

    # dicom tag address (32 bits) is 32 composed of two parts (group, element)
    #
    # Extracts group address to define private block which defines the
    # creator of the private tag and extracts low byte of tag to define
    # address of private tag using pydicom API.
    #
    if not private_tags:
      return
    group_address = int(ingest_const.DICOMTagKeywords.GROUP_ADDRESS, 16)
    block = dcm.private_block(
        group_address, ingest_const.PRIVATE_TAG_CREATOR, create=True
    )
    for private_tag in private_tags:
      address = DicomPrivateTagGenerator._get_address(private_tag.address)
      block.add_new(address & 0x000000FF, private_tag.vr, private_tag.value)
    if log_action:
      cloud_logging_client.info(
          'Adding private tag to dataset',
          {'private_dicom_tags': str(private_tags)},
      )

  @classmethod
  def add_dicom_private_tags_to_files(
      cls, private_tags: List[DicomPrivateTag], pathlist: List[str]
  ):
    """Defines a one or more DICOM private tags in a list of dicom files.

    Args:
      private_tags: Definitions for DICOM private tags to add.
      pathlist: List of dicom files to augment.
    """
    if not private_tags:
      return
    for file_name in pathlist:
      dcm = pydicom.dcmread(file_name)
      DicomPrivateTagGenerator.add_dicom_private_tags(
          private_tags, dcm, log_action=False
      )
      dcm.save_as(file_name, write_like_original=True)
      cloud_logging_client.info(
          'Adding private tags to DICOM',
          {
              ingest_const.LogKeywords.FILENAME: file_name,
              'private_dicom_tags': str(private_tags),
          },
      )
