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
"""Demonstrates using DICOM Standard Utils to Print out IOD Tables and Tags."""
import sys
from typing import List

from absl import flags

from transformation_pipeline.ingestion_lib.dicom_util import dicom_iod_util
from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard


DICOM_IOD_NAME_FLG = flags.DEFINE_string(
    'dicom_iod',
    'Microscopy Bulk Simple Annotations Storage',
    'Name of DICOM IOD to print.',
)


def _print_iod(
    dcm_util: dicom_iod_util.DicomIODDatasetUtil, path: List[str]
) -> None:
  """Recursive function to print out IOD, will not work for all IOD.

  Args:
    dcm_util: Loaded DICOM IOD DatasetUtil.
    path: List of DICOM tags defining SQ tag path to DICOM dataset.

  Returns:
    None.
  """
  dataset = dcm_util.get_iod_dicom_dataset(DICOM_IOD_NAME_FLG.value, path)
  if dataset is None:
    return
  prefix = '  ' * len(path)
  for tag_address, tag in dataset.items():
    if tag.retired:
      continue
    if not tag.is_sq():
      print(f'{prefix}{tag}')
      continue
    if tag_address in path:
      # Cyclic graph detected in IOD.
      tag = dicom_standard.dicom_standard_util().get_tag(tag_address)
      print(
          f'{prefix}{tag} references {tag.keyword} [Cyclic graph detected in'
          ' IOD sequence]'
      )
      continue
    print(f'{prefix}{tag}')
    _print_iod(dcm_util, path + [tag_address])


if __name__ == '__main__':
  # Parse commandline arguments.
  flags.FLAGS(sys.argv)
  iod_util = dicom_iod_util.DicomIODDatasetUtil()
  print(f'IOD {DICOM_IOD_NAME_FLG.value}\n')
  _print_iod(iod_util, [])
