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
"""Add transformation pipeline info to DICOM general equipment."""

import pydicom
from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_util import pydicom_util


def add_ingest_general_equipment(ds: pydicom.dataset.Dataset):
  """If undefined, adds general equipment tags to identify ingest created DICOM.

  Args:
    ds: pydicom dataset to add tags to.
  """
  pydicom_util.set_dataset_tag_value_if_undefined(
      ds, ingest_const.DICOMTagKeywords.MANUFACTURER, 'GOOGLE'
  )
  pydicom_util.set_dataset_tag_value_if_undefined(
      ds,
      ingest_const.DICOMTagKeywords.MANUFACTURER_MODEL_NAME,
      'DPAS_transformation_pipeline',
  )
  pydicom_util.set_dataset_tag_value_if_undefined(
      ds,
      ingest_const.DICOMTagKeywords.SOFTWARE_VERSIONS,
      cloud_logging_client.logger().build_version[:64],
  )
