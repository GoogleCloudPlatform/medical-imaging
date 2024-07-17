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
"""Tests for DicomPrivateTagGenerator."""

from absl import flags
from absl import logging
from absl.testing import absltest
import pydicom

from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_private_tag_generator
from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard

FLAGS = flags.FLAGS
TEST_VR = 'LO'
TEST_VALUE = 'test'
TAG_ADDRESS = 0x02


class DicomPrivateTagGeneratorTest(absltest.TestCase):
  """Test for DicomPrivateTagGenerator."""

  def testAddDicomPrivateTagToDataset(self):
    ds = pydicom.Dataset()

    private_tag = dicom_private_tag_generator.DicomPrivateTag(
        ingest_const.DICOMTagKeywords.OOF_SCORE_PRIVATE_TAG, TEST_VR, TEST_VALUE
    )
    dicom_private_tag_generator.DicomPrivateTagGenerator.add_dicom_private_tags(
        [private_tag], ds
    )
    logging.info(ds)

    # Verify private tag matches
    result_value = ds.get_private_item(
        int(ingest_const.DICOMTagKeywords.GROUP_ADDRESS, 16),
        TAG_ADDRESS,
        ingest_const.PRIVATE_TAG_CREATOR,
    ).value
    self.assertEqual(result_value, TEST_VALUE)

  def testAddDicomPrivateTagToFile(self):
    file_meta = pydicom.dataset.FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = (
        dicom_standard.dicom_standard_util().get_sop_classname_uid(
            'Secondary Capture Image Storage'
        )
    )
    file_meta.MediaStorageSOPInstanceUID = '1.2.3.4'
    base_ds = pydicom.Dataset()
    ds = pydicom.dataset.FileDataset(
        '', base_ds, file_meta=file_meta, preamble=b'\0' * 128
    )

    file_name = FLAGS.test_tmpdir + '/test_private_tag.dcm'
    ds.save_as(file_name)

    private_tag = dicom_private_tag_generator.DicomPrivateTag(
        ingest_const.DICOMTagKeywords.OOF_SCORE_PRIVATE_TAG, TEST_VR, TEST_VALUE
    )
    (
        dicom_private_tag_generator.DicomPrivateTagGenerator.add_dicom_private_tags_to_files(
            [private_tag], [file_name]
        )
    )

    # Read dicom and verify private tag matches
    ds2 = pydicom.dcmread(file_name)
    logging.info(ds2)

    # VR gets encoded as UN, so need to decode to string
    result_value = ds2.get_private_item(
        int(ingest_const.DICOMTagKeywords.GROUP_ADDRESS, 16),
        TAG_ADDRESS,
        ingest_const.PRIVATE_TAG_CREATOR,
    ).value.decode('UTF-8')
    self.assertEqual(result_value, TEST_VALUE)


if __name__ == '__main__':
  absltest.main()
