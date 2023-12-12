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
"""Test for Secondary Capture DICOM Builder."""

import base64
import json
from typing import Any, Dict

from absl import flags
from absl import logging
from absl.testing import absltest
import cv2
from hcls_imaging_ml_toolkit import dicom_json
from hcls_imaging_ml_toolkit import tags
import numpy as np
import PIL  # pydicom requires PIL to decode image to pixel_array
import pydicom
from pydicom.dataset import Dataset

from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_private_tag_generator
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_secondary_capture


FLAGS = flags.FLAGS


WSI_INSTANCE_UID = '1.2.3.4'
STUDY_UID = '1.2.3.4.5'
SERIES_UID = '1.2.3.4.5.6'
INSTANCE_UID = '1.2.3.4.5.6.7'
PRIVATE_TAG_ADDRESS = 0x02
PRIVATE_TAG_VALUE = 'private_tag_value'


def MakeImage() -> np.ndarray:
  testimage = np.zeros((10, 10, 3), dtype=np.uint8)
  for i in range(10):
    testimage[i, i, :] = 255
  return testimage


def EncodeBulkData(bulk_data):
  return base64.b64encode(bulk_data.data)


def CreateDicomDataset(dicom_dict, bulk_data):
  del dicom_dict[tags.PIXEL_DATA.number]
  ds = Dataset.from_json(json.dumps(dicom_dict))
  file_meta = pydicom.dataset.FileMetaDataset()
  file_meta.MediaStorageSOPClassUID = ds.SOPClassUID
  file_meta.TransferSyntaxUID = ingest_const.EXPLICIT_VR_LITTLE_ENDIAN
  file_meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID
  file_meta.ImplementationClassUID = ingest_const.SC_IMPLEMENTATION_CLASS_UID
  # end todo
  ds = pydicom.dataset.FileDataset(
      '', ds, file_meta=file_meta, preamble=b'\0' * 128
  )
  ds.is_little_endian = True
  # All DICOM's should be written as is_implicit_VR = False to preserve
  ds.is_implicit_VR = False
  ds.PixelData = EncodeBulkData(bulk_data)
  return ds


class DicomSecondaryCaptureTest(absltest.TestCase):
  """Test for Secondary Capture DICOM Builder."""

  def _AssertJsonTag(
      self, dicomjson: Dict[str, Any], tag: tags.DicomTag, expected: Any
  ) -> None:  # pytype: disable=invalid-annotation
    """Given a DICOM JSON dict asserts whether the tag's value matches the expected value.

    Args:
      dicomjson: Dict representing the DICOM JSON structure.
      tag: A tuple representing a DICOM Tag and its associated value.
      expected: Value that is expected.
    """
    self.assertEqual(dicom_json.GetValue(dicomjson, tag), expected)

  def testSecondaryCaptureGenerationFromJpg(self):
    logging.info('Starting Secondary Capture JPG Test')

    dicom = dicom_secondary_capture.DicomSecondaryCaptureBuilder()
    metadata = {}
    dicom_json.Insert(
        metadata, tags.SECONDARY_CAPTURE_DEVICE_MANUFACTURER, 'test'
    )
    # Open jpeg to get dimensions and encapsulate
    imgfile = gen_test_util.test_file_path('logo.jpg')

    # Use OpenCV to open image and get dimensions and pixel array
    img = cv2.imread(imgfile)
    size = (img.shape[0], img.shape[1])
    imgdata = img.tobytes()
    jpgarray = np.asarray(img)

    # Open as raw binary to build DICOM dataset
    with open(imgfile, 'rb') as testjpg:
      result = dicom.BuildJsonInstanceFromJpg(
          testjpg.read(), size, metadata, STUDY_UID, SERIES_UID, INSTANCE_UID
      )

    ds = CreateDicomDataset(result.dicom_dict, result.bulkdata_list[0])
    # Update PixelData to bytes array
    ds.PixelData = imgdata
    ds.LossyImageCompression = '01'
    ds.LossyImageCompressionMethod = 'ISO_10918_1'

    logging.info(ds)

    # Saves dicom file locally in user/tmp/dicom_secondary_capture_test
    logging.info('Saving Dicom')
    resultfile = FLAGS.test_tmpdir + '/test_sc_jpg.dcm'
    ds.save_as(resultfile, False)

    # Read in dicom and assert matches original jpg pixel array
    ds2 = pydicom.dcmread(resultfile)
    self.assertIsNone(
        np.testing.assert_array_almost_equal(ds2.pixel_array, jpgarray)
    )

    # Verify metadata tags
    self._AssertJsonTag(
        result.dicom_dict, tags.SECONDARY_CAPTURE_DEVICE_MANUFACTURER, 'test'
    )
    self._AssertJsonTag(result.dicom_dict, tags.STUDY_INSTANCE_UID, STUDY_UID)
    self._AssertJsonTag(result.dicom_dict, tags.SERIES_INSTANCE_UID, SERIES_UID)
    self._AssertJsonTag(result.dicom_dict, tags.SOP_INSTANCE_UID, INSTANCE_UID)
    self._AssertJsonTag(result.dicom_dict, tags.BITS_ALLOCATED, 8)
    self._AssertJsonTag(result.dicom_dict, tags.BITS_STORED, 8)
    self._AssertJsonTag(result.dicom_dict, tags.HIGH_BIT, 7)

  def testSecondaryCaptureGenerationFromJpg_as_RAW(self):
    imgfile = gen_test_util.test_file_path('logo.jpg')
    metadata = {}
    dicom_json.Insert(
        metadata, tags.SECONDARY_CAPTURE_DEVICE_MANUFACTURER, 'test'
    )
    private_tags = [
        dicom_private_tag_generator.DicomPrivateTag(
            ingest_const.DICOMTagKeywords.OOF_SCORE_PRIVATE_TAG,
            'LO',
            PRIVATE_TAG_VALUE,
        )
    ]
    instances = [
        dicom_secondary_capture.DicomReferencedInstance(
            ingest_const.WSI_IMPLEMENTATION_CLASS_UID, WSI_INSTANCE_UID
        )
    ]
    ds = dicom_secondary_capture.create_raw_dicom_secondary_capture_from_img(
        imgfile,
        STUDY_UID,
        SERIES_UID,
        INSTANCE_UID,
        dcm_json=metadata,
        instances=instances,
        private_tags=private_tags,
    )

    resultfile = FLAGS.test_tmpdir + '/test_sc_jpg.dcm'
    ds.save_as(resultfile, False)

    ds = pydicom.dcmread(resultfile)
    logging.info(ds)

    # pydicom requires PIL to decode image to pixel_array
    with PIL.Image.open(imgfile) as _:
      pass
    # testing jpg rgb equals, rgb values decode from encapsulated DICOM JPEG
    img_bytes = cv2.imread(imgfile)
    img_bytes = cv2.cvtColor(img_bytes, cv2.COLOR_RGB2BGR)
    self.assertTrue(np.all(np.equal(img_bytes, ds.pixel_array)))
    self.assertEqual(img_bytes.tobytes(), ds.PixelData)
    self.assertEqual(
        ds.get_item(tags.SECONDARY_CAPTURE_DEVICE_MANUFACTURER.number).value,
        'test',
    )
    # verify private tags
    self.assertEqual(
        ds.get_private_item(
            int(ingest_const.DICOMTagKeywords.GROUP_ADDRESS, 16),
            PRIVATE_TAG_ADDRESS,
            ingest_const.PRIVATE_TAG_CREATOR,
        ).value.strip(),
        PRIVATE_TAG_VALUE,
    )
    # verify referenced instances
    ref_series = ds.get_item(tags.REFERENCED_SERIES_SEQUENCE.number).value[0]
    ref_instance = ref_series['ReferencedInstanceSequence'].value[0]
    self.assertEqual(ref_series['SeriesInstanceUID'].value, SERIES_UID)
    self.assertEqual(
        ref_instance['ReferencedSOPClassUID'].value,
        ingest_const.WSI_IMPLEMENTATION_CLASS_UID,
    )
    self.assertEqual(
        ref_instance['ReferencedSOPInstanceUID'].value, WSI_INSTANCE_UID
    )


if __name__ == '__main__':
  absltest.main()
