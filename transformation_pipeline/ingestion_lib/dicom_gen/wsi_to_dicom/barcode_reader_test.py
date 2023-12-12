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
"""Tests for barcode_reader."""

import os
from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
import cv2
# cloud.vision not available in third_party/py/google/cloud/
from googleapiclient import discovery as cloud_discovery

from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import barcode_reader


FLAGS = flags.FLAGS


class BarcodeReaderTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    FLAGS.zxing_cli = os.path.join(
        FLAGS.test_srcdir, 'third_party/zxing/zxing_cli'
    )

  def test_file_not_exist(self):
    with self.assertRaisesRegex(ValueError, 'Loading "invalid_path" failed'):
      barcode_reader._zxing_read_barcode('invalid_path')

  def test_barcode_cant_read(self):
    with self.assertRaisesRegex(ValueError, 'decoding failed'):
      barcode_cant_read = gen_test_util.test_file_path('barcode_cant_read.jpg')
      barcode_reader._zxing_read_barcode(barcode_cant_read)

  def test_barcode_try_harder(self):
    barcode_cant_read = gen_test_util.test_file_path('barcode_cant_read.jpg')
    barcode = barcode_reader._zxing_read_barcode(
        barcode_cant_read, try_harder=True
    )
    self.assertEqual(barcode, '4210201129820')

  def test_read_barcode_zxing(self):
    wikipedia_pdf417 = gen_test_util.test_file_path(
        'barcode_wikipedia_pdf417.png'
    )

    self.assertEqual(
        barcode_reader._zxing_read_barcode(wikipedia_pdf417), 'Wikipedia'
    )

  @flagsaver.flagsaver(disable_barcode_decoder=True)
  def test_read_barcode_in_files_disable_barcode_reader(self):
    wikipedia_pdf417 = gen_test_util.test_file_path(
        'barcode_wikipedia_pdf417.png'
    )
    barcode_cant_read = gen_test_util.test_file_path('barcode_cant_read.jpg')
    self.assertEmpty(
        barcode_reader.read_barcode_in_files(
            [barcode_cant_read, wikipedia_pdf417]
        )
    )

  @mock.patch.object(cloud_discovery, 'build', autospec=True, spec_set=True)
  def test_read_barcode_in_files(self, mock_build):
    wikipedia_pdf417 = gen_test_util.test_file_path(
        'barcode_wikipedia_pdf417.png'
    )
    barcode_cant_read = gen_test_util.test_file_path('barcode_cant_read.jpg')
    mock_vision_service = mock.Mock()
    mock_build.return_value.images.return_value.annotate = mock_vision_service
    mock_vision_service.return_value.execute.return_value = {
        'responses': [{'localizedObjectAnnotations': []}]
    }
    result = barcode_reader.read_barcode_in_files(
        [barcode_cant_read, wikipedia_pdf417]
    )
    self.assertListEqual(list(result), ['4210201129820', 'Wikipedia'])

  @mock.patch.object(cloud_discovery, 'build', autospec=True, spec_set=True)
  def test_segmentation(self, mock_build):
    mock_vision_service = mock.Mock()
    mock_build.return_value.images.return_value.annotate = mock_vision_service
    mock_vision_service.return_value.execute.return_value = {
        'responses': [
            {
                'localizedObjectAnnotations': [{
                    'boundingPoly': {
                        'normalizedVertices': [
                            {'x': 0.34089074, 'y': 0.34503052},
                            {'x': 0.6542187, 'y': 0.34503052},
                            {'x': 0.6542187, 'y': 0.85203326},
                            {'x': 0.34089074, 'y': 0.85203326},
                        ]
                    },
                    'mid': '/j/863wbw',
                    'name': '2D barcode',
                    'score': 0.95108104,
                }]
            }
        ]
    }
    datamatrix = gen_test_util.test_file_path('barcode_datamatrix.jpeg')
    expected = gen_test_util.test_file_path('barcode_datamatrix_cropped.jpg')
    output = os.path.join(FLAGS.test_tmpdir, 'cropped.jpg')

    barcode_reader._segment_barcode(datamatrix, output)

    mock_vision_service.assert_called_with(body=mock.ANY)
    actual_image = cv2.imread(output)
    expected_image = cv2.imread(expected)
    self.assertEqual(actual_image.tobytes(), expected_image.tobytes())


if __name__ == '__main__':
  absltest.main()
