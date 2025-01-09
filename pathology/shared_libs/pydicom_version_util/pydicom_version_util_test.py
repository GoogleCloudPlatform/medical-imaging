# Copyright 2024 Google LLC
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
"""Tests for pydicom version util."""
import os

from absl.testing import absltest
import pydicom

from pathology.shared_libs.pydicom_version_util import pydicom_version_util


def _test_file_path(*path: str) -> str:
  return os.path.join(os.path.dirname(__file__), 'testdata', *path)


class PydicomVersionUtilTest(absltest.TestCase):

  def test_pydicom_version_defined(self):
    self.assertGreaterEqual(pydicom_version_util._PYDICOM_MAJOR_VERSION, 2)

  def test_multi_frame_dicom_with_no_offset_table(self):
    path = _test_file_path('multiframe_camelyon_challenge_image.dcm')
    with pydicom.dcmread(path) as dcm:
      self.assertEqual(pydicom_version_util.has_basic_offset_table(dcm), False)
      self.assertEqual(pydicom_version_util.get_frame_offset_table(dcm), [0])
      frame_length = [
          len(f)
          for f in pydicom_version_util.generate_frames(
              dcm.PixelData, dcm.NumberOfFrames
          )
      ]
    self.assertEqual(
        frame_length,
        [
            8418,
            9094,
            7348,
            7710,
            5172,
            11654,
            13040,
            9712,
            14188,
            3492,
            5796,
            4220,
            5184,
            5380,
            2558,
        ],
    )

  def test_multi_frame_dicom_with_offset_table(self):
    path = _test_file_path('test_jpeg_dicom.dcm')
    with pydicom.dcmread(path) as dcm:
      self.assertEqual(pydicom_version_util.has_basic_offset_table(dcm), True)
      self.assertEqual(pydicom_version_util.get_frame_offset_table(dcm), [0])
      frame_length = [
          len(f)
          for f in pydicom_version_util.generate_frames(
              dcm.PixelData, dcm.NumberOfFrames
          )
      ]
    self.assertEqual(frame_length, [63118])

  def test_multi_frame_dicom_with_one_frame_no_offset_table(self):
    path = _test_file_path('test_wikipedia.dcm')
    with pydicom.dcmread(path) as dcm:
      self.assertEqual(pydicom_version_util.has_basic_offset_table(dcm), False)
      self.assertEqual(pydicom_version_util.get_frame_offset_table(dcm), [0])
      frame_length = [
          len(f)
          for f in pydicom_version_util.generate_frames(
              dcm.PixelData, dcm.NumberOfFrames
          )
      ]
    self.assertEqual(frame_length, [11296])


if __name__ == '__main__':
  absltest.main()
