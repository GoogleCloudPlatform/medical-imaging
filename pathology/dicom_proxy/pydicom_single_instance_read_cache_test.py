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
"""Tests for pydicom single instance read cache."""
import dataclasses
import hashlib
import os

from absl.testing import absltest
from absl.testing import parameterized
import pydicom

from pathology.dicom_proxy import pydicom_single_instance_read_cache
from pathology.dicom_proxy import shared_test_util


_EXPECTED_METADATA = {
    'bits_allocated': 8,
    'bits_stored': 8,
    'columns': 256,
    'high_bit': 7,
    'lossy_compression_method': 'ISO_10918_1',
    'lossy_image_compression': '01',
    'planar_configuration': 0,
    'rows': 256,
    'samples_per_pixel': 3,
    'sop_class_uid': '1.2.840.10008.5.1.4.1.1.77.1.6',
    'total_pixel_matrix_columns': 1152,
    'total_pixel_matrix_rows': 700,
    'number_of_frames': 15,
    'dimension_organization_type': 'TILED_FULL',
    'dicom_transfer_syntax': '1.2.840.10008.1.2.4.50',
    'icc_profile_colorspace': '',
    'icc_profile_bulkdata_uri': '',
    'metadata_source': {
        'store_url': 'localhost',
        'sop_instance_uid': {
            'sop_instance_uid': (
                '1.2.276.0.7230010.3.1.4.296485376.89.1688794081.412405'
            )
        },
        'caching_enabled': False,
    },
}

_FAKE_ICC_PROFILE_BYTES_1 = b'1234'
_FAKE_ICC_PROFILE_BYTES_2 = b'5677'


class PydicomSingleInstanceReadCacheTest(parameterized.TestCase):

  @parameterized.named_parameters([
      dict(
          testcase_name='no_icc_profile',
          ds=pydicom.Dataset().from_json({}),
          expected=False,
      ),
      dict(
          testcase_name='null_icc_profile',
          ds=pydicom.Dataset().from_json({'00282000': {'vr': 'OB'}}),
          expected=False,
      ),
      dict(
          testcase_name='empty_icc_profile',
          ds=pydicom.Dataset().from_json(
              {'00282000': {'InlineBinary': '', 'vr': 'OB'}}
          ),
          expected=False,
      ),
      dict(
          testcase_name='defined_icc_profile',
          ds=pydicom.Dataset().from_json(
              {'00282000': {'InlineBinary': 'MTMzNA==', 'vr': 'OB'}}
          ),
          expected=True,
      ),
  ])
  def test_has_icc_profile_tag(self, ds: pydicom.Dataset, expected: bool):
    self.assertEqual(
        pydicom_single_instance_read_cache._has_icc_profile_tag(ds), expected
    )

  def test_metadata(self):
    test = shared_test_util.jpeg_encoded_pydicom_instance_cache()
    self.assertEqual(dataclasses.asdict(test.metadata), _EXPECTED_METADATA)

  def test_get_frame_returns_expected_data(self):
    test = shared_test_util.jpeg_encoded_pydicom_instance_cache()
    frame_data = test.get_frame(0)

    self.assertLen(frame_data, 8418)
    self.assertEqual(
        hashlib.md5(frame_data).hexdigest(), 'c7c77aab4987844196bf8fad35919463'
    )

  def test_path(self):
    path = shared_test_util.jpeg_encoded_dicom_instance_test_path()
    test = shared_test_util.jpeg_encoded_pydicom_instance_cache()
    self.assertEqual(test.path, path)

  def test_icc_profile(self):
    ds = shared_test_util.jpeg_encoded_dicom_instance()
    ds.ICCProfile = _FAKE_ICC_PROFILE_BYTES_1
    tmpdir = self.create_tempdir()
    path = os.path.join(tmpdir, 'tmp.dcm')
    ds.save_as(path)

    cache = pydicom_single_instance_read_cache.PyDicomSingleInstanceCache(
        pydicom_single_instance_read_cache.PyDicomFilePath(path)
    )

    self.assertEqual(cache.icc_profile, _FAKE_ICC_PROFILE_BYTES_1)

  def test_get_iccprofile_from_pydicom_dataset_root(self):
    ds = shared_test_util.jpeg_encoded_dicom_instance()
    ds.ICCProfile = _FAKE_ICC_PROFILE_BYTES_1
    dicom_icc_profile = (
        pydicom_single_instance_read_cache._get_iccprofile_from_pydicom_dataset(
            ds
        )
    )
    self.assertEqual(dicom_icc_profile.icc_profile, _FAKE_ICC_PROFILE_BYTES_1)
    self.assertEqual(dicom_icc_profile.dicom_tag_path, 'ICCProfile')

  def test_get_iccprofile_from_pydicom_dataset_optical_path_seq(self):
    ds = shared_test_util.jpeg_encoded_dicom_instance()
    ds.ICCProfile = _FAKE_ICC_PROFILE_BYTES_1
    inner_ds = pydicom.Dataset()
    inner_ds.ICCProfile = _FAKE_ICC_PROFILE_BYTES_2
    ds.OpticalPathSequence.append(inner_ds)
    dicom_icc_profile = (
        pydicom_single_instance_read_cache._get_iccprofile_from_local_dataset(
            ds
        )
    )
    self.assertEqual(dicom_icc_profile.icc_profile, _FAKE_ICC_PROFILE_BYTES_2)
    self.assertEqual(
        dicom_icc_profile.dicom_tag_path, 'OpticalPathSequence/1/ICCProfile'
    )

  def test_get_iccprofile_from_local_dataset(self):
    ds = shared_test_util.jpeg_encoded_dicom_instance()
    inner_ds = pydicom.Dataset()
    inner_ds.ICCProfile = _FAKE_ICC_PROFILE_BYTES_1
    ds.OpticalPathSequence = [inner_ds]
    dicom_icc_profile = (
        pydicom_single_instance_read_cache._get_iccprofile_from_pydicom_dataset(
            ds
        )
    )
    self.assertEqual(dicom_icc_profile.icc_profile, _FAKE_ICC_PROFILE_BYTES_1)
    self.assertEqual(
        dicom_icc_profile.dicom_tag_path, 'OpticalPathSequence/0/ICCProfile'
    )

    tmp = self.create_tempdir()
    path = os.path.join(tmp, 'tmp.dcm')
    ds.OpticalPathSequence[0].ICCProfile = _FAKE_ICC_PROFILE_BYTES_2
    ds.save_as(path)
    dicom_icc_profile = (
        pydicom_single_instance_read_cache._get_iccprofile_from_local_dataset(
            path
        )
    )
    self.assertEqual(dicom_icc_profile.icc_profile, _FAKE_ICC_PROFILE_BYTES_2)
    self.assertEqual(
        dicom_icc_profile.dicom_tag_path, 'OpticalPathSequence/0/ICCProfile'
    )


if __name__ == '__main__':
  absltest.main()
