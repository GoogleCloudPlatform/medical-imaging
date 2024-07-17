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
"""Tests for dicom_file_ref."""
import os
import typing

from absl import flags
from absl.testing import absltest
import pydicom

from shared_libs.test_utils.dicom_store_mock import dicom_store_mock_types
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_file_ref
from transformation_pipeline.ingestion_lib.dicom_util import dicom_test_util

_FLAGS = flags.FLAGS
_OOF_SCORE = '0.1'


class DicomFileRefTest(absltest.TestCase):

  def test_init_from_json(self):
    json_dict = dicom_test_util.create_metadata_dict()
    fref = dicom_file_ref.init_from_json(
        json_dict, dicom_file_ref.DicomFileRef()
    )

    self.assertEqual(
        fref.get_tag_value(ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID),
        dicom_test_util.STUDY_UID,
    )
    self.assertEqual(
        fref.get_tag_value(ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID),
        dicom_test_util.SERIES_UID,
    )
    self.assertEqual(
        fref.get_tag_value(ingest_const.DICOMTagKeywords.SOP_CLASS_UID),
        dicom_test_util.CLASS_UID,
    )
    self.assertEqual(
        fref.get_tag_value(ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID),
        dicom_test_util.INSTANCE_UID,
    )

  def test_is_abstract_get_dicom_uid_triple_interface(self):
    json_dict = dicom_test_util.create_metadata_dict()
    fref = dicom_file_ref.init_from_json(
        json_dict, dicom_file_ref.DicomFileRef()
    )
    self.assertIsInstance(
        fref, dicom_store_mock_types.AbstractGetDicomUidTripleInterface
    )

  def test_get_dicom_uid_triple_interface(self):
    json_dict = dicom_test_util.create_metadata_dict()
    fref = dicom_file_ref.init_from_json(
        json_dict, dicom_file_ref.DicomFileRef()
    )

    uid_triple = fref.get_dicom_uid_triple()

    self.assertEqual(uid_triple.study_instance_uid, fref.study_instance_uid)
    self.assertEqual(uid_triple.series_instance_uid, fref.series_instance_uid)
    self.assertEqual(uid_triple.sop_instance_uid, fref.sop_instance_uid)

  def test_equals(self):
    json_dict = dicom_test_util.create_metadata_dict()
    fref = dicom_file_ref.init_from_json(
        json_dict, dicom_file_ref.DicomFileRef()
    )
    fref2 = dicom_file_ref.init_from_json(
        json_dict, dicom_file_ref.DicomFileRef()
    )

    self.assertTrue(dicom_file_ref.DicomFileRef.equals(fref, fref2))

  def test_not_equal(self):
    json_dict = dicom_test_util.create_metadata_dict()
    fref = dicom_file_ref.init_from_json(
        json_dict, dicom_file_ref.DicomFileRef()
    )
    fref2 = dicom_file_ref.init_from_json(
        json_dict, dicom_file_ref.DicomFileRef()
    )
    fref.define_tag(ingest_const.DICOMTagKeywords.OOF_SCORE_PRIVATE_TAG)
    fref.set_tag_value(
        ingest_const.DICOMTagKeywords.OOF_SCORE_PRIVATE_TAG, _OOF_SCORE
    )

    self.assertFalse(dicom_file_ref.DicomFileRef.equals(fref, fref2))

  def test_add_private_tag(self):
    json_dict = dicom_test_util.create_metadata_dict()
    fref = dicom_file_ref.init_from_json(
        json_dict, dicom_file_ref.DicomFileRef()
    )
    fref.define_tag(ingest_const.DICOMTagKeywords.OOF_SCORE_PRIVATE_TAG)
    fref.set_tag_value(
        ingest_const.DICOMTagKeywords.OOF_SCORE_PRIVATE_TAG, _OOF_SCORE
    )

    self.assertEqual(
        fref.get_tag_value(ingest_const.DICOMTagKeywords.OOF_SCORE_PRIVATE_TAG),
        _OOF_SCORE,
    )

  def test_init_from_file(self):
    ds = typing.cast(
        pydicom.FileDataset,
        dicom_test_util.create_test_dicom_instance(
            dcm_json=dicom_test_util.create_metadata_dict()
        ),
    )
    ds.save_as(_FLAGS.test_tmpdir + '/test.dcm', write_like_original=False)

    filepath = os.path.join(_FLAGS.test_tmpdir, 'test.dcm')
    fref = dicom_file_ref.init_from_file(
        filepath, dicom_file_ref.DicomFileRef()
    )

    self.assertEqual(
        fref.get_tag_value(ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID),
        dicom_test_util.STUDY_UID,
    )
    self.assertEqual(
        fref.get_tag_value(ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID),
        dicom_test_util.SERIES_UID,
    )
    self.assertEqual(
        fref.get_tag_value(ingest_const.DICOMTagKeywords.SOP_CLASS_UID),
        dicom_test_util.CLASS_UID,
    )
    self.assertEqual(
        fref.get_tag_value(ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID),
        dicom_test_util.INSTANCE_UID,
    )
    self.assertEqual(fref.source, filepath)

  def test_get_dicomfileref_addressses(self):
    self.assertListEqual(
        dicom_file_ref.DicomFileRef().tag_addresses(),
        ['0x0020000D', '0x0020000E', '0x00080016', '0x00080018'],
    )

  def test_get_dicomfileref_equals_test_non_exist_tag(self):
    json_dict = dicom_test_util.create_metadata_dict()
    fref = dicom_file_ref.init_from_json(
        json_dict, dicom_file_ref.DicomFileRef()
    )
    with self.assertRaises(ValueError):
      fref.equals(fref, None, test_tags=['tag does not exist'])

  def test_get_dicomfileref_equals_test_no_tags(self):
    json_dict = dicom_test_util.create_metadata_dict()
    fref = dicom_file_ref.init_from_json(
        json_dict, dicom_file_ref.DicomFileRef()
    )
    with self.assertRaises(ValueError):
      fref.equals(
          fref,
          [
              ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID,
              ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID,
              ingest_const.DICOMTagKeywords.SOP_CLASS_UID,
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID,
          ],
      )

  def test_norm_tag_value_str_no_vr_return_str(self):
    self.assertEqual(dicom_file_ref._norm_tag_value_str('', '2342'), '2342')

  def test_norm_tag_value_str_no_str_return_empty_str(self):
    self.assertEqual(dicom_file_ref._norm_tag_value_str('PN', ''), '')

  def test_norm_tag_value_str_types(self):
    self.assertEqual(dicom_file_ref._norm_tag_value_str('PN', 'Bob'), 'Bob')

  def test_norm_tag_value_float_types(self):
    self.assertEqual(
        dicom_file_ref._norm_tag_value_str('FD', '123'), str(float(123))
    )
    self.assertEqual(
        dicom_file_ref._norm_tag_value_str('FD', '123.0'), str(float(123))
    )
    self.assertEqual(
        dicom_file_ref._norm_tag_value_str('FD', '123.01'), str(float(123.01))
    )
    self.assertEqual(
        dicom_file_ref._norm_tag_value_str('FD', 123.0), str(float(123))
    )

  def test_norm_tag_value_int_types(self):
    self.assertEqual(
        dicom_file_ref._norm_tag_value_str('SL', '123'), str(int(123))
    )
    self.assertEqual(
        dicom_file_ref._norm_tag_value_str('SL', 123), str(int(123))
    )


if __name__ == '__main__':
  absltest.main()
