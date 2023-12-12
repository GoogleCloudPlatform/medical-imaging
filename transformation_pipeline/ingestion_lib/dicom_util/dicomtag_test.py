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
"""Tests for dicomtag."""

from absl.testing import absltest
from transformation_pipeline.ingestion_lib.dicom_util import dicomtag

DicomTag = dicomtag.DicomTag


class DicomTagTest(absltest.TestCase):

  def test_dicom_tag_decodes_address_and_number_decode_to_same(self):
    global_tags = DicomTag.defined_tag_dict()
    for key in list(DicomTag.defined_tag_dict()):
      del global_tags[key]
    tag1 = DicomTag('0x00100020', 'PatientID')
    self.assertEqual(tag1.address, tag1.number)
    self.assertLen(DicomTag.defined_tag_dict(), 1)

  def test_dicom_tag_decodes_address_permutations(self):
    global_tags = DicomTag.defined_tag_dict()
    for key in list(DicomTag.defined_tag_dict()):
      del global_tags[key]
    tag1 = DicomTag('0x00100020', 'PatientID')
    tag2 = DicomTag('(0010, 0020)', 'PatientID')
    tag3 = DicomTag('(0x0010, 0x0020)', 'PatientID')
    tag4 = DicomTag('00100020', 'PatientID')
    tag5 = DicomTag('00100010', 'PatientName')
    tag6 = DicomTag('100010', 'PatientName')
    self.assertEqual(tag1.address, '00100020')
    self.assertEqual(tag1.address, tag2.address)
    self.assertEqual(tag1.address, tag3.address)
    self.assertEqual(tag1.address, tag4.address)
    self.assertNotEqual(tag1.address, tag5.address)
    self.assertEqual(tag5.address, tag6.address)
    self.assertLen(DicomTag.defined_tag_dict(), 2)

  def test_dicom_tag_keyword(self):
    tag1 = DicomTag('0x00100020', 'PatientID')
    tag5 = DicomTag('00100010', 'PatientName')
    self.assertEqual(tag1.keyword, 'PatientID')
    self.assertEqual(tag5.keyword, 'PatientName')

  def test_dicom_tag_decodes_vr(self):
    tag1 = DicomTag('0x00100020', 'PatientID', 'LO')
    tag5 = DicomTag('00100010', 'PatientName', 'PN')
    self.assertEqual(tag1.vr, 'LO')
    self.assertEqual(tag5.vr, 'PN')

  def test_dicom_tag_raises_error_invalid_vr(self):
    with self.assertRaises(ValueError):
      DicomTag('0x00100020', 'PatientID', 'PN')

  def test_dicom_tag_raises_error_invalid_keyword(self):
    with self.assertRaises(ValueError):
      DicomTag('0x00100020', 'PatientName', 'LO')
    with self.assertRaises(ValueError):
      DicomTag('0x00100020', 'PatientName')

  def test_dicom_tag_raises_error_invalid_address(self):
    with self.assertRaises(ValueError):
      DicomTag('00100010', 'PatientID', 'LO')
    with self.assertRaises(ValueError):
      DicomTag('00100010', 'PatientID')

  def test_dicom_tag_raises_error_malformed_address_too_long(self):
    with self.assertRaises(ValueError):
      DicomTag('000100020', 'PatientID', 'LO')

  def test_dicom_tag_raises_error_malformed_address_too_short(self):
    with self.assertRaises(ValueError):
      DicomTag('01020', 'PatientID')

  def test_dicom_tag_raises_error_malformed_address_invalid(self):
    with self.assertRaises(ValueError):
      DicomTag('F0100020', 'PatientID')

  def test_dicom_tag_address_verified_access(self):
    global_tags = DicomTag.defined_tag_dict()
    for key in list(DicomTag.defined_tag_dict()):
      del global_tags[key]
    self.assertEqual(
        DicomTag.tag_address('0x00100020', 'PatientID'), '00100020'
    )
    self.assertEqual(
        DicomTag.tag_address('0x00100020', 'PatientID', 'LO'), '00100020'
    )
    with self.assertRaises(ValueError):
      DicomTag.tag_address('0x00100010', 'PatientID')
    with self.assertRaises(ValueError):
      DicomTag.tag_address('0x00100020', 'PatientID', 'PN')
    self.assertEmpty(DicomTag.defined_tag_dict())

  def test_dicom_tag_keyword_verified_access(self):
    global_tags = DicomTag.defined_tag_dict()
    for key in list(DicomTag.defined_tag_dict()):
      del global_tags[key]
    self.assertEqual(
        DicomTag.tag_keyword('0x00100020', 'PatientID'), 'PatientID'
    )
    self.assertEqual(
        DicomTag.tag_keyword('0x00100020', 'PatientID', 'LO'), 'PatientID'
    )
    with self.assertRaises(ValueError):
      DicomTag.tag_keyword('0x00100010', 'PatientID')
    with self.assertRaises(ValueError):
      DicomTag.tag_keyword('0x00100020', 'PatientID', 'PN')
    self.assertEmpty(DicomTag.defined_tag_dict())

  def test_dicom_vr_verified_access(self):
    global_tags = DicomTag.defined_tag_dict()
    for key in list(DicomTag.defined_tag_dict()):
      del global_tags[key]
    self.assertEqual(DicomTag.tag_vr('0x00100020', 'PatientID'), 'LO')
    self.assertEqual(DicomTag.tag_vr('0x00100020', 'PatientID', 'LO'), 'LO')
    with self.assertRaises(ValueError):
      DicomTag.tag_vr('0x00100010', 'PatientID')
    with self.assertRaises(ValueError):
      DicomTag.tag_vr('0x00100020', 'PatientID', 'PN')
    self.assertEmpty(DicomTag.defined_tag_dict())


if __name__ == '__main__':
  absltest.main()
