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
"""Tests dicom_json_util."""
import json
from typing import Any, Dict

from absl.testing import absltest
from absl.testing import parameterized
import pydicom

from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_json_util
from transformation_pipeline.ingestion_lib.dicom_util import dicom_test_util


class DICOMJSONUtilTest(parameterized.TestCase):
  """Tests dicom_json_util."""

  def _validate_pydicom_matches_json(
      self, ds: pydicom.dataset.Dataset, ds_json: Dict[str, Any]
  ):
    """Recursively validates pydicom dataset matches DICOM formatted JSON.

    Args:
      ds: pydicom Dataset.
      ds_json: JSON formatted DICOM.
    """
    for tag_address, element in ds_json.items():
      ds_tag = ds[tag_address]
      self.assertEqual(ds_tag.VR, element[ingest_const.VR])
      if element[ingest_const.VR] == ingest_const.DICOMVRCodes.SQ:
        for index, sub_element in enumerate(element[ingest_const.VALUE]):
          self._validate_pydicom_matches_json(ds_tag[index], sub_element)
      elif element[ingest_const.VR] == ingest_const.DICOMVRCodes.PN:
        # In DICOM patient names can be described with alphabetic
        # ideographic and phonetic representations.
        # go/dicomspec/part05.html#sect_6.2.1.1
        # These representations are deliminated via = characters
        # Alphabetic=ideographic=phonetic
        # Within the alphabetic representation the ^ are used to
        # deliminate parts of a name.
        # Pydicom representation of a tag's value implements this spec.
        # As a result setting the pydicom tags.value for a tag with only
        # an alphabetic definition will == the alphabetic definition.
        #
        # json formatted dicom has an alternative representation for PN.
        # http://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_F.2.2.html
        # In the json format the PN value list contains a dictionary containing
        # one or more keys that describe the alphatbetic, ideographic, and/or
        # Phonetic components of the name
        # {'Value': [{'Alphabetic': ,  'Ideographic':  ,  'Phonetic': }]}
        self.assertEqual(
            ds_tag.value, element[ingest_const.VALUE][0]['Alphabetic']
        )
      else:
        self.assertEqual(ds_tag.value, element[ingest_const.VALUE][0])

  def test_merge_json_metadata_with_pydicom_ds_empty_ds(self):
    dcm = dicom_test_util.create_test_dicom_instance()
    merged_dicom = dicom_json_util.merge_json_metadata_with_pydicom_ds(dcm, {})
    self.assertEqual(merged_dicom, dicom_test_util.create_test_dicom_instance())

  def test_merge_json_metadata_with_pydicom_ds_overrides_tags(self):
    dcm = dicom_test_util.create_test_dicom_instance(
        study='1', series='2', sop_instance_uid='3'
    )
    dcm2 = pydicom.dataset.Dataset()
    dcm2.StudyInstanceUID = '4'
    dcm2.SeriesInstanceUID = '5'
    dcm2.SOPInstanceUID = '6'
    merged_dcm = dicom_json_util.merge_json_metadata_with_pydicom_ds(
        dcm, json.loads(dcm2.to_json())
    )
    self.assertEqual(merged_dcm.StudyInstanceUID, '4')
    self.assertEqual(merged_dcm.SeriesInstanceUID, '5')
    self.assertEqual(merged_dcm.SOPInstanceUID, '6')

  def test_merge_json_metadata_with_pydicom_ds_skipping_tags(self):
    dcm = dicom_test_util.create_test_dicom_instance(
        study='1', series='2', sop_instance_uid='3'
    )
    dcm2 = pydicom.dataset.Dataset()
    dcm2.StudyInstanceUID = '4'
    dcm2.SeriesInstanceUID = '5'
    dcm2.SOPInstanceUID = '6'
    merged_dcm = dicom_json_util.merge_json_metadata_with_pydicom_ds(
        dcm,
        json.loads(dcm2.to_json()),
        tags_to_skip=dicom_json_util.UID_TRIPLE_TAGS,
    )
    self.assertEqual(merged_dcm.StudyInstanceUID, '1')
    self.assertEqual(merged_dcm.SeriesInstanceUID, '2')
    self.assertEqual(merged_dcm.SOPInstanceUID, '3')

  def test_merge_json_metadata_with_pydicom_ds(self):
    ds_json_path = gen_test_util.test_file_path('expected_svs_metadata.json')
    with open(ds_json_path, 'rt') as infile:
      ds_json = json.load(infile)
    ds = pydicom.dataset.Dataset()
    ds.SeriesInstanceUID = '1234'
    dicom_json_util.merge_json_metadata_with_pydicom_ds(ds, ds_json)
    self._validate_pydicom_matches_json(ds, ds_json)
    # make sure pre-existing values are maintained following merge.
    self.assertEqual(ds.SeriesInstanceUID, '1234')

    # round trip the pydicom object back to json.  Validate that
    # json for imported fields matches the expected json
    pydicom_json = json.loads(ds.to_json())
    for key, original_value in ds_json.items():
      self.assertDictEqual(pydicom_json[key], original_value)

  def test_get_dicom_tag_key_val_present(self):
    json_metadata = {
        ingest_const.DICOMTagAddress.PATIENT_NAME: {
            ingest_const.VALUE: ['test'],
        }
    }
    self.assertEqual(
        dicom_json_util._get_dicom_tag_key_val(
            json_metadata, ingest_const.DICOMTagAddress.PATIENT_NAME
        ),
        'test',
    )

  def test_get_dicom_tag_key_val_missing_tag(self):
    json_metadata = {}
    self.assertIsNone(
        dicom_json_util._get_dicom_tag_key_val(
            json_metadata, ingest_const.DICOMTagAddress.PATIENT_NAME
        )
    )

  def test_get_study_instance_uid_from_metadata_present(self):
    test_uid = '1.9.3'
    json_tst = {
        ingest_const.DICOMTagAddress.STUDY_INSTANCE_UID: {
            ingest_const.VALUE: [test_uid]
        }
    }
    val = dicom_json_util.get_study_instance_uid(json_tst)
    self.assertEqual(val, test_uid)

  def test_get_study_instance_uid_from_metadata_missing(self):
    json_tst = {
        ingest_const.DICOMTagAddress.SERIES_DESCRIPTION: {
            ingest_const.VALUE: ['foo']
        }
    }
    with self.assertRaises(dicom_json_util.MissingStudyUIDInMetadataError):
      dicom_json_util.get_study_instance_uid(json_tst)

  def test_get_series_instance_uid_from_metadata_present(self):
    test_uid = '1.9.3'
    json_tst = {
        ingest_const.DICOMTagAddress.SERIES_INSTANCE_UID: {
            ingest_const.VALUE: [test_uid]
        }
    }
    val = dicom_json_util.get_series_instance_uid(json_tst)
    self.assertEqual(val, test_uid)

  def test_get_series_instance_uid_from_metadata_missing(self):
    json_tst = {
        ingest_const.DICOMTagAddress.SERIES_DESCRIPTION: {
            ingest_const.VALUE: ['foo']
        }
    }
    with self.assertRaises(dicom_json_util.MissingSeriesUIDInMetadataError):
      dicom_json_util.get_series_instance_uid(json_tst)

  def test_get_accession_number_from_metadata_present(self):
    test_uid = '1.9.3'
    json_tst = {
        ingest_const.DICOMTagAddress.ACCESSION_NUMBER: {
            ingest_const.VALUE: [test_uid]
        }
    }
    val = dicom_json_util.get_accession_number(json_tst)
    self.assertEqual(val, test_uid)

  def test_get_accession_number_from_metadata_missing(self):
    json_tst = {
        ingest_const.DICOMTagAddress.SERIES_DESCRIPTION: {
            ingest_const.VALUE: ['foo']
        }
    }
    with self.assertRaises(dicom_json_util.MissingAccessionNumberMetadataError):
      dicom_json_util.get_accession_number(json_tst)

  def test_get_patient_id_from_metadata_present(self):
    test_uid = '1.9.3'
    json_tst = {
        ingest_const.DICOMTagAddress.PATIENT_ID: {
            ingest_const.VALUE: [test_uid]
        }
    }
    val = dicom_json_util.get_patient_id(json_tst)
    self.assertEqual(val, test_uid)

  def test_get_patient_id_from_metadata_missing(self):
    json_tst = {
        ingest_const.DICOMTagAddress.SERIES_DESCRIPTION: {
            ingest_const.VALUE: ['foo']
        }
    }
    with self.assertRaises(dicom_json_util.MissingPatientIDMetadataError):
      dicom_json_util.get_patient_id(json_tst)

  def test_missing_patient_id_from_metadata_false(self):
    self.assertFalse(
        dicom_json_util.missing_patient_id({
            ingest_const.DICOMTagAddress.PATIENT_ID: {
                ingest_const.VALUE: ['1.9.3']
            }
        })
    )

  def test_missing_patient_id_from_metadata_true(self):
    self.assertTrue(dicom_json_util.missing_patient_id({}))

  @parameterized.named_parameters([
      dict(testcase_name='empty', metadata={}),
      dict(
          testcase_name='prexisting',
          metadata={'0020000D': {'Value': ['6.5.4'], 'vr': 'UI'}},
      ),
  ])
  def test_set_study_instance_uid_in_metadata(self, metadata):
    dicom_json_util.set_study_instance_uid_in_metadata(metadata, '1.2.3')
    self.assertEqual(metadata['0020000D'], {'Value': ['1.2.3'], 'vr': 'UI'})
    self.assertLen(metadata, 1)

  @parameterized.named_parameters([
      dict(testcase_name='empty', metadata={}),
      dict(
          testcase_name='prexisting',
          metadata={'0020000E': {'Value': ['6.5.4'], 'vr': 'UI'}},
      ),
  ])
  def test_set_series_instance_uid_in_metadata(self, metadata):
    dicom_json_util.set_series_instance_uid_in_metadata(metadata, '1.2.3')
    self.assertEqual(metadata['0020000E'], {'Value': ['1.2.3'], 'vr': 'UI'})
    self.assertLen(metadata, 1)

  @parameterized.named_parameters([
      dict(testcase_name='empty', metadata={}),
      dict(
          testcase_name='prexisting',
          metadata={'00080018': {'Value': ['6.5.4'], 'vr': 'UI'}},
      ),
  ])
  def remove_sop_instance_uid_from_metadata(self, metadata):
    dicom_json_util.remove_sop_instance_uid_from_metadata(metadata)
    self.assertNotIn('00080018', metadata)
    self.assertEmpty(metadata)

  @parameterized.named_parameters([
      dict(testcase_name='empty', metadata={}),
      dict(
          testcase_name='prexisting',
          metadata={'00080016': {'Value': ['6.5.4'], 'vr': 'UI'}},
      ),
  ])
  def test_remove_sop_class_uid_from_metadata(self, metadata):
    dicom_json_util.remove_sop_class_uid_from_metadata(metadata)
    self.assertNotIn('00080016', metadata)
    self.assertEmpty(metadata)


if __name__ == '__main__':
  absltest.main()
