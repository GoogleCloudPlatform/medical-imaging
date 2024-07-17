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
"""Tests dicom_schema_util."""
import json
import os
from typing import Any, Dict, List, Mapping, Optional

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized

from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import csv_util
from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_schema_util

FLAGS = flags.FLAGS


class SchemaDICOMTagTest(parameterized.TestCase):
  """Unit tests for SchemaDICOMTag."""

  @parameterized.named_parameters([
      dict(
          testcase_name='ss_dicom_vr_code',
          address='00189219',
          vr_type='SS',
          value=42,
          keyword='TagAngleSecondAxis',
      ),
      dict(
          testcase_name='sl_dicom_vr_code',
          address='0040A162',
          vr_type='SL',
          value=420,
          keyword='RationalNumeratorValue',
      ),
      dict(
          testcase_name='us_dicom_vr_code',
          address='0040A0B0',
          vr_type='US',
          value=39,
          keyword='ReferencedWaveformChannels',
      ),
      dict(
          testcase_name='ul_dicom_vr_code',
          address='0040A132',
          vr_type='UL',
          value=37,
          keyword='ReferencedSamplePositions',
      ),
      dict(
          testcase_name='fl_dicom_vr_code',
          address='00460117',
          vr_type='FL',
          value=42.01,
          keyword='RefractiveIndexOfCornea',
      ),
      dict(
          testcase_name='fd_dicom_vr_code',
          address='00460062',
          vr_type='FD',
          value=3.14159,
          keyword='NearPupillaryDistance',
      ),
  ])
  def test_dicom_schema_tag_tag_value_to_json(
      self, address: str, vr_type: str, value: Any, keyword: str
  ):
    tag_schema = {'Keyword': keyword, 'Meta': 'csv_column_name'}
    table_req = set(['2'])
    dicom_ds = None
    tag = dicom_schema_util.SchemaDICOMTag(
        address, set([vr_type]), tag_schema, table_req, dicom_ds
    )
    tag.value = str(value)
    json_dict = {}
    tag.add_tag_to_json(json_dict)
    expected = {address: {'vr': vr_type, 'Value': [value]}}
    self.assertEqual(json_dict, expected)


class DICOMSchemaUtilTest(parameterized.TestCase):
  """Tests the generation of DICOM metadata from csv and DICOM mapping schema."""

  @parameterized.named_parameters([
      dict(
          testcase_name='uncropped_tag_none',
          tag_len=63,
          expected_len=63,
          flg=ingest_flags.MetadataTagLengthValidation.NONE,
      ),
      dict(
          testcase_name='uncropped_tag_warning',
          tag_len=63,
          expected_len=63,
          flg=ingest_flags.MetadataTagLengthValidation.LOG_WARNING,
      ),
      dict(
          testcase_name='uncropped_tag_log_clip',
          tag_len=63,
          expected_len=63,
          flg=ingest_flags.MetadataTagLengthValidation.LOG_WARNING_AND_CLIP,
      ),
      dict(
          testcase_name='uncropped_tag_error',
          tag_len=63,
          expected_len=63,
          flg=ingest_flags.MetadataTagLengthValidation.ERROR,
      ),
      dict(
          testcase_name='cropped_tag_none',
          tag_len=65,
          expected_len=65,
          flg=ingest_flags.MetadataTagLengthValidation.NONE,
      ),
      dict(
          testcase_name='cropped_tag_log_warning',
          tag_len=65,
          expected_len=65,
          flg=ingest_flags.MetadataTagLengthValidation.LOG_WARNING,
      ),
      dict(
          testcase_name='cropped_tag_log_warn_and_clip',
          tag_len=65,
          expected_len=64,
          flg=ingest_flags.MetadataTagLengthValidation.LOG_WARNING_AND_CLIP,
      ),
  ])
  def test_dicom_schema_tag_value_setter_crops_long_tag_values(
      self, tag_len, expected_len, flg
  ):
    schema_dict = {}
    schema_dict['0x00100010'] = {
        'Keyword': 'PatientName',
        'Meta': 'Patient Name',
    }
    schema = dicom_schema_util.SchemaDicomDatasetBlock(
        schema_dict, 'VL Whole Slide Microscopy Image Storage'
    )
    tag = schema.tags[0]
    with flagsaver.flagsaver(metadata_tag_length_validation=flg):
      tag.value = 'b' * tag_len
    self.assertEqual(tag.value, 'b' * expected_len)

  @parameterized.parameters([
      ingest_flags.MetadataTagLengthValidation.NONE,
      ingest_flags.MetadataTagLengthValidation.LOG_WARNING,
      ingest_flags.MetadataTagLengthValidation.LOG_WARNING_AND_CLIP,
      ingest_flags.MetadataTagLengthValidation.ERROR,
  ])
  def test_dicom_schema_tag_value_setter_does_not_crop_tag_with_undef_len(
      self, flg
  ):
    schema_dict = {}
    schema_dict['0x0016002B'] = {
        'Keyword': 'MakerNote',
        'Meta': 'MakerNote',
    }
    schema = dicom_schema_util.SchemaDicomDatasetBlock(
        schema_dict, 'VL Photographic Image Storage'
    )
    tag = schema.tags[0]
    test_value = 'b' * 65
    with flagsaver.flagsaver(metadata_tag_length_validation=flg):
      tag.value = test_value
    self.assertEqual(tag.value, test_value)

  @flagsaver.flagsaver(
      metadata_tag_length_validation=ingest_flags.MetadataTagLengthValidation.ERROR
  )
  def test_dicom_schema_tag_value_setter_raises_error_if_long_value_and_flg(
      self,
  ):
    schema_dict = {}
    schema_dict['0x00100010'] = {
        'Keyword': 'PatientName',
        'Meta': 'Patient Name',
    }
    schema = dicom_schema_util.SchemaDicomDatasetBlock(
        schema_dict, 'VL Whole Slide Microscopy Image Storage'
    )
    tag = schema.tags[0]
    with self.assertRaises(dicom_schema_util.DICOMSchemaTagError):
      tag.value = 'b' * 65

  @parameterized.parameters(
      ['1.2.3', '1.0.2', '1.234.564', '1', f'1.{"1"*62}', '']
  )
  @flagsaver.flagsaver(
      metadata_uid_validation=ingest_flags.MetadataUidValidation.ERROR,
      metadata_tag_length_validation=ingest_flags.MetadataTagLengthValidation.LOG_WARNING_AND_CLIP,
  )
  def test_dicom_schema_tag_value_setter_test_vr_formatting_error_succeeds(
      self, test_value
  ):
    schema_dict = {}
    schema_dict['0x0020000D'] = {
        'Keyword': 'StudyInstanceUID',
        'Meta': 'StudyInstanceUID',
    }
    schema = dicom_schema_util.SchemaDicomDatasetBlock(
        schema_dict, 'VL Whole Slide Microscopy Image Storage'
    )
    tag = schema.tags[0]
    tag.value = test_value
    self.assertEqual(tag.value, test_value)

  @parameterized.parameters([
      '1.2.3',
      '1.0.2',
      '1.234.564',
      '1',
      f'1.{"1"*62}',
      '',
      f'1.{"1"*63}',
      '1.01',
      'abc',
      '1..1',
      '1.2.3.A',
  ])
  @flagsaver.flagsaver(
      metadata_uid_validation=ingest_flags.MetadataUidValidation.LOG_WARNING,
      metadata_tag_length_validation=ingest_flags.MetadataTagLengthValidation.NONE,
  )
  def test_dicom_schema_tag_value_setter_test_vr_formatting_warning_succeeds(
      self, test_value
  ):
    schema_dict = {}
    schema_dict['0x0020000D'] = {
        'Keyword': 'StudyInstanceUID',
        'Meta': 'StudyInstanceUID',
    }
    schema = dicom_schema_util.SchemaDicomDatasetBlock(
        schema_dict, 'VL Whole Slide Microscopy Image Storage'
    )
    tag = schema.tags[0]
    tag.value = test_value
    self.assertEqual(tag.value, test_value)

  @parameterized.parameters([
      '1.2.3',
      '1.0.2',
      '1.234.564',
      '1',
      f'1.{"1"*62}',
      '',
      f'1.{"1"*63}',
      '1.01',
      'abc',
      '1..1',
      '1.2.3.A',
  ])
  @flagsaver.flagsaver(
      metadata_uid_validation=ingest_flags.MetadataUidValidation.NONE,
      metadata_tag_length_validation=ingest_flags.MetadataTagLengthValidation.NONE,
  )
  def test_dicom_schema_tag_value_setter_test_vr_formatting_none_succeeds(
      self, test_value
  ):
    schema_dict = {}
    schema_dict['0x0020000D'] = {
        'Keyword': 'StudyInstanceUID',
        'Meta': 'StudyInstanceUID',
    }
    schema = dicom_schema_util.SchemaDicomDatasetBlock(
        schema_dict, 'VL Whole Slide Microscopy Image Storage'
    )
    tag = schema.tags[0]
    tag.value = test_value
    self.assertEqual(tag.value, test_value)

  @parameterized.parameters([f'1.{"1"*63}', '1.01', 'abc', '1..1', '1.2.3.A'])
  @flagsaver.flagsaver(
      metadata_uid_validation=ingest_flags.MetadataUidValidation.ERROR,
      metadata_tag_length_validation=ingest_flags.MetadataTagLengthValidation.LOG_WARNING_AND_CLIP,
  )
  def test_dicom_schema_tag_value_setter_test_vr_formatting_raises_invalid_uid(
      self, test_value
  ):
    schema_dict = {}
    schema_dict['0x0020000D'] = {
        'Keyword': 'StudyInstanceUID',
        'Meta': 'StudyInstanceUID',
    }
    schema = dicom_schema_util.SchemaDicomDatasetBlock(
        schema_dict, 'VL Whole Slide Microscopy Image Storage'
    )
    tag = schema.tags[0]
    with self.assertRaises(dicom_schema_util.DICOMSchemaTagError):
      tag.value = test_value

  def test_dicom_wsi_metadata_schema_gen(self):
    """Tests generation of example wsi dicom json metadata from schema."""

    barcode = 'CR-21-88-A-2'

    wsi_json_filename = gen_test_util.test_file_path('example_schema_wsi.json')
    metadata_filename = gen_test_util.test_file_path('metadata.csv')

    with open(wsi_json_filename, 'rt') as infile:
      schemadef = infile.read()
    schemadef = json.loads(schemadef)
    del schemadef['DICOMSchemaDef']
    csv_meta_data = csv_util.read_csv(metadata_filename)
    csv_metadata_row = csv_meta_data.loc[
        csv_meta_data['Bar Code Value'] == barcode
    ]
    metadata = dicom_schema_util.PandasMetadataTableWrapper(csv_metadata_row)
    ds_json = dicom_schema_util.get_json_dicom(
        'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
    )

    expected_json = gen_test_util.test_file_path('expected_svs_metadata.json')
    with open(expected_json, 'rt') as infile:
      expected_json = json.load(infile)

    # Assert the generated dicom matches the expectation
    self.assertEqual(ds_json, expected_json)

  def _get_first_csv_row(
      self,
      csv_text: str,
  ) -> dicom_schema_util.PandasMetadataTableWrapper:
    """Test utility.

    Write csv text to  file and return first row of data.

    Args:
      csv_text: text to write to file.

    Returns:
      First data row of CSV wrapped in PandasMetadataTableWrapper
    """
    path = os.path.join(self.create_tempdir(), 'test.csv')
    with open(path, 'wt') as outfile:
      outfile.write(csv_text)
    csv_meta_data = csv_util.read_csv(path)
    return dicom_schema_util.PandasMetadataTableWrapper(csv_meta_data.head(1))

  @parameterized.named_parameters([
      dict(
          testcase_name='basic_string',
          csv='Col 1, Col 2\n Bob, 12345',
          expected_name='Bob',
          expected_value='12345',
      ),
      dict(
          testcase_name='quote_and_escaped_string',
          csv='Col 1, Col 2\n "Bob,,,Smith", "\\"\\""',
          expected_name='Bob,,,Smith',
          expected_value='""',
      ),
      dict(
          testcase_name='escaped_comma',
          csv='Col 1, Col 2\n Bob\\,Smith, 12345',
          expected_name='Bob,Smith',
          expected_value='12345',
      ),
      dict(
          testcase_name='excess_csv_column',
          csv='Col 1, Col2, Col3\n Bob, 1345, Smith',
          expected_name='Bob',
          expected_value='1345',
      ),
      dict(
          testcase_name='auto_clipping_long_strings_based_on_VR_type',
          csv=f'Col 1, Col2, Col3\n Bob{"*"*100}, 1345',
          expected_name=f'Bob{"*"*61}',
          expected_value='1345',
      ),
  ])
  @flagsaver.flagsaver(
      metadata_tag_length_validation=ingest_flags.MetadataTagLengthValidation.LOG_WARNING_AND_CLIP
  )
  def test_simple_string_schema(
      self, csv: str, expected_name: str, expected_value: Optional[str]
  ):
    schemadef = {
        '0x00100010': {'Keyword': 'PatientName', 'Meta': 'Col 1'},
        '0x00100020': {'Keyword': 'PatientID', 'Meta': 'Col 2'},
    }
    metadata = self._get_first_csv_row(csv)
    ds_json = dicom_schema_util.get_json_dicom(
        'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
    )
    self.assertLen(ds_json, 2)
    self.assertEqual(
        ds_json['00100010']['Value'], [{'Alphabetic': expected_name}]
    )
    self.assertEqual(ds_json['00100020']['Value'], [expected_value])

  def test_missing_by_default_missing_metadata_does_not_include_value(self):
    csv = 'Col 1\n Bob'
    expected_name = 'Bob'
    schemadef = {
        '0x00100010': {'Keyword': 'PatientName', 'Meta': 'Col 1'},
        '0x00100020': {'Keyword': 'PatientID', 'Meta': 'Col 2'},
    }
    metadata = self._get_first_csv_row(csv)
    ds_json = dicom_schema_util.get_json_dicom(
        'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
    )
    self.assertLen(ds_json, 2)
    self.assertEqual(
        ds_json['00100010']['Value'], [{'Alphabetic': expected_name}]
    )
    self.assertNotIn('Value', ds_json['00100020'])

  @parameterized.named_parameters([
      dict(
          testcase_name='patientname_requires_patientid',
          csv='Col 1, Col 2, Col3\n Bob, 12345, Male',
          schemadef={
              '0x00100010': {
                  'Keyword': 'PatientName',
                  'Meta': 'Col 1',
                  'Conditional_On': 'Col 2',
              },
              '0x00100020': {
                  'Keyword': 'PatientID',
                  'Meta': 'Col 2',
              },
              '0x00100040': {'Keyword': 'PatientSex', 'Meta': 'Col 3'},
          },
          expected={
              '00100010': {'vr': 'PN', 'Value': [{'Alphabetic': 'Bob'}]},
              '00100020': {'vr': 'LO', 'Value': ['12345']},
              '00100040': {'vr': 'CS', 'Value': ['Male']},
          },
      ),
      dict(
          testcase_name='patientid_requires_patientname',
          csv='Col 1, Col 2, Col3\n Bob, 12345, Male',
          schemadef={
              '0x00100010': {'Keyword': 'PatientName', 'Meta': 'Col 1'},
              '0x00100020': {
                  'Keyword': 'PatientID',
                  'Meta': 'Col 2',
                  'Conditional_On': 'Col 1',
              },
              '0x00100040': {'Keyword': 'PatientSex', 'Meta': 'Col 3'},
          },
          expected={
              '00100010': {'vr': 'PN', 'Value': [{'Alphabetic': 'Bob'}]},
              '00100020': {'vr': 'LO', 'Value': ['12345']},
              '00100040': {'vr': 'CS', 'Value': ['Male']},
          },
      ),
      dict(
          testcase_name='type_2_tag_conditional_on_missing_value',
          csv='Col 1, Col 2, Col3\n , 12345, Male',
          schemadef={
              '0x00100010': {'Keyword': 'PatientName', 'Meta': 'Col 1'},
              '0x00100020': {
                  'Keyword': 'PatientID',
                  'Meta': 'Col 2',
                  'Conditional_On': 'Col 1',
              },
              '0x00100040': {'Keyword': 'PatientSex', 'Meta': 'Col 3'},
          },
          expected={
              '00100010': {'vr': 'PN'},
              '00100020': {'vr': 'LO'},
              '00100040': {'vr': 'CS', 'Value': ['Male']},
          },
      ),
      dict(
          testcase_name='type_3_tag_conditional_on_missing_value',
          csv='Col 1, Col 2, Col3\n, 12345, Male',
          schemadef={
              '0x00100010': {'Keyword': 'PatientName', 'Meta': 'Col 1'},
              '0x00100020': {
                  'Keyword': 'PatientID',
                  'Meta': 'Col 2',
              },
              '0x00100212': {
                  'Keyword': 'StrainDescription',
                  'Meta': 'Col 3',
                  'Conditional_On': 'Col 1',
              },
          },
          expected={
              '00100010': {'vr': 'PN'},
              '00100020': {'vr': 'LO', 'Value': ['12345']},
          },
      ),
  ])
  def test_conditional_schema(
      self, csv: str, schemadef: Dict[str, Any], expected: Mapping[str, Any]
  ):
    """Tests conditionals to make tag value assignment dependent on another def."""
    metadata = self._get_first_csv_row(csv)
    ds_json = dicom_schema_util.get_json_dicom(
        'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
    )
    self.assertEqual(ds_json, expected)

  def test_simple_string_and_numbers(self):
    """Test generation of tags with integer type VR codes."""
    csv = 'Col 1, Col 2, Col3\n Bob, 12345, 567'
    schemadef = {
        '0x00100010': {'Keyword': 'PatientName', 'Meta': 'Col 1'},
        '0x00280010': {
            'Keyword': 'Rows',
            'Meta': 'Col 2',
        },
        '0x00280011': {
            'Keyword': 'Columns',
            'Meta': 'Col 3',
        },
    }
    # type three tags with missing values can be omitted entirely.
    metadata = self._get_first_csv_row(csv)
    ds_json = dicom_schema_util.get_json_dicom(
        'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
    )
    self.assertEqual(ds_json['00100010']['Value'], [{'Alphabetic': 'Bob'}])
    self.assertEqual(ds_json['00280010']['Value'], [12345])
    self.assertEqual(ds_json['00280011']['Value'], [567])

  @parameterized.named_parameters(
      dict(
          testcase_name='populated',
          csv=(
              'Col 1, Col 2, Col3, Col4, Col 5\n Bob, 0x00280011, 00280012, '
              '"(0028, 0013)", (0028\\, 0014)'
          ),
          expected_seq_values=['00280011', '00280012', '00280013', '00280014'],
      ),
      dict(
          testcase_name='empty',
          csv='Col 1, Col 2, Col3, Col4, Col 5\n Bob,,,,',
          expected_seq_values=[],
      ),
  )
  def test_sequence_and_at_tags(self, csv: str, expected_seq_values: List[str]):
    """Test generation of tags with AT VR codes."""
    schemadef = {
        '0x00100010': {'Keyword': 'PatientName', 'Meta': 'Col 1'},
        '0x00209222': {
            'Keyword': 'DimensionIndexSequence',
            'SQ': [
                {
                    '00209165': {
                        'KeyWord': 'DimensionIndexPointer',
                        'Meta': 'Col 2',
                    }
                },
                {
                    '00209165': {
                        'KeyWord': 'DimensionIndexPointer',
                        'Meta': 'Col 3',
                    }
                },
                {
                    '00209165': {
                        'KeyWord': 'DimensionIndexPointer',
                        'Meta': 'Col 4',
                    }
                },
                {
                    '00209165': {
                        'KeyWord': 'DimensionIndexPointer',
                        'Meta': 'Col 5',
                    }
                },
            ],
        },
    }
    metadata = self._get_first_csv_row(csv)
    ds_json = dicom_schema_util.get_json_dicom(
        'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
    )
    self.assertEqual(ds_json['00100010']['Value'], [{'Alphabetic': 'Bob'}])
    if not expected_seq_values:
      self.assertNotIn('00209222', ds_json)
    else:
      self.assertLen(ds_json['00209222']['Value'], len(expected_seq_values))
      for index, value in enumerate(expected_seq_values):
        self.assertEqual(
            ds_json['00209222']['Value'][index]['00209165']['Value'], [value]
        )

  @parameterized.named_parameters([
      dict(
          testcase_name='short_full_value_uncropped',
          csv='Col 1, Col 2\n Bob, 12345',
          expected_value='Bob-12345',
          flg=ingest_flags.MetadataTagLengthValidation.LOG_WARNING_AND_CLIP,
      ),
      dict(
          testcase_name='long_value_cropped',
          csv=f'Col 1, Col 2\n Bob, {"*" * 64}',
          expected_value=f'Bob-{"*"*60}',
          flg=ingest_flags.MetadataTagLengthValidation.LOG_WARNING_AND_CLIP,
      ),
      dict(
          testcase_name='long_value_uncropped_length_cropping_disabled',
          csv=f'Col 1, Col 2\n Bob, {"*" * 64}',
          expected_value=f'Bob-{"*"*64}',
          flg=ingest_flags.MetadataTagLengthValidation.NONE,
      ),
      dict(
          testcase_name='long_value_uncropped_length_cropping_warning',
          csv=f'Col 1, Col 2\n Bob, {"*" * 64}',
          expected_value=f'Bob-{"*"*64}',
          flg=ingest_flags.MetadataTagLengthValidation.LOG_WARNING,
      ),
  ])
  def test_string_join_schema(self, csv, expected_value, flg):
    """Json generation of string tags gen from join of multiple csv cols."""
    schemadef = {
        '0x00100010': {
            'Keyword': 'PatientName',
            'Meta': ['Col 1', 'Col 2'],
            'Meta_Join': '-',
        },
    }
    metadata = self._get_first_csv_row(csv)
    with flagsaver.flagsaver(metadata_tag_length_validation=flg):
      ds_json = dicom_schema_util.get_json_dicom(
          'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
      )
    self.assertEqual(
        ds_json['00100010']['Value'], [{'Alphabetic': expected_value}]
    )

  def test_string_join_schema_tag_length_validation_raises_if_configured(self):
    """Json generation of string tags gen from join of multiple csv cols."""
    csv = f'Col 1, Col 2\n Bob, {"*" * 64}'
    schemadef = {
        '0x00100010': {
            'Keyword': 'PatientName',
            'Meta': ['Col 1', 'Col 2'],
            'Meta_Join': '-',
        },
    }
    metadata = self._get_first_csv_row(csv)
    with flagsaver.flagsaver(
        metadata_tag_length_validation=ingest_flags.MetadataTagLengthValidation.ERROR
    ):
      with self.assertRaises(dicom_schema_util.DICOMSchemaTagError):
        dicom_schema_util.get_json_dicom(
            'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
        )

  def test_invalid_uid_raises_during_get_json_dicom_if_configured(self):
    csv = 'Col 1, StudyInstanceUID\n Bob, 1.01'
    schemadef = {
        '0x0020000D': {
            'Keyword': 'StudyInstanceUID',
            'Meta': 'StudyInstanceUID',
        }
    }
    metadata = self._get_first_csv_row(csv)
    with flagsaver.flagsaver(
        metadata_uid_validation=ingest_flags.MetadataUidValidation.ERROR
    ):
      with self.assertRaises(dicom_schema_util.DICOMSchemaTagError):
        dicom_schema_util.get_json_dicom(
            'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
        )

  @parameterized.named_parameters([
      dict(
          testcase_name='short',
          csv='Col 1\n Bob',
          expected_tag='00102292',
          expected_value='Bob',
      ),
      dict(
          testcase_name='long',
          csv='Col 1\nBob_Is_A_Very_Large_Mouse',
          expected_tag='00102201',
          expected_value='Bob_Is_A_Very_Large_Mouse',
      ),
  ])
  def test_character_length_conditional(
      self, csv: str, expected_tag: str, expected_value: str
  ):
    """Tests character length condition for tags with few chars."""
    schemadef = {
        '0x00102292': {
            'Keyword': 'PatientBreedDescription',
            'VALUE_CHAR_LIMIT': '<=16',
            'Meta': 'Col 1',
        },
        '0x00102201': {
            'Keyword': 'PatientSpeciesDescription',
            'VALUE_CHAR_LIMIT': '>16',
            'Meta': 'Col 1',
        },
    }
    metadata = self._get_first_csv_row(csv)
    ds_json = dicom_schema_util.get_json_dicom(
        'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
    )
    self.assertLen(ds_json, 1)
    self.assertEqual(ds_json[expected_tag]['Value'], [expected_value])

  def test_basic_static_value(self):
    """Tests definition of json with tag defined as a static value."""
    csv = 'Col 1, Col2, Col3\n Bob, 1345, Smith'
    schemadef = {
        '0x00100010': {'Keyword': 'PatientName', 'Meta': 'Col 1'},
        '0x00100020': {'Keyword': 'PatientID', 'Static_Value': 'Magic'},
    }
    metadata = self._get_first_csv_row(csv)
    ds_json = dicom_schema_util.get_json_dicom(
        'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
    )
    self.assertLen(ds_json, 2)
    self.assertEqual(ds_json['00100010']['Value'], [{'Alphabetic': 'Bob'}])
    self.assertEqual(ds_json['00100020']['Value'], ['Magic'])

  def test_conditional_static_value(self):
    """Tests static tags can be excluded by absence of conditional."""
    csv = 'Col 1, Col2, Col3\n , 1345, Smith'
    schemadef = {
        '0x00100010': {'Keyword': 'PatientName', 'Meta': 'Col 1'},
        '0x00100020': {
            'Keyword': 'PatientID',
            'Static_Value': 'Magic',
            'Conditional_On': 'Col 1',
        },
    }
    metadata = self._get_first_csv_row(csv)
    ds_json = dicom_schema_util.get_json_dicom(
        'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
    )
    self.assertLen(ds_json, 2)
    self.assertNotIn('Value', ds_json['00100010'])
    self.assertNotIn('Value', ds_json['00100020'])

  def test_condition_tag_handler(self):
    cond_handle = dicom_schema_util.ConditionalTagHandler()

    tag1 = dicom_schema_util.SchemaDICOMTag(
        '00100010',
        set(['PN']),
        {
            'Keyword': 'PatientName',
            'Static_Value': 'Magic',
            'Conditional_On': ['Col2', 'Col3'],
        },
        set(),
        None,
    )
    tag2 = dicom_schema_util.SchemaDICOMTag(
        '00100010',
        set(['PN']),
        {
            'Keyword': 'PatientName',
            'Static_Value': 'Magic',
            'Conditional_On': 'Col2',
        },
        set(),
        None,
    )
    tag3 = dicom_schema_util.SchemaDICOMTag(
        '00100010',
        set(['PN']),
        {'Keyword': 'PatientName', 'Meta': 'Col2'},
        set(),
        None,
    )
    tag4 = dicom_schema_util.SchemaDICOMTag(
        '00100010',
        set(['PN']),
        {'Keyword': 'PatientName', 'Meta': 'Col3'},
        set(),
        None,
    )
    tag3._metadata_columns_with_values.add('COL2')
    tag4._metadata_columns_with_values.add('COL3')

    cond_handle.add_tag_to_monitor(tag1)
    cond_handle.add_tag_to_monitor(tag2)
    self.assertTrue(cond_handle.has_conditions(tag1))
    self.assertTrue(cond_handle.has_conditions(tag2))
    self.assertEqual(cond_handle.tag_processed(tag3), [tag2])
    self.assertTrue(cond_handle.has_conditions(tag1))
    self.assertEqual(cond_handle.tag_processed(tag4), [tag1])
    tag5 = dicom_schema_util.SchemaDICOMTag(
        '00100010',
        set(['PN']),
        {
            'Keyword': 'PatientName',
            'Static_Value': 'Magic',
            'Conditional_On': ['Col2', 'Col3'],
        },
        set(),
        None,
    )
    self.assertFalse(cond_handle.has_conditions(tag5))

  @parameterized.named_parameters([
      dict(
          testcase_name='short_value',
          csv='Col 1\n1234',
          expected_value={'00080100': {'vr': 'SH', 'Value': ['1234']}},
      ),
      dict(
          testcase_name='long_value',
          csv='Col 1\nThis_Tag_Has_A_Long_Code_Value',
          expected_value={
              '00080119': {
                  'vr': 'UC',
                  'Value': ['This_Tag_Has_A_Long_Code_Value'],
              }
          },
      ),
  ])
  def test_seq_containing_conditional(
      self, csv: str, expected_value: Mapping[str, Any]
  ):
    schemadef = {
        '00081084': {
            'Keyword': 'AdmittingDiagnosesCodeSequence',
            'SQ': [{
                '0x00080100': {
                    'Keyword': 'CodeValue',
                    'VALUE_CHAR_LIMIT': '<=16',
                    'Meta': 'Col 1',
                },
                '0x00080119': {
                    'Keyword': 'LongCodeValue',
                    'VALUE_CHAR_LIMIT': '>16',
                    'Meta': 'Col 1',
                },
            }],
        }
    }
    metadata = self._get_first_csv_row(csv)
    ds_json = dicom_schema_util.get_json_dicom(
        'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
    )
    self.assertLen(ds_json, 1)
    self.assertEqual(ds_json['00081084']['Value'], [expected_value])

  @parameterized.parameters([True, 'True'])
  def test_missing_required_tag_definition_throws(self, required_tag):
    csv = 'Col 2\nbroccoli^carrot'
    schemadef = {
        '0x00100010': {
            'Keyword': 'PatientName',
            'Meta': 'Col 1',
            'Required': required_tag,
        }
    }

    metadata = self._get_first_csv_row(csv)
    with self.assertRaises(dicom_schema_util.MissingRequiredMetadataValueError):
      dicom_schema_util.get_json_dicom(
          'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
      )

  @flagsaver.flagsaver(require_type1_dicom_tag_metadata_is_defined=True)
  def test_missing_type1_tag_metadata_required_flag_true(self):
    csv = 'Col 2\nbroccoli^carrot'
    schemadef = {
        '0x00400512': {
            'Keyword': 'ContainerIdentifier',
            'Meta': 'Col 1',
        }
    }
    metadata = self._get_first_csv_row(csv)
    with self.assertRaises(dicom_schema_util.MissingRequiredMetadataValueError):
      dicom_schema_util.get_json_dicom(
          'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
      )

  @flagsaver.flagsaver(require_type1_dicom_tag_metadata_is_defined=False)
  def test_missing_type1_tag_metadata_required_flag_true_set_param(self):
    csv = 'Col 2\nbroccoli^carrot'
    schemadef = {
        '0x00400512': {
            'Keyword': 'ContainerIdentifier',
            'Meta': 'Col 1',
            'Required': True,
        }
    }
    metadata = self._get_first_csv_row(csv)
    with self.assertRaises(dicom_schema_util.MissingRequiredMetadataValueError):
      dicom_schema_util.get_json_dicom(
          'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
      )

  @flagsaver.flagsaver(require_type1_dicom_tag_metadata_is_defined=True)
  def test_missing_type1_tag_metadata_required_set_to_false_does_not_throw(
      self,
  ):
    csv = 'Col 2\nbroccoli^carrot'
    schemadef = {
        '0x00400512': {
            'Keyword': 'ContainerIdentifier',
            'Meta': 'Col 1',
            'Required': False,
        }
    }
    metadata = self._get_first_csv_row(csv)
    result = dicom_schema_util.get_json_dicom(
        'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
    )
    self.assertEmpty(result)

  @flagsaver.flagsaver(require_type1_dicom_tag_metadata_is_defined=True)
  def test_has_type1_tag_metadata_required_flag_true_succeeds(self):
    csv = 'Col 1\nbroccoli^carrot'
    schemadef = {
        '00400512': {
            'Keyword': 'ContainerIdentifier',
            'Meta': 'Col 1',
        }
    }
    metadata = self._get_first_csv_row(csv)
    result = dicom_schema_util.get_json_dicom(
        'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
    )
    self.assertEqual(
        result, {'00400512': {'vr': 'LO', 'Value': ['broccoli^carrot']}}
    )

  @parameterized.parameters([False, 'False'])
  @flagsaver.flagsaver(require_type1_dicom_tag_metadata_is_defined=True)
  def test_missing_type2_tag_metadata_not_required_empty(self, required_tag):
    csv = 'Col 2\nbroccoli^carrot'
    schemadef = {
        '0x00100010': {
            'Keyword': 'PatientName',
            'Meta': 'Col 1',
            'Required': required_tag,
        }
    }
    metadata = self._get_first_csv_row(csv)
    ds_json = dicom_schema_util.get_json_dicom(
        'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
    )
    self.assertEqual(ds_json, {'00100010': {'vr': 'PN'}})

  def test_non_bool_required_param_raises(self):
    schemadef = {
        '0x00100010': {
            'Keyword': 'PatientName',
            'Meta': 'Col 1',
            'Required': 'FOO',
        }
    }
    metadata = self._get_first_csv_row('Col 2\nbroccoli^carrot')
    with self.assertRaises(dicom_schema_util.DICOMSchemaTagError):
      dicom_schema_util.get_json_dicom(
          'VL Whole Slide Microscopy Image IOD Modules', metadata, schemadef
      )

  def test_dict_metadata_table_wrapper_columns(self):
    self.assertEqual(
        dicom_schema_util.DictMetadataTableWrapper(
            {'a a': 1, 'b b': 2}
        ).column_names,
        ['a a', 'b b'],
    )

  @parameterized.named_parameters([
      dict(testcase_name='match', search='a a', expected='a a'),
      dict(testcase_name='norm_match', search='BB', expected='b b'),
      dict(testcase_name='no_match', search='CC', expected=None),
  ])
  def test_dict_metadata_table_wrapper_find_data_frame_column_name(
      self, search, expected
  ):
    self.assertEqual(
        dicom_schema_util.DictMetadataTableWrapper(
            {'a a': 1, 'b b': 2}
        ).find_data_frame_column_name(search),
        expected,
    )

  @parameterized.named_parameters([
      dict(testcase_name='match', search='a a', expected=1),
      dict(testcase_name='norm_match', search='BB', expected=2),
      dict(testcase_name='no_match', search='CC', expected=None),
  ])
  def test_dict_metadata_table_wrapper_lookup_metadata_value(
      self, search, expected
  ):
    self.assertEqual(
        dicom_schema_util.DictMetadataTableWrapper(
            {'a a': 1, 'b b': 2}
        ).lookup_metadata_value(search),
        expected,
    )

  def test_dict_metadata_table_wrapper_copy(self):
    tbl1 = dicom_schema_util.DictMetadataTableWrapper({'a a': 1, 'b b': 2})
    tbl2 = tbl1.copy()
    self.assertIsNot(tbl1, tbl2)
    self.assertEqual(tbl1.column_names, tbl2.column_names)
    for column_name in tbl1.column_names:
      self.assertEqual(
          tbl1.lookup_metadata_value(column_name),
          tbl2.lookup_metadata_value(column_name),
      )


if __name__ == '__main__':
  absltest.main()
