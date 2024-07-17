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
"""Test wsi_dicom_file_ref."""
import json

from absl.testing import absltest
import pydicom

from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import wsi_dicom_file_ref


def _test_wsi_path() -> str:
  return gen_test_util.test_file_path('test_wsi2.dcm')


class WsiDicomFileRefTest(absltest.TestCase):
  """Tests for wsi_dicom_file_ref."""

  def test_get_wsidcmref_addressses(self):
    self.assertListEqual(
        wsi_dicom_file_ref.WSIDicomFileRef().tag_addresses(),
        [
            '0x0020000D',
            '0x0020000E',
            '0x00080016',
            '0x00080018',
            '0x00080008',
            '0x00089007',
            '0x00280100',
            '0x00280101',
            '0x00280102',
            '0x00280103',
            '0x00280002',
            '0x00280006',
            '0x00080050',
            '0x00200011',
            '0x22000005',
            '0x00200013',
            '0x00280004',
            '0x00480001',
            '0x00480002',
            '0x00480006',
            '0x00480007',
            '0x00280008',
            '0x00280011',
            '0x00280010',
            '0x00209311',
            '0x00080060',
            '0x00480010',
            '0x00280301',
            '0x00209161',
            '0x00200052',
            '0x00201040',
            '0x00080019',
            '0x00200027',
            '0x00081088',
            '0x00209228',
            '0x00209162',
            '0x00480303',
            '0x30211001',
            '0x30210010',
        ],
    )

  def test_norm_json_equals_norm_pydicom(self):
    test_json = {}
    # ImagedVolumeWidth
    test_json['00480001'] = {'vr': 'FL', 'Value': [12]}
    # ImagedVolumeHeight
    test_json['00480002'] = {'vr': 'FL', 'Value': [13.0]}
    # Specimen Label in Image
    test_json['00480010'] = {'vr': 'CS', 'Value': ['YES']}
    # High Bit
    test_json['00280102'] = {'vr': 'US', 'Value': [7]}
    # image type
    test_json['00080008'] = {'vr': 'CS', 'Value': ['HELLO', 'WORLD']}
    ds = pydicom.Dataset()
    ds.ImagedVolumeWidth = 12
    ds.ImagedVolumeHeight = 13.0
    ds.SpecimenLabelInImage = 'YES'
    ds.HighBit = 7
    ds.ImageType = ['HELLO', 'WORLD']
    ds = pydicom.FileDataset('test', ds)
    json_fref = wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_json(test_json)
    pydicom_fref = wsi_dicom_file_ref.init_from_pydicom_dataset('test', ds)
    self.assertEqual(
        json_fref.imaged_volume_width, pydicom_fref.imaged_volume_width
    )
    self.assertEqual(
        json_fref.imaged_volume_height, pydicom_fref.imaged_volume_height
    )
    self.assertEqual(
        json_fref.specimen_label_in_image, pydicom_fref.specimen_label_in_image
    )
    self.assertEqual(json_fref.high_bit, pydicom_fref.high_bit)
    self.assertEqual(json_fref.image_type, pydicom_fref.image_type)
    self.assertEqual(json_fref.modality, pydicom_fref.modality)

  def test_norm_dicom(self):
    ds = pydicom.Dataset()
    ds.ImagedVolumeWidth = 12
    ds.ImagedVolumeHeight = 13.0
    ds.SpecimenLabelInImage = 'YES'
    ds.HighBit = 7
    ds.ImageType = ['HELLO', 'WORLD']
    ds = pydicom.FileDataset('test', ds)
    fref = wsi_dicom_file_ref.init_from_pydicom_dataset('test', ds)
    self.assertEqual(fref.imaged_volume_width, str(12.0))
    self.assertEqual(fref.imaged_volume_height, str(13.0))
    self.assertEqual(fref.specimen_label_in_image, 'YES')
    self.assertEqual(fref.high_bit, str(int(7)))
    self.assertEqual(fref.image_type, 'HELLO\\WORLD')
    self.assertEqual(fref.modality, '')

  def _assert_expected(
      self, fref: wsi_dicom_file_ref.WSIDicomFileRef, file_path: str
  ) -> None:
    tested_tags = set()

    def _tag(tag_keyword: str, tag_address: str = '') -> str:
      if tag_keyword not in dcm and tag_address not in dcm:
        raise ValueError(f'{file_path} does not define tag {tag_keyword}.')
      if tag_keyword in tested_tags:
        raise ValueError(
            f'_assert_expected test {file_path} defines duplicate tests for '
            f'tag: {tag_keyword}.'
        )
      tested_tags.add(tag_keyword)
      if tag_keyword in dcm:
        if isinstance(dcm[tag_keyword].value, pydicom.multival.MultiValue):
          return '\\'.join(dcm[tag_keyword].value)
        return str(dcm[tag_keyword].value)
      if isinstance(dcm[tag_address].value, pydicom.multival.MultiValue):
        return '\\'.join(dcm[tag_address].value)
      return str(dcm[tag_address].value)

    with pydicom.dcmread(file_path, defer_size='512 KB') as dcm:
      self.assertEqual(
          fref.study_instance_uid,
          _tag(ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID),
      )
      self.assertEqual(
          fref.series_instance_uid,
          _tag(ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID),
      )
      self.assertEqual(
          fref.sop_class_uid, _tag(ingest_const.DICOMTagKeywords.SOP_CLASS_UID)
      )
      self.assertEqual(
          fref.sop_instance_uid,
          _tag(ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID),
      )
      self.assertEqual(
          fref.position_reference_indicator,
          _tag(ingest_const.DICOMTagKeywords.POSITION_REFERENCE_INDICATOR),
      )
      self.assertEqual(
          fref.frame_of_reference_uid,
          _tag(ingest_const.DICOMTagKeywords.FRAME_OF_REFERENCE_UID),
      )
      self.assertEqual(
          fref.concatenation_uid,
          _tag(ingest_const.DICOMTagKeywords.CONCATENATION_UID),
      )
      self.assertEqual(
          fref.concatenation_frame_offset_number,
          _tag(ingest_const.DICOMTagKeywords.CONCATENATION_FRAME_OFFSET_NUMBER),
      )
      self.assertEqual(
          fref.in_concatenation_number,
          _tag(ingest_const.DICOMTagKeywords.IN_CONCATENATION_NUMBER),
      )
      self.assertEqual(
          fref.image_type, _tag(ingest_const.DICOMTagKeywords.IMAGE_TYPE)
      )
      self.assertEqual(
          fref.frame_type, _tag(ingest_const.DICOMTagKeywords.FRAME_TYPE)
      )
      self.assertEqual(
          fref.bits_allocated,
          _tag(ingest_const.DICOMTagKeywords.BITS_ALLOCATED),
      )
      self.assertEqual(
          fref.bits_stored, _tag(ingest_const.DICOMTagKeywords.BITS_STORED)
      )
      self.assertEqual(
          fref.high_bit, _tag(ingest_const.DICOMTagKeywords.HIGH_BIT)
      )
      self.assertEqual(
          fref.pixel_representation,
          _tag(ingest_const.DICOMTagKeywords.PIXEL_REPRESENTATION),
      )
      self.assertEqual(
          fref.samples_per_pixel,
          _tag(ingest_const.DICOMTagKeywords.SAMPLES_PER_PIXEL),
      )
      self.assertEqual(
          fref.planar_configuration,
          _tag(ingest_const.DICOMTagKeywords.PLANAR_CONFIGURATION),
      )
      self.assertEqual(
          fref.accession_number,
          _tag(ingest_const.DICOMTagKeywords.ACCESSION_NUMBER),
      )
      self.assertEqual(
          fref.series_number, _tag(ingest_const.DICOMTagKeywords.SERIES_NUMBER)
      )
      self.assertEqual(
          fref.barcode_value, _tag(ingest_const.DICOMTagKeywords.BARCODE_VALUE)
      )
      self.assertEqual(
          fref.instance_number,
          _tag(ingest_const.DICOMTagKeywords.INSTANCE_NUMBER),
      )
      self.assertEqual(
          fref.photometric_interpretation,
          _tag(ingest_const.DICOMTagKeywords.PHOTOMETRIC_INTERPRETATION),
      )
      self.assertEqual(
          fref.imaged_volume_width,
          _tag(ingest_const.DICOMTagKeywords.IMAGED_VOLUME_WIDTH),
      )
      self.assertEqual(
          fref.imaged_volume_height,
          _tag(ingest_const.DICOMTagKeywords.IMAGED_VOLUME_HEIGHT),
      )
      self.assertEqual(
          fref.total_pixel_matrix_columns,
          _tag(ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_COLUMNS),
      )
      self.assertEqual(
          fref.total_pixel_matrix_rows,
          _tag(ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_ROWS),
      )
      self.assertEqual(
          fref.number_of_frames,
          _tag(ingest_const.DICOMTagKeywords.NUMBER_OF_FRAMES),
      )
      self.assertEqual(
          fref.columns, _tag(ingest_const.DICOMTagKeywords.COLUMNS)
      )
      self.assertEqual(fref.rows, _tag(ingest_const.DICOMTagKeywords.ROWS))
      self.assertEqual(
          fref.hash, _tag(ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG)
      )
      self.assertEqual(
          fref.dimension_organization_type,
          _tag(ingest_const.DICOMTagKeywords.DIMENSION_ORGANIZATION_TYPE),
      )
      self.assertEqual(
          fref.modality, _tag(ingest_const.DICOMTagKeywords.MODALITY)
      )
      self.assertEqual(
          fref.specimen_label_in_image,
          _tag(ingest_const.DICOMTagKeywords.SPECIMEN_LABEL_IN_IMAGE),
      )
      self.assertEqual(
          fref.burned_in_annotation,
          _tag(ingest_const.DICOMTagKeywords.BURNED_IN_ANNOTATION),
      )
      self.assertEqual(
          fref.pyramid_uid,
          _tag(
              ingest_const.DICOMTagKeywords.PYRAMID_UID,
              ingest_const.DICOMTagAddress.PYRAMID_UID,
          ),
      )
      self.assertEqual(
          fref.pyramid_label,
          _tag(
              ingest_const.DICOMTagKeywords.PYRAMID_LABEL,
              ingest_const.DICOMTagAddress.PYRAMID_LABEL,
          ),
      )
      self.assertEqual(
          fref.pyramid_description,
          _tag(
              ingest_const.DICOMTagKeywords.PYRAMID_DESCRIPTION,
              ingest_const.DICOMTagAddress.PYRAMID_DESCRIPTION,
          ),
      )
      self.assertEqual(
          fref.total_pixel_matrix_focal_planes,
          _tag(ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_FOCAL_PLANES),
      )
    # Test all attributes were tested.
    self.assertEqual(set(fref.tags.keys()), tested_tags)

  def test_wsi_dicom_file_ref_init_from_pydicom(self):
    file_path = _test_wsi_path()
    with pydicom.dcmread(file_path) as dcm:
      fref = wsi_dicom_file_ref.init_from_pydicom_dataset(file_path, dcm)
    self._assert_expected(fref, file_path)

  def test_wsi_dicom_file_ref_init_from_json(self):
    file_path = _test_wsi_path()
    with pydicom.dcmread(file_path) as dcm:
      dcm_json = json.loads(dcm.to_json())
    self._assert_expected(
        wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_json(dcm_json),
        file_path,
    )

  def test_wsi_dicom_file_ref_init_from_file_path(self):
    file_path = _test_wsi_path()
    self._assert_expected(
        wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_file(file_path),
        file_path,
    )

  def test_get_wsi_dicom_file_ref_tag_address_list(self):
    self.assertEqual(
        set(wsi_dicom_file_ref.get_wsi_dicom_file_ref_tag_address_list()),
        {
            '30210010',
            '0020000D',
            '0020000E',
            '00080016',
            '00080018',
            '00080008',
            '00089007',
            '00280100',
            '00280101',
            '00280102',
            '00280103',
            '00280002',
            '00280006',
            '00080050',
            '00200011',
            '22000005',
            '00200013',
            '00280004',
            '00480001',
            '00480002',
            '00480006',
            '00480007',
            '00280008',
            '00280011',
            '00280010',
            '00209311',
            '00080060',
            '00480010',
            '00280301',
            '00209161',
            '00200052',
            '00201040',
            '00080019',
            '00200027',
            '00081088',
            '00209228',
            '00209162',
            '30211001',
            '00480303',
        },
    )


if __name__ == '__main__':
  absltest.main()
