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
"""Tests for validate_ingested_dicom."""
import math
import os
import typing
from typing import List, Optional, Tuple

from absl.testing import absltest
from absl.testing import parameterized
import pydicom

from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_wsi_dicom
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingested_dicom_file_ref
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import validate_ingested_dicom
from transformation_pipeline.ingestion_lib.dicom_util import dicom_test_util
from transformation_pipeline.ingestion_lib.dicom_util import pydicom_util


def _increment_uid(instance: str, inc: int = 1) -> str:
  """Increment DICOM UID."""
  parts = instance.split('.')
  parts[-1] = str(int(parts[-1]) + inc)
  return '.'.join(parts)


# string type const
_LABEL = ingest_const.LABEL
_OVERVIEW = ingest_const.OVERVIEW
_THUMBNAIL = ingest_const.THUMBNAIL
_ORIGINAL = ingest_const.ORIGINAL
_RESAMPLED = ingest_const.RESAMPLED
_CT = 'CT'
_NONE = ingest_const.NONE
_DERIVED = ingest_const.DERIVED
_PRIMARY = ingest_const.PRIMARY
_VOLUME = ingest_const.VOLUME
_ORIGINAL_IMAGE_TYPE = (
    f'{validate_ingested_dicom._ORIGINAL_IMAGE_TYPE}\\{_NONE}'
)
_DERIVED_IMAGE_TYPE = f'{_DERIVED}\\{_PRIMARY}\\{_VOLUME}\\{_RESAMPLED}'


class _GenTestDicom:
  """Generates Test DICOM for DICOM Validation Testing."""

  def __init__(
      self,
      image_type: str = _ORIGINAL,
      dcm_cpy: Optional[pydicom.Dataset] = None,
  ):
    if dcm_cpy:
      dicom_metadata = dcm_cpy.to_json_dict()
      self._dcm = typing.cast(
          pydicom.FileDataset,
          dicom_test_util.create_test_dicom_instance(dcm_json=dicom_metadata),
      )
      self.set_image_frame_type(image_type)
      return
    dicom_metadata = dicom_test_util.create_metadata_dict()
    self._dcm = typing.cast(
        pydicom.FileDataset,
        dicom_test_util.create_test_dicom_instance(dcm_json=dicom_metadata),
    )
    self._dcm.SOPClassUID = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    self._dcm.Modality = 'SM'
    self._dcm.BitsAllocated = 8
    self._dcm.BitsStored = 8
    self._dcm.HighBit = 7
    self._dcm.SamplesPerPixel = 3
    self._dcm.Rows = 256
    self._dcm.Columns = 256
    self._dcm.TotalPixelMatrixRows = 50000
    self._dcm.TotalPixelMatrixColumns = 40000
    self._dcm.BarcodeValue = '307772'
    self._dcm.BurnedInAnnotation = 'NO'
    self._dcm.SpecimenLabelInImage = 'NO'
    self._dcm.DimensionOrganizationType = ingest_const.TILED_FULL
    self._dcm.file_meta.TransferSyntaxUID = '1.2.840.10008.1.2.1'
    self.set_image_frame_type(image_type)

  def set_image_frame_type(self, image_type: str):
    """Sets DICOM ImageType and FrameType."""
    image_type = image_type.strip().upper()
    if image_type == _CT:
      del self._dcm.TotalPixelMatrixRows
      del self._dcm.TotalPixelMatrixColumns
      del self._dcm.NumberOfFrames
      self._dcm.Modality = 'CT'
      self._dcm.ImageType = 'CT'
      return
    set_one_frame = True
    if image_type == _RESAMPLED:
      self._dcm.ImageType = _DERIVED_IMAGE_TYPE
    elif image_type in (_THUMBNAIL, _LABEL, _OVERVIEW):
      self._dcm.ImageType = f'{_ORIGINAL}\\{_PRIMARY}\\{image_type}\\{_NONE}'
    else:
      self._dcm.ImageType = _ORIGINAL_IMAGE_TYPE
      set_one_frame = False
    self._dcm.FrameType = self._dcm.ImageType
    if set_one_frame:
      self._dcm.TotalPixelMatrixRows = self._dcm.Rows
      self._dcm.TotalPixelMatrixColumns = self._dcm.Columns
    total_rows = float(self._dcm.TotalPixelMatrixRows)
    total_columns = float(self._dcm.TotalPixelMatrixColumns)
    total_rows /= float(self._dcm.Rows)
    total_columns /= float(self._dcm.Columns)
    self._dcm.NumberOfFrames = str(
        int(math.ceil(total_rows)) * int(math.ceil(total_columns))
    )

  def increment_series(self, inc: int = 1):
    self._dcm.SeriesInstanceUID = _increment_uid(
        self._dcm.SeriesInstanceUID, inc
    )

  def increment_study(self, inc: int = 1):
    self._dcm.StudyInstanceUID = _increment_uid(self._dcm.StudyInstanceUID, inc)

  def increment_instance(self, inc: int = 1):
    self._dcm.SOPInstanceUID = _increment_uid(self._dcm.SOPInstanceUID, inc)

  @property
  def dicom(self) -> pydicom.Dataset:
    return self._dcm


def _gen_first_dicom(
    temp_dir: absltest._TempDir, image_type: str = _ORIGINAL
) -> Tuple[List[str], pydicom.Dataset]:
  """Generate first DICOM image in image list.

  Args:
    temp_dir: Temp dir to gen image in.
    image_type: Image type to generate.

  Returns:
    Tuple[List[path to dicom], pydicom.Dataset]
  """
  original_dcm = _GenTestDicom(image_type=image_type)
  path = os.path.join(temp_dir, 'dcm_0.dcm')
  original_dcm.dicom.save_as(path)
  return ([path], original_dcm.dicom)


def _gen_next_dicom(
    original_dcm: pydicom.Dataset,
    temp_dir: absltest._TempDir,
    image_type: str,
    dcm_list: List[str],
) -> Tuple[pydicom.Dataset, str]:
  """Generate next DICOM image in image list.

  Args:
    original_dcm: dicom to base image on.
    temp_dir: Temp dir to gen image in.
    image_type: Image type to generate.
    dcm_list: List of generated dicom file paths.

  Returns:
    Tuple[pydicom.Dataset  generated, path to image]
  """
  inc_count = len(dcm_list)
  dcm = _GenTestDicom(image_type, original_dcm)
  dcm.increment_instance(inc_count)
  path = os.path.join(temp_dir, f'dcm_{inc_count}.dcm')
  dcm_list.append(path)
  return dcm.dicom, path


def _gen_next_dicom_save(
    original_dcm: pydicom.Dataset,
    temp_dir: absltest._TempDir,
    image_type: str,
    dcm_list: List[str],
):
  """Generates and saves next DICOM image in image list.

  Args:
    original_dcm: dicom to base image on.
    temp_dir: Temp dir to gen image in.
    image_type: Image type to generate.
    dcm_list: List of generated dicom file paths.
  """
  dcm, path = _gen_next_dicom(original_dcm, temp_dir, image_type, dcm_list)
  dcm.save_as(path)


class ValidateIngestedDicomTest(parameterized.TestCase):
  """Tests for validate_ingested_dicom."""

  def test_validate_uid_succeeds(self):
    self.assertIsNone(validate_ingested_dicom._validate_uid('1.2.0.4', None))

  def test_validate_uid_max_len_succeeds(self):
    self.assertIsNone(validate_ingested_dicom._validate_uid('1' * 64, None))

  def test_validate_uid_multi_digits_block_dont_start_with_zero_raises(self):
    with self.assertRaises(ingested_dicom_file_ref.DicomIngestError):
      validate_ingested_dicom._validate_uid('1.2.01.4', None)

  def test_validate_uid_empty_uid_raises(self):
    with self.assertRaises(ingested_dicom_file_ref.DicomIngestError):
      validate_ingested_dicom._validate_uid('', None)

  def test_validate_uid_too_long_raises(self):
    with self.assertRaises(ingested_dicom_file_ref.DicomIngestError):
      validate_ingested_dicom._validate_uid('1' * 65, None)

  char_list = [chr(ch) for ch in range(0, ord('0'))]
  char_list.extend([chr(ch) for ch in range(ord('9') + 1, 256)])
  del char_list[char_list.index('.')]

  @parameterized.parameters(char_list)
  def test_validate_uid_not_numbers_raises(self, test_char: str):
    with self.assertRaises(ingested_dicom_file_ref.DicomIngestError):
      validate_ingested_dicom._validate_uid(f'2.{test_char}.4', None)

  def test_validate_wsi_dicom_instance_succeeds(self):
    temp_dir = self.create_tempdir()
    original_dcm = _GenTestDicom(image_type=_ORIGINAL)
    self.assertFalse(original_dcm.dicom.is_implicit_VR)
    self.assertTrue(original_dcm.dicom.is_little_endian)
    path = os.path.join(temp_dir, 'dcm_0.dcm')
    original_dcm.dicom.save_as(path)
    self.assertIsNone(
        validate_ingested_dicom._validate_wsi_dicom_instance(
            ingested_dicom_file_ref.load_ingest_dicom_fileref(path)
        )
    )

  def test_validate_wsi_dicom_instance_invalid_number_of_frames_raises(self):
    temp_dir = self.create_tempdir()
    original_dcm = _GenTestDicom(image_type=_ORIGINAL)
    path = os.path.join(temp_dir, 'dcm_0.dcm')
    original_dcm.dicom.NumberOfFrames = '4'
    original_dcm.dicom.save_as(path)
    with self.assertRaises(ingested_dicom_file_ref.DicomIngestError):
      result = ingested_dicom_file_ref.load_ingest_dicom_fileref(path)
      validate_ingested_dicom._validate_wsi_dicom_instance(result)

  def test_validate_wsi_dicom_instance_invalid_label_type_raises(self):
    temp_dir = self.create_tempdir()
    original_dcm = _GenTestDicom(image_type=_LABEL)
    path = os.path.join(temp_dir, 'dcm_0.dcm')
    original_dcm.dicom.ImageType = f'{_LABEL}\\{_OVERVIEW}\\{_THUMBNAIL}'
    original_dcm.dicom.FrameType = original_dcm.dicom.ImageType
    original_dcm.dicom.save_as(path)
    with self.assertRaises(ingested_dicom_file_ref.DicomIngestError):
      result = ingested_dicom_file_ref.load_ingest_dicom_fileref(path)
      validate_ingested_dicom._validate_wsi_dicom_instance(result)

  def test_validate_wsi_dicom_instance_incompatible_label_and_frame_raises(
      self,
  ):
    temp_dir = self.create_tempdir()
    original_dcm = _GenTestDicom(image_type=_LABEL)
    path = os.path.join(temp_dir, 'dcm_0.dcm')
    original_dcm.dicom.ImageType = (
        f'{_ORIGINAL}\\{_PRIMARY}\\{_THUMBNAIL}\\{_NONE}'
    )
    original_dcm.dicom.FrameType = f'{_ORIGINAL}\\{_PRIMARY}\\{_LABEL}\\{_NONE}'
    original_dcm.dicom.save_as(path)
    with self.assertRaises(ingested_dicom_file_ref.DicomIngestError):
      result = ingested_dicom_file_ref.load_ingest_dicom_fileref(path)
      validate_ingested_dicom._validate_wsi_dicom_instance(result)

  @parameterized.parameters(
      [_ORIGINAL, _RESAMPLED, _THUMBNAIL, _OVERVIEW, _LABEL]
  )
  def test_validate_wsi_dicom_instance_various_types_succeeds(
      self, image_type: str
  ):
    temp_dir = self.create_tempdir()
    dcm_list, _ = _gen_first_dicom(temp_dir, image_type)
    self.assertIsNone(
        validate_ingested_dicom._validate_wsi_dicom_instance(
            ingested_dicom_file_ref.load_ingest_dicom_fileref(dcm_list[0])
        )
    )

  @parameterized.parameters([
      'BitsAllocated',
      'BitsStored',
      'HighBit',
      'SamplesPerPixel',
      'NumberOfFrames',
      'Rows',
      'Columns',
      'TotalPixelMatrixColumns',
      'TotalPixelMatrixRows',
      'DimensionOrganizationType',
      'BurnedInAnnotation',
      'SpecimenLabelInImage',
  ])
  def test_validate_wsi_dicom_instance_with_invalid_tag_value_raises(
      self, tag_keyword: str
  ):
    temp_dir = self.create_tempdir()
    original_dcm = _GenTestDicom(image_type=_ORIGINAL)
    pydicom_util.set_dataset_tag_value(original_dcm.dicom, tag_keyword, '0')
    path = os.path.join(temp_dir, 'dcm_0.dcm')
    original_dcm.dicom.save_as(path)
    with self.assertRaises(ingested_dicom_file_ref.DicomIngestError):
      result = ingested_dicom_file_ref.load_ingest_dicom_fileref(path)
      validate_ingested_dicom._validate_wsi_dicom_instance(result)

  @parameterized.parameters([_LABEL, _THUMBNAIL, _OVERVIEW])
  def test_validate_wsi_dicom_instance_ancillary_image_with_multiple_frames_raise(
      self, image: str
  ):
    temp_dir = self.create_tempdir()
    original_dcm = _GenTestDicom(image_type=_ORIGINAL)
    path = os.path.join(temp_dir, 'dcm_0.dcm')
    original_dcm.dicom.ImageType = f'{_ORIGINAL}\\PRIMARY\\{image}\\{_NONE}'
    original_dcm.dicom.FrameType = original_dcm.dicom.ImageType
    original_dcm.dicom.save_as(path)
    with self.assertRaises(ingested_dicom_file_ref.DicomIngestError):
      result = ingested_dicom_file_ref.load_ingest_dicom_fileref(path)
      validate_ingested_dicom._validate_wsi_dicom_instance(result)

  def test_validate_dicom_files_with_full_set_of_instances_succeeds(self):
    # Build a set of test DICOM instances that contain required metadata.
    temp_dir = self.create_tempdir()
    dcm_list, original_dcm = _gen_first_dicom(temp_dir)
    # RESAMPLED represent lower level pyramid levels and can show up more than
    # once. All other image types should only appear once.
    for image_type in [_RESAMPLED, _RESAMPLED, _THUMBNAIL, _OVERVIEW, _LABEL]:
      _gen_next_dicom_save(original_dcm, temp_dir, image_type, dcm_list)
    # generate one which not represent wsi image.
    _gen_next_dicom_save(original_dcm, temp_dir, _CT, dcm_list)

    # Validate test dicom should not throw
    result = validate_ingested_dicom.validate_dicom_files(
        ingest_wsi_dicom.get_dicom_filerefs_list(dcm_list)
    )
    # Test results test dicom should not verify results
    original_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        dcm_list[0]
    )
    self.assertEqual(original_ref.study_instance_uid, result.study_uid)
    self.assertEqual(original_ref.series_instance_uid, result.series_uid)
    self.assertEqual(original_ref.barcode_value, result.barcode_value)
    self.assertTrue(original_ref.equals(result.original_image))
    self.assertLen(result.wsi_image_filerefs, len(dcm_list) - 1)
    returned_wsi_filerefs = set(result.wsi_image_filerefs)
    for path in dcm_list[:-1]:
      test_pathref = ingested_dicom_file_ref.load_ingest_dicom_fileref(path)
      found = None
      for returned_ref in returned_wsi_filerefs:
        if test_pathref.equals(returned_ref):
          found = returned_ref
          break
      self.assertIsNotNone(found)
      returned_wsi_filerefs.remove(found)
    self.assertEmpty(returned_wsi_filerefs, 0)

    self.assertLen(result.other_dicom_filerefs, 1)
    test_pathref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        dcm_list[-1]
    )
    self.assertTrue(test_pathref.equals(result.other_dicom_filerefs[0]))

  def test_validate_dicom_files_with_only_original_image_succeeds(self):
    temp_dir = self.create_tempdir()
    dcm_list, _ = _gen_first_dicom(temp_dir)

    result = validate_ingested_dicom.validate_dicom_files(
        ingest_wsi_dicom.get_dicom_filerefs_list(dcm_list)
    )

    # Test results test dicom should not verify results
    original_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        dcm_list[0]
    )
    self.assertEqual(original_ref.study_instance_uid, result.study_uid)
    self.assertEqual(original_ref.series_instance_uid, result.series_uid)
    self.assertEqual(original_ref.barcode_value, result.barcode_value)
    self.assertTrue(original_ref.equals(result.original_image))
    self.assertLen(result.wsi_image_filerefs, 1)
    test_pathref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        dcm_list[-1]
    )
    self.assertTrue(test_pathref.equals(result.wsi_image_filerefs[0]))
    self.assertEmpty(result.other_dicom_filerefs)

  def test_validate_dicom_files_without_original_image_raises(self):
    temp_dir = self.create_tempdir()
    dcm_list, _ = _gen_first_dicom(temp_dir, _THUMBNAIL)
    dcm_list = ingest_wsi_dicom.get_dicom_filerefs_list(dcm_list)

    with self.assertRaises(ingested_dicom_file_ref.DicomIngestError):
      validate_ingested_dicom.validate_dicom_files(dcm_list)

  def test_validate_dicom_files_with_original_and_thumbnail_succeeds(self):
    temp_dir = self.create_tempdir()
    dcm_list, original_dcm = _gen_first_dicom(temp_dir)
    _gen_next_dicom_save(original_dcm, temp_dir, _THUMBNAIL, dcm_list)
    dcm_list = ingest_wsi_dicom.get_dicom_filerefs_list(dcm_list)

    self.assertIsNotNone(validate_ingested_dicom.validate_dicom_files(dcm_list))

  def test_validate_dicom_files_with_duplicate_sop_instance_uid_raises(self):
    temp_dir = self.create_tempdir()
    dcm_list, original_dcm = _gen_first_dicom(temp_dir)
    dcm, path = _gen_next_dicom(original_dcm, temp_dir, _THUMBNAIL, dcm_list)
    dcm.SOPInstanceUID = original_dcm.SOPInstanceUID
    dcm.save_as(path)
    dcm_list = ingest_wsi_dicom.get_dicom_filerefs_list(dcm_list)

    with self.assertRaises(ingested_dicom_file_ref.DicomIngestError):
      validate_ingested_dicom.validate_dicom_files(dcm_list)

  @parameterized.parameters([_ORIGINAL, _THUMBNAIL, _OVERVIEW, _LABEL])
  def test_validate_dicom_files_with_duplicate_instances_raises(
      self, duplicate_type: str
  ):
    temp_dir = self.create_tempdir()
    dcm_list, original_dcm = _gen_first_dicom(temp_dir)
    for image_type in [_RESAMPLED, _RESAMPLED, _THUMBNAIL, _OVERVIEW, _LABEL]:
      _gen_next_dicom_save(original_dcm, temp_dir, image_type, dcm_list)
    _gen_next_dicom_save(original_dcm, temp_dir, duplicate_type, dcm_list)
    dcm_list = ingest_wsi_dicom.get_dicom_filerefs_list(dcm_list)

    with self.assertRaises(ingested_dicom_file_ref.DicomIngestError):
      validate_ingested_dicom.validate_dicom_files(dcm_list)

  @parameterized.parameters(
      ['StudyInstanceUID', 'SeriesInstanceUID', 'BarcodeValue']
  )
  def test_validate_dicom_files_with_tags_mismatch_raises(
      self, tag_keyword: str
  ):
    temp_dir = self.create_tempdir()
    dcm_list, original_dcm = _gen_first_dicom(temp_dir)
    dcm, path = _gen_next_dicom(original_dcm, temp_dir, _THUMBNAIL, dcm_list)
    pydicom_util.set_dataset_tag_value(dcm, tag_keyword, '99')
    dcm.save_as(path)
    dcm_list = ingest_wsi_dicom.get_dicom_filerefs_list(dcm_list)

    with self.assertRaises(ingested_dicom_file_ref.DicomIngestError):
      validate_ingested_dicom.validate_dicom_files(dcm_list)


if __name__ == '__main__':
  absltest.main()
