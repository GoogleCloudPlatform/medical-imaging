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
"""Unit tests for IngestDICOM."""
import json
import os
import random
import shutil
import typing
from typing import Any, Dict, Mapping, Tuple
from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import PIL
import pydicom

from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_file_ref
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_schema_util
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_wsi_dicom
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingested_dicom_file_ref
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client
from transformation_pipeline.ingestion_lib.dicom_util import dicom_test_util
from transformation_pipeline.ingestion_lib.dicom_util import pydicom_util

FLAGS = flags.FLAGS


_TEST_ZIP_FILE_NAME = 'test_file.zip'
_TEST_INGEST_FILENAME_WITH_SLIDEID = 'MD-01-1-A1-1_ingest.zip'
_BARCODE_NOT_LISTED_IN_METADATA = 'NotListed'
_FILENAME_DOES_NOT_CONTAIN_SLIDE_ID = 'NotListed-1-A1-1_ingest.zip'

# forward declare image types constants to reduce length.
_ORIGINAL = ingest_const.ORIGINAL
_DERIVED = ingest_const.DERIVED
_PRIMARY = ingest_const.PRIMARY
_SECONDARY = ingest_const.SECONDARY
_VOLUME = ingest_const.VOLUME
_LABEL = ingest_const.LABEL
_THUMBNAIL = ingest_const.THUMBNAIL
_OVERVIEW = ingest_const.OVERVIEW

# DICOM Tags Address
_STUDY_INSTANCE_UID = ingest_const.DICOMTagAddress.STUDY_INSTANCE_UID
_SERIES_INSTANCE_UID = ingest_const.DICOMTagAddress.SERIES_INSTANCE_UID
_SOP_INSTANCE_UID = '00080018'
_SOP_CLASS_UID = '00080016'
_INSTANCE_NUMBER = '00200013'
_IMAGE_TYPE = '00080008'
_TOTAL_PIXEL_MATRIX_COLUMNS = '00480006'
_TOTAL_PIXEL_MATRIX_ROWS = '00480007'


def _test_json_dicom_metadata(
    patient_name: str, patient_id: str
) -> Dict[str, Any]:
  return json.loads(
      '{"00100010": {"Value": [{"Alphabetic": "%s"}], "vr": "PN"}, '
      '"00100020": {"Value": ["%s"], "vr": "LO"}}' % (patient_name, patient_id)
  )


def _create_pydicom_ds(sop_instance_uid: str) -> pydicom.Dataset:
  ds = pydicom.Dataset()
  ds.StudyInstanceUID = '1.2.3'
  ds.SeriesInstanceUID = '1.2.3.4'
  ds.SOPInstanceUID = sop_instance_uid
  return ds


def _create_non_wsi_test_instance(
    instance_number: str,
) -> Tuple[str, str, str, str, str]:
  dicom_sop_class_uid = '4.5.7'
  image_type = 'magic'
  pixel_matrix_columns = '0'
  pixel_matrix_rows = '0'
  return (
      dicom_sop_class_uid,
      instance_number,
      image_type,
      pixel_matrix_columns,
      pixel_matrix_rows,
  )


def _create_wsi_test_instance(
    instance_number: str,
    image_type: str,
    pixel_matrix_columns: str,
    pixel_matrix_rows: str,
) -> Tuple[str, str, str, str, str]:
  return (
      ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid,
      instance_number,
      image_type,
      pixel_matrix_columns,
      pixel_matrix_rows,
  )


def _create_instance_number_assignment_json(
    sop_instance_uid: str, test_data: Tuple[str, str, str, str, str]
) -> Dict[str, Any]:
  (
      sop_class_uid,
      instance_number,
      image_type,
      total_pixel_matrix_columns,
      total_pixel_matrix_rows,
  ) = test_data
  return {
      _STUDY_INSTANCE_UID: {'Value': ['1.2.3']},
      _SERIES_INSTANCE_UID: {'Value': ['1.2.3.4']},
      _SOP_INSTANCE_UID: {'Value': [sop_instance_uid]},
      _SOP_CLASS_UID: {'Value': [sop_class_uid]},
      _INSTANCE_NUMBER: {'Value': [instance_number]},
      _IMAGE_TYPE: {'Value': [image_type]},
      _TOTAL_PIXEL_MATRIX_COLUMNS: {'Value': [total_pixel_matrix_columns]},
      _TOTAL_PIXEL_MATRIX_ROWS: {'Value': [total_pixel_matrix_rows]},
  }


def icc_bytes() -> bytes:
  path = gen_test_util.test_file_path('profile.icc')
  with open(path, 'rb') as infile:
    return infile.read()


class IngestWsiDicomTest(parameterized.TestCase):

  def test_get_icc_profile_from_general_image(self):
    ds = pydicom.Dataset()
    ds.ICCProfile = b'1234'
    fs = pydicom.FileDataset('test.dcm', dataset=ds)

    self.assertEqual(ingest_wsi_dicom._get_icc_profile(fs), b'1234')

  def test_get_icc_profile_from_optical_path_sequence(self):
    inner_ds = pydicom.Dataset()
    inner_ds.ICCProfile = b'123456'
    ds = pydicom.Dataset()
    ds.OpticalPathSequence = pydicom.Sequence([inner_ds])
    fs = pydicom.FileDataset('test.dcm', dataset=ds)

    self.assertEqual(ingest_wsi_dicom._get_icc_profile(fs), b'123456')

  def test_get_icc_profile_from_optical_path_raises(self):
    inner_ds = pydicom.Dataset()
    inner_ds.ICCProfile = b'123456'
    ds = pydicom.Dataset()
    ds.OpticalPathSequence = pydicom.Sequence([inner_ds, inner_ds])
    fs = pydicom.FileDataset('test.dcm', dataset=ds)

    with self.assertRaises(ValueError):
      ingest_wsi_dicom._get_icc_profile(fs)

  def test_get_icc_profile_missing(self):
    ds = pydicom.Dataset()
    fs = pydicom.FileDataset('test.dcm', dataset=ds)

    self.assertIsNone(ingest_wsi_dicom._get_icc_profile(fs))

  @mock.patch.object(
      PIL.ImageCms, 'ImageCmsProfile', return_value=mock.Mock(), autospec=True
  )
  @mock.patch.object(
      PIL.ImageCms,
      'getProfileDescription',
      return_value='result',
      autospec=True,
  )
  def test_add_correct_optical_path_sequence_from_ds(self, m1, m2):  # pylint: disable=unused-argument
    ds = pydicom.Dataset()
    profile = icc_bytes()
    ds.ICCProfile = profile
    fs = pydicom.FileDataset('test.dcm', dataset=ds)

    ingest_wsi_dicom._add_correct_optical_path_sequence(fs)

    self.assertLen(fs.OpticalPathSequence, 1)
    self.assertEqual(fs.OpticalPathSequence[0].ICCProfile, profile)
    self.assertEqual(fs.OpticalPathSequence[0].ColorSpace, 'RESULT')

  @mock.patch.object(
      PIL.ImageCms, 'ImageCmsProfile', return_value=mock.Mock(), autospec=True
  )
  @mock.patch.object(
      PIL.ImageCms,
      'getProfileDescription',
      return_value='result',
      autospec=True,
  )
  def test_add_correct_optical_path_sequence_from_ds_icc(self, m1, m2):  # pylint: disable=unused-argument
    ds = pydicom.Dataset()
    fs = pydicom.FileDataset('test.dcm', dataset=ds)

    ingest_wsi_dicom._add_correct_optical_path_sequence(fs, icc_bytes())

    self.assertLen(fs.OpticalPathSequence, 1)
    self.assertEqual(fs.OpticalPathSequence[0].ICCProfile, icc_bytes())
    self.assertEqual(fs.OpticalPathSequence[0].ColorSpace, 'RESULT')

  def test_add_correct_optical_path_sequence_missing(self):
    ds = pydicom.Dataset()
    fs = pydicom.FileDataset('test.dcm', dataset=ds)

    ingest_wsi_dicom._add_correct_optical_path_sequence(fs)

    self.assertLen(fs.OpticalPathSequence, 1)
    self.assertNotIn(
        ingest_const.DICOMTagKeywords.ICC_PROFILE, fs.OpticalPathSequence[0]
    )
    self.assertNotIn(
        ingest_const.DICOMTagKeywords.COLOR_SPACE, fs.OpticalPathSequence[0]
    )

  @mock.patch.object(
      PIL.ImageCms, 'ImageCmsProfile', return_value=mock.Mock(), autospec=True
  )
  @mock.patch.object(
      PIL.ImageCms,
      'getProfileDescription',
      return_value='result',
      autospec=True,
  )
  def test_add_correct_optical_path_sequence_missing_colorspace(self, m1, m2):  # pylint: disable=unused-argument
    profile = icc_bytes()
    inner_ds = pydicom.Dataset()
    inner_ds.ICCProfile = profile
    ds = pydicom.Dataset()
    ds.OpticalPathSequence = pydicom.Sequence([pydicom.Dataset(), inner_ds])
    fs = pydicom.FileDataset('test.dcm', dataset=ds)

    ingest_wsi_dicom._add_correct_optical_path_sequence(fs)

    self.assertLen(fs.OpticalPathSequence, 2)
    self.assertNotIn(
        ingest_const.DICOMTagKeywords.ICC_PROFILE, fs.OpticalPathSequence[0]
    )
    self.assertNotIn(
        ingest_const.DICOMTagKeywords.COLOR_SPACE, fs.OpticalPathSequence[0]
    )
    self.assertEqual(fs.OpticalPathSequence[1].ICCProfile, profile)
    self.assertEqual(fs.OpticalPathSequence[1].ColorSpace, 'RESULT')

  def test_read_zipfile_expected_length(self):
    temp_dir = self.create_tempdir()
    file_list = ingest_wsi_dicom._unarchive_zipfile(
        gen_test_util.test_file_path(_TEST_ZIP_FILE_NAME),
        temp_dir.full_path,
    )

    self.assertLen(file_list, 2)

  def test_read_zipfile_files_generates_expected_files(self):
    temp_dir = self.create_tempdir()
    file_list = ingest_wsi_dicom._unarchive_zipfile(
        gen_test_util.test_file_path(_TEST_ZIP_FILE_NAME),
        temp_dir.full_path,
    )

    for index, filepath in enumerate(file_list):
      expected_path = os.path.join(temp_dir, f'file_{index}.dcm')
      self.assertEqual(filepath, expected_path)

  def test_read_zipfile_files_generates_files_contain_expected_values(self):
    temp_dir = self.create_tempdir()
    file_list = ingest_wsi_dicom._unarchive_zipfile(
        gen_test_util.test_file_path(_TEST_ZIP_FILE_NAME),
        temp_dir.full_path,
    )
    expected_values = {'Hello\n', 'World\n'}

    for filepath in file_list:
      with open(filepath, 'rt') as infile:
        text = infile.read()
      self.assertIn(text, expected_values)
      expected_values.remove(text)
    self.assertEmpty(expected_values)

  def test_read_zipfile_non_zip_file(self):
    temp_dir = self.create_tempdir()
    non_zip_file_path = gen_test_util.test_file_path('barcode_datamatrix.jpg')
    file_list = ingest_wsi_dicom._unarchive_zipfile(
        non_zip_file_path, temp_dir.full_path
    )

    self.assertListEqual(file_list, [non_zip_file_path])

  def test_get_dicom_filerefs_list_no_valid_files(self):
    temp_dir = self.create_tempdir()
    bad_file_path = os.path.join(temp_dir, 'bad_file.txt')
    with open(bad_file_path, 'wt') as outfile:
      outfile.write('bad dicom file.')

    with self.assertRaises(ingest_base.GenDicomFailedError):
      ingest_wsi_dicom.get_dicom_filerefs_list([bad_file_path])

  def test_get_dicom_filerefs_list_valid_file(self):
    test_file_list = []
    temp_dir = self.create_tempdir()
    bad_file_path = os.path.join(temp_dir, 'bad_file.txt')
    with open(bad_file_path, 'wt') as outfile:
      outfile.write('bad dicom file.')
    test_file_list.append(bad_file_path)
    wikipedia_dicom_path = gen_test_util.test_file_path('test_wikipedia.dcm')
    test_dicom_path = os.path.join(temp_dir, 'temp.dcm')
    shutil.copyfile(wikipedia_dicom_path, test_dicom_path)
    test_file_list.append(test_dicom_path)

    dcm_refs = ingest_wsi_dicom.get_dicom_filerefs_list(test_file_list)

    self.assertLen(dcm_refs, 1)
    self.assertEqual(dcm_refs[0].source, test_dicom_path)

  def test_wsi_image_type_sort_key(self):
    ordered_list = [
        f'{_ORIGINAL}\\{_PRIMARY}\\{_VOLUME}',
        f'{_ORIGINAL}\\{_SECONDARY}\\{_VOLUME}',
        f'{_DERIVED}\\{_PRIMARY}\\{_VOLUME}',
        f'{_DERIVED}\\{_SECONDARY}\\{_VOLUME}',
        f'{_ORIGINAL}\\{_PRIMARY}\\{_LABEL}',
        f'{_ORIGINAL}\\{_SECONDARY}\\{_LABEL}',
        f'{_DERIVED}\\{_PRIMARY}\\{_LABEL}',
        f'{_DERIVED}\\{_SECONDARY}\\{_LABEL}',
        f'{_ORIGINAL}\\{_PRIMARY}\\{_THUMBNAIL}',
        f'{_ORIGINAL}\\{_SECONDARY}\\{_THUMBNAIL}',
        f'{_DERIVED}\\{_PRIMARY}\\{_THUMBNAIL}',
        f'{_DERIVED}\\{_SECONDARY}\\{_THUMBNAIL}',
        f'{_ORIGINAL}\\{_PRIMARY}\\{_OVERVIEW}',
        f'{_ORIGINAL}\\{_SECONDARY}\\{_OVERVIEW}',
        f'{_DERIVED}\\{_PRIMARY}\\{_OVERVIEW}',
        f'{_DERIVED}\\{_SECONDARY}\\{_OVERVIEW}',
    ]
    test_list = []
    for img_type in ordered_list:
      tst_json = {_IMAGE_TYPE: {'Value': [img_type]}}
      test_list.append(
          dicom_file_ref.init_from_json(
              tst_json, ingested_dicom_file_ref.IngestDicomFileRef()
          )
      )
    random.shuffle(test_list)

    test_list.sort(key=ingest_wsi_dicom._wsi_image_type_sort_key)

    self.assertListEqual([test.image_type for test in test_list], ordered_list)

  def test_other_instance_number_sort_key(self):
    test_list = []
    for instance_number in [None, '', '-100', '1', '10']:
      if instance_number is None:
        tst_json = {_INSTANCE_NUMBER: {}}
      else:
        tst_json = {_INSTANCE_NUMBER: {'Value': [instance_number]}}
      test_list.append(
          dicom_file_ref.init_from_json(
              tst_json, ingested_dicom_file_ref.IngestDicomFileRef()
          )
      )
    random.shuffle(test_list)

    test_list.sort(key=ingest_wsi_dicom._other_instance_number_sort_key)

    self.assertListEqual(
        [test.instance_number for test in test_list],
        ['', '', '-100', '1', '10'],
    )

  def test_dcmref_instance_key(self):
    tst_json = {
        _STUDY_INSTANCE_UID: {'Value': ['1.2']},
        _SERIES_INSTANCE_UID: {'Value': ['1.2.3']},
        _SOP_INSTANCE_UID: {'Value': ['1.2.3.4']},
    }
    tst_ref = dicom_file_ref.init_from_json(
        tst_json, ingested_dicom_file_ref.IngestDicomFileRef()
    )

    test_key = ingest_wsi_dicom._dcmref_instance_key(tst_ref)

    self.assertTupleEqual(test_key, ('1.2', '1.2.3', '1.2.3.4'))

  def test_pydicom_instance_key(self):
    wikipedia_dicom_path = gen_test_util.test_file_path('test_wikipedia.dcm')
    dcm = pydicom.dcmread(wikipedia_dicom_path)

    test_key = ingest_wsi_dicom._pydicom_instance_key(dcm)

    self.assertTupleEqual(
        test_key,
        (
            '1.2.276.0.7230010.3.1.2.296485376.46.1648688993.578030',
            '1.2.276.0.7230010.3.1.3.296485376.46.1648688993.578031',
            '1.2.276.0.7230010.3.1.4.296485376.46.1648688993.578032',
        ),
    )

  def test_remove_duplicate_generated_dicom_for_duplicate(self):
    temp_dir = self.create_tempdir()
    ds = typing.cast(
        pydicom.FileDataset,
        dicom_test_util.create_test_dicom_instance(
            dcm_json=dicom_test_util.create_metadata_dict()
        ),
    )
    ingested_dicom_path = os.path.join(temp_dir, 'test1.dcm')
    ds.SOPClassUID = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    ds.SOPInstanceUID = f'{ingest_const.DPAS_UID_PREFIX}.31234'
    ds.save_as(ingested_dicom_path, write_like_original=False)
    ds.AccessionNumber = 'C1234'
    test_dicom_path = [os.path.join(temp_dir, 'test2.dcm')]
    ds.save_as(test_dicom_path[0], write_like_original=False)
    dcm_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        ingested_dicom_path
    )

    self.assertEmpty(
        ingest_wsi_dicom._remove_duplicate_generated_dicom(
            test_dicom_path, [dcm_ref]
        ).file_paths
    )

  def test_remove_duplicate_generated_dicom_for_simulated_ingest_dup(self):
    # DICOM generated in C++ code (wsi2dicom) are initially not generated
    # with DPAS UID prefix or embedded hash codes. These are added later for
    # performance. De-duplication should be able to detect dicoms such as
    # these are dup of dicoms provided in DICOM ingest (b/237086607).
    temp_dir = self.create_tempdir()
    ds = typing.cast(
        pydicom.FileDataset,
        dicom_test_util.create_test_dicom_instance(
            dcm_json=dicom_test_util.create_metadata_dict()
        ),
    )
    ingested_dicom_path = os.path.join(temp_dir, 'test1.dcm')
    ds.SOPClassUID = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    ds.SOPInstanceUID = f'{ingest_const.DPAS_UID_PREFIX}.31234'
    ds.save_as(ingested_dicom_path, write_like_original=False)
    test_dicom_path = [os.path.join(temp_dir, 'test2.dcm')]
    # SOPINSTANCEUID should not start with DPAS UID Prefix.
    ds.SOPInstanceUID = '1.2.3.4.31234'
    if ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG in ds:
      # If has hash tag.  Remove it.
      del ds[ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG]
    ds.save_as(test_dicom_path[0], write_like_original=False)
    dcm_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        ingested_dicom_path
    )

    self.assertEmpty(
        ingest_wsi_dicom._remove_duplicate_generated_dicom(
            test_dicom_path, [dcm_ref]
        ).file_paths
    )

  def test_remove_duplicate_generated_dicom_for_simulated_ingest_dup2(self):
    # DICOM generated in C++ code (wsi2dicom) are initially not generated
    # with DPAS UID prefix or embedded hash codes. These are added later for
    # performance. De-duplication should be able to detect dicoms such as
    # these are dup of dicoms provided in DICOM ingest (b/237086607).
    # Generated dicom and ingested DICOM may not have same StudyUID if
    # dicom is ingested using the metadata as the source for the studyuid.
    temp_dir = self.create_tempdir()
    ds = typing.cast(
        pydicom.FileDataset,
        dicom_test_util.create_test_dicom_instance(
            dcm_json=dicom_test_util.create_metadata_dict()
        ),
    )
    ingested_dicom_path = os.path.join(temp_dir, 'test1.dcm')
    ds.StudyInstanceUID = '1.2.3.4'
    ds.SOPClassUID = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    ds.SOPInstanceUID = f'{ingest_const.DPAS_UID_PREFIX}.31234'
    ds.save_as(ingested_dicom_path, write_like_original=False)
    test_dicom_path = [os.path.join(temp_dir, 'test2.dcm')]
    # SOPINSTANCEUID should not start with DPAS UID Prefix.
    ds.SOPInstanceUID = '1.2.3.4.31234'
    ds.StudyInstanceUID = '1.2.5'
    if ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG in ds:
      # If tag has hash code.  Remove it.
      del ds[ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG]
    ds.save_as(test_dicom_path[0], write_like_original=False)
    dcm_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        ingested_dicom_path
    )

    self.assertEmpty(
        ingest_wsi_dicom._remove_duplicate_generated_dicom(
            test_dicom_path, [dcm_ref]
        ).file_paths
    )

  def test_DicomInstanceNumberAssignmentUtil(self):
    util = ingest_wsi_dicom._DicomInstanceNumberAssignmentUtil()
    test_cases = [
        _create_non_wsi_test_instance('99'),
        _create_non_wsi_test_instance('40'),
        _create_non_wsi_test_instance('140'),
        _create_non_wsi_test_instance(''),
        _create_non_wsi_test_instance('1'),
        _create_non_wsi_test_instance('1'),
        _create_non_wsi_test_instance('2'),
        _create_wsi_test_instance('1', _VOLUME, '1000', '2'),
        _create_wsi_test_instance('', _VOLUME, '999', '2'),
        _create_wsi_test_instance('2', _VOLUME, '500', '2'),
        _create_wsi_test_instance('3', _LABEL, '500', '2'),
        _create_wsi_test_instance('', _VOLUME, '2', '2'),
    ]
    test_list = []
    for count, test_data in enumerate(test_cases):
      sop_instance_uid = f'1.{count+1}'
      tst_json = _create_instance_number_assignment_json(
          sop_instance_uid, test_data
      )
      test_list.append(
          dicom_file_ref.init_from_json(
              tst_json, ingested_dicom_file_ref.IngestDicomFileRef()
          )
      )

    util.add_inst(test_list)
    instance_number_list = []
    for test_uid in range(1, len(test_cases) + 1):
      instance_number_list.append(
          util.get_instance_number(_create_pydicom_ds(f'1.{test_uid}'))
      )

    self.assertListEqual(
        instance_number_list, [99, 40, 140, None, 6, 6, 7, 1, 2, 3, 5, 4]
    )

  def test_DicomInstanceNumberAssignmentUtil_incorrect_adding_raises(self):
    util = ingest_wsi_dicom._DicomInstanceNumberAssignmentUtil()
    test_cases = [_create_non_wsi_test_instance('99')]
    test_list = []
    for count, test_data in enumerate(test_cases):
      sop_instance_uid = f'1.{count+1}'
      tst_json = _create_instance_number_assignment_json(
          sop_instance_uid, test_data
      )
      test_list.append(
          dicom_file_ref.init_from_json(
              tst_json, ingested_dicom_file_ref.IngestDicomFileRef()
          )
      )

    util.add_inst(test_list)
    for test_uid in range(1, len(test_cases) + 1):
      util.get_instance_number(_create_pydicom_ds(f'1.{test_uid}'))
    with self.assertRaises(ValueError):
      util.add_inst(test_list)

  @flagsaver.flagsaver(metadata_bucket='test')
  def test_determine_slideid_from_barcode(self):
    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata(
        [gen_test_util.test_file_path('metadata_duplicate.csv')]
    )
    barcode = 'SR-21-2-A1-5'
    filename = _TEST_INGEST_FILENAME_WITH_SLIDEID

    slide_id = ingest_wsi_dicom._determine_slideid(
        barcode, filename, [], meta_client
    )
    self.assertEqual(slide_id, barcode)

  @flagsaver.flagsaver(metadata_bucket='test')
  def test_determine_slideid_from_filename(self):
    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata(
        [gen_test_util.test_file_path('metadata_duplicate.csv')]
    )
    barcode = _BARCODE_NOT_LISTED_IN_METADATA
    filename = _TEST_INGEST_FILENAME_WITH_SLIDEID

    slide_id = ingest_wsi_dicom._determine_slideid(
        barcode, filename, [], meta_client
    )
    self.assertEqual(slide_id, filename[:12])

  @flagsaver.flagsaver(metadata_bucket='test')
  @flagsaver.flagsaver(testing_disable_cloudvision=True)
  def test_determine_slideid_from_image(self):
    saved_flag = FLAGS.zxing_cli
    try:
      FLAGS.zxing_cli = os.path.join(
          FLAGS.test_srcdir, 'third_party/zxing/zxing_cli'
      )
      meta_client = metadata_storage_client.MetadataStorageClient()
      meta_client.set_debug_metadata(
          [gen_test_util.test_file_path('metadata_duplicate.csv')]
      )
      barcode = _BARCODE_NOT_LISTED_IN_METADATA
      filename = _FILENAME_DOES_NOT_CONTAIN_SLIDE_ID
      wikipedia_dicom_path = gen_test_util.test_file_path('test_wikipedia.dcm')
      dcm_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
          wikipedia_dicom_path
      )

      slide_id = ingest_wsi_dicom._determine_slideid(
          barcode, filename, [dcm_ref], meta_client
      )
      self.assertEqual(slide_id, 'Wikipedia')
    finally:
      FLAGS.zxing_cli = saved_flag

  @flagsaver.flagsaver(metadata_bucket='test')
  @flagsaver.flagsaver(testing_disable_cloudvision=True)
  def test_determine_slideid_from_cannot_load_from_image(self):
    saved_flag = FLAGS.zxing_cli
    try:
      FLAGS.zxing_cli = os.path.join(
          FLAGS.test_srcdir, 'third_party/zxing/zxing_cli'
      )
      meta_client = metadata_storage_client.MetadataStorageClient()
      meta_client.set_debug_metadata(
          [gen_test_util.test_file_path('metadata_duplicate.csv')]
      )
      barcode = _BARCODE_NOT_LISTED_IN_METADATA
      filename = _FILENAME_DOES_NOT_CONTAIN_SLIDE_ID
      dicom_path = gen_test_util.test_file_path('test_dicom_matrix.dcm')
      dcm_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(dicom_path)

      with self.assertRaises(ingest_base.GenDicomFailedError):
        ingest_wsi_dicom._determine_slideid(
            barcode, filename, [dcm_ref], meta_client
        )

    finally:
      FLAGS.zxing_cli = saved_flag

  @mock.patch.object(
      PIL.ImageCms, 'ImageCmsProfile', return_value=mock.Mock(), autospec=True
  )
  @mock.patch.object(
      PIL.ImageCms,
      'getProfileDescription',
      return_value='result',
      autospec=True,
  )
  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_add_metadata_to_generated_image(self, unused_mk1, unused_mk2):
    temp_dir = self.create_tempdir()
    wikipedia_dicom_path = gen_test_util.test_file_path('test_wikipedia.dcm')
    test_dicom_path = os.path.join(temp_dir, 'temp.dcm')
    temp_wiki = os.path.join(temp_dir, 'temp_wiki.dcm')
    shutil.copyfile(wikipedia_dicom_path, temp_wiki)
    shutil.copyfile(wikipedia_dicom_path, test_dicom_path)

    # Add ICCProfile root to test that generated DICOM detects and
    # adds ICCProfile embedded at DICOM ROOT in generated DICOM.
    icc_profile_bytes = icc_bytes()
    with pydicom.dcmread(temp_wiki) as dcm:
      dcm.ICCProfile = icc_profile_bytes
      dcm.save_as(temp_wiki)

    source_dicom_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        temp_wiki
    )
    source_dicom_ref.set_tag_value('BurnedInAnnotation', 'No')
    source_dicom_ref.set_tag_value('SpecimenLabelInImage', 'No')
    gen_dicom = [test_dicom_path]
    patient_name = 'Test^Bob'
    patient_id = '1234-54321'
    metadata = _test_json_dicom_metadata(patient_name, patient_id)
    test_pubsubmessage_id = 'pubsub_message_id=1'
    dicom_gen = abstract_dicom_generation.GeneratedDicomFiles('/test.svs', None)
    private_tags = abstract_dicom_generation.get_private_tags_for_gen_dicoms(
        dicom_gen, test_pubsubmessage_id
    )
    gen_file_ref = dicom_file_ref.init_from_file(
        test_dicom_path, ingested_dicom_file_ref.IngestDicomFileRef()
    )
    instance_util = ingest_wsi_dicom._DicomInstanceNumberAssignmentUtil()
    instance_util.add_inst([gen_file_ref])

    study_instance_uid = '1.8.9'
    gen_dicom_paths = ingest_wsi_dicom._add_metadata_to_generated_dicom_files(
        source_dicom_ref,
        gen_dicom,
        private_tags,
        instance_util,
        metadata,
        study_instance_uid,
        dicom_util.DicomFrameOfReferenceModuleMetadata('', 'Edge'),
    )

    self.assertLen(gen_dicom_paths, 1)
    with pydicom.dcmread(gen_dicom_paths[0]) as ds:
      self.assertEqual(ds.BurnedInAnnotation, 'No')
      self.assertEqual(ds.SpecimenLabelInImage, 'No')
      self.assertEqual(ds.InstanceNumber, 1)
      self.assertEqual(ds.PatientName, patient_name)
      self.assertEqual(ds.PatientID, patient_id)
      self.assertEqual(ds.StudyInstanceUID, study_instance_uid)
      self.assertEqual(ds.FrameOfReferenceUID, '')
      self.assertEqual(ds.PositionReferenceIndicator, 'Edge')
      self.assertEqual(ds.OpticalPathSequence[0].ICCProfile, icc_profile_bytes)
      self.assertEqual(
          ds[ingest_const.DICOMTagKeywords.PUBSUB_MESSAGE_ID_TAG].value,
          test_pubsubmessage_id,
      )
      self.assertEqual(
          ds[ingest_const.DICOMTagKeywords.INGEST_FILENAME_TAG].value,
          'test.svs',
      )

  @parameterized.parameters([
      ({}, False),
      ({}, True),
      ({'ImagedVolumeWidth': ''}, True),
      ({'ImagedVolumeHeight': ''}, True),
      ({'TotalPixelMatrixRows': '0'}, True),
      ({'TotalPixelMatrixColumns': '0'}, True),
      ({'ImageType': ingest_const.OVERVIEW}, True),
      ({'ImageType': ingest_const.THUMBNAIL}, True),
      ({'ImageType': ingest_const.LABEL}, True),
  ])
  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_add_metadata_to_ingested_image(
      self,
      param_metadata: Mapping[str, str],
      init_highest_magnification_image: bool,
  ):
    temp_dir = self.create_tempdir()
    wikipedia_dicom_path = gen_test_util.test_file_path('test_wikipedia.dcm')
    test_dicom_path = os.path.join(temp_dir, 'temp.dcm')
    shutil.copyfile(wikipedia_dicom_path, test_dicom_path)
    source_dicom_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        test_dicom_path
    )
    patient_name = 'Test^Susan'
    patient_id = '54321-54321'
    metadata = _test_json_dicom_metadata(patient_name, patient_id)
    test_pubsubmessage_id = 'pubsub_message_id=1'
    dicom_gen = abstract_dicom_generation.GeneratedDicomFiles('/test.svs', None)
    private_tags = abstract_dicom_generation.get_private_tags_for_gen_dicoms(
        dicom_gen, test_pubsubmessage_id
    )
    for tag_name, tag_value in param_metadata.items():
      source_dicom_ref.set_tag_value(tag_name, tag_value)
    highest_mag_file_ref = (
        source_dicom_ref if init_highest_magnification_image else None
    )
    instance_util = ingest_wsi_dicom._DicomInstanceNumberAssignmentUtil()
    instance_util.add_inst([source_dicom_ref])

    study_instance_uid = '1.8.9'
    gen_dicom_paths = ingest_wsi_dicom._add_metadata_to_ingested_wsi_dicom(
        [source_dicom_ref],
        private_tags,
        instance_util,
        study_instance_uid,
        metadata,
        dicom_util.DicomFrameOfReferenceModuleMetadata('', ''),
        highest_mag_file_ref,
    )

    self.assertLen(gen_dicom_paths, 1)
    ds = pydicom.dcmread(gen_dicom_paths[0])
    self.assertEqual(ds.InstanceNumber, 1)
    self.assertEqual(ds.StudyInstanceUID, study_instance_uid)
    self.assertEqual(ds.PatientName, patient_name)
    self.assertEqual(ds.PatientID, patient_id)
    self.assertEqual(
        ds[ingest_const.DICOMTagKeywords.PUBSUB_MESSAGE_ID_TAG].value,
        test_pubsubmessage_id,
    )
    self.assertEqual(
        ds[ingest_const.DICOMTagKeywords.INGEST_FILENAME_TAG].value, 'test.svs'
    )
    self.assertEmpty(ds.FrameOfReferenceUID)
    self.assertEmpty(ds.PositionReferenceIndicator)

  @flagsaver.flagsaver(
      pod_hostname='1234',
      dicom_guid_prefix=uid_generator.TEST_UID_PREFIX,
      metadata_bucket='test',
  )
  def test_add_metadata_to_non_wsi_ingested_image(self):
    temp_dir = self.create_tempdir()
    wikipedia_dicom_path = gen_test_util.test_file_path('test_wikipedia.dcm')
    test_dicom_path = os.path.join(temp_dir, 'temp.dcm')
    shutil.copyfile(wikipedia_dicom_path, test_dicom_path)
    source_dicom_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        test_dicom_path
    )

    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata([
        gen_test_util.test_file_path('metadata_duplicate.csv'),
        gen_test_util.test_file_path('example_schema_wsi.json'),
    ])
    csv_metadata = meta_client.get_slide_metadata_from_csv('MD-01-1-A1-1')
    csv_metadata = dicom_schema_util.PandasMetadataTableWrapper(csv_metadata)
    default_metadata = _test_json_dicom_metadata('Test^Susan', '54321-54321')
    test_pubsubmessage_id = 'pubsub_message_id=1'
    dicom_gen = abstract_dicom_generation.GeneratedDicomFiles('/test.svs', None)
    private_tags = abstract_dicom_generation.get_private_tags_for_gen_dicoms(
        dicom_gen, test_pubsubmessage_id
    )

    instance_util = ingest_wsi_dicom._DicomInstanceNumberAssignmentUtil()
    instance_util.add_inst([source_dicom_ref])

    gen_dicom = ingest_wsi_dicom._add_metadata_to_other_ingested_dicom(
        [source_dicom_ref],
        private_tags,
        instance_util,
        csv_metadata,
        meta_client,
        default_metadata,
        '1.2.3.4',
    )

    self.assertLen(gen_dicom, 1)
    ds = pydicom.dcmread(gen_dicom[0])
    self.assertEqual(ds.StudyInstanceUID, '1.2.3.4')
    self.assertEqual(ds.InstanceNumber, 1)
    self.assertEqual(ds.PatientName, 'Jefferson^Thomas')
    self.assertEqual(ds.PatientID, 'R01236')
    self.assertEqual(ds.BarcodeValue, 'MD-01-1-A1-1')
    self.assertEqual(
        ds[ingest_const.DICOMTagKeywords.PUBSUB_MESSAGE_ID_TAG].value,
        test_pubsubmessage_id,
    )
    self.assertEqual(
        ds[ingest_const.DICOMTagKeywords.INGEST_FILENAME_TAG].value, 'test.svs'
    )

  @parameterized.parameters([
      ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID,
      ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID,
      ingest_const.DICOMTagKeywords.IMAGED_VOLUME_WIDTH,
      ingest_const.DICOMTagKeywords.IMAGED_VOLUME_HEIGHT,
      ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_COLUMNS,
      ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_ROWS,
      ingest_const.DICOMTagKeywords.MODALITY,
      ingest_const.DICOMTagKeywords.COLUMNS,
      ingest_const.DICOMTagKeywords.ROWS,
  ])
  def test_remove_duplicate_generated_dicom_for_different_dicom(
      self, tag_keyword
  ):
    temp_dir = self.create_tempdir()
    ds = typing.cast(
        pydicom.FileDataset,
        dicom_test_util.create_test_dicom_instance(
            dcm_json=dicom_test_util.create_metadata_dict()
        ),
    )
    ingested_dicom_path = os.path.join(temp_dir, 'test1.dcm')
    ds.SOPClassUID = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    ds.SOPInstanceUID = f'{ingest_const.DPAS_UID_PREFIX}.31234'
    ds.save_as(ingested_dicom_path, write_like_original=False)
    test_dicom_path = [os.path.join(temp_dir, 'test2.dcm')]
    pydicom_util.set_dataset_tag_value(ds, tag_keyword, '0')
    ds.save_as(test_dicom_path[0], write_like_original=False)
    dcm_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        ingested_dicom_path
    )
    if tag_keyword == ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID:
      self.assertEmpty(
          ingest_wsi_dicom._remove_duplicate_generated_dicom(
              test_dicom_path, [dcm_ref]
          ).file_paths
      )
    else:
      self.assertLen(
          ingest_wsi_dicom._remove_duplicate_generated_dicom(
              test_dicom_path, [dcm_ref]
          ).file_paths,
          1,
      )


if __name__ == '__main__':
  absltest.main()
