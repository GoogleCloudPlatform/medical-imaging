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
from __future__ import annotations

import contextlib
import copy
import json
import math
import os
import random
import shutil
import subprocess
import time
import typing
from typing import Any, Dict, List, Mapping, Set, Tuple
from unittest import mock
import zipfile

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import pydicom

from shared_libs.logging_lib import cloud_logging_client
from shared_libs.test_utils.dicom_store_mock import dicom_store_mock
from shared_libs.test_utils.gcs_mock import gcs_mock
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_file_ref
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_schema_util
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_gcs_handler
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
_MOCK_DICOM_GEN_UID = '1.2.3.9'

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
_MOCK_DICOM_GEN_UID = '1.2.3.9'


def _test_json_dicom_metadata(
    study_uid: str, series_uid: str, patient_name: str, patient_id: str
) -> Dict[str, Any]:
  ds = pydicom.Dataset()
  ds.StudyInstanceUID = study_uid
  ds.SeriesInstanceUID = series_uid
  ds.PatientName = patient_name
  ds.PatientID = patient_id
  return ds.to_json_dict()


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


def _get_instances_json(dicom_paths: Set[str]) -> List[Dict[str, Any]]:
  sorted_dicoms = sorted(
      [pydicom.dcmread(path) for path in dicom_paths],
      key=lambda x: x.SOPInstanceUID,
  )
  for dicom in sorted_dicoms:
    del dicom['PixelData']
    try:
      del dicom['30211001']
    except KeyError:
      pass
  return [dicom.to_json_dict() for dicom in sorted_dicoms]


def _load_expected_json(
    expected_json: str,
    override_study_uid_with_metadata: bool,
    dcm: pydicom.Dataset,
) -> List[Dict[str, Any]]:
  with open(gen_test_util.test_file_path(expected_json), 'rt') as infile:
    expected = json.load(infile)
    first_type_element = dcm.ImageType
    if not isinstance(first_type_element, str):
      first_type_element = first_type_element[0]
    for dcm_json in expected:
      dcm_json['00080008']['Value'][0] = first_type_element
      try:
        dcm_json['52009229']['Value'][0]['00400710']['Value'][0]['00089007'][
            'Value'
        ][0] = first_type_element
      except (KeyError, IndexError) as _:
        pass
      if not override_study_uid_with_metadata:
        dcm_json['0020000D']['Value'][0] = dcm.StudyInstanceUID
      try:
        del dcm_json['30211001']
      except KeyError:
        pass
  return expected


def _add_required_dicom_metadata_for_valid_wsi(dcm: pydicom.Dataset) -> None:
  if 'BarcodeValue' not in dcm:
    dcm.BarcodeValue = _BARCODE_NOT_LISTED_IN_METADATA
  if 'SpecimenLabelInImage' not in dcm:
    dcm.SpecimenLabelInImage = 'NO'
  if 'BurnedInAnnotation' not in dcm:
    dcm.BurnedInAnnotation = 'NO'


class _IngestWsiDicomTest(contextlib.ExitStack):

  def __init__(
      self,
      test_instance: parameterized.TestCase,
      input_file: pydicom.FileDataset,
      ingest_filename: str,
      schema='example_schema_wsi.json',
      metadata='metadata.csv',
      dicom_store_url='https://healthcare.googleapis.com/v1/dcm_store',
  ):
    super().__init__()
    self._test_instance = test_instance
    self._dicom_store_url = dicom_store_url
    self._input_bucket_path = self._test_instance.create_tempdir()
    self._filename = ingest_filename
    self._input_file_container_path = os.path.join(
        self._test_instance.create_tempdir(), self._filename
    )
    input_file.save_as(os.path.join(self._input_bucket_path, self._filename))
    input_file.save_as(self._input_file_container_path)

    metadata_path = self._test_instance.create_tempdir()
    shutil.copyfile(
        gen_test_util.test_file_path(schema),
        os.path.join(metadata_path, schema),
    )
    if metadata:
      shutil.copyfile(
          gen_test_util.test_file_path(metadata),
          os.path.join(metadata_path, metadata),
      )
    self.enter_context(
        gcs_mock.GcsMock({
            'input': self._input_bucket_path,
            'output': self._test_instance.create_tempdir(),
            'metadata': metadata_path,
        })
    )
    self.enter_context(dicom_store_mock.MockDicomStores(self._dicom_store_url))
    self.dicom_gen = abstract_dicom_generation.GeneratedDicomFiles(
        self._input_file_container_path, f'gs://input/{ self._filename}'
    )
    self.handler: ingest_gcs_handler.IngestGcsPubSubHandler = None

  def __enter__(self) -> _IngestWsiDicomTest:
    super().__enter__()
    self.handler = ingest_gcs_handler.IngestGcsPubSubHandler(
        'gs://output/success',
        'gs://output/failure',
        self._dicom_store_url,
        frozenset(),
        oof_trigger_config=None,
        metadata_client=metadata_storage_client.MetadataStorageClient(),
    )
    self.handler.root_working_dir = (
        self._test_instance.create_tempdir().full_path
    )
    os.mkdir(self.handler.img_dir)
    return self


def _ingest_wsi_dicom_generate_dicom_test_shim(
    test_instance: parameterized.TestCase,
    source_dicom: pydicom.FileDataset,
    downsampled_factors: List[int],
    override_study_uid_with_metadata: bool,
    slide_id: str = 'MD-04-3-A1-2',
) -> ingest_base.GenDicomResult:
  filename = os.path.basename(source_dicom.filename)
  filename = f'{slide_id}_{filename}'
  study_uid_source = (
      ingest_flags.UidSource.METADATA
      if override_study_uid_with_metadata
      else ingest_flags.UidSource.DICOM
  )
  with flagsaver.flagsaver(
      gcs_ingest_study_instance_uid_source=study_uid_source
  ):
    with _IngestWsiDicomTest(
        test_instance,
        source_dicom,
        filename,
        metadata='metadata.csv',
    ) as ingest_test:
      ingest = ingest_wsi_dicom.IngestWsiDicom(
          ingest_test.handler._ingest_buckets,
          is_oof_ingestion_enabled=False,
          override_study_uid_with_metadata=override_study_uid_with_metadata,
          metadata_client=metadata_storage_client.MetadataStorageClient(),
      )
      ingest.init_handler_for_ingestion()
      ingest.update_metadata()
      ingest.get_slide_id(ingest_test.dicom_gen, ingest_test.handler)
      dicom_gen_dir = os.path.join(
          ingest_test.handler.root_working_dir, 'gen_dicom'
      )
      os.mkdir(dicom_gen_dir)
      downsampled_dicom = copy.deepcopy(source_dicom)
      for factor in downsampled_factors:
        output_path = os.path.join(
            dicom_gen_dir, f'downsample-{factor}-foo.dcm'
        )
        if factor > 1:
          # Mock downsampling by just changing metadata.
          # actual pixels are not downsampled.
          height = int(source_dicom.TotalPixelMatrixRows / factor)
          width = int(source_dicom.TotalPixelMatrixColumns / factor)
          number_of_frames = int(
              math.ceil(height / source_dicom.Rows)
              * math.ceil(width / source_dicom.Columns)
          )
          downsampled_dicom.TotalPixelMatrixRows = height
          downsampled_dicom.TotalPixelMatrixColumns = width
          downsampled_dicom.NumberOfFrames = number_of_frames
        downsampled_dicom.save_as(output_path)
      return ingest.generate_dicom(
          dicom_gen_dir,
          ingest_test.dicom_gen,
          'mock_pubsub_msg_id',
          ingest_test.handler,
      )


class IngestWsiDicomTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(metadata_bucket='metadata'))
    self.enter_context(
        mock.patch.object(
            dicom_util,
            '_get_colorspace_description_from_iccprofile_bytes',
            autospec=True,
            return_value='SRGB',
        )
    )
    self.enter_context(
        mock.patch.object(
            dicom_util,
            '_get_srgb_iccprofile',
            autospec=True,
            return_value='SRGB_ICCPROFILE_BYTES'.encode('utf-8'),
        )
    )

  def _get_unique_test_file_path(self, filename: str) -> str:  # pylint: disable=g-unreachable-test-method
    path = gen_test_util.test_file_path(filename)
    temp_path = os.path.join(self.create_tempdir(), filename)
    shutil.copyfile(path, temp_path)
    return temp_path

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

  def test_add_correct_optical_path_sequence_from_ds(self):
    ds = pydicom.Dataset()
    profile = icc_bytes()
    ds.ICCProfile = profile
    fs = pydicom.FileDataset('test.dcm', dataset=ds)

    ingest_wsi_dicom._add_correct_optical_path_sequence(fs)

    self.assertLen(fs.OpticalPathSequence, 1)
    self.assertEqual(fs.OpticalPathSequence[0].ICCProfile, profile)
    self.assertEqual(fs.OpticalPathSequence[0].ColorSpace, 'SRGB')

  def test_add_correct_optical_path_sequence_from_ds_icc(self):
    ds = pydicom.Dataset()
    fs = pydicom.FileDataset('test.dcm', dataset=ds)

    ingest_wsi_dicom._add_correct_optical_path_sequence(fs, icc_bytes())

    self.assertLen(fs.OpticalPathSequence, 1)
    self.assertEqual(fs.OpticalPathSequence[0].ICCProfile, icc_bytes())
    self.assertEqual(fs.OpticalPathSequence[0].ColorSpace, 'SRGB')

  def test_add_correct_optical_path_sequence_missing(self):
    ds = pydicom.Dataset()
    fs = pydicom.FileDataset('test.dcm', dataset=ds)

    ingest_wsi_dicom._add_correct_optical_path_sequence(fs)

    self.assertLen(fs.OpticalPathSequence, 1)
    self.assertEqual(
        fs.OpticalPathSequence[0].ICCProfile, b'SRGB_ICCPROFILE_BYTES\x00'
    )
    self.assertEqual(fs.OpticalPathSequence[0].ColorSpace, 'SRGB')

  def test_add_correct_optical_path_sequence_missing_colorspace(self):
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
    self.assertEqual(fs.OpticalPathSequence[1].ColorSpace, 'SRGB')

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

  def test_get_dicom_filerefs_list_no_valid_pydicom_files_raises(self):
    bad_file_path = os.path.join(self.create_tempdir(), 'bad_file.txt')
    with open(bad_file_path, 'wt') as outfile:
      outfile.write('bad dicom file.')
    with self.assertRaises(ingest_base.GenDicomFailedError):
      ingest_wsi_dicom.get_dicom_filerefs_list([bad_file_path])

  def test_get_dicom_filerefs_list_no_files_missing_transfer_syntax_raises(
      self,
  ):
    path = os.path.join(self.create_tempdir(), 'test_dicom.dcm')
    file_meta = pydicom.dataset.FileMetaDataset()
    ds = pydicom.dataset.FileDataset(
        '', pydicom.Dataset(), file_meta=file_meta, preamble=b'\0' * 128
    )
    ds.save_as(path)
    with self.assertRaises(ingest_base.GenDicomFailedError):
      ingest_wsi_dicom.get_dicom_filerefs_list([path])

  def test_get_dicom_filerefs_list_valid_file(self):
    test_file_list = []
    bad_file_path = os.path.join(self.create_tempdir(), 'bad_file.txt')
    with open(bad_file_path, 'wt') as outfile:
      outfile.write('bad dicom file.')
    test_file_list.append(bad_file_path)
    test_dicom_path = self._get_unique_test_file_path('test_wikipedia.dcm')
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

  @parameterized.named_parameters([
      dict(
          testcase_name='filename_preceeds_barcode',
          filename=_TEST_INGEST_FILENAME_WITH_SLIDEID,
          expected='MD-01-1-A1-1',
      ),
      dict(
          testcase_name='barcode_if_not_from_filename',
          filename='not_found.svs',
          expected='SR-21-2-A1-5',
      ),
  ])
  def test_get_slide_id_from_(self, filename, expected):
    temp_dir = self.create_tempdir()
    dicom_path = os.path.join(temp_dir, filename)
    gen_test_util.write_test_dicom(
        dicom_path,
        gen_test_util.create_mock_wsi_dicom_dataset(
            barcode_value='SR-21-2-A1-5'
        ),
    )
    ingest_dicom = ingest_wsi_dicom.IngestWsiDicom(
        ingest_base.GcsIngestionBuckets('gs://success', 'gs://failure'),
        False,
        True,
        metadata_storage_client.MetadataStorageClient(),
    )
    ingest_dicom.metadata_storage_client.set_debug_metadata(
        [gen_test_util.test_file_path('metadata_duplicate.csv')]
    )
    handler = mock.create_autospec(
        ingest_gcs_handler.IngestGcsPubSubHandler, instance=True
    )
    handler.img_dir = self.create_tempdir().full_path
    self.assertEqual(
        ingest_dicom.get_slide_id(
            abstract_dicom_generation.GeneratedDicomFiles(
                dicom_path, 'gs://mock/path.dcm'
            ),
            handler,
        ),
        expected,
    )

  @flagsaver.flagsaver(testing_disable_cloudvision=True)
  def test_get_slide_id_from_image(self):
    with flagsaver.flagsaver(
        zxing_cli=os.path.join(
            FLAGS.test_srcdir, 'third_party/zxing/zxing_cli'
        )
    ):
      # Create two images and zip images togeather. ID determined from
      # zipped label images.
      temp_dir = self.create_tempdir()
      with pydicom.dcmread(
          gen_test_util.test_file_path('test_wikipedia.dcm')
      ) as dcm:
        _add_required_dicom_metadata_for_valid_wsi(dcm)
        dcm.SOPClassUID = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
        dcm.ImageType = '\\'.join(
            [ingest_const.ORIGINAL, ingest_const.PRIMARY, ingest_const.LABEL]
        )
        fp1 = os.path.join(temp_dir, 'file1.dcm')
        dcm.save_as(fp1)
        dcm.ImageType = '\\'.join(
            [ingest_const.ORIGINAL, ingest_const.PRIMARY, ingest_const.VOLUME]
        )
        fp2 = os.path.join(temp_dir, 'file2.dcm')
        dcm.SOPInstanceUID = '1.2'
        dcm.save_as(fp2)
      dicom_path = os.path.join(temp_dir, _FILENAME_DOES_NOT_CONTAIN_SLIDE_ID)
      with zipfile.ZipFile(dicom_path, 'w') as myzip:
        myzip.write(fp1)
        myzip.write(fp2)

      ingest_dicom = ingest_wsi_dicom.IngestWsiDicom(
          ingest_base.GcsIngestionBuckets('gs://success', 'gs://failure'),
          False,
          True,
          metadata_storage_client.MetadataStorageClient(),
      )
      ingest_dicom.metadata_storage_client.set_debug_metadata(
          [gen_test_util.test_file_path('metadata_duplicate.csv')]
      )
      handler = mock.create_autospec(
          ingest_gcs_handler.IngestGcsPubSubHandler, instance=True
      )
      handler.img_dir = os.path.join(temp_dir, 'handler')
      os.mkdir(handler.img_dir)
      self.assertEqual(
          ingest_dicom.get_slide_id(
              abstract_dicom_generation.GeneratedDicomFiles(
                  dicom_path, 'gs://mock/path.dcm'
              ),
              handler,
          ),
          'Wikipedia',
      )

  @flagsaver.flagsaver(testing_disable_cloudvision=True)
  def test_get_slide_id_cannot_determine_slide_id_raises(self):
    with flagsaver.flagsaver(
        zxing_cli=os.path.join(
            FLAGS.test_srcdir, 'third_party/zxing/zxing_cli'
        )
    ):
      # Create two images and zip images togeather. ID determined from
      # zipped label images.
      temp_dir = self.create_tempdir()
      with pydicom.dcmread(
          gen_test_util.test_file_path('test_dicom_matrix.dcm')
      ) as dcm:
        _add_required_dicom_metadata_for_valid_wsi(dcm)
        dcm.SOPClassUID = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
        dcm.ImageType = '\\'.join(
            [ingest_const.ORIGINAL, ingest_const.PRIMARY, ingest_const.LABEL]
        )
        fp1 = os.path.join(temp_dir, 'file1.dcm')
        dcm.save_as(fp1)
        dcm.ImageType = '\\'.join(
            [ingest_const.ORIGINAL, ingest_const.PRIMARY, ingest_const.VOLUME]
        )
        fp2 = os.path.join(temp_dir, 'file2.dcm')
        dcm.SOPInstanceUID = '1.2'
        dcm.save_as(fp2)
      dicom_path = os.path.join(temp_dir, _FILENAME_DOES_NOT_CONTAIN_SLIDE_ID)
      with zipfile.ZipFile(dicom_path, 'w') as myzip:
        myzip.write(fp1)
        myzip.write(fp2)

      ingest_dicom = ingest_wsi_dicom.IngestWsiDicom(
          ingest_base.GcsIngestionBuckets('gs://success', 'gs://failure'),
          False,
          True,
          metadata_storage_client.MetadataStorageClient(),
      )
      ingest_dicom.metadata_storage_client.set_debug_metadata(
          [gen_test_util.test_file_path('metadata_duplicate.csv')]
      )
      handler = mock.create_autospec(
          ingest_gcs_handler.IngestGcsPubSubHandler, instance=True
      )
      handler.img_dir = os.path.join(temp_dir, 'handler')
      os.mkdir(handler.img_dir)
      dest_uri = ''
      try:
        ingest_dicom.get_slide_id(
            abstract_dicom_generation.GeneratedDicomFiles(
                dicom_path, 'gs://mock/path.dcm'
            ),
            handler,
        )
      except ingest_base.DetermineSlideIDError as exp:
        dest_uri = exp._dest_uri
      self.assertEqual(
          dest_uri,
          'gs://failure/slide_id_error__slide_metadata_primary_key_missing_from_csv_metadata',
      )

  @flagsaver.flagsaver(
      testing_disable_cloudvision=True,
      enable_metadata_free_ingestion=True,
  )
  def test_get_slide_id_cannot_determine_slide_id_triggers_metadata_free_id(
      self,
  ):
    with flagsaver.flagsaver(
        zxing_cli=os.path.join(
            FLAGS.test_srcdir, 'third_party/zxing/zxing_cli'
        )
    ):
      # Create two images and zip images togeather. ID determined from
      # zipped label images.
      temp_dir = self.create_tempdir()
      with pydicom.dcmread(
          gen_test_util.test_file_path('test_dicom_matrix.dcm')
      ) as dcm:
        _add_required_dicom_metadata_for_valid_wsi(dcm)
        dcm.SOPClassUID = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
        dcm.ImageType = '\\'.join(
            [ingest_const.ORIGINAL, ingest_const.PRIMARY, ingest_const.LABEL]
        )
        fp1 = os.path.join(temp_dir, 'file1.dcm')
        dcm.save_as(fp1)
        dcm.ImageType = '\\'.join(
            [ingest_const.ORIGINAL, ingest_const.PRIMARY, ingest_const.VOLUME]
        )
        fp2 = os.path.join(temp_dir, 'file2.dcm')
        dcm.SOPInstanceUID = '1.2'
        dcm.save_as(fp2)
      dicom_path = os.path.join(temp_dir, _FILENAME_DOES_NOT_CONTAIN_SLIDE_ID)
      with zipfile.ZipFile(dicom_path, 'w') as myzip:
        myzip.write(fp1)
        myzip.write(fp2)

      ingest_dicom = ingest_wsi_dicom.IngestWsiDicom(
          ingest_base.GcsIngestionBuckets('gs://success', 'gs://failure'),
          False,
          True,
          metadata_storage_client.MetadataStorageClient(),
      )
      ingest_dicom.metadata_storage_client.set_debug_metadata(
          [gen_test_util.test_file_path('metadata_duplicate.csv')]
      )
      handler = mock.create_autospec(
          ingest_gcs_handler.IngestGcsPubSubHandler, instance=True
      )
      handler.img_dir = os.path.join(temp_dir, 'handler')
      os.mkdir(handler.img_dir)
      self.assertEqual(
          ingest_dicom.get_slide_id(
              abstract_dicom_generation.GeneratedDicomFiles(
                  dicom_path, 'gs://mock/path.dcm'
              ),
              handler,
          ),
          'NotListed-1-A1-1_ingest',
      )

  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_add_metadata_to_generated_image(self):
    test_dicom_path = self._get_unique_test_file_path('test_wikipedia.dcm')
    temp_wiki = self._get_unique_test_file_path('test_wikipedia.dcm')

    # Add ICCProfile root to test that generated DICOM detects and
    # adds ICCProfile embedded at DICOM ROOT in generated DICOM.
    icc_profile_bytes = icc_bytes()
    with pydicom.dcmread(temp_wiki) as dcm:
      dcm.AccessionNumber = '123'  # test data does overwrite existing metadata
      dcm.ICCProfile = icc_profile_bytes
      _add_required_dicom_metadata_for_valid_wsi(dcm)
      dcm.save_as(temp_wiki)

    with pydicom.dcmread(test_dicom_path) as dcm:
      dcm.AccessionNumber = '456'
      dcm.save_as(test_dicom_path)
    source_dicom_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        temp_wiki
    )
    gen_dicom = [test_dicom_path]
    patient_name = 'Test^Bob'
    patient_id = '1234-54321'
    study_instance_uid = '1.8.9'
    series_instance_uid = '1.8.9.1'
    additional_metadata = pydicom.Dataset()
    additional_metadata.FrameOfReferenceUID = ''
    additional_metadata.PositionReferenceIndicator = 'Edge'
    metadata = _test_json_dicom_metadata(
        study_instance_uid, series_instance_uid, patient_name, patient_id
    )
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

    gen_dicom_paths = ingest_wsi_dicom._add_metadata_to_generated_dicom_files(
        source_dicom_ref,
        gen_dicom,
        private_tags,
        instance_util,
        metadata,
        additional_metadata,
    )

    self.assertLen(gen_dicom_paths, 1)
    with pydicom.dcmread(gen_dicom_paths[0]) as ds:
      self.assertEqual(ds.BurnedInAnnotation, 'NO')
      self.assertEqual(ds.SpecimenLabelInImage, 'NO')
      self.assertEqual(ds.InstanceNumber, 1)
      self.assertEqual(ds.AccessionNumber, '456')  # accession not changed.
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
    source_dicom_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        self._get_unique_test_file_path('test_wikipedia.dcm')
    )
    patient_name = 'Test^Susan'
    patient_id = '54321-54321'
    study_instance_uid = '1.8.9'
    series_instance_uid = '1.8.9.1'
    additional_metadata = pydicom.Dataset()
    additional_metadata.FrameOfReferenceUID = ''
    additional_metadata.PositionReferenceIndicator = ''
    metadata = _test_json_dicom_metadata(
        study_instance_uid, series_instance_uid, patient_name, patient_id
    )
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

    gen_dicom_paths = ingest_wsi_dicom._add_metadata_to_ingested_wsi_dicom(
        [source_dicom_ref],
        private_tags,
        instance_util,
        metadata,
        additional_metadata,
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
  )
  def test_add_metadata_to_non_wsi_ingested_image(self):
    source_dicom_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(
        self._get_unique_test_file_path('test_wikipedia.dcm')
    )
    metadata_path = os.path.dirname(
        self._get_unique_test_file_path('example_schema_wsi.json')
    )
    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata([
        gen_test_util.test_file_path('metadata_duplicate.csv'),
    ])
    csv_metadata = meta_client.get_slide_metadata_from_csv('MD-01-1-A1-1')
    csv_metadata = dicom_schema_util.PandasMetadataTableWrapper(csv_metadata)
    test_pubsubmessage_id = 'pubsub_message_id=1'
    private_tags = abstract_dicom_generation.get_private_tags_for_gen_dicoms(
        abstract_dicom_generation.GeneratedDicomFiles('/test.svs', None),
        test_pubsubmessage_id,
    )
    instance_util = ingest_wsi_dicom._DicomInstanceNumberAssignmentUtil()
    instance_util.add_inst([source_dicom_ref])

    output_path = self.create_tempdir().full_path
    with gcs_mock.GcsMock(
        buckets={'output': output_path, 'metadata': metadata_path}
    ):
      ingest_dicom = ingest_wsi_dicom.IngestWsiDicom(
          ingest_base.GcsIngestionBuckets(
              'gs://test/success', 'gs://test/failure'
          ),
          False,
          True,
          metadata_storage_client.MetadataStorageClient(),
      )
      ingest_dicom.update_metadata()
      ingest_dicom.set_slide_id('MD-01-1-A1-1', False)
      gen_dicom = ingest_dicom._add_metadata_to_other_ingested_dicom(
          [source_dicom_ref],
          private_tags,
          instance_util,
          csv_metadata,
          _test_json_dicom_metadata(
              '1.2.3.4', '1.2.3.4.1', 'Test^Susan', '54321-54321'
          ),
          mock.create_autospec(
              dicom_store_client.DicomStoreClient, instance=True
          ),
      )

    self.assertLen(gen_dicom, 1)
    ds = pydicom.dcmread(gen_dicom[0])
    self.assertEqual(ds.StudyInstanceUID, '1.2.3.4')
    self.assertEqual(ds.SeriesInstanceUID, '1.2.3.4.1')
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
    self.assertLen(
        ingest_wsi_dicom._remove_duplicate_generated_dicom(
            test_dicom_path, [dcm_ref]
        ).file_paths,
        1,
    )

  @parameterized.parameters([
      ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID,
      ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID,
  ])
  def test_remove_duplicate_generated_dicom_ingores_study_series_uid(
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
    self.assertEmpty(
        ingest_wsi_dicom._remove_duplicate_generated_dicom(
            test_dicom_path, [dcm_ref]
        ).file_paths
    )

  def test_get_additional_tag_keywords_to_copy(self):
    with open(
        gen_test_util.test_file_path('root_level_vl_wsi_tags_to_copy.txt'), 'rt'
    ) as infile:
      tags = sorted(json.load(infile))
    self.assertEqual(
        sorted(ingest_wsi_dicom._get_additional_tag_keywords_to_copy()),
        tags,
    )

  def _verify_generated_dicom_json_equal(
      self,
      results: ingest_base.GenDicomResult,
      expected_json: List[Mapping[str, Any]],
  ):
    instance_json = _get_instances_json(
        results.files_to_upload.main_store_instances
    )
    self.assertLen(instance_json, len(expected_json))
    for index, expected_instance_json in enumerate(expected_json):
      self.assertEqual(instance_json[index], expected_instance_json)

  @parameterized.named_parameters([
      dict(
          testcase_name='original_main_image',
          image_type='ORIGINAL\\PRIMARY\\VOLUME',
          override_study_uid_with_metadata=False,
      ),
      dict(
          testcase_name='derived_main_image',
          image_type='DERIVED\\PRIMARY\\VOLUME',
          override_study_uid_with_metadata=False,
      ),
      dict(
          testcase_name='derived_main_image_metadata_study_uid_source',
          image_type='DERIVED\\PRIMARY\\VOLUME',
          override_study_uid_with_metadata=True,
      ),
  ])
  @mock.patch.object(
      cloud_logging_client,
      'get_build_version',
      autospec=True,
      return_value='Build_Version:123',
  )
  @mock.patch.object(
      uid_generator,
      'generate_uid',
      autospec=True,
      return_value=_MOCK_DICOM_GEN_UID,
  )
  @mock.patch.object(dicom_util, 'set_content_date_time_to_now', autospec=True)
  @mock.patch.object(
      time, 'time', autospec=True, return_value=1705894937.3804379
  )
  @mock.patch.object(subprocess, 'run', autospec=True)
  def test_generate_dicom_source_wsi_image_de_duplication_instance(
      self,
      *unused_mocks,
      image_type,
      override_study_uid_with_metadata,
  ):
    expected_json = 'wsi_dicom_json/derived_volume.json'
    sop_class_uid = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    modality = 'SM'
    input_filepath = gen_test_util.test_file_path('test_wikipedia.dcm')
    dcm = pydicom.dcmread(input_filepath)
    dcm.SOPClassUID = sop_class_uid
    dcm.Modality = modality
    dcm.ImageType = image_type
    _add_required_dicom_metadata_for_valid_wsi(dcm)

    result = _ingest_wsi_dicom_generate_dicom_test_shim(
        self, dcm, [1], override_study_uid_with_metadata
    )
    self.assertLen(result.files_to_upload.main_store_instances, 1)
    self.assertFalse(result.generated_series_instance_uid)
    self.assertEqual(result.dest_uri, 'gs://output/success')
    self._verify_generated_dicom_json_equal(
        result,
        _load_expected_json(
            expected_json, override_study_uid_with_metadata, dcm
        ),
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='original_main_image',
          image_type='ORIGINAL\\PRIMARY\\VOLUME',
          override_study_uid_with_metadata=False,
      ),
      dict(
          testcase_name='derived_main_image',
          image_type='DERIVED\\PRIMARY\\VOLUME',
          override_study_uid_with_metadata=False,
      ),
      dict(
          testcase_name='derived_main_image_metadata_study_uid_source',
          image_type='DERIVED\\PRIMARY\\VOLUME',
          override_study_uid_with_metadata=True,
      ),
  ])
  @mock.patch.object(
      cloud_logging_client,
      'get_build_version',
      autospec=True,
      return_value='Build_Version:123',
  )
  @mock.patch.object(
      uid_generator,
      'generate_uid',
      autospec=True,
      return_value=_MOCK_DICOM_GEN_UID,
  )
  @mock.patch.object(dicom_util, 'set_content_date_time_to_now', autospec=True)
  @mock.patch.object(
      time, 'time', autospec=True, return_value=1705894937.3804379
  )
  @mock.patch.object(subprocess, 'run', autospec=True)
  def test_generate_dicom_source_wsi_image_with_downsampled_instance(
      self,
      *unused_mocks,
      image_type,
      override_study_uid_with_metadata,
  ):
    expected_json = (
        'wsi_dicom_json/derived_volume_with_downsampled_instance.json'
    )
    sop_class_uid = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    modality = 'SM'
    input_filepath = gen_test_util.test_file_path('test_wikipedia.dcm')
    dcm = pydicom.dcmread(input_filepath)
    dcm.SOPClassUID = sop_class_uid
    dcm.Modality = modality
    dcm.ImageType = image_type
    _add_required_dicom_metadata_for_valid_wsi(dcm)
    result = _ingest_wsi_dicom_generate_dicom_test_shim(
        self, dcm, [1, 2], override_study_uid_with_metadata
    )
    self.assertLen(result.files_to_upload.main_store_instances, 2)
    self.assertFalse(result.generated_series_instance_uid)
    self.assertEqual(result.dest_uri, 'gs://output/success')
    expected_json = _load_expected_json(
        expected_json, override_study_uid_with_metadata, dcm
    )
    self._verify_generated_dicom_json_equal(result, expected_json)

  @parameterized.parameters([True, False])
  @mock.patch.object(
      uid_generator,
      'generate_uid',
      autospec=True,
      return_value=_MOCK_DICOM_GEN_UID,
  )
  @mock.patch.object(dicom_util, 'set_content_date_time_to_now', autospec=True)
  @mock.patch.object(
      time, 'time', autospec=True, return_value=1705894937.3804379
  )
  @mock.patch.object(subprocess, 'run', autospec=True)
  @flagsaver.flagsaver(enable_metadata_free_ingestion=True)
  def test_generate_dicom_wsi_image_metadata_free_studyinstance_uid(
      self, override_with_metadata, *unused_mocks
  ):
    image_type = 'ORIGINAL\\PRIMARY\\VOLUME'
    sop_class_uid = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    modality = 'SM'
    input_filepath = gen_test_util.test_file_path('test_wikipedia.dcm')
    dcm = pydicom.dcmread(input_filepath)
    dcm.SOPClassUID = sop_class_uid
    dcm.Modality = modality
    dcm.ImageType = image_type
    _add_required_dicom_metadata_for_valid_wsi(dcm)
    result = _ingest_wsi_dicom_generate_dicom_test_shim(
        self,
        dcm,
        [1, 2],
        override_study_uid_with_metadata=override_with_metadata,
        slide_id='not_found',
    )
    self.assertLen(result.files_to_upload.main_store_instances, 2)
    self.assertFalse(result.generated_series_instance_uid)
    self.assertEqual(result.dest_uri, 'gs://output/success')
    study_instance_uid = {
        pydicom.dcmread(path).StudyInstanceUID
        for path in result.files_to_upload.main_store_instances
    }
    self.assertEqual(
        study_instance_uid,
        {'1.2.276.0.7230010.3.1.2.296485376.46.1648688993.578030'},
    )
    self.assertEmpty(
        pydicom.dcmread(
            list(result.files_to_upload.main_store_instances)[0]
        ).PatientID
    )

  @mock.patch.object(subprocess, 'run', autospec=True)
  @flagsaver.flagsaver(init_series_instance_uid_from_metadata=True)
  def test_generate_dicom_wsi_image_init_series_from_metadata(
      self, *unused_mocks
  ):
    image_type = 'ORIGINAL\\PRIMARY\\VOLUME'
    sop_class_uid = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    modality = 'SM'
    input_filepath = gen_test_util.test_file_path('test_wikipedia.dcm')
    dcm = pydicom.dcmread(input_filepath)
    dcm.SOPClassUID = sop_class_uid
    dcm.Modality = modality
    dcm.ImageType = image_type
    _add_required_dicom_metadata_for_valid_wsi(dcm)
    result = _ingest_wsi_dicom_generate_dicom_test_shim(
        self,
        dcm,
        [1, 2],
        override_study_uid_with_metadata=False,
        slide_id='MD-06-3-A1-2',
    )
    self.assertLen(result.files_to_upload.main_store_instances, 2)
    self.assertFalse(result.generated_series_instance_uid)
    self.assertEqual(result.dest_uri, 'gs://output/success')
    series_instance_uid = {
        pydicom.dcmread(path).SeriesInstanceUID
        for path in result.files_to_upload.main_store_instances
    }
    self.assertEqual(series_instance_uid, {'1.2.3.4.5'})

  @mock.patch.object(subprocess, 'run', autospec=True)
  @flagsaver.flagsaver(init_series_instance_uid_from_metadata=True)
  def test_generate_dicom_wsi_image_init_series_undefined_from_metadata_error(
      self, *unused_mocks
  ):
    image_type = 'ORIGINAL\\PRIMARY\\VOLUME'
    sop_class_uid = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    modality = 'SM'
    input_filepath = gen_test_util.test_file_path('test_wikipedia.dcm')
    dcm = pydicom.dcmread(input_filepath)
    dcm.SOPClassUID = sop_class_uid
    dcm.Modality = modality
    dcm.ImageType = image_type
    _add_required_dicom_metadata_for_valid_wsi(dcm)
    result = _ingest_wsi_dicom_generate_dicom_test_shim(
        self,
        dcm,
        [1, 2],
        override_study_uid_with_metadata=False,
        slide_id='MD-05-3-A1-2',
    )
    self.assertEmpty(result.files_to_upload.main_store_instances)
    self.assertFalse(result.generated_series_instance_uid)
    self.assertEqual(
        result.dest_uri,
        'gs://output/failure/missing_series_instance_uid_in_metadata',
    )

  @mock.patch.object(subprocess, 'run', autospec=True)
  def test_generate_dicom_determine_slide_id_error(
      self,
      *unused_mocks,
  ):
    dcm = pydicom.dcmread(gen_test_util.test_file_path('test_wikipedia.dcm'))
    _add_required_dicom_metadata_for_valid_wsi(dcm)
    with self.assertRaises(ingest_base.DetermineSlideIDError):
      _ingest_wsi_dicom_generate_dicom_test_shim(
          self,
          dcm,
          [1],
          override_study_uid_with_metadata=False,
          slide_id='not_found',
      )

  @parameterized.named_parameters([
      dict(
          testcase_name='generate_study_instance_uid',
          override_with_metadata=True,
          expected_study_instance_uid=_MOCK_DICOM_GEN_UID,
      ),
      dict(
          testcase_name='use_uid_in_dicom',
          override_with_metadata=False,
          expected_study_instance_uid=(
              '1.2.276.0.7230010.3.1.2.296485376.46.1648688993.578030'
          ),
      ),
  ])
  @mock.patch.object(
      uid_generator,
      'generate_uid',
      autospec=True,
      return_value=_MOCK_DICOM_GEN_UID,
  )
  @mock.patch.object(dicom_util, 'set_content_date_time_to_now', autospec=True)
  @mock.patch.object(
      time, 'time', autospec=True, return_value=1705894937.3804379
  )
  @mock.patch.object(subprocess, 'run', autospec=True)
  @flagsaver.flagsaver(
      enable_metadata_free_ingestion=False,
      enable_create_missing_study_instance_uid=True,
  )
  def test_generate_dicom_wsi_image_metadata_lite_free_studyinstance_uid(
      self, *unused_mocks, override_with_metadata, expected_study_instance_uid
  ):
    slide_id = 'MD-05-3-A1-2'
    image_type = 'ORIGINAL\\PRIMARY\\VOLUME'
    sop_class_uid = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    modality = 'SM'
    input_filepath = gen_test_util.test_file_path('test_wikipedia.dcm')
    dcm = pydicom.dcmread(input_filepath)
    dcm.SOPClassUID = sop_class_uid
    dcm.Modality = modality
    dcm.ImageType = image_type
    _add_required_dicom_metadata_for_valid_wsi(dcm)
    result = _ingest_wsi_dicom_generate_dicom_test_shim(
        self,
        dcm,
        [1, 2],
        override_study_uid_with_metadata=override_with_metadata,
        slide_id=slide_id,
    )
    self.assertLen(result.files_to_upload.main_store_instances, 2)
    self.assertFalse(result.generated_series_instance_uid)
    self.assertEqual(result.dest_uri, 'gs://output/success')
    study_instance_uid = {
        pydicom.dcmread(path).StudyInstanceUID
        for path in result.files_to_upload.main_store_instances
    }
    self.assertEqual(study_instance_uid, {expected_study_instance_uid})

  @parameterized.named_parameters([
      dict(
          testcase_name='original_ancillary_image',
          image_type='ORIGINAL\\PRIMARY\\THUMBNAIL',
          override_study_uid_with_metadata=False,
      ),
      dict(
          testcase_name='derived_ancillary_image',
          image_type='DERIVED\\PRIMARY\\THUMBNAIL',
          override_study_uid_with_metadata=False,
      ),
      dict(
          testcase_name='derived_ancillary_image_metadata_study_instance_uid',
          image_type='DERIVED\\PRIMARY\\THUMBNAIL',
          override_study_uid_with_metadata=True,
      ),
  ])
  @mock.patch.object(
      cloud_logging_client,
      'get_build_version',
      autospec=True,
      return_value='Build_Version:123',
  )
  @mock.patch.object(
      uid_generator,
      'generate_uid',
      autospec=True,
      return_value=_MOCK_DICOM_GEN_UID,
  )
  @mock.patch.object(dicom_util, 'set_content_date_time_to_now', autospec=True)
  @mock.patch.object(
      time, 'time', autospec=True, return_value=1705894937.3804379
  )
  def test_generate_dicom_ancillary_wsi_image(
      self,
      *unused_mocks,
      image_type,
      override_study_uid_with_metadata,
  ):
    expected_json = 'wsi_dicom_json/derived_anclillary.json'
    sop_class_uid = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    modality = 'SM'
    input_filepath = gen_test_util.test_file_path('test_wikipedia.dcm')
    dcm = pydicom.dcmread(input_filepath)
    dcm.SOPClassUID = sop_class_uid
    dcm.Modality = modality
    dcm.ImageType = image_type
    _add_required_dicom_metadata_for_valid_wsi(dcm)
    result = _ingest_wsi_dicom_generate_dicom_test_shim(
        self, dcm, [], override_study_uid_with_metadata
    )
    self.assertLen(result.files_to_upload.main_store_instances, 1)
    self.assertFalse(result.generated_series_instance_uid)
    self.assertEqual(result.dest_uri, 'gs://output/success')
    self._verify_generated_dicom_json_equal(
        result,
        _load_expected_json(
            expected_json, override_study_uid_with_metadata, dcm
        ),
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='metadata_study_uid_source',
          override_study_uid_with_metadata=True,
      ),
      dict(
          testcase_name='dicom_study_uid_source',
          override_study_uid_with_metadata=False,
      ),
  ])
  @mock.patch.object(
      cloud_logging_client,
      'get_build_version',
      autospec=True,
      return_value='Build_Version:123',
  )
  @mock.patch.object(
      uid_generator,
      'generate_uid',
      autospec=True,
      return_value=_MOCK_DICOM_GEN_UID,
  )
  @mock.patch.object(dicom_util, 'set_content_date_time_to_now', autospec=True)
  @mock.patch.object(
      time, 'time', autospec=True, return_value=1705894937.3804379
  )
  def test_generate_dicom_from_other_image(
      self, *unused_mocks, override_study_uid_with_metadata
  ):
    expected_json = 'wsi_dicom_json/non_wsi_dicom_ingest.json'
    input_filepath = gen_test_util.test_file_path('test_wikipedia.dcm')
    dcm = pydicom.dcmread(input_filepath)
    dcm.SOPClassUID = '1.2.840.10008.5.1.4.1.1.2'
    dcm.Modality = 'CT'
    dcm.ImageType = 'CT'
    _add_required_dicom_metadata_for_valid_wsi(dcm)
    result = _ingest_wsi_dicom_generate_dicom_test_shim(
        self, dcm, [], override_study_uid_with_metadata
    )
    self.assertLen(result.files_to_upload.main_store_instances, 1)
    self.assertFalse(result.generated_series_instance_uid)
    self.assertEqual(result.dest_uri, 'gs://output/success')
    self._verify_generated_dicom_json_equal(
        result,
        _load_expected_json(
            expected_json, override_study_uid_with_metadata, dcm
        ),
    )

  @flagsaver.flagsaver(
      gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.METADATA,
  )
  def test_get_slide_id_with_invalid_dicom_raises(self):
    input_filepath = gen_test_util.test_file_path('test_wikipedia.dcm')
    with self.assertRaises(ingest_base.DetermineSlideIDError):
      _ingest_wsi_dicom_generate_dicom_test_shim(
          self, pydicom.dcmread(input_filepath), [], False
      )

  @mock.patch.object(subprocess, 'run', autospec=True)
  @flagsaver.flagsaver(
      gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.METADATA,
  )
  def test_generate_dicom_failes_to_generate_dicom_errors(
      self, unused_mock_convert_to_dicom_sub_process
  ):
    input_filepath = gen_test_util.test_file_path('test_wikipedia.dcm')
    dcm = pydicom.dcmread(input_filepath)
    dcm.ImageType = f'{_ORIGINAL}\\{_PRIMARY}\\{_VOLUME}'
    _add_required_dicom_metadata_for_valid_wsi(dcm)
    result = _ingest_wsi_dicom_generate_dicom_test_shim(self, dcm, [], False)
    self.assertEmpty(result.files_to_upload.main_store_instances)
    self.assertFalse(result.generated_series_instance_uid)
    self.assertEqual(
        result.dest_uri, 'gs://output/failure/wsi_to_dicom_conversion_failed'
    )

  @mock.patch.object(subprocess, 'run', autospec=True)
  @flagsaver.flagsaver(
      gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.METADATA,
  )
  def test_generate_dicom_generate_invalid_dicom_errors(
      self, unused_mock_convert_to_dicom_sub_process
  ):
    input_filepath = gen_test_util.test_file_path('test_wikipedia.dcm')
    dcm = pydicom.dcmread(input_filepath)
    dcm.ImageType = f'{_ORIGINAL}\\{_PRIMARY}\\{_VOLUME}'
    _add_required_dicom_metadata_for_valid_wsi(dcm)
    filename = f'MD-04-3-A1-2_{os.path.basename(input_filepath)}'
    with _IngestWsiDicomTest(self, dcm, filename) as ingest_test:
      ingest = ingest_wsi_dicom.IngestWsiDicom(
          ingest_test.handler._ingest_buckets,
          is_oof_ingestion_enabled=False,
          override_study_uid_with_metadata=False,
          metadata_client=metadata_storage_client.MetadataStorageClient(),
      )
      ingest.init_handler_for_ingestion()
      ingest.update_metadata()
      ingest.get_slide_id(ingest_test.dicom_gen, ingest_test.handler)
      dicom_gen_dir = os.path.join(
          ingest_test.handler.root_working_dir, 'gen_dicom'
      )
      os.mkdir(dicom_gen_dir)
      with open(
          os.path.join(dicom_gen_dir, 'downsample-1-foo.dcm'), 'wb'
      ) as outfile:
        outfile.write(b'1234')
      result = ingest.generate_dicom(
          dicom_gen_dir,
          ingest_test.dicom_gen,
          'mock_pubsub_msg_id',
          ingest_test.handler,
      )
      self.assertEmpty(result.files_to_upload.main_store_instances)
      self.assertFalse(result.generated_series_instance_uid)
      self.assertEqual(
          result.dest_uri,
          'gs://output/failure/wsi_to_dicom_invalid_dicom_generated',
      )

  @flagsaver.flagsaver(
      gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.METADATA,
  )
  def test_generate_dicom_generate_raises_if_slide_id_not_set(self):
    input_filepath = gen_test_util.test_file_path('test_wikipedia.dcm')
    dcm = pydicom.dcmread(input_filepath)
    dcm.ImageType = f'{_ORIGINAL}\\{_PRIMARY}\\{_VOLUME}'
    _add_required_dicom_metadata_for_valid_wsi(dcm)
    filename = f'MD-04-3-A1-2_{os.path.basename(input_filepath)}'
    with _IngestWsiDicomTest(self, dcm, filename) as ingest_test:
      ingest = ingest_wsi_dicom.IngestWsiDicom(
          ingest_test.handler._ingest_buckets,
          is_oof_ingestion_enabled=False,
          override_study_uid_with_metadata=False,
          metadata_client=metadata_storage_client.MetadataStorageClient(),
      )
      ingest.init_handler_for_ingestion()
      with self.assertRaises(ValueError):
        ingest.generate_dicom(
            self.create_tempdir().full_path,
            ingest_test.dicom_gen,
            'mock_pubsub_msg_id',
            ingest_test.handler,
        )

  @flagsaver.flagsaver(
      gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.METADATA,
  )
  def test_generate_metadata_free_slide_metadata(self):
    dicom_store_url = 'https://mock.dicom.store.com/dicomWeb'
    input_filepath = gen_test_util.test_file_path('test_wikipedia.dcm')
    dcm = pydicom.dcmread(input_filepath)
    filename = f'MD-04-3-A1-2_{os.path.basename(input_filepath)}'
    with _IngestWsiDicomTest(
        self, dcm, filename, dicom_store_url=dicom_store_url
    ) as ingest_test:
      ingest = ingest_wsi_dicom.IngestWsiDicom(
          ingest_test.handler._ingest_buckets,
          is_oof_ingestion_enabled=False,
          override_study_uid_with_metadata=False,
          metadata_client=metadata_storage_client.MetadataStorageClient(),
      )
      dicom_client = dicom_store_client.DicomStoreClient(dicom_store_url)
      result = ingest._generate_metadata_free_slide_metadata(
          'mock_slide_id', dicom_client
      )
    self.assertEmpty(result.dicom_json)

  @parameterized.named_parameters([
      dict(
          testcase_name='both_factors_true',
          override_study_uid_with_metadata=True,
          is_metadata_free=True,
          expected=False,
      ),
      dict(
          testcase_name='metadata_free',
          override_study_uid_with_metadata=False,
          is_metadata_free=True,
          expected=False,
      ),
      dict(
          testcase_name='init_series_from_metadata',
          override_study_uid_with_metadata=True,
          is_metadata_free=False,
          expected=True,
      ),
      dict(
          testcase_name='both_factors_false',
          override_study_uid_with_metadata=False,
          is_metadata_free=False,
          expected=False,
      ),
  ])
  def test_set_study_instance_uid_from_metadata(
      self, override_study_uid_with_metadata, is_metadata_free, expected
  ) -> bool:
    dicom_store_url = 'https://mock.dicom.store.com/dicomWeb'
    input_filepath = gen_test_util.test_file_path('test_wikipedia.dcm')
    dcm = pydicom.dcmread(input_filepath)
    filename = f'MD-04-3-A1-2_{os.path.basename(input_filepath)}'
    study_uid_source = (
        ingest_flags.UidSource.METADATA
        if override_study_uid_with_metadata
        else ingest_flags.UidSource.DICOM
    )
    with flagsaver.flagsaver(
        gcs_ingest_study_instance_uid_source=study_uid_source
    ):
      with _IngestWsiDicomTest(
          self, dcm, filename, dicom_store_url=dicom_store_url
      ) as ingest_test:
        ingest = ingest_wsi_dicom.IngestWsiDicom(
            ingest_test.handler._ingest_buckets,
            is_oof_ingestion_enabled=False,
            override_study_uid_with_metadata=override_study_uid_with_metadata,
            metadata_client=metadata_storage_client.MetadataStorageClient(),
        )
        ingest.set_slide_id('mock_slide_id', is_metadata_free)
        self.assertEqual(
            ingest._set_study_instance_uid_from_metadata(), expected
        )

  @parameterized.named_parameters([
      dict(
          testcase_name='both_factors_true',
          init_series_from_metadata=True,
          is_metadata_free=True,
          expected=False,
      ),
      dict(
          testcase_name='metadata_free',
          init_series_from_metadata=False,
          is_metadata_free=True,
          expected=False,
      ),
      dict(
          testcase_name='init_series_from_metadata',
          init_series_from_metadata=True,
          is_metadata_free=False,
          expected=True,
      ),
      dict(
          testcase_name='both_factors_false',
          init_series_from_metadata=False,
          is_metadata_free=False,
          expected=False,
      ),
  ])
  @flagsaver.flagsaver(
      gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.DICOM,
  )
  def test_set_series_instance_uid_from_metadata(
      self, init_series_from_metadata, is_metadata_free, expected
  ) -> bool:
    dicom_store_url = 'https://mock.dicom.store.com/dicomWeb'
    input_filepath = gen_test_util.test_file_path('test_wikipedia.dcm')
    dcm = pydicom.dcmread(input_filepath)
    filename = f'MD-04-3-A1-2_{os.path.basename(input_filepath)}'
    with _IngestWsiDicomTest(
        self, dcm, filename, dicom_store_url=dicom_store_url
    ) as ingest_test:
      ingest = ingest_wsi_dicom.IngestWsiDicom(
          ingest_test.handler._ingest_buckets,
          is_oof_ingestion_enabled=False,
          override_study_uid_with_metadata=False,
          metadata_client=metadata_storage_client.MetadataStorageClient(),
      )
      ingest.set_slide_id('mock_slide_id', is_metadata_free)
      with flagsaver.flagsaver(
          init_series_instance_uid_from_metadata=init_series_from_metadata
      ):
        self.assertEqual(
            ingest._set_series_instance_uid_from_metadata(), expected
        )


if __name__ == '__main__':
  absltest.main()
