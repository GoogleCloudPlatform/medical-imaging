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
"""Tests for dicom_util."""

import binascii
import datetime
import io
import os
import shutil
from typing import Any, MutableMapping, Optional
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import numpy as np
import openslide
import pydicom

from shared_libs.test_utils.dicom_store_mock import dicom_store_mock
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen import wsi_dicom_file_ref
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util


_WIKIPEDIA_DCM_FILENAME = 'test_wikipedia.dcm'


def _get_value(val: Optional[pydicom.dataelem.DataElement]) -> Any:
  if val is not None:
    return val.value
  return None


def _mock_study_series_uid_dicom_json(
    study_uid: str = '1.2.3', series_uid='1.2.3.4'
) -> MutableMapping[str, Any]:
  ds = pydicom.Dataset()
  ds.StudyInstanceUID = study_uid
  ds.SeriesInstanceUID = series_uid
  return ds.to_json_dict()


def icc_bytes() -> bytes:
  path = gen_test_util.test_file_path('profile.icc')
  with open(path, 'rb') as infile:
    return infile.read()


class DicomUtilTest(parameterized.TestCase):

  @parameterized.named_parameters([
      dict(
          testcase_name='srgb',
          icc_profile_color=ingest_flags.DefaultIccProfile.SRGB,
          expected_len=60960,
      ),
      dict(
          testcase_name='adobergb',
          icc_profile_color=ingest_flags.DefaultIccProfile.ADOBERGB,
          expected_len=560,
      ),
      dict(
          testcase_name='rommrgb',
          icc_profile_color=ingest_flags.DefaultIccProfile.ROMMRGB,
          expected_len=864,
      ),
  ])
  def test_get_default_icc_profile_color(self, icc_profile_color, expected_len):
    with flagsaver.flagsaver(default_iccprofile=icc_profile_color):
      self.assertLen(dicom_util.get_default_icc_profile_color(), expected_len)

  @flagsaver.flagsaver(default_iccprofile=ingest_flags.DefaultIccProfile.NONE)
  def test_get_default_icc_profile_color_none_returns_none(self):
    self.assertIsNone(dicom_util.get_default_icc_profile_color())

  @flagsaver.flagsaver(default_iccprofile=10)
  def test_get_default_icc_profile_color_unexpected_enum_value_raises(self):
    with self.assertRaises(ValueError):
      dicom_util.get_default_icc_profile_color()

  def test_unable_to_load_icc_profile_raises(self):
    with self.assertRaises(FileNotFoundError):
      dicom_util._read_icc_profile('file_not_found.icc')

  @parameterized.parameters([
      ingest_flags.DefaultIccProfile.ADOBERGB,
      ingest_flags.DefaultIccProfile.ROMMRGB,
  ])
  @mock.patch.object(
      dicom_util,
      '_read_icc_profile',
      autospec=True,
      side_effect=FileNotFoundError(),
  )
  def test_get_default_icc_profile_color_unable_to_load_profile_raises_file_not_found(
      self, profile, _
  ):
    with flagsaver.flagsaver(default_iccprofile=profile):
      with self.assertRaises(FileNotFoundError):
        dicom_util.get_default_icc_profile_color()

  @mock.patch.object(
      dicom_util,
      '_read_icc_profile',
      autospec=True,
      side_effect=FileNotFoundError(),
  )
  @flagsaver.flagsaver(default_iccprofile=ingest_flags.DefaultIccProfile.SRGB)
  def test_get_default_srgb_icc_profile_color_unable_to_load_profile_returns_pill_version_if_file_not_found(
      self, _
  ):
    profile = dicom_util.get_default_icc_profile_color()
    self.assertLen(profile, 588)

  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  def test_add_add_openslide_dicom_properties(self, unused_mock):
    ds = pydicom.Dataset()
    dicom_util.add_default_optical_path_sequence(ds, None)
    path = gen_test_util.test_file_path('ndpi_test.ndpi')
    dicom_util.add_openslide_dicom_properties(ds, path)
    self.assertEqual(ds.OpticalPathSequence[0].ObjectiveLensPower, '20')
    self.assertNotIn('RecommendedAbsentPixelCIELabValue', ds)

  @parameterized.parameters(
      ['TotalPixelMatrixOriginSequence', 'OpticalPathSequence']
  )
  @flagsaver.flagsaver(add_openslide_total_pixel_matrix_origin_seq=True)
  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  def test_add_add_openslide_dicom_properties_raises_value_error_missing_sq(
      self, sq_name, unused_mock
  ):
    ds = pydicom.Dataset()
    dicom_util.add_default_optical_path_sequence(ds, None)
    dicom_util.add_default_total_pixel_matrix_origin_sequence_if_not_defined(ds)
    path = gen_test_util.test_file_path('ndpi_test.ndpi')
    del ds[sq_name]
    with self.assertRaises(ValueError):
      dicom_util.add_openslide_dicom_properties(ds, path)

  @flagsaver.flagsaver(add_openslide_total_pixel_matrix_origin_seq=True)
  def test_add_total_pixel_matrix_origin_sequence(self):
    ds = pydicom.Dataset()
    ds.TotalPixelMatrixOriginSequence = [pydicom.Dataset()]
    ds.TotalPixelMatrixOriginSequence[0].XOffsetInSlideCoordinateSystem = 0
    ds.TotalPixelMatrixOriginSequence[0].YOffsetInSlideCoordinateSystem = 0
    mock_openslide = mock.create_autospec(openslide.OpenSlide, instance=True)
    mock_openslide.properties = {
        openslide.PROPERTY_NAME_BOUNDS_X: 50,
        openslide.PROPERTY_NAME_BOUNDS_Y: 75,
        openslide.PROPERTY_NAME_MPP_X: 1000,
        openslide.PROPERTY_NAME_MPP_Y: 500,
    }
    dicom_util._add_total_pixel_matrix_origin_sequence(ds, mock_openslide)
    origin_seq = ds.TotalPixelMatrixOriginSequence[0]
    self.assertEqual(origin_seq.XOffsetInSlideCoordinateSystem, 50)
    self.assertEqual(origin_seq.YOffsetInSlideCoordinateSystem, 37.5)

  @parameterized.named_parameters([
      dict(
          testcase_name='missing_bounds_x',
          properties={
              openslide.PROPERTY_NAME_BOUNDS_Y: 75,
              openslide.PROPERTY_NAME_MPP_X: 1000,
              openslide.PROPERTY_NAME_MPP_Y: 500,
          },
          flag_enabled=True,
      ),
      dict(
          testcase_name='missing_bounds_y',
          properties={
              openslide.PROPERTY_NAME_BOUNDS_X: 50,
              openslide.PROPERTY_NAME_MPP_X: 1000,
              openslide.PROPERTY_NAME_MPP_Y: 500,
          },
          flag_enabled=True,
      ),
      dict(
          testcase_name='missing_mpp_x',
          properties={
              openslide.PROPERTY_NAME_BOUNDS_X: 50,
              openslide.PROPERTY_NAME_BOUNDS_Y: 75,
              openslide.PROPERTY_NAME_MPP_Y: 500,
          },
          flag_enabled=True,
      ),
      dict(
          testcase_name='missing_mpp_y',
          properties={
              openslide.PROPERTY_NAME_BOUNDS_X: 50,
              openslide.PROPERTY_NAME_BOUNDS_Y: 75,
              openslide.PROPERTY_NAME_MPP_X: 1000,
          },
          flag_enabled=True,
      ),
      dict(
          testcase_name='flag_disabled',
          properties={
              openslide.PROPERTY_NAME_BOUNDS_X: 50,
              openslide.PROPERTY_NAME_BOUNDS_Y: 75,
              openslide.PROPERTY_NAME_MPP_X: 1000,
              openslide.PROPERTY_NAME_MPP_Y: 500,
          },
          flag_enabled=False,
      ),
  ])
  def test_add_total_pixel_matrix_origin_sequence_not_set_if_property_missing(
      self, properties, flag_enabled
  ):
    ds = pydicom.Dataset()
    ds.TotalPixelMatrixOriginSequence = [pydicom.Dataset()]
    ds.TotalPixelMatrixOriginSequence[0].XOffsetInSlideCoordinateSystem = 0
    ds.TotalPixelMatrixOriginSequence[0].YOffsetInSlideCoordinateSystem = 0
    mock_openslide = mock.create_autospec(openslide.OpenSlide, instance=True)
    mock_openslide.properties = properties
    with flagsaver.flagsaver(
        add_openslide_total_pixel_matrix_origin_seq=flag_enabled
    ):
      dicom_util._add_total_pixel_matrix_origin_sequence(ds, mock_openslide)
    origin_seq = ds.TotalPixelMatrixOriginSequence[0]
    self.assertEqual(origin_seq.XOffsetInSlideCoordinateSystem, 0)
    self.assertEqual(origin_seq.YOffsetInSlideCoordinateSystem, 0)

  def test_should_fix_icc_colorspace_no_colorspace(self):
    ds = pydicom.Dataset()
    ds.ICCProfile = b'234'
    self.assertTrue(dicom_util._should_fix_icc_colorspace(ds))

  def test_should_fix_icc_colorspace_empty_colorspace(self):
    ds = pydicom.Dataset()
    ds.ICCProfile = b'234'
    ds.ColorSpace = ''
    self.assertTrue(dicom_util._should_fix_icc_colorspace(ds))

  def test_has_optical_path_sq_missing(self):
    self.assertFalse(dicom_util.has_optical_path_sequence(pydicom.Dataset()))

  def test_has_optical_path_sq_empty(self):
    ds = pydicom.Dataset()
    ds.OpticalPathSequence = pydicom.Sequence([])

    self.assertFalse(dicom_util.has_optical_path_sequence(ds))

  def test_has_optical_path_sq_true(self):
    ds = pydicom.Dataset()
    ds.OpticalPathSequence = pydicom.Sequence([pydicom.Dataset()])

    self.assertTrue(dicom_util.has_optical_path_sequence(ds))

  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  def test_add_icc_colorspace_if_missing(self, unused_mock):
    profile = icc_bytes()
    inner_ds = pydicom.Dataset()
    inner_ds.ICCProfile = profile
    ds = pydicom.Dataset()
    ds.OpticalPathSequence = pydicom.Sequence([inner_ds])
    ds.ICCProfile = profile
    test = pydicom.FileDataset('foo.dcm', ds)

    dicom_util.add_icc_colorspace_if_not_defined(test)

    self.assertEqual(test.ColorSpace, 'SRGB')
    self.assertEqual(test.OpticalPathSequence[0].ColorSpace, 'SRGB')

  def test_single_code_val_sq(self):
    ds = dicom_util._single_code_val_sq('123', 'bar', 'foo')

    self.assertLen(ds, 1)
    self.assertEqual(ds[0].CodeValue, '123')
    self.assertEqual(ds[0].CodingSchemeDesignator, 'bar')
    self.assertEqual(ds[0].CodeMeaning, 'foo')

  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  def test_add_default_optical_path_sequence(self, unused_mock):
    profile = icc_bytes()
    ds = pydicom.FileDataset('foo.dcm', pydicom.Dataset())
    self.assertTrue(dicom_util.add_default_optical_path_sequence(ds, profile))
    self.assertLen(ds.OpticalPathSequence, 1)
    self.assertEqual(ds.NumberOfOpticalPaths, 1)
    first_seq = ds.OpticalPathSequence[0]
    self.assertLen(first_seq.IlluminationTypeCodeSequence, 1)
    self.assertLen(first_seq.IlluminationColorCodeSequence, 1)
    illumination_color = (
        first_seq.IlluminationColorCodeSequence[0].CodeValue,
        first_seq.IlluminationColorCodeSequence[0].CodingSchemeDesignator,
        first_seq.IlluminationColorCodeSequence[0].CodeMeaning,
    )
    illumination_type = (
        first_seq.IlluminationTypeCodeSequence[0].CodeValue,
        first_seq.IlluminationTypeCodeSequence[0].CodingSchemeDesignator,
        first_seq.IlluminationTypeCodeSequence[0].CodeMeaning,
    )
    optical_path_seq_tags = (
        first_seq.OpticalPathIdentifier,
        first_seq.OpticalPathDescription,
        first_seq.ICCProfile,
        first_seq.ColorSpace,
    )
    self.assertEqual(
        illumination_color, ('11744', 'DCM', 'Brightfield illumination')
    )
    self.assertEqual(
        illumination_type, ('111741', 'DCM', 'Transmission illumination')
    )
    self.assertEqual(
        optical_path_seq_tags, ('1', 'Transmitted Light', profile, 'SRGB')
    )

  def test_add_default_total_pixel_matrix_origin_sequence_if_not_defined(self):
    ds = pydicom.FileDataset('foo.dcm', pydicom.Dataset())
    self.assertTrue(
        dicom_util.add_default_total_pixel_matrix_origin_sequence_if_not_defined(
            ds
        )
    )
    self.assertLen(ds.TotalPixelMatrixOriginSequence, 1)
    self.assertEqual(
        ds.TotalPixelMatrixOriginSequence[0].XOffsetInSlideCoordinateSystem,
        0,
    )
    self.assertEqual(
        ds.TotalPixelMatrixOriginSequence[0].YOffsetInSlideCoordinateSystem,
        0,
    )
    self.assertFalse(
        dicom_util.add_default_total_pixel_matrix_origin_sequence_if_not_defined(
            ds
        )
    )

  def test_add_default_total_pixel_matrix_origin_sequence_not_overwrite_defined(
      self,
  ):
    ds = pydicom.FileDataset('foo.dcm', pydicom.Dataset())
    dicom_util.add_default_total_pixel_matrix_origin_sequence_if_not_defined(ds)
    ds.TotalPixelMatrixOriginSequence[0].XOffsetInSlideCoordinateSystem = 10
    ds.TotalPixelMatrixOriginSequence[0].YOffsetInSlideCoordinateSystem = 10
    self.assertFalse(
        dicom_util.add_default_total_pixel_matrix_origin_sequence_if_not_defined(
            ds
        )
    )
    self.assertLen(ds.TotalPixelMatrixOriginSequence, 1)
    self.assertEqual(
        ds.TotalPixelMatrixOriginSequence[0].XOffsetInSlideCoordinateSystem,
        10,
    )
    self.assertEqual(
        ds.TotalPixelMatrixOriginSequence[0].YOffsetInSlideCoordinateSystem,
        10,
    )

  def test_add_default_optical_path_sequence_has_existing(self):
    profile = icc_bytes()
    existing = pydicom.Dataset()
    existing.OpticalPathSequence = pydicom.Sequence([pydicom.Dataset()])
    ds = pydicom.FileDataset('foo.dcm', existing)

    self.assertFalse(dicom_util.add_default_optical_path_sequence(ds, profile))
    self.assertNotIn('ICCProfile', ds.OpticalPathSequence[0])

  def test_add_icc_colorspace_to_dicom_no_profile(self):
    ds = pydicom.Dataset()
    ds.ColorSpace = 'foo'

    dicom_util.add_icc_colorspace_to_dicom(ds)

    self.assertNotIn('ColorSpace', ds)

  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  def test_add_icc_colorspace_to_dicom(self, unused_mock):
    profile = icc_bytes()
    ds = pydicom.Dataset()
    ds.ColorSpace = 'foo'
    ds.ICCProfile = profile

    dicom_util.add_icc_colorspace_to_dicom(ds)

    self.assertEqual(ds.ColorSpace, 'SRGB')

  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      side_effect=dicom_util.InvalidICCProfileError,
  )
  def test_add_icc_colorspace_to_dicom_invalid_profile(self, unused_mock):
    ds = pydicom.Dataset()
    ds.ColorSpace = 'foo'
    ds.ICCProfile = b'1234'

    with self.assertRaises(dicom_util.InvalidICCProfileError):
      dicom_util.add_icc_colorspace_to_dicom(ds)

  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  def test_add_icc_profile_to_dicom(self, unused_mock):
    profile = icc_bytes()
    ds = pydicom.Dataset()
    ds.ICCProfile = b'1234'
    ds.ColorSpace = 'foobar'

    dicom_util._add_icc_profile_to_dicom(ds, profile)

    self.assertEqual(ds.ICCProfile, profile)
    self.assertEqual(ds.ColorSpace, 'SRGB')

  def test_add_icc_profile_to_dicom_no_icc(self):
    ds = pydicom.Dataset()
    ds.ICCProfile = b'1234'
    ds.ColorSpace = 'foobar'

    dicom_util._add_icc_profile_to_dicom(ds, None)

    self.assertNotIn('ICCProfile', ds)
    self.assertNotIn('ColorSpace', ds)

  def test_get_svs_metadata(self):
    props = {}
    props['aperio.Date'] = '03/12/2021'
    props['aperio.Time'] = '16:06:55'
    props['aperio.Time Zone'] = 'GMT-0600'
    mock_result = mock.Mock()
    mock_result.properties = props
    _ = openslide.OpenSlide
    with mock.patch.object(openslide, 'OpenSlide', return_value=mock_result):
      md = dicom_util.get_svs_metadata('foobar.svs')

    self.assertEqual(md['AcquisitionDate'].value, '20210312')
    self.assertEqual(md['AcquisitionTime'].value, '160655.000000')
    self.assertEqual(md['AcquisitionDateTime'].value, '20210312160655.000000')

  def test_set_content_date_time_to_now(self):
    ds = pydicom.Dataset()
    dt = datetime.datetime(
        2022, 5, 2, 21, 57, 39, 338663, tzinfo=datetime.timezone.utc
    )
    save_func = datetime.datetime.strftime
    with mock.patch('datetime.datetime') as mk:
      mk.now = mock.Mock(return_value=dt)
      mk.strftime = save_func

      dicom_util.set_content_date_time_to_now(ds)

    self.assertEqual(ds.ContentTime, '215739.338663')
    self.assertEqual(ds.ContentDate, '20220502')

  def test_set_acquisition_date_time(self):
    ds = pydicom.Dataset()
    dt = datetime.datetime(
        2022, 5, 2, 21, 57, 39, 338663, tzinfo=datetime.timezone.utc
    )

    dicom_util.set_acquisition_date_time(ds, dt)

    self.assertEqual(ds.AcquisitionTime, '215739.338663')
    self.assertEqual(ds.AcquisitionDate, '20220502')
    self.assertEqual(ds.AcquisitionDateTime, '20220502215739.338663')

  def test_get_pydicom_tags_from_path(self):
    temp_dir = self.create_tempdir()
    wikipedia_dicom_path = gen_test_util.test_file_path(_WIKIPEDIA_DCM_FILENAME)
    test_dicom_path = os.path.join(temp_dir, 'temp.dcm')
    shutil.copyfile(wikipedia_dicom_path, test_dicom_path)
    sq_ds = pydicom.Dataset()
    sq_ds.PatientName = 'Test'
    with pydicom.dcmread(test_dicom_path) as ds:
      ds.NumberOfOpticalPaths = 5
      ds.OpticalPathSequence = [sq_ds, sq_ds]
      ds.save_as(test_dicom_path)

    tags = dicom_util.get_pydicom_tags(
        test_dicom_path,
        'BarcodeValue',
        ('NumberOfOpticalPaths', 'OpticalPathSequence'),
    )

    self.assertLen(tags, 3)
    self.assertIsNone(tags['BarcodeValue'])
    self.assertEqual(_get_value(tags['NumberOfOpticalPaths']), 5)
    self.assertEqual(tags['OpticalPathSequence'][0].PatientName, 'Test')
    self.assertEqual(tags['OpticalPathSequence'][1].PatientName, 'Test')

  def test_get_pydicom_tags_from_dataset(self):
    temp_dir = self.create_tempdir()
    wikipedia_dicom_path = gen_test_util.test_file_path(_WIKIPEDIA_DCM_FILENAME)
    test_dicom_path = os.path.join(temp_dir, 'temp.dcm')
    shutil.copyfile(wikipedia_dicom_path, test_dicom_path)
    sq_ds = pydicom.Dataset()
    sq_ds.PatientName = 'Test'
    with pydicom.dcmread(test_dicom_path) as ds:
      ds.NumberOfOpticalPaths = 5
      ds.OpticalPathSequence = [sq_ds, sq_ds]
      ds.save_as(test_dicom_path)

    with pydicom.dcmread(test_dicom_path, defer_size='512 KB') as reference_dcm:
      tags = dicom_util.get_pydicom_tags(
          reference_dcm,
          'BarcodeValue',
          ('NumberOfOpticalPaths', 'OpticalPathSequence'),
      )

    self.assertLen(tags, 3)
    self.assertIsNone(tags['BarcodeValue'])
    self.assertEqual(_get_value(tags['NumberOfOpticalPaths']), 5)
    self.assertEqual(tags['OpticalPathSequence'][0].PatientName, 'Test')
    self.assertEqual(tags['OpticalPathSequence'][1].PatientName, 'Test')

  @parameterized.named_parameters(
      dict(
          testcase_name='overwrite_existing_values',
          overwrite_existing_values=True,
          expected_num=5,
      ),
      dict(
          testcase_name='do_not_overwrite_existing_values',
          overwrite_existing_values=False,
          expected_num=1,
      ),
  )
  def test_set_defined_pydicom_tags(
      self, overwrite_existing_values, expected_num
  ):
    temp_dir = self.create_tempdir()
    wikipedia_dicom_path = gen_test_util.test_file_path(_WIKIPEDIA_DCM_FILENAME)
    test_dicom_path = os.path.join(temp_dir, 'temp.dcm')
    shutil.copyfile(wikipedia_dicom_path, test_dicom_path)
    sq_ds = pydicom.Dataset()
    sq_ds.PatientName = 'Test'
    with pydicom.dcmread(test_dicom_path) as ds:
      ds.NumberOfOpticalPaths = 5
      ds.OpticalPathSequence = [sq_ds, sq_ds]
      ds.save_as(test_dicom_path)
    tags = dicom_util.get_pydicom_tags(
        test_dicom_path,
        ('BarcodeValue', 'NumberOfOpticalPaths', 'OpticalPathSequence'),
    )
    test_ds = pydicom.Dataset()
    test_ds.NumberOfOpticalPaths = 1
    dicom_util.set_defined_pydicom_tags(
        test_ds,
        tags,
        ('BarcodeValue', 'NumberOfOpticalPaths', 'OpticalPathSequence'),
        overwrite_existing_values=overwrite_existing_values,
    )

    self.assertLen(test_ds, 2)
    self.assertNotIn('BarcodeValue', test_ds)
    self.assertEqual(_get_value(test_ds['NumberOfOpticalPaths']), expected_num)
    self.assertEqual(test_ds['OpticalPathSequence'][0].PatientName, 'Test')
    self.assertEqual(test_ds['OpticalPathSequence'][1].PatientName, 'Test')

  def test_copy_pydicom_dataset(self):
    source = pydicom.Dataset()
    source.FrameOfReferenceUID = '1.2.3'
    source.PositionReferenceIndicator = 'bar'
    dest = pydicom.Dataset()

    dicom_util.copy_pydicom_dataset(source, dest)
    source.FrameOfReferenceUID = '1.2.3.4'

    self.assertEqual(dest.FrameOfReferenceUID, '1.2.3')
    self.assertEqual(dest.PositionReferenceIndicator, 'bar')

  def test_set_sop_instance_uid(self):
    ds = pydicom.Dataset()
    ds.file_meta = pydicom.dataset.FileMetaDataset()
    sop_instance_uid = pydicom.uid.generate_uid()
    dicom_util.set_sop_instance_uid(ds, sop_instance_uid)

    self.assertEqual(ds.file_meta.MediaStorageSOPInstanceUID, sop_instance_uid)
    self.assertEqual(ds.SOPInstanceUID, sop_instance_uid)

  @mock.patch.object(
      uid_generator, 'generate_uid', autospec=True, return_value='1.9.9'
  )
  def test_get_additional_wsi_specific_dicom_metadata_no_prior_data(
      self, unused_mock
  ):
    dicom_store_url = 'https://mock.dicom.store.com/dicomWeb'
    client = dicom_store_client.DicomStoreClient(dicom_store_url)
    with dicom_store_mock.MockDicomStores(dicom_store_url):

      additional_data = dicom_util.get_additional_wsi_specific_dicom_metadata(
          client, _mock_study_series_uid_dicom_json()
      )

    self.assertLen(list(additional_data.keys()), 2)
    self.assertEqual(additional_data.FrameOfReferenceUID, '1.9.9')
    self.assertEqual(
        additional_data[ingest_const.DICOMTagAddress.PYRAMID_UID].value,
        '1.9.9',
    )

  def test_get_additional_wsi_specific_dicom_metadata_from_store_data(self):
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.3.4'
    prior = pydicom.Dataset()
    prior.SOPClassUID = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    prior.StudyInstanceUID = study_instance_uid
    prior.SeriesInstanceUID = series_instance_uid
    prior.SOPInstanceUID = '1.2'
    prior.FrameOfReferenceUID = '1.2.3.4'
    prior.PositionReferenceIndicator = 'bar'
    prior.add(
        pydicom.DataElement(
            ingest_const.DICOMTagAddress.PYRAMID_UID, 'UI', '1.2.3.4.5'
        )
    )
    prior.add(
        pydicom.DataElement(
            ingest_const.DICOMTagAddress.PYRAMID_LABEL, 'LO', 'mock_label'
        )
    )
    prior.add(
        pydicom.DataElement(
            ingest_const.DICOMTagAddress.PYRAMID_DESCRIPTION,
            'LO',
            'mock_description',
        )
    )
    dicom_store_url = 'https://mock.dicom.store.com/dicomWeb'
    client = dicom_store_client.DicomStoreClient(dicom_store_url)
    with dicom_store_mock.MockDicomStores(dicom_store_url) as mock_store:
      mock_store[dicom_store_url].add_instance(prior.to_json_dict())
      additional_data = dicom_util.get_additional_wsi_specific_dicom_metadata(
          client,
          _mock_study_series_uid_dicom_json(
              study_instance_uid, series_instance_uid
          ),
      )

    self.assertLen(list(additional_data.keys()), 5)
    self.assertEqual(additional_data.FrameOfReferenceUID, '1.2.3.4')
    self.assertEqual(additional_data.PositionReferenceIndicator, 'bar')
    self.assertEqual(
        additional_data[ingest_const.DICOMTagAddress.PYRAMID_UID].value,
        '1.2.3.4.5',
    )
    self.assertEqual(
        additional_data[ingest_const.DICOMTagAddress.PYRAMID_LABEL].value,
        'mock_label',
    )
    self.assertEqual(
        additional_data[ingest_const.DICOMTagAddress.PYRAMID_DESCRIPTION].value,
        'mock_description',
    )

  def test_get_additional_wsi_specific_dicom_metadata_from_passed_data(self):
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.3.4'
    prior = pydicom.Dataset()
    prior.SOPClassUID = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    prior.StudyInstanceUID = study_instance_uid
    prior.SeriesInstanceUID = series_instance_uid
    prior.SOPInstanceUID = '1.2'
    prior.FrameOfReferenceUID = '1.2.3.4'
    prior.PositionReferenceIndicator = 'bar'
    prior.add(
        pydicom.DataElement(
            ingest_const.DICOMTagAddress.PYRAMID_UID, 'UI', '1.2.3.4.6'
        )
    )
    prior.add(
        pydicom.DataElement(
            ingest_const.DICOMTagAddress.PYRAMID_LABEL, 'LO', 'mock_label2'
        )
    )
    prior.add(
        pydicom.DataElement(
            ingest_const.DICOMTagAddress.PYRAMID_DESCRIPTION,
            'LO',
            'mock_description2',
        )
    )

    dicom_store_url = 'https://mock.dicom.store.com/dicomWeb'
    client = dicom_store_client.DicomStoreClient(dicom_store_url)
    with dicom_store_mock.MockDicomStores(dicom_store_url):
      additional_data = dicom_util.get_additional_wsi_specific_dicom_metadata(
          client,
          _mock_study_series_uid_dicom_json(
              study_instance_uid, series_instance_uid
          ),
          [
              wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_json(
                  prior.to_json_dict()
              )
          ],
      )

    self.assertLen(list(additional_data.keys()), 5)
    self.assertEqual(additional_data.FrameOfReferenceUID, '1.2.3.4')
    self.assertEqual(additional_data.PositionReferenceIndicator, 'bar')
    self.assertEqual(
        additional_data[ingest_const.DICOMTagAddress.PYRAMID_UID].value,
        '1.2.3.4.6',
    )
    self.assertEqual(
        additional_data[ingest_const.DICOMTagAddress.PYRAMID_LABEL].value,
        'mock_label2',
    )
    self.assertEqual(
        additional_data[ingest_const.DICOMTagAddress.PYRAMID_DESCRIPTION].value,
        'mock_description2',
    )

  def test_add_date_and_equipment_metadata_to_dicom(self):
    dcm = pydicom.dataset.FileDataset(filename_or_obj='', dataset={})
    dicom_util.add_general_metadata_to_dicom(dcm)
    self.assertEqual(dcm.Manufacturer, 'GOOGLE')
    self.assertIn('ContentDate', dcm)

  @parameterized.named_parameters(
      dict(
          testcase_name='add_type2_tags',
          add_type2c=False,
          additional_expected_tags=set(),
      ),
      dict(
          testcase_name='add_type2_and_2c_tags',
          add_type2c=True,
          additional_expected_tags={
              'PatientBreedCodeSequence',
              'ResponsibleOrganization',
              'BreedRegistrationSequence',
              'PatientPosition',
              'ResponsiblePerson',
              'PatientOrientation',
              'Laterality',
              'PatientBreedDescription',
          },
      ),
  )
  def test_add_missing_type2_dicom_metadata(
      self, add_type2c, additional_expected_tags
  ):
    def _get_dicom_tags(ds):
      keywords = set()
      for tag, tag_value in ds.items():
        tag_address = f'{tag.group:04X}{tag.element:04X}'
        kw = pydicom.datadict.keyword_for_tag(tag_address)
        vr = pydicom.datadict.dictionary_VR(tag_address)
        if vr != 'SQ' or not tag_value.value:
          keywords.add(kw)
        else:
          for index, ds in enumerate(tag_value):
            for inner_kw in _get_dicom_tags(ds):
              keywords.add(f'{kw}[{index}].{inner_kw}')
      return keywords

    expected_tags = {
        'PatientName',
        'PatientID',
        'PatientBirthDate',
        'PatientSex',
        'StudyDate',
        'StudyTime',
        'ReferringPhysicianName',
        'StudyID',
        'AccessionNumber',
        'SeriesNumber',
        'PositionReferenceIndicator',
        'AcquisitionContextSequence',
        'IssuerOfTheContainerIdentifierSequence',
        'ContainerTypeCodeSequence',
        'BarcodeValue',
        'AlternateContainerIdentifierSequence[0].IssuerOfTheContainerIdentifierSequence',
        'LabelText',
    }
    expected_tags = additional_expected_tags | expected_tags
    ds = pydicom.Dataset()
    ds.SOPClassUID = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    ds.AlternateContainerIdentifierSequence = [pydicom.Dataset()]
    with flagsaver.flagsaver(
        create_null_type2c_dicom_tag_if_metadata_if_undefined=add_type2c
    ):
      dicom_util.add_missing_type2_dicom_metadata(ds)
    self.assertEqual(_get_dicom_tags(ds) - {'SOPClassUID'}, expected_tags)

  def test_add_missing_type2_dicom_metadata_doesnt_overwrite_existing_tags(
      self,
  ):
    ds = pydicom.Dataset()
    ds.SOPClassUID = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    ds.StudyID = 'abc'
    dicom_util.add_missing_type2_dicom_metadata(ds)
    self.assertEqual(ds.StudyID, 'abc')

  def test_set_frametype_to_imagetype_no_image_type(self):
    ds = pydicom.Dataset()
    dicom_util.set_frametype_to_imagetype(ds)
    self.assertNotIn('ImageType', ds)
    self.assertNotIn('FrameType', ds)

  def test_set_frametype_to_imagetype_create_structure(self):
    ds = pydicom.Dataset()
    ds.ImageType = ['FOO', 'BAR']
    dicom_util.set_frametype_to_imagetype(ds)
    self.assertEqual(ds.ImageType, ['FOO', 'BAR'])
    self.assertNotIn(
        ds.SharedFunctionalGroupsSequence[0]
        .WholeSlideMicroscopyImageFrameTypeSequence[0]
        .FrameType,
        ['FOO', 'BAR'],
    )

  def test_set_frametype_to_imagetype_merge_with_structure(self):
    ds = pydicom.Dataset()
    ds.ImageType = ['FOO', 'BAR']
    ds.SharedFunctionalGroupsSequence = [pydicom.Dataset()]
    ds.SharedFunctionalGroupsSequence[0].StudyInstanceUID = '1.2.3'
    ds.SharedFunctionalGroupsSequence[
        0
    ].WholeSlideMicroscopyImageFrameTypeSequence = [pydicom.Dataset()]
    ds.SharedFunctionalGroupsSequence[
        0
    ].WholeSlideMicroscopyImageFrameTypeSequence[0].StudyInstanceUID = '4.6.3'
    dicom_util.set_frametype_to_imagetype(ds)
    self.assertEqual(ds.ImageType, ['FOO', 'BAR'])
    self.assertNotIn(
        ds.SharedFunctionalGroupsSequence[0]
        .WholeSlideMicroscopyImageFrameTypeSequence[0]
        .FrameType,
        ['FOO', 'BAR'],
    )
    self.assertEqual(
        ds.SharedFunctionalGroupsSequence[0].StudyInstanceUID, '1.2.3'
    )
    self.assertEqual(
        ds.SharedFunctionalGroupsSequence[0]
        .WholeSlideMicroscopyImageFrameTypeSequence[0]
        .StudyInstanceUID,
        '4.6.3',
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='image_volume_depth_defined',
          metadata='{"00480003": {"Value": [5.0], "vr": "FL"}}',
          expected=5.0,
      ),
      dict(
          testcase_name='slice_thickness_undefined_init_from_flg',
          metadata='{}',
          expected=12.0,
      ),
      dict(
          testcase_name='slice_thickness_defined_from_pixel_measurment_sq',
          metadata=(
              '{"52009229": {"vr": "SQ", "Value": [{"00289110": {"vr": "SQ",'
              ' "Value": [{"00180050": {"vr": "DS", "Value":["0.004"]}}]}}]}}'
          ),
          expected=4.0,
      ),
  )
  def test_init_slice_thickness_metadata_if_undefined(self, metadata, expected):
    ds = pydicom.Dataset().from_json(metadata)
    dicom_util._init_slice_thickness_metadata_if_undefined(ds)
    self.assertEqual(ds.ImagedVolumeDepth, expected)
    self.assertEqual(
        ds.SharedFunctionalGroupsSequence[0]
        .PixelMeasuresSequence[0]
        .SliceThickness,
        expected / 1000.0,
    )

  def test_init_acquision_date_time_if_undefined_nop_defined_in_metadata(self):
    metadata = '{"0008002A": {"Value": ["20230617120160.50"], "vr": "DT"}}'
    ds = pydicom.Dataset().from_json(metadata)
    dicom_util._init_acquision_date_time_if_undefined(ds)
    self.assertEqual(ds.AcquisitionDateTime, '20230617120160.50')
    self.assertNotIn('AcquisitionDate', ds)
    self.assertNotIn('AcquisitionTime', ds)

  @parameterized.named_parameters([
      dict(
          testcase_name='acquision_date_time_tags',
          metadata=(
              '{"00080022": {"Value": ["20230617"], "vr": "DA"},'
              ' "00080032":{"Value":["111213.1"], "vr":"TM"}, "00080023":'
              ' {"Value": ["10230617"], "vr": "DA"},'
              ' "00080033":{"Value":["211213.1"], "vr":"TM"}}'
          ),
          exp_date='20230617',
          exp_time='111213.1',
          exp_datetime='20230617111213.1',
      ),
      dict(
          testcase_name='content_date_time_tags',
          metadata=(
              '{"00080023": {"Value": ["10230617"], "vr": "DA"},'
              ' "00080033":{"Value":["211213.1"], "vr":"TM"}}'
          ),
          exp_date='10230617',
          exp_time='211213.1',
          exp_datetime='10230617211213.1',
      ),
      dict(
          testcase_name='undefined_use_current_datetime',
          metadata='{}',
          exp_date='19960612',
          exp_time='061215.123',
          exp_datetime='19960612061215.123',
      ),
      dict(
          testcase_name='pad_acquision_time',
          metadata=(
              '{"00080022": {"Value": ["2023"], "vr": "DA"},'
              ' "00080032":{"Value":["11"], "vr":"TM"}, "00080023":'
              ' {"Value": ["10230617"], "vr": "DA"},'
              ' "00080033":{"Value":["211213.1"], "vr":"TM"}}'
          ),
          exp_date='2023',
          exp_time='11',
          exp_datetime='20230000110000',
      ),
      dict(
          testcase_name='pad_content_date_time',
          metadata=(
              '{"00080023": {"Value": ["1023"], "vr": "DA"},'
              ' "00080033":{"Value":["21"], "vr":"TM"}}'
          ),
          exp_date='1023',
          exp_time='21',
          exp_datetime='10230000210000',
      ),
  ])
  @mock.patch.object(
      dicom_util,
      '_dicom_formatted_date',
      autospec=True,
      return_value='19960612',
  )
  @mock.patch.object(
      dicom_util,
      '_dicom_formatted_time',
      autospec=True,
      return_value='061215.123',
  )
  def test_init_acquision_date_time_if_undefined(
      self, *unused_mocks, metadata, exp_date, exp_time, exp_datetime
  ):
    ds = pydicom.Dataset().from_json(metadata)
    dicom_util._init_acquision_date_time_if_undefined(ds)
    self.assertEqual(ds.AcquisitionDateTime, exp_datetime)
    self.assertEqual(ds.AcquisitionDate, exp_date)
    self.assertEqual(ds.AcquisitionTime, exp_time)

  def test_create_basic_offset_tables(self):
    mock_frame_data = (''.join([str(i) for i in range(0, 10)])).encode('utf-8')
    file_meta = pydicom.dataset.FileMetaDataset()
    file_meta.TransferSyntaxUID = (
        ingest_const.DicomImageTransferSyntax.JPEG_LOSSY
    )
    ds = pydicom.FileDataset(
        '', pydicom.Dataset(), file_meta=file_meta, preamble=b'\0' * 128
    )
    ds.NumberOfFrames = 3
    ds.PixelData = pydicom.encaps.encapsulate(
        [mock_frame_data, mock_frame_data, mock_frame_data]
    )

    dicom_util.if_missing_create_encapsulated_frame_offset_table(ds)

    # test basic offset table points to Frame Data.
    file_like = pydicom.filebase.DicomFileLike(io.BytesIO(ds.PixelData))
    file_like.is_little_endian = True
    has_basic_offset_table, offset_table = pydicom.encaps.get_frame_offsets(
        file_like
    )
    self.assertTrue(has_basic_offset_table)
    self.assertNotIn('ExtendedOffsetTable', ds)
    self.assertNotIn('ExtendedOffsetTableLengths', ds)
    # Basic offset table format:
    # https://dicom.nema.org/medical/dicom/current/output/chtml/part05/sect_A.4.html
    # tag(0xfeff00e0)(4 byte basic offset table length)[(4 byte offset to start
    # of frame)*number_of_frames][(frame pixel data tag 4 bytes, feff00e0)
    # (4 byte frame data length)(frame data) * number_of_frames].

    # Validate basic offset table
    with io.BytesIO(ds.PixelData) as pixe_data:
      tag = binascii.hexlify(bytearray(pixe_data.read(4)))
      # offset table tag.
      self.assertEqual(tag, b'feff00e0')
      # read length of offset table
      table_size = int.from_bytes(pixe_data.read(4), 'little')
      self.assertEqual(table_size, 12)
      # size of header is table size + tag size + size of table length.
      table_size += 8
    # Validate can use basic offset table to access encapsulated frame
    # data.
    with io.BytesIO(ds.PixelData) as pixe_data:
      for index in range(3):
        offset = offset_table[index] + table_size
        # Seek to start of frame
        pixe_data.seek(offset, os.SEEK_CUR)
        # Read frame tag.
        tag = binascii.hexlify(bytearray(pixe_data.read(4)))
        self.assertEqual(tag, b'feff00e0')
        # read length of frame data.
        length = int.from_bytes(pixe_data.read(4), 'little')
        # read frame data.
        frame_data = pixe_data.read(length)
        self.assertEqual(frame_data, mock_frame_data)
        # seek back to start of first frame
        pixe_data.seek(-offset - 8 - length, os.SEEK_CUR)

  def test_create_extended_offset_tables(self):
    large_frame_data = np.arange(0, 1024, dtype=np.uint32).tobytes()
    file_meta = pydicom.dataset.FileMetaDataset()
    file_meta.TransferSyntaxUID = (
        ingest_const.DicomImageTransferSyntax.JPEG_LOSSY
    )
    ds = pydicom.FileDataset(
        '', pydicom.Dataset(), file_meta=file_meta, preamble=b'\0' * 128
    )
    ds.NumberOfFrames = 3
    ds.PixelData = pydicom.encaps.encapsulate(
        [large_frame_data] * ds.NumberOfFrames, has_bot=False
    )

    original_max_pixel_data_size = (
        dicom_util._MAX_PIXEL_DATA_SIZE_FOR_BASIC_OFFSET_TABLE
    )
    try:
      dicom_util._MAX_PIXEL_DATA_SIZE_FOR_BASIC_OFFSET_TABLE = 0xFF
      dicom_util.if_missing_create_encapsulated_frame_offset_table(ds)
    finally:
      dicom_util._MAX_PIXEL_DATA_SIZE_FOR_BASIC_OFFSET_TABLE = (
          original_max_pixel_data_size
      )
    # test basic offset table points to Frame Data.
    file_like = pydicom.filebase.DicomFileLike(io.BytesIO(ds.PixelData))
    file_like.is_little_endian = True
    has_basic_offset_table, _ = pydicom.encaps.get_frame_offsets(file_like)
    self.assertFalse(has_basic_offset_table)
    extended_offset_table = np.frombuffer(
        ds.ExtendedOffsetTable, dtype=np.uint64
    )
    extended_offset_table_length = np.frombuffer(
        ds.ExtendedOffsetTableLengths, dtype=np.uint64
    )
    with io.BytesIO(ds.PixelData) as pixe_data:
      for index in range(3):
        offset = int(extended_offset_table[index] + 8)
        extended_offset_length = extended_offset_table_length[index]
        # Seek to start of frame
        pixe_data.seek(offset, os.SEEK_CUR)
        # Read frame tag.
        tag = binascii.hexlify(bytearray(pixe_data.read(4)))
        self.assertEqual(tag, b'feff00e0')
        # read length of frame data.
        length = int.from_bytes(pixe_data.read(4), 'little')
        self.assertEqual(length, extended_offset_length)
        # read frame data.
        frame_data = pixe_data.read(length)
        self.assertEqual(frame_data, large_frame_data)
        # seek back to start of first frame
        pixe_data.seek(-offset - 8 - length, os.SEEK_CUR)

  def test_create_offset_tables_nop_if_no_pixel_data(self):
    file_meta = pydicom.dataset.FileMetaDataset()
    file_meta.TransferSyntaxUID = (
        ingest_const.DicomImageTransferSyntax.JPEG_LOSSY
    )
    ds = pydicom.FileDataset(
        '', pydicom.Dataset(), file_meta=file_meta, preamble=b'\0' * 128
    )
    dicom_util.if_missing_create_encapsulated_frame_offset_table(ds)
    self.assertNotIn('PixelData', ds)
    self.assertNotIn('ExtendedOffsetTable', ds)
    self.assertNotIn('ExtendedOffsetTableLengths', ds)

  def test_create_offset_tables_nop_if_not_encapsulated_pixel_data(self):
    file_meta = pydicom.dataset.FileMetaDataset()
    file_meta.TransferSyntaxUID = (
        ingest_const.DicomImageTransferSyntax.IMPLICIT_VR_LITTLE_ENDIAN
    )
    ds = pydicom.FileDataset(
        '', pydicom.Dataset(), file_meta=file_meta, preamble=b'\0' * 128
    )
    ds.PixelData = b'1234'
    dicom_util.if_missing_create_encapsulated_frame_offset_table(ds)

    with io.BytesIO(ds.PixelData) as pixe_data:
      tag = binascii.hexlify(bytearray(pixe_data.read(4)))
      # offset table tag.
      self.assertNotEqual(tag, b'feff00e0')

    self.assertNotIn('ExtendedOffsetTable', ds)
    self.assertNotIn('ExtendedOffsetTableLengths', ds)

  def test_create_offset_tables_nop_if_has_basic_offset_tables(self):
    file_meta = pydicom.dataset.FileMetaDataset()
    file_meta.TransferSyntaxUID = (
        ingest_const.DicomImageTransferSyntax.JPEG_LOSSY
    )
    ds = pydicom.FileDataset(
        '', pydicom.Dataset(), file_meta=file_meta, preamble=b'\0' * 128
    )
    ds.NumberOfFrames = 4
    ds.PixelData = pydicom.encaps.encapsulate([b'1234'] * ds.NumberOfFrames)
    with mock.patch.object(
        pydicom.encaps, 'encapsulate', autospec=True
    ) as mock_encapsulate:
      dicom_util.if_missing_create_encapsulated_frame_offset_table(ds)
      mock_encapsulate.assert_not_called()
    self.assertNotIn('ExtendedOffsetTable', ds)
    self.assertNotIn('ExtendedOffsetTableLengths', ds)

  def test_create_offset_tables_nop_if_has_extended_offset_tables(self):
    file_meta = pydicom.dataset.FileMetaDataset()
    file_meta.TransferSyntaxUID = (
        ingest_const.DicomImageTransferSyntax.JPEG_LOSSY
    )
    ds = pydicom.FileDataset(
        '', pydicom.Dataset(), file_meta=file_meta, preamble=b'\0' * 128
    )
    ds.NumberOfFrames = 4
    ds.PixelData = pydicom.encaps.encapsulate(
        [b'1234'] * ds.NumberOfFrames, has_bot=False
    )
    ds.ExtendedOffsetTable = b'12345678'
    with mock.patch.object(
        pydicom.encaps, 'encapsulate', autospec=True
    ) as mock_encapsulate:
      dicom_util.if_missing_create_encapsulated_frame_offset_table(ds)
      mock_encapsulate.assert_not_called()
    self.assertNotIn('ExtendedOffsetTableLengths', ds)


if __name__ == '__main__':
  absltest.main()
