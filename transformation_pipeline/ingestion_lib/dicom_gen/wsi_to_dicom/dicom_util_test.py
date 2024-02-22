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

import datetime
import os
import shutil
from typing import Any, MutableMapping, Optional
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import openslide
import PIL
import pydicom

from shared_libs.test_utils.dicom_store_mock import dicom_store_mock
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

  def test_add_add_openslide_dicom_properties(self):
    ds = pydicom.Dataset()
    dicom_util.add_default_optical_path_sequence(ds, None)
    path = gen_test_util.test_file_path('ndpi_test.ndpi')
    dicom_util.add_openslide_dicom_properties(ds, path)
    self.assertEqual(ds.OpticalPathSequence[0].ObjectiveLensPower, '20')
    self.assertNotIn('RecommendedAbsentPixelCIELabValue', ds)
    self.assertEqual(
        ds.TotalPixelMatrixOriginSequence[0].XOffsetInSlideCoordinateSystem, 0
    )
    self.assertEqual(
        ds.TotalPixelMatrixOriginSequence[0].YOffsetInSlideCoordinateSystem, 0
    )

  @parameterized.parameters(
      ['TotalPixelMatrixOriginSequence', 'OpticalPathSequence']
  )
  @flagsaver.flagsaver(add_openslide_total_pixel_matrix_origin_seq=True)
  def test_add_add_openslide_dicom_properties_raises_value_error_missing_sq(
      self, sq_name
  ):
    ds = pydicom.Dataset()
    dicom_util.add_default_optical_path_sequence(ds, None)
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
      PIL.ImageCms, 'ImageCmsProfile', return_value=mock.Mock(), autospec=True
  )
  @mock.patch.object(
      PIL.ImageCms,
      'getProfileDescription',
      return_value='result',
      autospec=True,
  )
  def test_add_icc_colorspace_if_missing(self, m1, m2):  # pylint: disable=unused-argument
    profile = icc_bytes()
    inner_ds = pydicom.Dataset()
    inner_ds.ICCProfile = profile
    ds = pydicom.Dataset()
    ds.OpticalPathSequence = pydicom.Sequence([inner_ds])
    ds.ICCProfile = profile
    test = pydicom.FileDataset('foo.dcm', ds)

    dicom_util.add_icc_colorspace_if_not_defined(test)

    self.assertEqual(test.ColorSpace, 'RESULT')
    self.assertEqual(test.OpticalPathSequence[0].ColorSpace, 'RESULT')

  def test_single_code_val_sq(self):
    ds = dicom_util._single_code_val_sq('123', 'bar', 'foo')

    self.assertLen(ds, 1)
    self.assertEqual(ds[0].CodeValue, '123')
    self.assertEqual(ds[0].CodingSchemeDesignator, 'bar')
    self.assertEqual(ds[0].CodeMeaning, 'foo')

  @mock.patch.object(
      PIL.ImageCms, 'ImageCmsProfile', return_value=mock.Mock(), autospec=True
  )
  @mock.patch.object(
      PIL.ImageCms,
      'getProfileDescription',
      return_value='result',
      autospec=True,
  )
  def test_add_default_optical_path_sequence(self, m1, m2):  # pylint: disable=unused-argument
    profile = icc_bytes()
    ds = pydicom.FileDataset('foo.dcm', pydicom.Dataset())
    self.assertTrue(dicom_util.add_default_optical_path_sequence(ds, profile))
    self.assertLen(ds.OpticalPathSequence, 1)
    self.assertEqual(ds.NumberOfOpticalPaths, 1)
    first_seq = ds.OpticalPathSequence[0]
    self.assertLen(first_seq.IlluminationTypeCodeSequence, 1)
    self.assertLen(first_seq.IlluminationColorCodeSequence, 1)
    self.assertLen(ds.TotalPixelMatrixOriginSequence, 1)
    self.assertEqual(
        ds.TotalPixelMatrixOriginSequence[0].XOffsetInSlideCoordinateSystem,
        0,
    )
    self.assertEqual(
        ds.TotalPixelMatrixOriginSequence[0].YOffsetInSlideCoordinateSystem,
        0,
    )
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
        optical_path_seq_tags, ('1', 'Transmitted Light', profile, 'RESULT')
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
      PIL.ImageCms, 'ImageCmsProfile', return_value=mock.Mock(), autospec=True
  )
  @mock.patch.object(
      PIL.ImageCms,
      'getProfileDescription',
      return_value='result',
      autospec=True,
  )
  def test_add_icc_colorspace_to_dicom(self, m1, m2):  # pylint: disable=unused-argument
    profile = icc_bytes()
    ds = pydicom.Dataset()
    ds.ColorSpace = 'foo'
    ds.ICCProfile = profile

    dicom_util.add_icc_colorspace_to_dicom(ds)

    self.assertEqual(ds.ColorSpace, 'RESULT')

  @mock.patch.object(
      PIL.ImageCms, 'ImageCmsProfile', return_value=mock.Mock(), autospec=True
  )
  @mock.patch.object(
      PIL.ImageCms,
      'getProfileDescription',
      side_effect=dicom_util.InvalidICCProfileError(),
      autospec=True,
  )
  def test_add_icc_colorspace_to_dicom_invalid_profile(self, m1, m2):  # pylint: disable=unused-argument
    ds = pydicom.Dataset()
    ds.ColorSpace = 'foo'
    ds.ICCProfile = b'1234'

    with self.assertRaises(dicom_util.InvalidICCProfileError):
      dicom_util.add_icc_colorspace_to_dicom(ds)

  @mock.patch.object(
      PIL.ImageCms, 'ImageCmsProfile', return_value=mock.Mock(), autospec=True
  )
  @mock.patch.object(
      PIL.ImageCms,
      'getProfileDescription',
      return_value='result',
      autospec=True,
  )
  def test_add_icc_profile_to_dicom(self, m1, m2):  # pylint: disable=unused-argument
    profile = icc_bytes()
    ds = pydicom.Dataset()
    ds.ICCProfile = b'1234'
    ds.ColorSpace = 'foobar'

    dicom_util._add_icc_profile_to_dicom(ds, profile)

    self.assertEqual(ds.ICCProfile, profile)
    self.assertEqual(ds.ColorSpace, 'RESULT')

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

  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_set_sop_instance_uid(self):
    ds = pydicom.Dataset()
    ds.file_meta = pydicom.dataset.FileMetaDataset()
    dicom_util.set_sop_instance_uid(ds)

    self.assertEqual(ds.file_meta.MediaStorageSOPInstanceUID, ds.SOPInstanceUID)
    self.assertStartsWith(
        ds.file_meta.MediaStorageSOPInstanceUID, ingest_const.DPAS_UID_PREFIX
    )
    self.assertStartsWith(ds.SOPInstanceUID, ingest_const.DPAS_UID_PREFIX)

  def test_set_sop_instance_uid_custom_uid(self):
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


if __name__ == '__main__':
  absltest.main()
