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
from typing import Any, Optional
from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
import PIL
import pydicom

from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen import wsi_dicom_file_ref
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util

# The following try/except block allows for two separate executions of the code:
# A docker container deployed in GKE and within unit testing framework.
# pylint: disable=g-import-not-at-top
try:
  import openslide  # pytype: disable=import-error  # Deployed GKE container

  _OPENSLIDE_MOCK = 'openslide.OpenSlide'
except ImportError:
  import openslide_python as openslide  # Google3

  _OPENSLIDE_MOCK = 'openslide_python.OpenSlide'

FLAGS = flags.FLAGS

_WIKIPEDIA_DCM_FILENAME = 'test_wikipedia.dcm'


def _get_value(val: Optional[pydicom.dataelem.DataElement]) -> Any:
  if val is not None:
    return val.value
  return None


def _create_frame_of_reference_metadata(
    uid: str, ref_indicator: str
) -> wsi_dicom_file_ref.WSIDicomFileRef:
  metadata = {
      '00200052': {  # FrameOfReferenceUID
          'Value': [uid],
          'vr': 'UI',
      },
      '00201040': {  # PositionReferenceIndicator
          'Value': [ref_indicator],
          'vr': 'LO',
      },
  }
  return wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_json(metadata)


def icc_bytes() -> bytes:
  path = gen_test_util.test_file_path('profile.icc')
  with open(path, 'rb') as infile:
    return infile.read()


class DicomUtilTest(absltest.TestCase):

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
    with mock.patch(_OPENSLIDE_MOCK) as mock_method:
      mock_method.return_value = mock_result

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

  def test_set_defined_pydicom_tags(self):
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

    dicom_util.set_defined_pydicom_tags(
        test_ds,
        tags,
        ('BarcodeValue', 'NumberOfOpticalPaths', 'OpticalPathSequence'),
    )

    self.assertLen(test_ds, 2)
    self.assertNotIn('BarcodeValue', test_ds)
    self.assertEqual(_get_value(test_ds['NumberOfOpticalPaths']), 5)
    self.assertEqual(test_ds['OpticalPathSequence'][0].PatientName, 'Test')
    self.assertEqual(test_ds['OpticalPathSequence'][1].PatientName, 'Test')

  def test_set_wsi_frame_of_ref_metadata(self):
    ds = pydicom.Dataset()

    dicom_util.set_wsi_frame_of_ref_metadata(
        ds, dicom_util.DicomFrameOfReferenceModuleMetadata('1.2.3', 'bar')
    )

    self.assertEqual(ds.FrameOfReferenceUID, '1.2.3')
    self.assertEqual(ds.PositionReferenceIndicator, 'bar')

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

  def test_create_dicom_frame_of_ref_module_metadata_from_file_ref(self):
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.3.4'
    dicom_store = mock.Mock()
    dicom_ref = _create_frame_of_reference_metadata('1.2.3', 'foobar')

    result = dicom_util.create_dicom_frame_of_ref_module_metadata(
        study_instance_uid, series_instance_uid, dicom_store, dicom_ref, None
    )

    self.assertEqual(result.uid, '1.2.3')
    self.assertEqual(result.position_reference_indicator, 'foobar')

  def test_create_dicom_frame_of_ref_module_metadata_from_file_ref_list(self):
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.3.4'
    dicom_ref1 = _create_frame_of_reference_metadata('', 'foobar')
    dicom_ref2 = _create_frame_of_reference_metadata('1.2.3.4', 'second')
    dicom_store = mock.Mock()

    result = dicom_util.create_dicom_frame_of_ref_module_metadata(
        study_instance_uid,
        series_instance_uid,
        dicom_store,
        None,
        [dicom_ref1, dicom_ref2],
    )

    self.assertEqual(result.uid, '1.2.3.4')
    self.assertEqual(result.position_reference_indicator, 'second')

  def test_create_dicom_frame_of_ref_module_metadata_from_dicom_store(self):
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.3.4'
    dicom_ref1 = _create_frame_of_reference_metadata('', 'foobar')
    dicom_ref2 = _create_frame_of_reference_metadata('1.2.3.4.5', 'third')
    dicom_store = mock.Mock()
    dicom_store.get_study_dicom_file_ref.return_value = [dicom_ref1, dicom_ref2]

    result = dicom_util.create_dicom_frame_of_ref_module_metadata(
        study_instance_uid, series_instance_uid, dicom_store, None, None
    )

    self.assertEqual(result.uid, '1.2.3.4.5')
    self.assertEqual(result.position_reference_indicator, 'third')

  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_create_dicom_frame_of_ref_module_metadata_from_partial_file_ref(
      self,
  ):
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.3.4'
    dicom_ref = _create_frame_of_reference_metadata('', 'foobar')
    dicom_store = mock.Mock()
    dicom_store.get_study_dicom_file_ref.return_value = []

    result = dicom_util.create_dicom_frame_of_ref_module_metadata(
        study_instance_uid, series_instance_uid, dicom_store, dicom_ref, None
    )

    self.assertStartsWith(result.uid, ingest_const.DPAS_UID_PREFIX)
    self.assertEqual(result.position_reference_indicator, 'foobar')

  @flagsaver.flagsaver(
      pod_hostname='1234', dicom_guid_prefix=uid_generator.TEST_UID_PREFIX
  )
  def test_create_dicom_frame_of_ref_module_metadata_from_none(self):
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.3.4'
    dicom_store = mock.Mock()
    dicom_store.get_study_dicom_file_ref.return_value = []

    result = dicom_util.create_dicom_frame_of_ref_module_metadata(
        study_instance_uid, series_instance_uid, dicom_store, None, None
    )

    self.assertStartsWith(result.uid, ingest_const.DPAS_UID_PREFIX)
    self.assertEmpty(result.position_reference_indicator)

  def test_add_date_and_equipment_metadata_to_dicom(self):
    dcm = pydicom.dataset.FileDataset(filename_or_obj='', dataset={})
    dicom_util.add_general_metadata_to_dicom(dcm)
    self.assertEqual(dcm.Manufacturer, 'GOOGLE')
    self.assertIn('ContentDate', dcm)


if __name__ == '__main__':
  absltest.main()
