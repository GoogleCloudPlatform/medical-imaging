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
"""Tests for pydicom_util."""
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
import pydicom

from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_util import dicom_iod_util
from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard_util
from transformation_pipeline.ingestion_lib.dicom_util import pydicom_util


class PydicomUtilTest(parameterized.TestCase):

  def test_set_string_keyword(self):
    ds = pydicom.Dataset()
    ds.PatientName = 'Smith^Bob'
    self.assertEqual(ds.PatientName, 'Smith^Bob')

    pydicom_util.set_dataset_tag_value(ds, 'PatientName', 'Smith^Sarah')

    self.assertEqual(ds.PatientName, 'Smith^Sarah')

  def test_set_int_keyword_from_int(self):
    ds = pydicom.Dataset()
    ds.HighBit = 6
    self.assertEqual(ds.HighBit, 6)

    pydicom_util.set_dataset_tag_value(ds, 'HighBit', 7)

    self.assertEqual(ds.HighBit, 7)

  def test_set_float_keyword_from_float(self):
    ds = pydicom.Dataset()
    ds.ExaminedBodyThickness = 1.0
    self.assertEqual(ds.ExaminedBodyThickness, 1.0)

    pydicom_util.set_dataset_tag_value(ds, 'ExaminedBodyThickness', 1.5)

    self.assertEqual(ds.ExaminedBodyThickness, 1.5)

  def test_set_int_keyword_from_str(self):
    ds = pydicom.Dataset()
    ds.HighBit = 6
    self.assertEqual(ds.HighBit, 6)

    pydicom_util.set_dataset_tag_value(ds, 'HighBit', '7')

    self.assertEqual(ds.HighBit, 7)

  def test_set_float_keyword_from_str(self):
    ds = pydicom.Dataset()
    ds.ExaminedBodyThickness = 1.0
    self.assertEqual(ds.ExaminedBodyThickness, 1.0)

    pydicom_util.set_dataset_tag_value(ds, 'ExaminedBodyThickness', '1.5')

    self.assertEqual(ds.ExaminedBodyThickness, 1.5)

  def test_set_dataset_tag_value_if_undefined_not_set(self):
    ds = pydicom.Dataset()
    ds.Manufacturer = 'Foo'

    self.assertFalse(
        pydicom_util.set_dataset_tag_value_if_undefined(
            ds, 'Manufacturer', 'Bar'
        )
    )
    self.assertEqual(ds.Manufacturer, 'Foo')

  def test_set_dataset_tag_value_if_undefined_set(self):
    ds = pydicom.Dataset()

    self.assertTrue(
        pydicom_util.set_dataset_tag_value_if_undefined(
            ds, 'Manufacturer', 'Bar'
        )
    )
    self.assertEqual(ds.Manufacturer, 'Bar')

  @parameterized.named_parameters(
      dict(
          testcase_name='retired_tag_ignored',
          retired='retired',
          tag_type='2',
          module_requirement='M',
          expected=[],
      ),
      dict(
          testcase_name='required_tag_found',
          retired='',
          tag_type='2',
          module_requirement='M',
          expected=['PatientName'],
      ),
      dict(
          testcase_name='type_3_tags_ignored',
          retired='',
          tag_type='3',
          module_requirement='M',
          expected=[],
      ),
      dict(
          testcase_name='type_1_tags_ignored',
          retired='',
          tag_type='1',
          module_requirement='M',
          expected=[],
      ),
      dict(
          testcase_name='type_1c_tags_ignored',
          retired='',
          tag_type='1C',
          module_requirement='M',
          expected=[],
      ),
      dict(
          testcase_name='type_2c_tags_ignored',
          retired='',
          tag_type='2C',
          module_requirement='M',
          expected=[],
      ),
      dict(
          testcase_name='type_c_module_ignored',
          retired='',
          tag_type='2',
          module_requirement='C',
          expected=[],
      ),
      dict(
          testcase_name='type_u_module_ignored',
          retired='',
          tag_type='2',
          module_requirement='U',
          expected=[],
      ),
  )
  @mock.patch.object(
      dicom_iod_util.DicomIODDatasetUtil, 'get_iod_dicom_dataset', autospec=True
  )
  def test_get_undefined_dicom_tags_by_type_ignores_tags_not_matching_search(
      self, mk_get_iod_dataset, retired, expected, tag_type, module_requirement
  ):
    iod_name = dicom_standard_util.IODName(
        ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.name
    )
    tag_def = dicom_standard_util.DcmTagDef(
        dicom_standard_util.DicomTagAddress('0x00100010'),
        dicom_standard_util.DicomKeyword('PatientName'),
        {dicom_standard_util.DicomVRCode('PN')},
        dicom_standard_util.DicomVMCode('1'),
        retired,
        'foo',
    )
    mock_dicom_dataset = {
        str(tag_def.address): dicom_iod_util.DICOMTag(
            tag_def,
            dicom_standard_util.DicomTableTagRequirement(tag_type),
            dicom_iod_util.DicomModuleUsageRequirement(module_requirement),
        )
    }
    mk_get_iod_dataset.return_value = mock_dicom_dataset
    ds = pydicom.Dataset()
    tag_list = pydicom_util._get_undefined_dicom_tags_by_type(
        iod_name, ds, {'2'}, pydicom_util.DICOMTagPath(), None
    )
    self.assertEqual([tag_path.tag.keyword for tag_path in tag_list], expected)

  def test_get_undefined_dicom_tags_by_type_with_undefined_iod_raises(self):
    ds = pydicom.Dataset()
    ds.SOPClassUID = '1.2.3.4'
    with self.assertRaises(pydicom_util.UndefinedIODError):
      pydicom_util.get_undefined_dicom_tags_by_type(ds, {'2'})

  def test_get_undefined_type_1_dicom_tags(self):
    ds = pydicom.Dataset()
    ds.SOPClassUID = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    tag_paths = pydicom_util.get_undefined_dicom_tags_by_type(ds, {'1'})
    self.assertEqual(
        [tag_path.tag.keyword for tag_path in tag_paths],
        [
            'StudyInstanceUID',
            'Modality',
            'SeriesInstanceUID',
            'FrameOfReferenceUID',
            'Manufacturer',
            'ManufacturerModelName',
            'DeviceSerialNumber',
            'SoftwareVersions',
            'AcquisitionDateTime',
            'InstanceNumber',
            'ContentDate',
            'ContentTime',
            'ImageType',
            'BurnedInAnnotation',
            'LossyImageCompression',
            'TotalPixelMatrixColumns',
            'TotalPixelMatrixRows',
            'TotalPixelMatrixOriginSequence',
            'SamplesPerPixel',
            'PhotometricInterpretation',
            'Rows',
            'Columns',
            'BitsAllocated',
            'BitsStored',
            'HighBit',
            'PixelRepresentation',
            'SharedFunctionalGroupsSequence',
            'NumberOfFrames',
            'DimensionOrganizationSequence',
            'ContainerIdentifier',
            'SpecimenDescriptionSequence',
            'VolumetricProperties',
            'SpecimenLabelInImage',
            'FocusMethod',
            'ExtendedDepthOfField',
            'OpticalPathSequence',
            'SOPInstanceUID',
        ],
    )


if __name__ == '__main__':
  absltest.main()
