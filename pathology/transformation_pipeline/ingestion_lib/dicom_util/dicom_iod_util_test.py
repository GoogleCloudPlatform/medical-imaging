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
from absl.testing import absltest
from absl.testing import parameterized

from transformation_pipeline.ingestion_lib.dicom_util import dicom_iod_util
from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard_util


class DICOMIODUtilTest(parameterized.TestCase):

  @parameterized.named_parameters([
      dict(
          testcase_name='root',
          iod_path=None,
          expected=[
              'PixelMeasuresSequence',
              'FrameContentSequence',
              'ReferencedImageSequence',
              'DerivationImageSequence',
              'RealWorldValueMappingSequence',
              'PlanePositionSlideSequence',
              'OpticalPathIdentificationSequence',
              'SpecimenReferenceSequence',
              'WholeSlideMicroscopyImageFrameTypeSequence',
          ],
      ),
      dict(
          testcase_name='root_path',
          iod_path=['WholeSlideMicroscopyImageFrameTypeSequence'],
          expected=['FrameType'],
      ),
  ])
  def test_get_iod_function_group_dicom_dataset(self, iod_path, expected):
    standard_util = dicom_standard_util.DicomStandardIODUtil()
    iod_util = dicom_iod_util.DicomIODDatasetUtil()
    dataset = iod_util.get_iod_function_group_dicom_dataset(
        dicom_standard_util.IODName(
            'VL Whole Slide Microscopy Image IOD Modules'
        ),
        iod_path,
    )
    self.assertEqual(
        [standard_util.get_tag(address).keyword for address in dataset.keys()],  # pytype: disable=attribute-error
        expected,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='root',
          iod_path=None,
          modality_flag=set(['M']),
          requirement_flag=set(['1']),
          expected=[
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
              'SOPClassUID',
              'SOPInstanceUID',
          ],
      ),
      dict(
          testcase_name='referenced_study_sequence',
          iod_path=['ReferencedStudySequence'],
          modality_flag=set(['M']),
          requirement_flag=set(['1']),
          expected=['ReferencedSOPClassUID', 'ReferencedSOPInstanceUID'],
      ),
      dict(
          testcase_name='shared_functional_groups_sequence',
          iod_path=['SharedFunctionalGroupsSequence'],
          modality_flag=set(['M']),
          requirement_flag=set(['1']),
          expected=[
              'WholeSlideMicroscopyImageFrameTypeSequence',
              'PixelMeasuresSequence',
          ],
      ),
      dict(
          testcase_name='shared_functional_groups_sequence.pixel_measures',
          iod_path=['SharedFunctionalGroupsSequence', 'PixelMeasuresSequence'],
          modality_flag=set(['M']),
          requirement_flag=set(['1C']),
          expected=['PixelSpacing', 'SliceThickness', 'SpacingBetweenSlices'],
      ),
  ])
  def test_get_iod_dicom_dataset(
      self, iod_path, expected, modality_flag, requirement_flag
  ):
    iod_util = dicom_iod_util.DicomIODDatasetUtil()
    dataset = iod_util.get_iod_dicom_dataset(
        dicom_standard_util.IODName(
            'VL Whole Slide Microscopy Image IOD Modules'
        ),
        iod_path,
    )
    keyword_list = []
    for tag in dataset.values():
      if tag.required & requirement_flag and tag.module_usage & modality_flag:
        keyword_list.append(tag.keyword)
    self.assertEqual(keyword_list, expected)

  def test_get_iod_dicom_dataset_module_subset(self):
    iod_util = dicom_iod_util.DicomIODDatasetUtil()
    dataset = iod_util.get_iod_dicom_dataset(
        dicom_standard_util.IODName(
            'VL Whole Slide Microscopy Image IOD Modules'
        ),
        None,
        module_name_subset={'Patient'},
    )
    self.assertEqual(
        [tag.keyword for tag in dataset.values() if tag.required & {'2'}],
        ['PatientName', 'PatientID', 'PatientBirthDate', 'PatientSex'],
    )

  def test_get_iod_dicom_dataset_module_subset_raises_undefined_module(self):
    iod_util = dicom_iod_util.DicomIODDatasetUtil()
    with self.assertRaises(dicom_iod_util.IODDoesNotDefineModuleError):
      iod_util.get_iod_dicom_dataset(
          dicom_standard_util.IODName(
              'VL Whole Slide Microscopy Image IOD Modules'
          ),
          None,
          module_name_subset={'Patient', 'Missing'},
      )

  def test_get_root_level_iod_tag_keywords(self):
    iod_util = dicom_iod_util.DicomIODDatasetUtil()
    self.assertEqual(
        iod_util.get_root_level_iod_tag_keywords(
            dicom_standard_util.IODName(
                'VL Whole Slide Microscopy Image IOD Modules'
            ),
            {'Slide Label'},
        ),
        ['BarcodeValue', 'LabelText'],
    )


if __name__ == '__main__':
  absltest.main()
