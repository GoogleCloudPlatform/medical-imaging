# Copyright 2024 Google LLC
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

"""Tests for filter file generator."""

import json
import os
from typing import Any, List, Mapping, Optional
from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import google.auth

from pathology.orchestrator import filter_file_generator
from pathology.orchestrator.v1alpha import cohorts_pb2
from pathology.orchestrator.v1alpha import slides_pb2
from pathology.transformation_pipeline.ingestion_lib.dicom_util import dicomtag


def _sop_uid_dicom_json(uid: Optional[str]) -> Mapping[str, Any]:
  if uid is None:
    return {'00080016': {'vr': 'UI'}}
  return {'00080016': {'Value': [uid], 'vr': 'UI'}}


def _test_file_dir() -> str:
  return os.path.join(os.path.dirname(__file__), 'test_data')


def _read_test_json(filename: str) -> Mapping[str, Any]:
  json_path = os.path.join(_test_file_dir(), filename)
  with open(json_path, 'rt') as infile:
    return json.load(infile)


def _create_test_pathology_cohort(
    slide_url: List[str],
) -> cohorts_pb2.PathologyCohort:
  return cohorts_pb2.PathologyCohort(
      cohort_metadata=cohorts_pb2.PathologyCohortMetadata(
          display_name='TestCohort', description='UNITTEST'
      ),
      slides=[slides_pb2.PathologySlide(dicom_uri=url) for url in slide_url],
  )


class FilterFileGeneratorTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    flags.FLAGS([''], known_only=True)

  def test_instance_metadata_query(self):
    self.assertEqual(
        filter_file_generator._LABEL_METADATA_QUERY,
        (
            'instances?includefield=00080016&includefield=52009229&'
            'includefield=00289110&includefield=00280030&'
            'includefield=00480006&includefield=00480007&'
            'includefield=00480002&includefield=00480001&'
            'includefield=00080008&includefield=00280302&'
            'includefield=00480010&includefield=00080018'
        ),
    )

  def test_dicom_tag_address(self):
    self.assertEqual(
        dicomtag.DicomTag.tag_address('00080016', 'SOPClassUID'), '00080016'
    )

  def test_get_float_env_default(self):
    self.assertNotIn('TEST_FLG', os.environ)
    self.assertEqual(
        filter_file_generator._get_float_env('TEST_FLG', '0.0'), 0.0
    )

  def test_get_float_env_raises(self):
    with mock.patch.dict(os.environ, {'TEST_FLG': 'ABC'}):
      with self.assertRaises(ValueError):
        filter_file_generator._get_float_env('TEST_FLG', '0.0')

  def test_get_float_env_get_env(self):
    with mock.patch.dict(os.environ, {'TEST_FLG': '3.14159'}):
      self.assertEqual(
          filter_file_generator._get_float_env('TEST_FLG', '0.0'), 3.14159
      )

  def test_pixelspacing_is_defined_true(self):
    self.assertTrue(filter_file_generator._PixelSpacing(0.1, 0.1).is_defined)

  def test_pixelspacing_is_defined_false(self):
    self.assertFalse(filter_file_generator._PixelSpacing(0.0, 0.1).is_defined)
    self.assertFalse(filter_file_generator._PixelSpacing(0.1, 0.0).is_defined)

  def test_pixelspacing_min_spacing_false(self):
    self.assertEqual(
        filter_file_generator._PixelSpacing(0.0, 0.1).min_spacing, 0.0
    )
    self.assertEqual(
        filter_file_generator._PixelSpacing(0.1, 0.0).min_spacing, 0.0
    )

  def test_get_series_path_from_dicom_uri(self):
    path = filter_file_generator.get_full_path_from_dicom_uri(
        'https://healthcare.googleapis'
        '.com/v1/projects/PROJECT_ID/'
        'locations/LOCATION/datasets/'
        'DATASET_ID/dicomStores/'
        'DICOM_STORE_ID/dicomWeb/'
        'studies/STUDY_UID/series/'
        'SERIES_UID/instances/'
        'INSTANCE_UID'
    )
    self.assertEqual(
        path, 'studies/STUDY_UID/series/SERIES_UID/instances/INSTANCE_UID'
    )

  def test_get_slides_for_deid_filter_list(self):
    export_list = filter_file_generator.get_slides_for_deid_filter_list(
        _create_test_pathology_cohort(['abc/dicomWeb/123', 'abc/dicomWeb/456'])
    )

    self.assertEqual(export_list, ['123', '456'])

  def test_get_iod_success(self):
    dicom_json = _sop_uid_dicom_json('1.2.3')
    self.assertEqual(filter_file_generator._get_iod(dicom_json), '1.2.3')

  def test_is_vl_whole_slide_microscope_image_iod_true(self):
    dicom_json = _sop_uid_dicom_json('1.2.840.10008.5.1.4.1.1.77.1.6')
    self.assertTrue(
        filter_file_generator._is_vl_whole_slide_microscope_image_iod(
            dicom_json
        )
    )

  def test_is_flat_microscope_image_iod_false(self):
    dicom_json = _sop_uid_dicom_json('1.2.840.10008.5.1.4.1.1.77.1.6')
    self.assertFalse(
        filter_file_generator._is_flat_microscope_image_iod(dicom_json)
    )

  @flagsaver.flagsaver(deid_iod_list='1.2.3')
  def test_get_iod_deid_list_single_value(self):
    self.assertEqual(filter_file_generator._get_iod_deid_list(), ['1.2.3'])

  @flagsaver.flagsaver(deid_iod_list='1.2.3, 4.5.6')
  def test_get_iod_deid_list_list_value(self):
    self.assertEqual(
        filter_file_generator._get_iod_deid_list(), ['1.2.3', '4.5.6']
    )

  def test_get_iod_deid_list_default(self):
    self.assertEqual(
        filter_file_generator._get_iod_deid_list(),
        ['1.2.840.10008.5.1.4.1.1.77.1.6'],
    )

  @flagsaver.flagsaver(deid_max_magnification=' 20x ')
  def test_max_magnification_flg_val(self):
    self.assertEqual(filter_file_generator._max_magnification_flg_val(), '20X')

  @flagsaver.flagsaver(deid_max_magnification=' 20x ')
  def test_is_deid_max_magnification_flag_enabled_true(self):
    self.assertTrue(
        filter_file_generator._is_deid_max_magnification_flag_enabled()
    )

  @flagsaver.flagsaver(deid_max_magnification=' False ')
  def test_is_deid_max_magnification_flag_enabled_false(self):
    self.assertFalse(
        filter_file_generator._is_deid_max_magnification_flag_enabled()
    )

  def test_get_instance_pixel_spacing_functional_group(self):
    pixel_spacing = filter_file_generator._get_instance_pixel_spacing(
        _read_test_json('pyramid_40x.json')
    )

    self.assertTrue(pixel_spacing.is_defined)  # type: ignore
    self.assertEqual(pixel_spacing.min_spacing, 0.000263)  # type: ignore

  @mock.patch.object(
      filter_file_generator,
      '_get_shared_functional_group_pixel_spacing',
      autospec=True,
  )
  def test_get_instance_pixel_derived_spacing(self, mock_func):
    mock_func.side_effect = KeyError()

    pixel_spacing = filter_file_generator._get_instance_pixel_spacing(
        _read_test_json('pyramid_40x.json')
    )

    self.assertTrue(pixel_spacing.is_defined)  # type: ignore
    self.assertEqual(
        pixel_spacing.min_spacing, 0.0002630310034122261  # pytype: disable=attribute-error
    )

  @mock.patch.object(
      filter_file_generator,
      '_get_shared_functional_group_pixel_spacing',
      autospec=True,
  )
  @mock.patch.object(
      filter_file_generator, '_get_derived_pixel_spacing', autospec=True
  )
  def test_get_instance_pixel_missing_pixel_spacing(
      self, mock_func_1, mock_func_2
  ):
    mock_func_2.side_effect = IndexError()
    mock_func_1.side_effect = ZeroDivisionError()
    metadata = _read_test_json('pyramid_40x.json')

    self.assertIsNone(
        filter_file_generator._get_instance_pixel_spacing(metadata)
    )

  def test_get_instance_pixel_unknown_iod(self):
    self.assertIsNone(
        filter_file_generator._get_instance_pixel_spacing(
            _sop_uid_dicom_json('1.2.34')
        )
    )

  @mock.patch.object(google.auth, 'default', autospec=True)
  @mock.patch.object(
      google.auth.transport.requests.AuthorizedSession, 'get', autospec=True
  )
  def test_get_instances_for_deid(self, get_mock, default_mock):
    default_mock.return_value = ['mock_default']
    cohort = _create_test_pathology_cohort(['abc/dicomWeb/123'])

    mock_response = mock.MagicMock()
    mock_response.json.return_value = [
        _read_test_json('pyramid_40x.json'),
        _read_test_json('pyramid_20x.json'),
    ]
    get_mock.return_value = mock_response

    instance_list = filter_file_generator.get_instances_for_deid(cohort)
    self.assertEqual(
        instance_list,
        [
            (
                '123/instances/1.3.6.1.4.1.11129.5.7.0.1.512753102393.27167502.'
                '1658166457922126'
            ),
            (
                '123/instances/1.3.6.1.4.1.11129.5.7.0.1.512753102393.27167502.'
                '1658166462706128'
            ),
        ],
    )

  @mock.patch.object(google.auth, 'default', autospec=True)
  @mock.patch.object(
      google.auth.transport.requests.AuthorizedSession, 'get', autospec=True
  )
  @flagsaver.flagsaver(deid_max_magnification=' 20x ')
  def test_get_instances_for_deid_20x(self, get_mock, default_mock):
    default_mock.return_value = ['mock_default']
    cohort = _create_test_pathology_cohort(['abc/dicomWeb/123'])

    mock_response = mock.MagicMock()
    mock_response.json.return_value = [
        _read_test_json('pyramid_40x.json'),
        _read_test_json('pyramid_20x.json'),
    ]
    get_mock.return_value = mock_response

    instance_list = filter_file_generator.get_instances_for_deid(cohort)
    self.assertEqual(
        instance_list,
        [
            (
                '123/instances/1.3.6.1.4.1.11129.5.7.0.1.512753102393.27167502.'
                '1658166462706128'
            )
        ],
    )

  @mock.patch.object(google.auth, 'default', autospec=True)
  @flagsaver.flagsaver(deid_max_magnification='ABC')
  def test_get_instances_for_deid_invalid_mag(self, default_mock):
    default_mock.return_value = ['mock_default']
    cohort = _create_test_pathology_cohort(['abc/dicomWeb/123'])
    with self.assertRaises(ValueError):
      filter_file_generator.get_instances_for_deid(cohort)

  @parameterized.parameters(
      [({},), ({'00080016': {}},), ({'00080016': {'Value': []}},)]
  )
  def test_get_iod_missing(self, dicom_json):
    iod = filter_file_generator._get_iod(dicom_json)
    self.assertEqual(iod, '')

  @parameterized.parameters(
      ['1.2.840.10008.5.1.4.1.1.77.1.2', '1.2.840.10008.5.1.4.1.1.77.1.3']
  )
  def test_is_vl_whole_slide_microscope_image_iod_false(self, iod_str):
    dicom_json = _sop_uid_dicom_json(iod_str)
    self.assertFalse(
        filter_file_generator._is_vl_whole_slide_microscope_image_iod(
            dicom_json
        )
    )

  @parameterized.parameters(
      ['1.2.840.10008.5.1.4.1.1.77.1.2', '1.2.840.10008.5.1.4.1.1.77.1.3']
  )
  def test_is_flat_microscope_image_iod_true(self, iod_str):
    dicom_json = _sop_uid_dicom_json(iod_str)
    self.assertTrue(
        filter_file_generator._is_flat_microscope_image_iod(dicom_json)
    )

  @parameterized.parameters([
      (' False ', False),
      (' 100X ', True),
      (' 80X ', True),
      (' 40X ', True),
      (' 20X ', True),
      (' 10X ', True),
      (' 5X ', True),
      (' 2.5X ', True),
      (' 1.25X ', True),
      (' 0.625X ', True),
      (' 0.3125X ', True),
      (' 0.15625X ', True),
      (' 0.078125X ', True),
      (' 0.0390625X ', True),
  ])
  def test_estimate_mag(self, max_magnification, enabled):
    with flagsaver.flagsaver(deid_max_magnification=max_magnification):
      if enabled:
        self.assertTrue(
            filter_file_generator._is_deid_max_magnification_flag_enabled()
        )
      else:
        self.assertFalse(
            filter_file_generator._is_deid_max_magnification_flag_enabled()
        )

  @parameterized.parameters([
      ('False', None),
      ('40X', 0.0001875),
      ('20X', 0.000375),
      ('10X', 0.00075),
      ('5X', 0.0015),
      ('2.5X', 0.003),
      ('1.25X', 0.006),
      ('0.625X', 0.012),
      ('0.3125X', 0.024),
      ('0.15625X', 0.048),
      ('0.078125X', 0.096),
      ('0.0390625X', 0.192),
  ])
  @flagsaver.flagsaver(deid_pixel_spacing_threshold_offset=0.0)
  def test_get_deid_min_pixel_spacing(self, max_magnification, spacing):
    with flagsaver.flagsaver(deid_max_magnification=max_magnification):
      if spacing is None:
        self.assertIsNone(filter_file_generator._get_deid_min_pixel_spacing())
      else:
        self.assertEqual(
            filter_file_generator._get_deid_min_pixel_spacing(), spacing
        )

  @parameterized.parameters([
      ('False', None),
      ('100X', 0.0),
      ('80X', 0.0),
      ('40X', 0.0),
      ('20X', 0.0),
      ('10X', 0.0),
      ('5X', 0.001),
      ('2.5X', 0.003),
      ('1.25X', 0.007),
      ('0.625X', 0.015),
      ('0.3125X', 0.031),
      ('0.15625X', 0.063),
      ('0.078125X', 0.127),
      ('0.0390625X', 0.255),
  ])
  @flagsaver.flagsaver(deid_pixel_spacing_threshold_offset=0.001)
  def test_get_deid_min_pixel_spacing_value_offset(
      self, max_magnification, spacing
  ):
    with flagsaver.flagsaver(deid_max_magnification=max_magnification):
      if spacing is None:
        self.assertIsNone(filter_file_generator._get_deid_min_pixel_spacing())
      else:
        self.assertEqual(
            filter_file_generator._get_deid_min_pixel_spacing(), spacing
        )

  @parameterized.parameters([
      ('40x', 0.000263),
      ('20x', 0.000526),
      ('10x', 0.001052),
      ('5x', 0.002104),
      ('2.5x', 0.004209),
      ('1.25x', 0.008419),
      ('0.625x', 0.01684),
      ('0.3125x', 0.033681),
      ('0.15625x', 0.067361),
  ])
  def test_get_shared_functional_group_pixel_spacing(
      self, mag, expected_spacing
  ):
    pixel_spacing = (
        filter_file_generator._get_shared_functional_group_pixel_spacing(
            _read_test_json(f'pyramid_{mag}.json')
        )
    )

    self.assertTrue(pixel_spacing.is_defined)
    self.assertEqual(pixel_spacing.min_spacing, expected_spacing)

  @parameterized.parameters([
      ('40x', 0.0002630310034122261),
      ('20x', 0.0005260732561189271),
      ('10x', 0.0010521465122378542),
      ('5x', 0.00210447303243332),
      ('2.5x', 0.004209666281510183),
      ('1.25x', 0.008419332563020366),
      ('0.625x', 0.01685019845831884),
      ('0.3125x', 0.03370039691663768),
      ('0.15625x', 0.06758596084930085),
  ])
  def test_get_derived_pixel_spacing(self, mag, expected_spacing):
    pixel_spacing = filter_file_generator._get_derived_pixel_spacing(
        _read_test_json(f'pyramid_{mag}.json')
    )

    self.assertTrue(pixel_spacing.is_defined)
    self.assertEqual(pixel_spacing.min_spacing, expected_spacing)

  @parameterized.parameters([
      ('slide_coord_microscope.json', 0.03),
      ('microscope_image.json', 0.04),
  ])
  def test_get_flat_image_pixel_spacing(self, filename, expected_spacing):
    self.assertEqual(
        filter_file_generator._get_flat_image_pixel_spacing(
            _read_test_json(filename)
        ).min_spacing,
        expected_spacing,
    )

  @parameterized.parameters([
      ('slide_coord_microscope.json', 0.03),
      ('microscope_image.json', 0.04),
  ])
  def test_get_flat_image_get_instance_pixel_spacing(
      self, filename, expected_spacing
  ):
    self.assertEqual(
        filter_file_generator._get_instance_pixel_spacing(
            _read_test_json(filename)
        ).min_spacing,  # type: ignore
        expected_spacing,
    )

  @parameterized.parameters([
      (
          'False',
          (
              'pyramid_0.3125x.json',
              'pyramid_40x.json',
              'pyramid_5x.json',
              'pyramid_1.25x.json',
              'pyramid_2.5x.json',
              'pyramid_10x.json',
              'pyramid_0.15625x.json',
              'pyramid_0.625x.json',
              'pyramid_20x.json',
          ),
      ),
      (
          '40x',
          (
              'pyramid_0.3125x.json',
              'pyramid_40x.json',
              'pyramid_5x.json',
              'pyramid_1.25x.json',
              'pyramid_2.5x.json',
              'pyramid_10x.json',
              'pyramid_0.15625x.json',
              'pyramid_0.625x.json',
              'pyramid_20x.json',
          ),
      ),
      (
          '20x',
          (
              'pyramid_0.3125x.json',
              'pyramid_5x.json',
              'pyramid_1.25x.json',
              'pyramid_2.5x.json',
              'pyramid_10x.json',
              'pyramid_0.15625x.json',
              'pyramid_0.625x.json',
              'pyramid_20x.json',
          ),
      ),
      (
          '10x',
          (
              'pyramid_0.3125x.json',
              'pyramid_5x.json',
              'pyramid_1.25x.json',
              'pyramid_2.5x.json',
              'pyramid_10x.json',
              'pyramid_0.15625x.json',
              'pyramid_0.625x.json',
          ),
      ),
      (
          '5x',
          (
              'pyramid_0.3125x.json',
              'pyramid_5x.json',
              'pyramid_1.25x.json',
              'pyramid_2.5x.json',
              'pyramid_0.15625x.json',
              'pyramid_0.625x.json',
          ),
      ),
      (
          '2.5x',
          (
              'pyramid_0.3125x.json',
              'pyramid_1.25x.json',
              'pyramid_2.5x.json',
              'pyramid_0.15625x.json',
              'pyramid_0.625x.json',
          ),
      ),
      (
          '1.25x',
          (
              'pyramid_0.3125x.json',
              'pyramid_1.25x.json',
              'pyramid_0.15625x.json',
              'pyramid_0.625x.json',
          ),
      ),
      (
          '0.625x',
          (
              'pyramid_0.3125x.json',
              'pyramid_0.15625x.json',
              'pyramid_0.625x.json',
          ),
      ),
      ('0.3125x', ('pyramid_0.3125x.json', 'pyramid_0.15625x.json')),
      (
          '0.15625x',
          ('pyramid_0.15625x.json',),
      ),
  ])
  def test_should_deid_instance(self, max_mag, expected_instances):
    with flagsaver.flagsaver(deid_max_magnification=max_mag):
      min_pixel_spacing_threshold = (
          filter_file_generator._get_deid_min_pixel_spacing()
      )
    cohort_files = [
        'pyramid_0.3125x.json',
        'pyramid_40x.json',
        'slide_coord_microscope.json',
        'microscope_image.json',
        'label.json',
        'pyramid_5x.json',
        'macro.json',
        'pyramid_1.25x.json',
        'pyramid_2.5x.json',
        'pyramid_10x.json',
        'thumbnail.json',
        'pyramid_0.15625x.json',
        'pyramid_0.625x.json',
        'pyramid_20x.json',
        'recognize_features.json',
        'specmine_label.json',
        'bad_iod.json',
    ]
    instance_path = 'bogus'
    deid_instance_list = []

    for file_name in cohort_files:
      dicom_json = _read_test_json(file_name)
      if filter_file_generator._should_deid_instance(
          dicom_json, instance_path, min_pixel_spacing_threshold
      ):
        deid_instance_list.append(file_name)

    self.assertEqual(tuple(deid_instance_list), expected_instances)

  @parameterized.parameters([
      (_sop_uid_dicom_json('1.2.34'), True),
      (_sop_uid_dicom_json('1.2.40'), False),
      (_sop_uid_dicom_json(None), False),
      ({}, False),
  ])
  def test_cs_tag_has_value(self, metadata, expected):
    if expected:
      self.assertTrue(
          filter_file_generator._cs_tag_has_value(
              metadata, '00080016', '1.2.34'
          )
      )
    else:
      self.assertFalse(
          filter_file_generator._cs_tag_has_value(
              metadata, '00080016', '1.2.34'
          )
      )


if __name__ == '__main__':
  absltest.main()
