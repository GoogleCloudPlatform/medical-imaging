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
"""Tests for WSI pyramid generation config."""
import os
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import pydicom

from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import wsi_pyramid_gen_config


_DOWNSAMPLE_TEST_CASES = [
    dict(
        testcase_name='px_spacing < 0.00025',
        pixel_spacing=0.00005,
        expected={1, 8, 32},
    ),
    dict(
        testcase_name='px_spacing = 0.00025',
        pixel_spacing=0.00025,
        expected={1, 8, 32},
    ),
    dict(
        testcase_name='px_spacing > 0.00025 and < 0.001 test 1',
        pixel_spacing=0.00030,
        expected={1, 8, 32},
    ),
    dict(
        testcase_name='px_spacing > 0.00025 and < 0.001 test 2',
        pixel_spacing=0.0009,
        expected={1, 8, 32},
    ),
    dict(
        testcase_name='px_spacing = 0.001', pixel_spacing=0.001, expected={1, 8}
    ),
    dict(
        testcase_name='px_spacing > 0.001 and < 0.004 test 1',
        pixel_spacing=0.0011,
        expected={1, 8},
    ),
    dict(
        testcase_name='px_spacing > 0.001 and < 0.004 test 2',
        pixel_spacing=0.002,
        expected={1, 8},
    ),
    dict(testcase_name='px_spacing = 0.004', pixel_spacing=0.004, expected={1}),
    dict(testcase_name='px_spacing > 0.004', pixel_spacing=1, expected={1}),
]


class WsiPyramidGenerationConfigTest(parameterized.TestCase):

  @parameterized.parameters(['non-exist.yaml', 'bad_path.json'])
  def test_missing_config(self, filename: str):
    with flagsaver.flagsaver(
        ingestion_pyramid_layer_generation_config_path=gen_test_util.test_file_path(
            filename
        )
    ):
      with self.assertRaises(
          wsi_pyramid_gen_config._IngestPyramidConfigFileNotFoundError
      ):
        wsi_pyramid_gen_config.WsiPyramidGenerationConfig()

  def test_invalid_yaml_formatted_config(self):
    with flagsaver.flagsaver(
        ingestion_pyramid_layer_generation_config_path=gen_test_util.test_file_path(
            'layer_config_invalid_dashes.yaml'
        )
    ):
      with self.assertRaises(
          wsi_pyramid_gen_config._IngestPyramidConfigYamlFormatError
      ):
        wsi_pyramid_gen_config.WsiPyramidGenerationConfig()

  @parameterized.named_parameters([
      ('Does not describe dict.', 'layer_config_invalid_empty.yaml'),
      (
          'Downsample config has string non-float value in yaml key',
          'layer_config_invalid_key.yaml',
      ),
      (
          'Downsample list contains value less than 1',
          'layer_config_invalid_value.yaml',
      ),
      (
          'List of downsamples contains string value.',
          'layer_config_invalid_string_value.json',
      ),
  ])
  def test_invalid_downsample_config(self, filename):
    with flagsaver.flagsaver(
        ingestion_pyramid_layer_generation_config_path=gen_test_util.test_file_path(
            filename
        )
    ):
      with self.assertRaises(
          wsi_pyramid_gen_config._IngestPyramidConfigValidationError
      ):
        wsi_pyramid_gen_config.WsiPyramidGenerationConfig()

  def test_no_downsample_config_defined(self):
    with flagsaver.flagsaver(ingestion_pyramid_layer_generation_config_path=''):
      config = wsi_pyramid_gen_config.WsiPyramidGenerationConfig()
      self.assertIsNone(config.get_downsample(0.001))

  def test_invalid_downsample_config_file_type(self):
    with flagsaver.flagsaver(
        ingestion_pyramid_layer_generation_config_path=gen_test_util.test_file_path(
            'google.tif'
        )
    ):
      with self.assertRaises(
          wsi_pyramid_gen_config._IngestPyramidConfigFileTypeError
      ):
        wsi_pyramid_gen_config.WsiPyramidGenerationConfig()

  @parameterized.named_parameters(_DOWNSAMPLE_TEST_CASES)
  def test_get_json_downsample(self, pixel_spacing, expected):
    with flagsaver.flagsaver(
        ingestion_pyramid_layer_generation_config_path=gen_test_util.test_file_path(
            'layer_config_valid.json'
        )
    ):
      config = wsi_pyramid_gen_config.WsiPyramidGenerationConfig()
      self.assertEqual(config.get_downsample(pixel_spacing), expected)

  def test_invalid_json_formatted_config(self):
    with mock.patch(
        'os.path.splitext', autospec=True, return_value=('', '.json')
    ):
      with flagsaver.flagsaver(
          ingestion_pyramid_layer_generation_config_path=gen_test_util.test_file_path(
              'layer_config_invalid_json.txt'
          )
      ):
        with self.assertRaises(
            wsi_pyramid_gen_config._IngestPyramidConfigJsonFormatError
        ):
          wsi_pyramid_gen_config.WsiPyramidGenerationConfig()

  @parameterized.parameters([
      wsi_pyramid_gen_config._IngestPyramidConfigFileTypeError(),
      wsi_pyramid_gen_config._IngestPyramidConfigValidationError(),
      wsi_pyramid_gen_config._IngestPyramidConfigFileNotFoundError(),
      wsi_pyramid_gen_config._IngestPyramidConfigJsonFormatError(),
      wsi_pyramid_gen_config._IngestPyramidConfigYamlFormatError(),
  ])
  def test_exception_inheritance(self, instance):
    self.assertIsInstance(
        instance, wsi_pyramid_gen_config.IngestPyramidConfigError
    )

  @parameterized.named_parameters(_DOWNSAMPLE_TEST_CASES)
  def test_get_yaml_downsample_dict(self, pixel_spacing, expected):
    with flagsaver.flagsaver(
        ingestion_pyramid_layer_generation_config_path=gen_test_util.test_file_path(
            'layer_config_valid.yaml'
        )
    ):
      config = wsi_pyramid_gen_config.WsiPyramidGenerationConfig()
      self.assertEqual(config.get_downsample(pixel_spacing), expected)

  def test_get_dicom_pixel_spacing_from_shared_functional_group(self):
    self.assertEqual(
        wsi_pyramid_gen_config.get_dicom_pixel_spacing(
            gen_test_util.test_file_path('test_wikipedia.dcm')
        ),
        0.1,
    )

  def test_get_dicom_pixel_spacing_from_dim(self):
    with pydicom.dcmread(
        gen_test_util.test_file_path('test_wikipedia.dcm')
    ) as dcm:
      del dcm.SharedFunctionalGroupsSequence
      dcm.ImagedVolumeHeight = 5.0
      dcm.ImagedVolumeWidth = 5.0
      temp_dir = self.create_tempdir()
      temp_path = os.path.join(temp_dir, 'temp.dcm')
      dcm.save_as(temp_path)
      pixel_spacing = wsi_pyramid_gen_config.get_dicom_pixel_spacing(temp_path)
    self.assertEqual(round(pixel_spacing, 5), 0.04274)

  def test_unable_to_get_dicom_pixel_spacing(self):
    with pydicom.dcmread(
        gen_test_util.test_file_path('test_wikipedia.dcm')
    ) as dcm:
      del dcm.SharedFunctionalGroupsSequence
      temp_dir = self.create_tempdir()
      temp_path = os.path.join(temp_dir, 'temp.dcm')
      dcm.save_as(temp_path)
    with self.assertRaises(wsi_pyramid_gen_config.MissingPixelSpacingError):
      wsi_pyramid_gen_config.get_dicom_pixel_spacing(temp_path)

  def test_unable_to_get_openslide_pixel_spacing(self):
    with self.assertRaises(wsi_pyramid_gen_config.MissingPixelSpacingError):
      wsi_pyramid_gen_config.get_openslide_pixel_spacing(
          gen_test_util.test_file_path('test_wikipedia.dcm')
      )

  def test_get_oof_downsample_levels(self):
    self.assertEqual(
        wsi_pyramid_gen_config.get_oof_downsample_levels(0.00025), {2, 32}
    )

  def test_compute_relative_downsamples_source_px_greater_requested(self):
    self.assertEqual(
        wsi_pyramid_gen_config._compute_relative_downsamples(
            0.01, [0.001, 0.1]
        ),
        {8},
    )

  def test_compute_relative_downsamples_source_px_raises_div_zero(self):
    with self.assertRaises(ZeroDivisionError):
      wsi_pyramid_gen_config._compute_relative_downsamples(0.0, [0.001, 0.1])


if __name__ == '__main__':
  absltest.main()
