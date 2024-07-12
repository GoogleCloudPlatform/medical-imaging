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
# ==============================================================================
"""Test for localhost main."""

import os
import shutil
from typing import List
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver

from transformation_pipeline import gke_main
from transformation_pipeline import ingest_flags
from transformation_pipeline import localhost_main
from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import redis_client
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util

_CONTAINER_BASE_DIR = localhost_main._CONTAINER_BASE_DIR
_EXAMPLE_METADATA_MAPPING_SCHEMA_PATH = (
    localhost_main._EXAMPLE_METADATA_MAPPING_SCHEMA_PATH
)
_LICENSE_PATH = gke_main._LICENSE_FILE_PATH

_EXAMPLE_METADATA_MAPPING_SCHEMAS = {
    'vl_whole_slide_microscope_image.json',
    'vl_slide_coordinates_microscopic_image.json',
    'vl_microscopic_image.json',
}


def _get_generated_files(base_dir: str) -> List[str]:
  file_list = []
  for root, _, filenames in os.walk(base_dir):
    for filename in filenames:
      file_list.append(os.path.join(root, filename))
  return file_list


def _make_container_dirs(container_dir: localhost_main._ContainerDirs) -> None:
  for path in (
      container_dir.metadata,
      container_dir.imaging,
      container_dir.processed_images,
      container_dir.dicom_store,
  ):
    os.mkdir(path)


class LocalhostMainTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    gke_main._LICENSE_FILE_PATH = gen_test_util.test_file_path(
        '../ThirdParty_LICENSE.docker'
    )
    localhost_main._CONTAINER_BASE_DIR = self.create_tempdir().full_path
    localhost_main._EXAMPLE_METADATA_MAPPING_SCHEMA_PATH = (
        gen_test_util.test_file_path('../example/metadata_mapping_schemas')
    )

  def tearDown(self):
    gke_main._LICENSE_FILE_PATH = _LICENSE_PATH
    localhost_main._CONTAINER_BASE_DIR = _CONTAINER_BASE_DIR
    localhost_main._EXAMPLE_METADATA_MAPPING_SCHEMA_PATH = (
        _EXAMPLE_METADATA_MAPPING_SCHEMA_PATH
    )
    super().tearDown()

  def test_call_if_no_files_succeeds(self):
    self.assertIsNone(localhost_main._call_if_no_files())

  @flagsaver.flagsaver(localhost_dicom_store='/mock/dicom_store')
  def test_cannot_read_write_metadata_dirs_raises(self):
    localhost_main._CONTAINER_BASE_DIR = gen_test_util.test_file_path(
        'localhost'
    )
    with flagsaver.flagsaver(
        poll_image_ingestion_dir=False,
        gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.DICOM,
        enable_metadata_free_ingestion=True,
        enable_create_missing_study_instance_uid=True,
        test_whole_filename_as_slideid=True,
        ingestion_pyramid_layer_generation_config_path='',
        testing_disable_cloudvision=True,
    ):
      with self.assertRaises(localhost_main._UnableToReadWriteFromDirError):
        localhost_main.main(unused_argv=None)

  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  def test_ingest_flat_image_file_list(self, unused_mock):
    with flagsaver.flagsaver(
        poll_image_ingestion_dir=False,
        gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.DICOM,
        enable_metadata_free_ingestion=True,
        enable_create_missing_study_instance_uid=True,
        test_whole_filename_as_slideid=True,
        ingestion_pyramid_layer_generation_config_path='',
        testing_disable_cloudvision=True,
    ):
      container_dirs = localhost_main._get_container_dirs()
      _make_container_dirs(container_dirs)
      test_image_path = gen_test_util.test_file_path('logo.jpg')
      with flagsaver.flagsaver(
          localhost_dicom_store=container_dirs.dicom_store
      ):
        input_img_path = os.path.join(
            container_dirs.imaging, os.path.basename(test_image_path)
        )
        # write file in ingestion bucket which should be processed.
        shutil.copyfile(test_image_path, input_img_path)
        # write file in ingestion bucket which should be ignored.
        with open(
            os.path.join(container_dirs.imaging, '.ignore'), 'wt'
        ) as outfile:
          outfile.write('ignore')

        localhost_main.main(unused_argv=None)

    self.assertLen(_get_generated_files(container_dirs.dicom_store), 1)

  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  @mock.patch.object(
      localhost_main,
      '_call_if_no_files',
      autospec=True,
      side_effect=gke_main.stop_polling_client,
  )
  def test_poll_ingest_flat_image_file(self, *unused_mocks):
    with flagsaver.flagsaver(
        poll_image_ingestion_dir=True,
        gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.DICOM,
        enable_metadata_free_ingestion=True,
        enable_create_missing_study_instance_uid=True,
        test_whole_filename_as_slideid=True,
        ingestion_pyramid_layer_generation_config_path='',
        # File copied before test starts zero delay is safe.
        pubsub_mock_message_delay_sec=0.0,
        testing_disable_cloudvision=True,
    ):
      container_dirs = localhost_main._get_container_dirs()
      _make_container_dirs(container_dirs)
      test_image_path = gen_test_util.test_file_path('logo.jpg')
      with flagsaver.flagsaver(
          localhost_dicom_store=container_dirs.dicom_store
      ):
        input_img_path = os.path.join(
            container_dirs.imaging, os.path.basename(test_image_path)
        )
        shutil.copyfile(test_image_path, input_img_path)

        localhost_main.main(unused_argv=None)

    self.assertLen(_get_generated_files(container_dirs.dicom_store), 1)

  @flagsaver.flagsaver(localhost_dicom_store='/mock/dicom_store')
  @mock.patch.object(gke_main, 'main', side_effect=ValueError)
  def test_unexpected_exceptions_are_caught_and_raised(self, unused_mock):
    with flagsaver.flagsaver(
        poll_image_ingestion_dir=False,
        gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.DICOM,
        enable_metadata_free_ingestion=True,
        enable_create_missing_study_instance_uid=True,
        test_whole_filename_as_slideid=True,
        ingestion_pyramid_layer_generation_config_path='',
        testing_disable_cloudvision=True,
    ):
      container_dirs = localhost_main._get_container_dirs()
      _make_container_dirs(container_dirs)
      test_image_path = gen_test_util.test_file_path('logo.jpg')
      with flagsaver.flagsaver(
          localhost_dicom_store=container_dirs.dicom_store
      ):
        input_img_path = os.path.join(
            container_dirs.imaging, os.path.basename(test_image_path)
        )
        shutil.copyfile(test_image_path, input_img_path)
        with self.assertRaises(ValueError):
          localhost_main.main(unused_argv=None)

  def test_gke_main_requires_license(self):
    gke_main._LICENSE_FILE_PATH = ''
    with flagsaver.flagsaver(
        poll_image_ingestion_dir=False,
        gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.DICOM,
        enable_metadata_free_ingestion=True,
        enable_create_missing_study_instance_uid=True,
        test_whole_filename_as_slideid=True,
        ingestion_pyramid_layer_generation_config_path='',
        testing_disable_cloudvision=True,
    ):
      container_dirs = localhost_main._get_container_dirs()
      _make_container_dirs(container_dirs)
      with flagsaver.flagsaver(
          localhost_dicom_store=container_dirs.dicom_store
      ):
        with self.assertRaises(gke_main.LicenseMissingError):
          localhost_main.main(unused_argv=None)

  def test_localhost_main_missing_dicom_store_def_raises(self):
    gke_main._LICENSE_FILE_PATH = ''
    with flagsaver.flagsaver(
        poll_image_ingestion_dir=False,
        gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.DICOM,
        enable_metadata_free_ingestion=True,
        enable_create_missing_study_instance_uid=True,
        test_whole_filename_as_slideid=True,
        ingestion_pyramid_layer_generation_config_path='',
        testing_disable_cloudvision=True,
    ):
      _make_container_dirs(localhost_main._get_container_dirs())
      with self.assertRaises(
          localhost_main._LocalhostMissingDicomStoreConfigurationError
      ):
        localhost_main.main(unused_argv=None)

  @flagsaver.flagsaver(transformation_pipeline='invalid')
  def test_gke_main_invalid_pipeline_confige_rises(self):
    with flagsaver.flagsaver(
        poll_image_ingestion_dir=False,
        gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.DICOM,
        enable_metadata_free_ingestion=True,
        enable_create_missing_study_instance_uid=True,
        test_whole_filename_as_slideid=True,
        ingestion_pyramid_layer_generation_config_path='',
        testing_disable_cloudvision=True,
    ):
      container_dirs = localhost_main._get_container_dirs()
      _make_container_dirs(container_dirs)
      with flagsaver.flagsaver(
          localhost_dicom_store=container_dirs.dicom_store
      ):
        with self.assertRaises(ValueError):
          localhost_main.main(unused_argv=None)

  @flagsaver.flagsaver(localhost_dicom_store='/mock/dicom_store')
  @mock.patch.object(
      redis_client.RedisClient,
      'has_redis_client',
      autospec=True,
      return_value=True,
  )
  @mock.patch.object(
      redis_client.RedisClient, 'ping', autospec=True, return_value=False
  )
  def test_unable_to_connect_to_redis_server_raises(self, *unused_mocks):
    with flagsaver.flagsaver(
        poll_image_ingestion_dir=False,
        gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.DICOM,
        enable_metadata_free_ingestion=True,
        enable_create_missing_study_instance_uid=True,
        test_whole_filename_as_slideid=True,
        ingestion_pyramid_layer_generation_config_path='',
        testing_disable_cloudvision=True,
    ):
      _make_container_dirs(localhost_main._get_container_dirs())
      with self.assertRaises(gke_main.RedisConnectionFailedError):
        localhost_main.main(unused_argv=None)

  @flagsaver.flagsaver(poll_image_ingestion_dir=True)
  def test_build_ingest_file_list_returns_none_if_polling(self):
    self.assertIsNone(localhost_main._build_ingest_file_list('foo'))

  def test_has_metadata_schema_in_dir_false(self):
    self.assertFalse(
        localhost_main._has_metadata_schema_in_dir(
            self.create_tempdir().full_path
        )
    )

  def test_has_metadata_schema_in_dir_true(self):
    self.assertTrue(
        localhost_main._has_metadata_schema_in_dir(
            gen_test_util.test_file_path('../example/metadata_mapping_schemas')
        )
    )

  def test_copy_default_schema_to_metadata_dir(self):
    temp_dir = self.create_tempdir().full_path
    localhost_main._copy_default_schema_to_metadata_dir(temp_dir)
    self.assertEqual(
        set(os.listdir(temp_dir)), _EXAMPLE_METADATA_MAPPING_SCHEMAS
    )

  def test_ignore_non_schema_files(self):
    temp_dir = self.create_tempdir().full_path
    localhost_main._copy_default_schema_to_metadata_dir(temp_dir)
    with open(os.path.join(temp_dir, 'foo_magic.bad'), 'wt') as outfile:
      outfile.write('foo')
    localhost_main._EXAMPLE_METADATA_MAPPING_SCHEMA_PATH = temp_dir
    temp_test_dir = self.create_tempdir().full_path
    localhost_main._copy_default_schema_to_metadata_dir(temp_test_dir)
    self.assertEqual(
        set(os.listdir(temp_test_dir)), _EXAMPLE_METADATA_MAPPING_SCHEMAS
    )

  def test_copy_default_schema_to_metadata_dir_consume_os_error(self):
    self.assertIsNone(
        localhost_main._copy_default_schema_to_metadata_dir('bad_dir')
    )

  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  def test_empty_metadata_dir_inits_with_example_schemas(self, unused_mock):
    with flagsaver.flagsaver(
        poll_image_ingestion_dir=False,
        gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.DICOM,
        enable_metadata_free_ingestion=True,
        enable_create_missing_study_instance_uid=True,
        test_whole_filename_as_slideid=True,
        ingestion_pyramid_layer_generation_config_path='',
        testing_disable_cloudvision=True,
    ):
      container_dirs = localhost_main._get_container_dirs()
      _make_container_dirs(container_dirs)
      test_image_path = gen_test_util.test_file_path('logo.jpg')
      with flagsaver.flagsaver(
          localhost_dicom_store=container_dirs.dicom_store
      ):
        input_img_path = os.path.join(
            container_dirs.imaging, os.path.basename(test_image_path)
        )
        # write file in ingestion bucket which should be processed.
        shutil.copyfile(test_image_path, input_img_path)
        localhost_main.main(unused_argv=None)

    self.assertEqual(
        set(os.listdir(container_dirs.metadata)),
        _EXAMPLE_METADATA_MAPPING_SCHEMAS,
    )

  @mock.patch.object(
      dicom_util,
      '_get_colorspace_description_from_iccprofile_bytes',
      autospec=True,
      return_value='SRGB',
  )
  def test_empty_metadata_dir_does_not_init_if_has_mapping_schemas(
      self, unused_mock
  ):
    with flagsaver.flagsaver(
        poll_image_ingestion_dir=False,
        gcs_ingest_study_instance_uid_source=ingest_flags.UidSource.DICOM,
        enable_metadata_free_ingestion=True,
        enable_create_missing_study_instance_uid=True,
        test_whole_filename_as_slideid=True,
        ingestion_pyramid_layer_generation_config_path='',
        testing_disable_cloudvision=True,
    ):
      container_dirs = localhost_main._get_container_dirs()
      _make_container_dirs(container_dirs)
      with open(
          os.path.join(container_dirs.metadata, 'temp_schema.json'), 'wt'
      ) as outfile:
        outfile.write('temp')
      test_image_path = gen_test_util.test_file_path('logo.jpg')
      with flagsaver.flagsaver(
          localhost_dicom_store=container_dirs.dicom_store
      ):
        input_img_path = os.path.join(
            container_dirs.imaging, os.path.basename(test_image_path)
        )
        # write file in ingestion bucket which should be processed.
        shutil.copyfile(test_image_path, input_img_path)
        localhost_main.main(unused_argv=None)

    self.assertEqual(os.listdir(container_dirs.metadata), ['temp_schema.json'])


if __name__ == '__main__':
  absltest.main()
