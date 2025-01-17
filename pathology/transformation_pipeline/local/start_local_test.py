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
"""Test for start local."""

import argparse
from concurrent import futures
import contextlib
import copy
import dataclasses
import io
import json
import logging
import os
import pathlib
import platform
import re
import shutil
import subprocess
from typing import List, Mapping, Set
from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import parameterized

from pathology.transformation_pipeline.ingestion_lib import gen_test_util
from pathology.transformation_pipeline.local import start_local

_LINUX_ADC_PATH = start_local._LINUX_ADC_PATH
_WINDOWS_ADC_PATH = start_local._WINDOWS_ADC_PATH


@dataclasses.dataclass(frozen=True)
class _MockPopenResult:
  stdout: List[str]


def _get_mounts(args: List[str]) -> Mapping[str, str]:
  mount_dict = {}
  for mount in start_local._get_argument_settings('--mount', args):
    match = re.fullmatch('type=bind,source=(.*?),target=(.*)', mount)
    if match is None:
      raise ValueError('Unexpected')
    source_dir = match.groups()[0]
    target_dir = match.groups()[1]
    if source_dir.lower().startswith('/tmp/'):
      if target_dir.endswith(',readonly'):
        source_dir = f'tempdir{target_dir[:-len(",readonly")]}'
      else:
        source_dir = f'tempdir{target_dir}'
    mount_dict[source_dir] = target_dir
  return mount_dict


def _get_other_args(skip_argument: Set[str], args: List[str]) -> List[str]:
  index = 0
  element_list = []
  while index < len(args):
    if args[index] in skip_argument:
      index += 2
      continue
    element_list.append(args[index])
    index += 1
  return element_list


class _ArgParserFlagsaver(contextlib.ContextDecorator):

  def __init__(self, **kwargs):
    self._flag_overrides = kwargs

  def __enter__(self):
    self._pre_existing_flags = start_local._parsed_args
    existing_flag_values = copy.deepcopy(vars(start_local._parsed_args))
    for flag_name in self._flag_overrides:
      if flag_name not in existing_flag_values:
        raise ValueError(f'Invalid flag name: {flag_name}')
      if (
          self._flag_overrides[flag_name] is not None
          and existing_flag_values[flag_name] is not None
      ):
        override_flag_value_type = type(self._flag_overrides[flag_name])
        existing_flag_value_type = type(existing_flag_values[flag_name])
        if override_flag_value_type != existing_flag_value_type:
          raise ValueError(
              f'Invalid mock value, for key: {flag_name}, types:'
              f' {override_flag_value_type} != {existing_flag_value_type}'
          )
    existing_flag_values.update(self._flag_overrides)
    start_local._parsed_args = argparse.Namespace(**existing_flag_values)

  def __exit__(self, *exc):
    start_local._parsed_args = self._pre_existing_flags


class StartLocalhostTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    start_local._parsed_args = start_local._get_arg_parser()
    start_local._LINUX_ADC_PATH = _LINUX_ADC_PATH
    start_local._WINDOWS_ADC_PATH = _WINDOWS_ADC_PATH

  @parameterized.named_parameters([
      dict(testcase_name='not_found', mock_cred='', expected=False),
      dict(
          testcase_name='found',
          mock_cred='expected_svs_metadata.json',
          expected=True,
      ),
  ])
  @mock.patch.object(os.path, 'expandvars', side_effect=lambda x: x)
  @mock.patch.object(platform, 'system', autospec=True, return_value='Windows')
  def test_discover_adc_credentials_windows(
      self, *unused_mocks, mock_cred, expected
  ):
    start_local._WINDOWS_ADC_PATH = gen_test_util.test_file_path(mock_cred)
    self.assertEqual(
        bool(start_local._discover_adc_credentials()), expected
    )

  @parameterized.named_parameters([
      dict(testcase_name='not_found', mock_cred='', expected=False),
      dict(
          testcase_name='found',
          mock_cred='expected_svs_metadata.json',
          expected=True,
      ),
  ])
  @mock.patch.object(pathlib.Path, 'home', autospec=True, return_value='/')
  @mock.patch.object(platform, 'system', autospec=True, return_value='Linux')
  def test_discover_adc_credentials_linux(
      self, *unused_mocks, mock_cred, expected
  ):
    start_local._LINUX_ADC_PATH = gen_test_util.test_file_path(mock_cred)
    self.assertEqual(
        bool(start_local._discover_adc_credentials()), expected
    )

  @parameterized.parameters('y', 'yes', 't', 'true', 'on', '1')
  def test_str_to_bool_true(self, val):
    self.assertTrue(start_local._str_to_bool(f'  {val.upper()}  '))

  @parameterized.parameters('n', 'no', 'f', 'false', 'off', '0')
  def test_str_to_bool_false(self, val):
    self.assertFalse(start_local._str_to_bool(f'  {val.upper()}  '))

  def test_str_to_bool_raises(self):
    with self.assertRaises(start_local._StartLocalHostError):
      start_local._str_to_bool('  foo  ')

  @parameterized.named_parameters([
      dict(
          testcase_name='readonly',
          readonly=True,
          expected=['--mount', 'type=bind,source=foo,target=bar,readonly'],
      ),
      dict(
          testcase_name='writeable',
          readonly=False,
          expected=['--mount', 'type=bind,source=foo,target=bar'],
      ),
  ])
  def test_mount(self, readonly, expected):
    self.assertEqual(start_local._mount('foo', 'bar', readonly), expected)

  def test_env(self):
    self.assertEqual(start_local._env('foo', 'bar'), ['-e', 'foo=bar'])

  @_ArgParserFlagsaver(google_application_credentials='abc')
  def test_are_gcp_credentials_does_not_define_file_raises(self):
    with self.assertRaises(start_local._StartLocalHostError):
      start_local._are_gcp_credentials_defined()

  def test_are_gcp_credentials_true(self):
    with _ArgParserFlagsaver(
        google_application_credentials=self.create_tempfile().full_path
    ):
      self.assertTrue(start_local._are_gcp_credentials_defined())

  @_ArgParserFlagsaver(google_application_credentials='')
  def test_are_gcp_credentials_defined_false(self):
    self.assertFalse(start_local._are_gcp_credentials_defined())

  def test_copy_file_to_image_ingestion_dir_success(self):
    source = self.create_tempdir()
    dest = self.create_tempdir()
    source = os.path.join(source, 'test.txt')
    with open(source, 'wt') as outfile:
      outfile.write('test')
    start_local._copy_file_to_image_ingestion_dir(source, dest.full_path)
    self.assertTrue(os.path.exists(os.path.join(dest, 'test.txt')))

  def test_failure_to_copy_file_fails_silently_source_does_not_exist(self):
    source = self.create_tempdir()
    dest = self.create_tempdir()
    source = os.path.join(source, 'test.txt')
    start_local._copy_file_to_image_ingestion_dir(source, dest.full_path)
    self.assertFalse(os.path.exists(os.path.join(dest, 'test.txt')))

  def test_failure_to_copy_file_fails_silently_dest_does_not_exist(self):
    source = self.create_tempdir()
    dest = os.path.join(source, 'bad')
    source = os.path.join(source, 'test.txt')
    with open(source, 'wt') as outfile:
      outfile.write('test')
    start_local._copy_file_to_image_ingestion_dir(source, dest)
    self.assertFalse(os.path.exists(os.path.join(dest, 'test.txt')))

  @mock.patch.object(shutil, 'copy', side_effect=OSError)
  def test_copy_file_to_image_ingestion_dir_faile_os_error(self, unused_mock):
    source = self.create_tempdir()
    dest = self.create_tempdir()
    source = os.path.join(source, 'test.txt')
    with open(source, 'wt') as outfile:
      outfile.write('test')
    start_local._copy_file_to_image_ingestion_dir(source, dest.full_path)
    self.assertFalse(os.path.exists(os.path.join(dest, 'test.txt')))

  @mock.patch.object(subprocess, 'run', autospec=True)
  def test_copy_file_to_image_ingestion_dir_gsutil(self, mock_run):
    source = 'gs://mock_bucket/bucket_folder/file.txt'
    dest = '/temp/dir/dir_folder'
    start_local._copy_file_to_image_ingestion_dir(source, dest)
    mock_run.assert_called_once_with(
        [
            'gsutil',
            '-m',
            'cp',
            '-r',
            'gs://mock_bucket/bucket_folder/file.txt',
            '/temp/dir/dir_folder/bucket_folder/file.txt',
        ],
        check=True,
    )

  @mock.patch.object(
      subprocess, 'run', side_effect=subprocess.CalledProcessError(1, 'foo')
  )
  def test_copy_file_to_image_ingestion_dir_gsutil_handles_subprocess_exp(
      self, mock_run
  ):
    source = 'gs://mock_bucket/bucket_folder/file.txt'
    dest = '/temp/dir/dir_folder'
    start_local._copy_file_to_image_ingestion_dir(source, dest)
    mock_run.assert_called_once_with(
        [
            'gsutil',
            '-m',
            'cp',
            '-r',
            'gs://mock_bucket/bucket_folder/file.txt',
            '/temp/dir/dir_folder/bucket_folder/file.txt',
        ],
        check=True,
    )

  @parameterized.parameters([
      (None, False),
      ('', False),
      ('/abc/efg', False),
      ('Http://foo.bar', True),
      ('HTtPS://foo.bar', True),
  ])
  def test_is_dicom_store_gcp_store(self, dicom_store, expected):
    with _ArgParserFlagsaver(dicom_store=dicom_store):
      self.assertEqual(start_local._is_dicom_store_gcp_store(), expected)

  @_ArgParserFlagsaver(
      dicom_store='http://foo.bar', google_application_credentials=''
  )
  def test_http_dicom_store_with_no_credentials_raises(self):
    with self.assertRaises(start_local._StartLocalHostError):
      start_local._test_dicom_store()

  @_ArgParserFlagsaver(dicom_store='http://foo.bar')
  @mock.patch.object(
      start_local,
      '_are_gcp_credentials_defined',
      autospec=True,
      return_value=True,
  )
  def test_http_dicom_store_with_credentials_pass(self, unused_mock):
    self.assertIsNone(start_local._test_dicom_store())

  @_ArgParserFlagsaver(dicom_store='bad_dir')
  def test_http_dicom_store_with_bad_dir_raises(self):
    with self.assertRaises(start_local._StartLocalHostError):
      start_local._test_dicom_store()

  def test_http_dicom_store_with_good_dir_pass(self):
    tempdir = self.create_tempdir()
    with _ArgParserFlagsaver(dicom_store=tempdir.full_path):
      self.assertIsNone(start_local._test_dicom_store())

  def test_get_dir_return_temp_dir(self):
    with contextlib.ExitStack() as stack:
      result = start_local._get_dir('foo', stack, '')
      self.assertTrue(result.is_temp)
      self.assertTrue(os.path.isdir(result.path))

  def test_get_dir_with_existing_dir_succeeds(self):
    with contextlib.ExitStack() as stack:
      tdir = self.create_tempdir().full_path
      result = start_local._get_dir('foo', stack, tdir)
      self.assertFalse(result.is_temp)
      self.assertEqual(result.path, tdir)

  def test_get_dir_with_bad_path_dir_raises(self):
    with contextlib.ExitStack() as stack:
      with self.assertRaises(start_local._StartLocalHostError):
        start_local._get_dir('foo', stack, 'BAD_DIR')

  def test_handle_log(self):
    testfile = os.path.join(self.create_tempdir(), 'foo.txt')
    mock_instance = mock.create_autospec(subprocess.Popen[bytes], instance=True)
    mock_instance.stdout = ['foo\n'.encode('utf-8'), 'bar'.encode('utf-8')]
    with open(testfile, 'wt') as log:
      start_local._handle_log(mock_instance, log)
    with open(testfile, 'rt') as log:
      self.assertEqual([line for line in log], ['foo\n', 'bar'])

  @_ArgParserFlagsaver(cloud_ops_logging=True)
  def test_handle_log_does_nothing_if_logging_to_cloud_ops(self):
    testfile = os.path.join(self.create_tempdir(), 'foo.txt')
    mock_instance = mock.create_autospec(subprocess.Popen[bytes], instance=True)
    mock_instance.stdout = ['foo\n'.encode('utf-8'), 'bar'.encode('utf-8')]
    with open(testfile, 'wt') as log:
      start_local._handle_log(mock_instance, log)
    with open(testfile, 'rt') as log:
      self.assertEmpty(log.read())

  @_ArgParserFlagsaver(google_application_credentials='fake.json')
  @mock.patch.object(
      start_local,
      '_are_gcp_credentials_defined',
      autospec=True,
      return_value=True,
  )
  def test_gen_docker_execution_args_temp_dir_mappings_and_fs_dicom_store(
      self, unused_mock
  ) -> None:
    with contextlib.ExitStack() as stack:
      ingest_dir = start_local._get_dir('foo', stack, '')
      metadata_dir = start_local._get_dir('foo', stack, '')
      output_dir = start_local._get_dir('foo', stack, '')
      dicom_store = '/transform/dicom_store'

      args = start_local._gen_docker_execution_args(
          ingest_dir, metadata_dir, output_dir, dicom_store, False
      )
      mounts = _get_mounts(args)
      with open(
          gen_test_util.test_file_path('start_local/mount_temp_test.json'),
          'rt',
      ) as infile:
        self.assertEqual(mounts, json.load(infile))

  @mock.patch.object(os.path, 'isdir', autospec=True, return_value=True)
  @mock.patch.object(os.path, 'isfile', autospec=True, return_value=True)
  @_ArgParserFlagsaver(
      poll=True,
      google_application_credentials='',
      metadata_free_ingestion=False,
      cloud_vision_barcode_segmentation=True,
      pyramid_generation_config_path='/fake/config.json',
  )
  def test_gen_docker_execution_args_fs_dir_mappings_and_cloud_dicom_store(
      self, *unused_mocks
  ) -> None:
    with contextlib.ExitStack() as stack:
      ingest_dir = start_local._get_dir(
          'foo', stack, '/localhost/input_imaging'
      )
      metadata_dir = start_local._get_dir(
          'foo', stack, 'localhost/input_metadata'
      )
      output_dir = start_local._get_dir(
          'foo', stack, 'localhost/processed_imaging'
      )
      dicom_store = 'http://dicom_store'

      args = start_local._gen_docker_execution_args(
          ingest_dir, metadata_dir, output_dir, dicom_store, True
      )
      mounts = _get_mounts(args)
      with open(
          gen_test_util.test_file_path('start_local/mount_fs_test.json'),
          'rt',
      ) as infile:
        self.assertEqual(mounts, json.load(infile))

  @_ArgParserFlagsaver(google_cloud_project='fake_gcp_project')
  @mock.patch.object(
      start_local,
      '_are_gcp_credentials_defined',
      autospec=True,
      return_value=True,
  )
  def test_gen_docker_execution_args(self, unused_mock) -> None:
    with contextlib.ExitStack() as stack:
      ingest_dir = start_local._get_dir('foo', stack, '')
      metadata_dir = start_local._get_dir('foo', stack, '')
      output_dir = start_local._get_dir('foo', stack, '')
      dicom_store = '/transform/dicom_store'

      args = start_local._gen_docker_execution_args(
          ingest_dir, metadata_dir, output_dir, dicom_store, False
      )
      envs = dict(start_local._get_env_settings(args))
      del envs['LOCAL_UID']
      del envs['LOCAL_GID']
      with open(
          gen_test_util.test_file_path('start_local/env_test.json'), 'rt'
      ) as infile:
        self.assertEqual(envs, json.load(infile))

  @parameterized.named_parameters([
      dict(testcase_name='full_pyramid', config_path='', expected=''),
      dict(
          testcase_name='default',
          config_path='Default',
          expected='/default_config.json',
      ),
      dict(
          testcase_name='custom`',
          config_path='/foo/bar.json',
          expected='/bar.json',
      ),
  ])
  @mock.patch.object(os.path, 'isfile', autospec=True, return_value=True)
  def test_pyramid_level_config_args(self, unused_mock, config_path, expected):
    with contextlib.ExitStack() as stack:
      ingest_dir = start_local._get_dir('foo', stack, '')
      metadata_dir = start_local._get_dir('foo', stack, '')
      output_dir = start_local._get_dir('foo', stack, '')
      dicom_store = '/transform/dicom_store'

      with _ArgParserFlagsaver(pyramid_generation_config_path=config_path):
        args = start_local._gen_docker_execution_args(
            ingest_dir, metadata_dir, output_dir, dicom_store, False
        )
        envs = start_local._get_env_settings(args)
        self.assertEqual(
            envs['INGESTION_PYRAMID_LAYER_GENERATION_CONFIG_PATH'], expected
        )

  @_ArgParserFlagsaver(pyramid_generation_config_path='/bad/path')
  def test_missing_pyramid_config_raises(self):
    with contextlib.ExitStack() as stack:
      ingest_dir = start_local._get_dir('foo', stack, '')
      metadata_dir = start_local._get_dir('foo', stack, '')
      output_dir = start_local._get_dir('foo', stack, '')
      dicom_store = '/transform/dicom_store'
      with self.assertRaises(start_local._StartLocalHostError):
        start_local._gen_docker_execution_args(
            ingest_dir, metadata_dir, output_dir, dicom_store, False
        )

  def test_get_other_config_settings(self):
    with contextlib.ExitStack() as stack:
      ingest_dir = start_local._get_dir('foo', stack, '')
      metadata_dir = start_local._get_dir('foo', stack, '')
      output_dir = start_local._get_dir('foo', stack, '')
      dicom_store = '/transform/dicom_store'
      args = start_local._gen_docker_execution_args(
          ingest_dir, metadata_dir, output_dir, dicom_store, False
      )
      args = _get_other_args({'--mount', '-e'}, args)
      self.assertEqual(
          args,
          [
              'docker',
              'run',
              '-it',
              '--init',
              '--name',
              'local_transform_pipeline',
              'local_transform_pipeline_docker_container',
          ],
      )

  def test_copy_file_list_into_input_dir_succeeds(self):
    input_dir = self.create_tempdir().full_path
    input_image_list = []
    for index in range(100):
      path = os.path.join(input_dir, f'foo_{index}.txt')
      with open(path, 'wt') as outfile:
        outfile.write(f'foo_{index}')
      input_image_list.append(path)
    outputdir = self.create_tempdir().full_path
    with contextlib.ExitStack() as stack:
      start_local._copy_file_list_to_input_dir(
          stack, input_image_list, outputdir, False
      )
      self.assertLen(os.listdir(outputdir), 100)

  @mock.patch.object(futures, 'wait', autospec=True)
  def test_copy_file_list_into_input_dir_does_not_wait_if_polling(
      self, mock_futures_wait
  ):
    path = os.path.join(self.create_tempdir(), 'foo.txt')
    with open(path, 'wt') as outfile:
      outfile.write('foo')
    input_image_list = [path]
    outputdir = self.create_tempdir().full_path
    with contextlib.ExitStack() as stack:
      start_local._copy_file_list_to_input_dir(
          stack, input_image_list, outputdir, True
      )
      mock_futures_wait.assert_not_called()

  def test_get_transform_env_log(self):
    test_env = {}
    for name in [
        'GOOGLE_APPLICATION_CREDENTIALS',
        'LOCAL_UID',
        'LOCAL_GID',
        'POLL_IMAGE_INGESTION_DIR',
        'GOOGLE_CLOUD_PROJECT',
    ]:
      test_env[name] = 'Fake'
    test_env['LOCALHOST_DICOM_STORE'] = 'http://foo.bar'
    test_env['second'] = 'one'
    test_env['another'] = 'one'
    test_env['third'] = 'val"with_a_quote'
    test_env['forth'] = 'val"with_\'_both_quotes'
    expected = [
        '  DICOMWEB_URL: "http://foo.bar"',
        '  another: "one"',
        '  forth: val"with_\'_both_quotes',
        '  second: "one"',
        "  third: 'val\"with_a_quote'",
    ]
    self.assertEqual(
        start_local._get_transform_env_log(test_env), '\n'.join(expected)
    )

  def test_unexpected_formatted_env_raises(self):
    with self.assertRaises(start_local._StartLocalHostError):
      start_local._get_env_settings(['-e', 'abc'])

  @parameterized.named_parameters([
      dict(
          testcase_name='polling_temp_dir',
          is_temp_dir=True,
          poll=True,
          expected=False,
      ),
      dict(
          testcase_name='not_polling_temp_dir',
          is_temp_dir=True,
          poll=False,
          expected=False,
      ),
      dict(
          testcase_name='polling_dir',
          is_temp_dir=False,
          poll=True,
          expected=True,
      ),
      dict(
          testcase_name='not_polling_dir',
          is_temp_dir=False,
          poll=False,
          expected=False,
      ),
  ])
  def test_get_poll_image_ingestion_dir(self, is_temp_dir, poll, expected):
    ingest_dir = start_local._Directory('/foo', is_temp_dir)
    with _ArgParserFlagsaver(poll=poll):
      self.assertEqual(
          start_local._get_poll_image_ingestion_dir(ingest_dir), expected
      )

  @parameterized.named_parameters([
      dict(
          testcase_name='enable_meta_free_temp_dir',
          is_temp_dir=True,
          metadata_free=True,
          expected=True,
      ),
      dict(
          testcase_name='not_enable_meta_free_temp_dir',
          is_temp_dir=True,
          metadata_free=False,
          expected=True,
      ),
      dict(
          testcase_name='enable_meta_free_reg_dir',
          is_temp_dir=False,
          metadata_free=True,
          expected=True,
      ),
      dict(
          testcase_name='not_enable_meta_free_reg_dir',
          is_temp_dir=False,
          metadata_free=False,
          expected=False,
      ),
  ])
  def test_get_metadata_free_ingestion(
      self, is_temp_dir, metadata_free, expected
  ):
    metadata_dir = start_local._Directory('/foo', is_temp_dir)
    with _ArgParserFlagsaver(metadata_free_ingestion=metadata_free):
      self.assertEqual(
          start_local._get_metadata_free_ingestion(metadata_dir), expected
      )

  @parameterized.named_parameters([
      dict(
          testcase_name='gcp_credientals_and_table',
          gcp_credientals=True,
          bq_table='fake.bq.table',
      ),
      dict(
          testcase_name='gcp_credientals_no_table',
          gcp_credientals=True,
          bq_table='',
      ),
      dict(
          testcase_name='no_gcp_credientals_no_table',
          gcp_credientals=False,
          bq_table='',
      ),
  ])
  @mock.patch.object(
      start_local, '_are_gcp_credentials_defined', autospec=True
  )
  def test_get_big_query_metadata_table(
      self, mock_are_gcp_credientals_defined, gcp_credientals, bq_table
  ):
    mock_are_gcp_credientals_defined.return_value = gcp_credientals
    with _ArgParserFlagsaver(big_query=bq_table):
      self.assertEqual(
          start_local._get_big_query_metadata_table(), bq_table
      )

  @parameterized.named_parameters([
      dict(
          testcase_name='gcp_credientals_and_bad_table_name',
          gcp_credientals=True,
          bq_table='bad_table_name',
      ),
      dict(
          testcase_name='no_gcp_credientals',
          gcp_credientals=False,
          bq_table='fake.bq.table',
      ),
  ])
  @mock.patch.object(
      start_local, '_are_gcp_credentials_defined', autospec=True
  )
  def test_get_big_query_metadata_table_raises(
      self, mock_are_gcp_credientals_defined, gcp_credientals, bq_table
  ):
    mock_are_gcp_credientals_defined.return_value = gcp_credientals
    with _ArgParserFlagsaver(big_query=bq_table):
      with self.assertRaises(start_local._StartLocalHostError):
        start_local._get_big_query_metadata_table()

  @parameterized.named_parameters([
      dict(
          testcase_name='has_credientals_and_request_segment',
          segmentation=True,
          credentials=True,
          expected=True,
      ),
      dict(
          testcase_name='no_credientals_and_request_segment',
          segmentation=True,
          credentials=False,
          expected=False,
      ),
      dict(
          testcase_name='has_credientals_and_no_request_segment',
          segmentation=False,
          credentials=True,
          expected=False,
      ),
      dict(
          testcase_name='no_credientals_and_no_request_segment',
          segmentation=False,
          credentials=False,
          expected=False,
      ),
  ])
  def test_get_cloud_vision_barcode_segmentation(
      self, segmentation, credentials, expected
  ):
    with mock.patch.object(
        start_local,
        '_are_gcp_credentials_defined',
        autospec=True,
        return_value=credentials,
    ):
      with _ArgParserFlagsaver(cloud_vision_barcode_segmentation=segmentation):
        self.assertEqual(
            start_local._get_cloud_vision_barcode_segmentation_enabled(),
            expected,
        )

  @_ArgParserFlagsaver(cloud_ops_logging=False)
  def test_get_logging_ops_not_logging_to_cloud_ops(self):
    self.assertEqual(
        start_local._get_env_settings(start_local._get_logging_ops()),
        {'ENABLE_CLOUD_OPS_LOGGING': 'False'},
    )

  @_ArgParserFlagsaver(cloud_ops_logging=True)
  @mock.patch.object(
      start_local,
      '_are_gcp_credentials_defined',
      autospec=True,
      return_value=False,
  )
  def test_get_logging_ops_no_google_credentials_raises(self, unused_mock):
    with self.assertRaises(start_local._StartLocalHostError):
      start_local._get_logging_ops()

  @_ArgParserFlagsaver(
      cloud_ops_logging=True, google_cloud_project='', ops_log_project=''
  )
  @mock.patch.object(
      start_local,
      '_are_gcp_credentials_defined',
      autospec=True,
      return_value=True,
  )
  def test_get_logging_ops_no_google_project_raises(self, unused_mock):
    with self.assertRaises(start_local._StartLocalHostError):
      start_local._get_logging_ops()

  @_ArgParserFlagsaver(
      cloud_ops_logging=True,
      google_cloud_project='',
      ops_log_project='fake_gcp_project',
  )
  @mock.patch.object(
      start_local,
      '_are_gcp_credentials_defined',
      autospec=True,
      return_value=True,
  )
  def test_get_logging_ops_with_log_google_project_succeeds(self, unused_mock):
    self.assertEqual(
        start_local._get_env_settings(start_local._get_logging_ops()),
        {
            'CLOUD_OPS_LOG_NAME': 'transformation_pipeline',
            'CLOUD_OPS_LOG_PROJECT': 'fake_gcp_project',
            'ENABLE_CLOUD_OPS_LOGGING': 'True',
        },
    )

  @_ArgParserFlagsaver(
      cloud_ops_logging=True,
      google_cloud_project='default_gcp_project',
      ops_log_project='',
  )
  @mock.patch.object(
      start_local,
      '_are_gcp_credentials_defined',
      autospec=True,
      return_value=True,
  )
  def test_get_logging_ops_with_default_google_project_succeeds(
      self, unused_mock
  ):
    self.assertEqual(
        start_local._get_env_settings(start_local._get_logging_ops()),
        {
            'CLOUD_OPS_LOG_NAME': 'transformation_pipeline',
            'ENABLE_CLOUD_OPS_LOGGING': 'True',
        },
    )

  @_ArgParserFlagsaver(
      google_application_credentials='fake.json',
      image_ingestion_dir='',
      metadata_dir='',
      processed_image_dir='',
      dicom_store='/transform/dicom_store',
      google_cloud_project='fake_gcp_project',
  )
  @mock.patch.object(
      start_local,
      '_are_gcp_credentials_defined',
      autospec=True,
      return_value=True,
  )
  @mock.patch.object(os.path, 'isdir', autospec=True, return_value=True)
  @mock.patch.object(subprocess, 'run', autospec=True)
  def test_main_write_logs_locally(self, *unused_mocks):
    with io.BytesIO('one_line'.encode('utf-8')) as mock_stdout:
      path = os.path.join(self.create_tempdir(), 'log.txt')
      with _ArgParserFlagsaver(log=path):
        mock_instance = mock.create_autospec(subprocess.Popen, instance=True)
        mock_instance.stdout = mock_stdout
        with mock.patch.object(
            subprocess, 'Popen', autospec=True, return_value=mock_instance
        ) as mock_popen:
          start_local.main(None)
          mock_popen.assert_called_once()
          args = mock_popen.call_args.kwargs['args']

          mounts = _get_mounts(args)
          with open(
              gen_test_util.test_file_path(
                  'start_local/mount_temp_test.json'
              ),
              'rt',
          ) as infile:
            self.assertEqual(mounts, json.load(infile))
          envs = dict(start_local._get_env_settings(args))
          del envs['LOCAL_UID']
          del envs['LOCAL_GID']
          with open(
              gen_test_util.test_file_path('start_local/env_test.json'),
              'rt',
          ) as infile:
            self.assertEqual(envs, json.load(infile))

      with open(path, 'rt') as log_file:
        self.assertEqual(log_file.read(), 'one_line')

  @_ArgParserFlagsaver(
      google_application_credentials='fake.json',
      dicom_store='/transform/dicom_store',
      google_cloud_project='fake_gcp_project',
      user_id='123',
      group_id='456',
  )
  @mock.patch.object(
      start_local,
      '_are_gcp_credentials_defined',
      autospec=True,
      return_value=True,
  )
  @mock.patch.object(os.path, 'isdir', autospec=True, return_value=True)
  @mock.patch.object(subprocess, 'run', autospec=True)
  def test_main_write_docker_run_shellscript(self, *unused_mocks):
    with io.BytesIO('one_line'.encode('utf-8')) as mock_stdout:
      path = os.path.join(self.create_tempdir(), 'log.txt')
      with _ArgParserFlagsaver(
          write_docker_run_shellscript=path,
          image_ingestion_dir='/mock_run/image_ingestion',
          metadata_dir='/mock_run/metadata_ingestion',
          processed_image_dir='/mock_run/processed_images',
      ):
        mock_instance = mock.create_autospec(subprocess.Popen, instance=True)
        mock_instance.stdout = mock_stdout
        with mock.patch.object(
            subprocess, 'Popen', autospec=True, return_value=mock_instance
        ) as mock_popen:
          start_local.main(None)
          mock_popen.assert_not_called()
    with open(
        gen_test_util.test_file_path('start_local/expected_shellscript.sh'),
        'rt',
    ) as shellscript:
      expected = shellscript.read()
    with open(path, 'rt') as generated_shellscript:
      self.assertEqual(generated_shellscript.read(), expected)

  @_ArgParserFlagsaver(
      google_application_credentials='fake.json',
      dicom_store='/transform/dicom_store',
      google_cloud_project='fake_gcp_project',
      image_ingestion_dir='',
      metadata_dir='',
      processed_image_dir='',
  )
  @mock.patch.object(
      start_local,
      '_are_gcp_credentials_defined',
      autospec=True,
      return_value=True,
  )
  @mock.patch.object(os.path, 'isdir', autospec=True, return_value=True)
  @mock.patch.object(subprocess, 'run', autospec=True)
  def test_main_write_docker_run_shellscript_raises_if_dir_mapped_to_temp_dirs(
      self, *unused_mocks
  ):
    with io.BytesIO('one_line'.encode('utf-8')) as mock_stdout:
      path = os.path.join(self.create_tempdir(), 'log.txt')
      with _ArgParserFlagsaver(write_docker_run_shellscript=path):
        mock_instance = mock.create_autospec(subprocess.Popen, instance=True)
        mock_instance.stdout = mock_stdout
        with self.assertRaises(start_local._StartLocalHostError):
          start_local.main(None)

  @_ArgParserFlagsaver(
      google_application_credentials='fake.json',
      image_ingestion_dir='',
      metadata_dir='',
      processed_image_dir='',
      dicom_store='/transform/dicom_store',
      google_cloud_project='fake_gcp_project',
      cloud_ops_logging=True,
  )
  @mock.patch.object(
      start_local,
      '_are_gcp_credentials_defined',
      autospec=True,
      return_value=True,
  )
  @mock.patch.object(os.path, 'isdir', autospec=True, return_value=True)
  @mock.patch.object(subprocess, 'run', autospec=True)
  def test_main_write_logs_to_cloud_ops(self, *unused_mocks):
    with io.BytesIO('one_line'.encode('utf-8')) as mock_stdout:
      mock_instance = mock.create_autospec(subprocess.Popen, instance=True)
      mock_instance.stdout = mock_stdout
      with mock.patch.object(
          subprocess, 'Popen', autospec=True, return_value=mock_instance
      ) as mock_popen:
        start_local.main(None)
        mock_popen.assert_called_once()
        args = mock_popen.call_args.kwargs['args']

        mounts = _get_mounts(args)
        with open(
            gen_test_util.test_file_path(
                'start_local/mount_temp_test.json'
            ),
            'rt',
        ) as infile:
          self.assertEqual(mounts, json.load(infile))
        envs = dict(start_local._get_env_settings(args))
        del envs['LOCAL_UID']
        del envs['LOCAL_GID']
        with open(
            gen_test_util.test_file_path('start_local/env_test.json'),
            'rt',
        ) as infile:
          expected_env = json.load(infile)
        expected_env['ENABLE_CLOUD_OPS_LOGGING'] = 'True'
        expected_env['CLOUD_OPS_LOG_NAME'] = 'transformation_pipeline'
        self.assertEqual(envs, expected_env)

  @_ArgParserFlagsaver(
      log='',
      google_application_credentials='fake.json',
      image_ingestion_dir='',
      processed_image_dir='',
      metadata_dir='',
      dicom_store='/transform/dicom_store',
      google_cloud_project='fake_gcp_project',
      log_environment_variables=True,
  )
  @mock.patch.object(
      start_local,
      '_are_gcp_credentials_defined',
      autospec=True,
      return_value=True,
  )
  @mock.patch.object(os.path, 'isdir', autospec=True, return_value=True)
  @mock.patch.object(logging, 'info', autospec=True)
  def test_main_log_environment_variables(self, mock_logging, *unused_mocks):
    path = os.path.join(self.create_tempdir(), 'log.txt')
    with _ArgParserFlagsaver(log=path):
      start_local.main(None)
    with open(
        gen_test_util.test_file_path(
            'start_local/log_environment_variables.txt'
        ),
        'rt',
    ) as infile:
      self.assertEqual(mock_logging.call_args.args[0], infile.read())
    with open(path, 'rt') as log_file:
      self.assertEmpty(log_file.read())

  @_ArgParserFlagsaver(
      log='fake_log_path',
      cloud_ops_logging=True,
  )
  def test_main_raises_if_configured_to_write_logs_to_file_and_cloud_ops(self):
    with self.assertRaises(start_local._StartLocalHostError):
      start_local.main(None)

  @_ArgParserFlagsaver(
      google_application_credentials='fake.json',
      image_ingestion_dir='/mock_run/image_ingestion',
      metadata_dir='/mock_run/metadata_ingestion',
      processed_image_dir='/mock_run/processed_images',
      dicom_store='/transform/dicom_store',
      google_cloud_project='fake_gcp_project',
      cloud_ops_logging=True,
      poll=True,
      metadata_free_ingestion=True,
      create_missing_study_instance_uid=True,
      dicom_study_instance_uid_source='DICOM',
      whole_filename_metadata_primary_key=True,
      include_upload_path_in_whole_filename_metadata_primary_key=True,
      filename_metadata_primary_key_split_str='*',
      metadata_primary_key_regex='.*',
      metadata_primary_key_column_name='primary_key',
      barcode_decoder=False,
      cloud_vision_barcode_segmentation=False,
      big_query='project.dataset.table',
      metadata_uid_validation='NONE',
      metadata_tag_length_validation='ERROR',
      require_type1_dicom_tag_metadata_are_defined=True,
      frame_height=312,
      frame_width=512,
      compression='JPEG2000',
      jpeg_quality=1,
      jpeg_subsampling='SUBSAMPLE_420',
      icc_profile=False,
      pixel_equivalent_transform='DISABLED',
      uid_prefix='1.3.6.1.4.1.11129.5.7.999',
      ignore_root_dirs=['foo', 'magic'],
      ignore_file_exts=['.doc', '.txt'],
      move_image_on_ingest_sucess_or_failure=False,
      ops_log_name='magic_log',
      ops_log_project='magic_log_project',
  )
  @mock.patch.object(
      start_local,
      '_are_gcp_credentials_defined',
      autospec=True,
      return_value=True,
  )
  @mock.patch.object(os.path, 'isdir', autospec=True, return_value=True)
  @mock.patch.object(subprocess, 'run', autospec=True)
  def test_parse_env_init_expected_transform_pipeline_flags(
      self, *unused_mocks
  ):
    with io.BytesIO('one_line'.encode('utf-8')) as mock_stdout:
      mock_instance = mock.create_autospec(subprocess.Popen, instance=True)
      mock_instance.stdout = mock_stdout
      with mock.patch.object(
          subprocess, 'Popen', autospec=True, return_value=mock_instance
      ) as mock_popen:
        start_local.main(None)
        mock_popen.assert_called_once()
        args = mock_popen.call_args.kwargs['args']
        envs = dict(start_local._get_env_settings(args))
        with mock.patch.dict(os.environ, envs):

          # This tests validates that enviromental variables set start_local
          # inits flags in the transform pipeline as expected. The transform
          # pipeline inits flags from environmental variables using defaults
          # that are set when the python module defining the flag loads. To
          # perfrom the desired test, the unit test needs to mock environmental
          # variables before loading the flag definition module. This is why the
          # ingest_flags module is imported inline within this unit test.

          # pylint: disable=g-import-not-at-top
          from pathology.shared_libs.logging_lib import cloud_logging_client
          from pathology.transformation_pipeline import ingest_flags
          # pylint: enable=g-import-not-at-top
          flags.FLAGS(['temp'])
          self.assertTrue(ingest_flags.ENABLE_METADATA_FREE_INGESTION_FLG.value)
          self.assertTrue(
              ingest_flags.ENABLE_CREATE_MISSING_STUDY_INSTANCE_UID_FLG.value
          )
          self.assertEqual(
              ingest_flags.GCS_INGEST_STUDY_INSTANCE_UID_SOURCE_FLG.value,
              ingest_flags.UidSource.DICOM,
          )
          self.assertTrue(ingest_flags.TEST_WHOLE_FILENAME_AS_SLIDEID_FLG.value)
          self.assertTrue(
              ingest_flags.INCLUDE_UPLOAD_BUCKET_PATH_IN_WHOLE_FILENAME_SLIDEID_FLG.value
          )
          self.assertEqual(
              ingest_flags.FILENAME_SLIDEID_SPLIT_STR_FLG.value, '*'
          )
          self.assertEqual(ingest_flags.FILENAME_SLIDEID_REGEX_FLG.value, '.*')
          self.assertEqual(
              ingest_flags.METADATA_PRIMARY_KEY_COLUMN_NAME_FLG.value,
              'primary_key',
          )
          self.assertTrue(ingest_flags.DISABLE_BARCODE_DECODER_FLG.value)
          self.assertTrue(
              ingest_flags.DISABLE_CLOUD_VISION_BARCODE_SEG_FLG.value
          )
          self.assertEqual(
              ingest_flags.BIG_QUERY_METADATA_TABLE_FLG.value,
              'project.dataset.table',
          )
          self.assertEqual(
              ingest_flags.METADATA_UID_VALIDATION_FLG.value,
              ingest_flags.MetadataUidValidation.NONE,
          )
          self.assertEqual(
              ingest_flags.METADATA_TAG_LENGTH_VALIDATION_FLG.value,
              ingest_flags.MetadataTagLengthValidation.ERROR,
          )
          self.assertTrue(
              ingest_flags.REQUIRE_TYPE1_DICOM_TAG_METADATA_IS_DEFINED_FLG.value
          )
          self.assertEqual(
              ingest_flags.WSI2DCM_DICOM_FRAME_HEIGHT_FLG.value, 312
          )
          self.assertEqual(
              ingest_flags.WSI2DCM_DICOM_FRAME_WIDTH_FLG.value, 512
          )
          self.assertEqual(
              ingest_flags.WSI2DCM_COMPRESSION_FLG.value,
              ingest_flags.Wsi2DcmCompression.JPEG2000,
          )
          self.assertEqual(
              ingest_flags.WSI2DCM_JPEG_COMPRESSION_QUALITY_FLG.value, 1
          )
          self.assertEqual(
              ingest_flags.WSI2DCM_JPEG_COMPRESSION_SUBSAMPLING_FLG.value,
              ingest_flags.Wsi2DcmJpegCompressionSubsample.SUBSAMPLE_420,
          )
          self.assertFalse(ingest_flags.EMBED_ICC_PROFILE_FLG.value)
          self.assertEqual(
              ingest_flags.WSI2DCM_PIXEL_EQUIVALENT_TRANSFORM_FLG.value,
              ingest_flags.Wsi2DcmPixelEquivalentTransform.DISABLED,
          )
          self.assertEqual(
              ingest_flags.DICOM_GUID_PREFIX_FLG.value,
              '1.3.6.1.4.1.11129.5.7.999',
          )
          self.assertEqual(
              set(ingest_flags.INGEST_IGNORE_ROOT_DIR_FLG.value),
              {'foo', 'magic'},
          )
          self.assertEqual(
              set(ingest_flags.GCS_UPLOAD_IGNORE_FILE_EXTS_FLG.value),
              {'.doc', '.txt'},
          )
          self.assertFalse(
              ingest_flags.DELETE_FILE_FROM_INGEST_AT_BUCKET_AT_INGEST_SUCCESS_OR_FAILURE_FLG.value
          )
          self.assertEqual(
              cloud_logging_client.CLOUD_OPS_LOG_NAME_FLG.value, 'magic_log'
          )
          self.assertEqual(
              cloud_logging_client.CLOUD_OPS_LOG_PROJECT_FLG.value,
              'magic_log_project',
          )


if __name__ == '__main__':
  absltest.main()
