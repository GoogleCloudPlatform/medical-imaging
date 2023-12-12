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
"""Tests for decode_slideid."""

import os
from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized

from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_image_extractor
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import barcode_reader
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import decode_slideid
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client


FLAGS = flags.FLAGS

DEFAULT_SLIDE_ID_REGEX = '^[a-zA-Z0-9]+-[a-zA-Z0-9]+(-[a-zA-Z0-9]+)+'


class DecodeSlideIdTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self._zxing_cli_path = os.path.join(
        FLAGS.test_srcdir, 'third_party/zxing/zxing_cli'
    )

  @parameterized.named_parameters([
      dict(testcase_name='empty', filename_parts=[], filename='', expected=[]),
      dict(
          testcase_name='filename_only',
          filename_parts=[],
          filename='abc',
          expected=['abc'],
      ),
      dict(
          testcase_name='filename_parts_only',
          filename_parts=['123', '456'],
          filename='',
          expected=['123', '456'],
      ),
      dict(
          testcase_name='both',
          filename_parts=['123', '456'],
          filename='abc',
          expected=['123', '456', 'abc'],
      ),
  ])
  def test_filenameslide_id_parts(self, filename_parts, filename, expected):
    parts = decode_slideid.FilenameSlideId(
        filename, filename_parts, bool(filename), []
    )
    self.assertEqual(parts.candidate_filename_slide_id_parts, filename_parts)
    self.assertEqual(parts.filename, filename)
    self.assertEqual(parts.candidate_slide_ids, expected)

  def test_get_candidate_slide_ids_from_long_slideid_filename(self):
    """Tests identifying long slideid in filename path."""
    filename = 'SR-21-2-B1-5_test_img.svs'
    with flagsaver.flagsaver(
        wsi2dcm_filename_slideid_regex=DEFAULT_SLIDE_ID_REGEX
    ):
      result = decode_slideid.get_candidate_slide_ids_from_filename(filename)
    self.assertEqual(result.candidate_slide_ids, ['SR-21-2-B1-5'])
    self.assertEqual(result.ignored_text, ['test', 'img'])

  @flagsaver.flagsaver(filename_slideid_split_str='&')
  def test_get_candidate_slide_ids_from_long_slideid_filename_config_split(
      self,
  ):
    """Tests identifying long slideid in filename path."""
    filename = 'SR_21_2_B1-5&test_img.svs'
    with flagsaver.flagsaver(
        wsi2dcm_filename_slideid_regex=DEFAULT_SLIDE_ID_REGEX
    ):
      result = decode_slideid.get_candidate_slide_ids_from_filename(filename)
    self.assertEqual(result.ignored_text, ['SR_21_2_B1-5', 'test_img'])

  @parameterized.named_parameters([
      dict(
          testcase_name='include_filename',
          test_whole_filename_as_slideid=True,
          expected_candidates=['SR_21_2_B1-5&test_img'],
      ),
      dict(
          testcase_name='filename_parts_only',
          test_whole_filename_as_slideid=False,
          expected_candidates=[],
      ),
  ])
  @flagsaver.flagsaver(filename_slideid_split_str='')
  def test_get_candidate_slide_ids_from_long_slideid_filename_no_split(
      self, test_whole_filename_as_slideid, expected_candidates
  ):
    """Tests identifying long slideid in filename path."""
    filename = 'SR_21_2_B1-5&test_img.svs'
    with flagsaver.flagsaver(
        test_whole_filename_as_slideid=test_whole_filename_as_slideid,
        wsi2dcm_filename_slideid_regex=DEFAULT_SLIDE_ID_REGEX,
    ):
      result = decode_slideid.get_candidate_slide_ids_from_filename(filename)
    self.assertEqual(result.candidate_slide_ids, expected_candidates)
    self.assertEqual(result.ignored_text, ['SR_21_2_B1-5&test_img'])

  @parameterized.named_parameters([
      dict(
          testcase_name='include_filename',
          test_whole_filename_as_slideid=True,
          expected_candidates=['SR-21-2', 'SR-21-2_test_img'],
      ),
      dict(
          testcase_name='filename_parts_only',
          test_whole_filename_as_slideid=False,
          expected_candidates=['SR-21-2'],
      ),
  ])
  def test_get_candidate_slide_ids_from_short_slideid_filename(
      self, test_whole_filename_as_slideid, expected_candidates
  ):
    """Tests identifying short slideid in filename path."""
    filename = 'SR-21-2_test_img.svs'
    with flagsaver.flagsaver(
        test_whole_filename_as_slideid=test_whole_filename_as_slideid,
        wsi2dcm_filename_slideid_regex=DEFAULT_SLIDE_ID_REGEX,
    ):
      result = decode_slideid.get_candidate_slide_ids_from_filename(filename)
    self.assertEqual(result.candidate_slide_ids, expected_candidates)
    self.assertEqual(result.ignored_text, ['test', 'img'])

  @flagsaver.flagsaver(metadata_bucket='test')
  def test_find_barcode_in_metadata(self):
    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata(
        [gen_test_util.test_file_path('metadata_duplicate.csv')]
    )
    self.assertEqual(
        decode_slideid.find_slide_id_in_metadata('SR-21-2-A1-3', meta_client),
        'SR-21-2-A1-3',
    )

  @flagsaver.flagsaver(metadata_bucket='test')
  def test_find_null_barcode_in_metadata(self):
    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata(
        [gen_test_util.test_file_path('metadata_duplicate.csv')]
    )
    with self.assertRaisesRegex(
        decode_slideid.SlideIdIdentificationError,
        ingest_const.ErrorMsgs.CANDIDATE_SLIDE_ID_MISSING,
    ):
      decode_slideid.find_slide_id_in_metadata(None, meta_client)

  @flagsaver.flagsaver(metadata_bucket='test')
  def test_find_missing_barcode_in_metadata(self):
    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata(
        [gen_test_util.test_file_path('metadata_duplicate.csv')]
    )
    with self.assertRaisesRegex(
        decode_slideid.SlideIdIdentificationError,
        ingest_const.ErrorMsgs.SLIDE_ID_MISSING_FROM_CSV_METADATA,
    ):
      decode_slideid.find_slide_id_in_metadata('abc', meta_client)

  @flagsaver.flagsaver(metadata_bucket='test')
  def test_find_duplicate_barcode_in_metadata(self):
    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata(
        [gen_test_util.test_file_path('metadata_duplicate.csv')]
    )
    with self.assertRaisesRegex(
        decode_slideid.SlideIdIdentificationError,
        ingest_const.ErrorMsgs.SLIDE_ID_DEFINED_ON_MULTIPLE_ROWS,
    ):
      decode_slideid.find_slide_id_in_metadata('MD-03-3-A1-2', meta_client)

  @parameterized.named_parameters([
      dict(testcase_name='candidate_slide_id_is_none', candidate_slide_id=None),
      dict(testcase_name='candidate_slide_id_is_empty', candidate_slide_id=''),
  ])
  @flagsaver.flagsaver(metadata_bucket='test')
  def test_find_slide_in_metadata_raises_if_slide_id_undefined(
      self, candidate_slide_id
  ):
    with self.assertRaisesRegex(
        decode_slideid.SlideIdIdentificationError,
        ingest_const.ErrorMsgs.CANDIDATE_SLIDE_ID_MISSING,
    ):
      decode_slideid.find_slide_id_in_metadata(
          candidate_slide_id, metadata_storage_client.MetadataStorageClient()
      )

  @flagsaver.flagsaver(metadata_bucket='test')
  def test_find_slideid_in_filename_no_match(self):
    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata(
        [gen_test_util.test_file_path('metadata_duplicate.csv')]
    )
    with self.assertRaisesRegex(
        decode_slideid.SlideIdIdentificationError,
        ingest_const.ErrorMsgs.FILE_NAME_MISSING_SLIDE_ID_CANDIDATES,
    ):
      decode_slideid.get_slide_id_from_filename('BF-MD.zip', meta_client)

  @flagsaver.flagsaver(metadata_bucket='test')
  def test_find_slideid_in_empty_filename_no_match(self):
    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata(
        [gen_test_util.test_file_path('metadata_duplicate.csv')]
    )
    with self.assertRaisesRegex(
        decode_slideid.SlideIdIdentificationError,
        ingest_const.ErrorMsgs.SLIDE_ID_MISSING_FROM_FILE_NAME,
    ):
      decode_slideid.get_slide_id_from_filename('', meta_client)

  @flagsaver.flagsaver(metadata_bucket='test')
  def test_find_slideid_in_filename_not_in_metadata(self):
    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata(
        [gen_test_util.test_file_path('metadata_duplicate.csv')]
    )
    with self.assertRaisesRegex(
        decode_slideid.SlideIdIdentificationError,
        ingest_const.ErrorMsgs.SLIDE_ID_MISSING_FROM_CSV_METADATA,
    ):
      decode_slideid.get_slide_id_from_filename(
          'NF-03-3-A1-2_BF-MD.zip', meta_client
      )

  @flagsaver.flagsaver(metadata_bucket='test')
  def test_find_slideid_in_filename_duplicate_in_metadata(self):
    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata(
        [gen_test_util.test_file_path('metadata_duplicate.csv')]
    )
    with self.assertRaisesRegex(
        decode_slideid.SlideIdIdentificationError,
        ingest_const.ErrorMsgs.SLIDE_ID_DEFINED_ON_MULTIPLE_ROWS,
    ):
      decode_slideid.get_slide_id_from_filename(
          'MD-03-3-A1-2_BF-MD.zip', meta_client
      )

  @flagsaver.flagsaver(metadata_bucket='test')
  def test_find_slideid_in_filename_in_metadata(self):
    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata(
        [gen_test_util.test_file_path('metadata_duplicate.csv')]
    )
    self.assertEqual(
        decode_slideid.get_slide_id_from_filename(
            'SR-21-2-A1-3_BF-MD.zip', meta_client
        ),
        'SR-21-2-A1-3',
    )

  @flagsaver.flagsaver(metadata_bucket='test')
  @flagsaver.flagsaver(testing_disable_cloudvision=True)
  @flagsaver.flagsaver(wsi2dcm_filename_slideid_regex='^.*')
  def test_find_slide_id_in_barcode_in_metadata(self):
    zxing_cli_flag_value = FLAGS.zxing_cli
    try:
      FLAGS.zxing_cli = self._zxing_cli_path
      meta_client = metadata_storage_client.MetadataStorageClient()
      meta_client.set_debug_metadata(
          [gen_test_util.test_file_path('metadata_duplicate.csv')]
      )
      barcode_image = ancillary_image_extractor.AncillaryImage(
          gen_test_util.test_file_path('barcode_wikipedia_pdf417.png'),
          'RGB',
          False,
      )
      self.assertEqual(
          decode_slideid.get_slide_id_from_ancillary_images(
              [barcode_image], meta_client, None
          ),
          'Wikipedia',
      )
    finally:
      FLAGS.zxing_cli = zxing_cli_flag_value

  @flagsaver.flagsaver(metadata_bucket='test')
  @flagsaver.flagsaver(testing_disable_cloudvision=True)
  @flagsaver.flagsaver(wsi2dcm_filename_slideid_regex='^.*')
  def test_find_slide_id_in_barcode_not_in_metadata(self):
    zxing_cli_flag_value = FLAGS.zxing_cli
    try:
      FLAGS.zxing_cli = self._zxing_cli_path
      meta_client = metadata_storage_client.MetadataStorageClient()
      meta_client.set_debug_metadata(
          [gen_test_util.test_file_path('metadata_duplicate.csv')]
      )
      barcode_image = ancillary_image_extractor.AncillaryImage(
          gen_test_util.test_file_path('barcode_cant_read.jpg'),
          'RGB',
          False,
      )
      with self.assertRaisesRegex(
          decode_slideid.SlideIdIdentificationError,
          ingest_const.ErrorMsgs.SLIDE_ID_MISSING_FROM_CSV_METADATA,
      ):
        decode_slideid.get_slide_id_from_ancillary_images(
            [barcode_image], meta_client, None
        )
    finally:
      FLAGS.zxing_cli = zxing_cli_flag_value

  @flagsaver.flagsaver(metadata_bucket='test')
  def test_find_slide_id_in_no_barcode_image(self):
    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata(
        [gen_test_util.test_file_path('metadata_duplicate.csv')]
    )
    with self.assertRaisesRegex(
        decode_slideid.SlideIdIdentificationError,
        ingest_const.ErrorMsgs.BARCODE_IMAGE_MISSING,
    ):
      decode_slideid.get_slide_id_from_ancillary_images([], meta_client, None)

  @flagsaver.flagsaver(metadata_bucket='test')
  @flagsaver.flagsaver(testing_disable_cloudvision=True)
  @flagsaver.flagsaver(wsi2dcm_filename_slideid_regex='^.*')
  def test_find_slide_id_in_barcode_cannot_read(self):
    zxing_cli_flag_value = FLAGS.zxing_cli
    try:
      FLAGS.zxing_cli = self._zxing_cli_path
      meta_client = metadata_storage_client.MetadataStorageClient()
      meta_client.set_debug_metadata(
          [gen_test_util.test_file_path('metadata_duplicate.csv')]
      )
      barcode_image = ancillary_image_extractor.AncillaryImage(
          gen_test_util.test_file_path('datamatrix_croped.jpg'),
          'RGB',
          False,
      )
      with self.assertRaisesRegex(
          decode_slideid.SlideIdIdentificationError,
          ingest_const.ErrorMsgs.BARCODE_MISSING_FROM_IMAGES,
      ):
        decode_slideid.get_slide_id_from_ancillary_images(
            [barcode_image], meta_client, None
        )
    finally:
      FLAGS.zxing_cli = zxing_cli_flag_value

  @flagsaver.flagsaver(metadata_bucket='test')
  @flagsaver.flagsaver(testing_disable_cloudvision=True)
  @flagsaver.flagsaver(wsi2dcm_filename_slideid_regex='^.*')
  def test_find_slide_id_in_barcode_ignores_empty_barcodes(self):
    meta_client = metadata_storage_client.MetadataStorageClient()
    barcode_image = ancillary_image_extractor.AncillaryImage(
        gen_test_util.test_file_path('datamatrix_croped.jpg'),
        'RGB',
        False,
    )
    with self.assertRaisesRegex(
        decode_slideid.SlideIdIdentificationError,
        ingest_const.ErrorMsgs.BARCODE_MISSING_FROM_IMAGES,
    ):
      with mock.patch.object(
          barcode_reader,
          'read_barcode_in_files',
          autospec=True,
          return_value={'': {'unused'}},
      ):
        decode_slideid.get_slide_id_from_ancillary_images(
            [barcode_image], meta_client, None
        )

  @parameterized.named_parameters([
      dict(
          testcase_name='dicom_barcode_value',
          filename_path='abc_foo.svs',
          candidate_barcode_values=['efg'],
          test_whole_filename_as_slideid=False,
          candidate_dicom_barcode_tag_value='google',
          expected_slide_id='google',
      ),
      dict(
          testcase_name='filename_part_value',
          filename_path='abc.svs',
          candidate_barcode_values=['efg'],
          test_whole_filename_as_slideid=False,
          candidate_dicom_barcode_tag_value='',
          expected_slide_id='abc',
      ),
      dict(
          testcase_name='barcode_value',
          filename_path='_.svs',
          candidate_barcode_values=['efg'],
          test_whole_filename_as_slideid=False,
          candidate_dicom_barcode_tag_value='',
          expected_slide_id='efg',
      ),
      dict(
          testcase_name='barcode_value_ignores_empty_barcodes',
          filename_path='_.svs',
          candidate_barcode_values=['efg', ''],
          test_whole_filename_as_slideid=False,
          candidate_dicom_barcode_tag_value='',
          expected_slide_id='efg',
      ),
      dict(
          testcase_name='use_whole_filename_as_slide_id',
          filename_path='.bc1_.svs',
          candidate_barcode_values=['efg', ''],
          test_whole_filename_as_slideid=True,
          candidate_dicom_barcode_tag_value='',
          expected_slide_id='.bc1_',
      ),
      dict(
          testcase_name='slide_id_starts_with_period',
          filename_path='.svs',
          test_whole_filename_as_slideid=True,
          candidate_barcode_values=[],
          candidate_dicom_barcode_tag_value='',
          expected_slide_id='.svs',
      ),
  ])
  @flagsaver.flagsaver(wsi2dcm_filename_slideid_regex=r'[a-z]+')
  def test_get_metadata_free_slide_id_success(
      self,
      filename_path,
      candidate_barcode_values,
      test_whole_filename_as_slideid,
      candidate_dicom_barcode_tag_value,
      expected_slide_id,
  ):
    with mock.patch.object(
        barcode_reader,
        'read_barcode_in_files',
        autospec=True,
        return_value={
            bar_code: {'unused'} for bar_code in candidate_barcode_values
        },
    ):
      with flagsaver.flagsaver(
          test_whole_filename_as_slideid=test_whole_filename_as_slideid
      ):
        self.assertEqual(
            decode_slideid.get_metadata_free_slide_id(
                filename_path,
                [
                    ancillary_image_extractor.AncillaryImage(
                        'foo.png', 'RGB', True
                    )
                ]
                * len(candidate_barcode_values),
                candidate_dicom_barcode_tag_value,
            ),
            expected_slide_id,
        )

  @parameterized.named_parameters([
      dict(
          testcase_name='multiple_filename_slide_id_candidates',
          filename_path='abc_foo.svs',
          test_whole_filename_as_slideid=True,
          candidate_barcode_values=[''],
          expected_error_msg=ingest_const.ErrorMsgs.FILE_NAME_CONTAINS_MULTIPLE_SLIDE_ID_CANDIDATES,
      ),
      dict(
          testcase_name='multiple_barcode_slide_id_candidates',
          filename_path='_.svs',
          test_whole_filename_as_slideid=False,
          candidate_barcode_values=['123', '456'],
          expected_error_msg=ingest_const.ErrorMsgs.MULTIPLE_BARCODES_SLIDE_ID_CANDIDATES,
      ),
      dict(
          testcase_name='no_slide_id_filename_does_not_match_regex',
          filename_path='a2bc_fo3o.svs',
          test_whole_filename_as_slideid=False,
          candidate_barcode_values=[''],
          expected_error_msg=ingest_const.ErrorMsgs.SLIDE_ID_MISSING,
      ),
  ])
  @flagsaver.flagsaver(wsi2dcm_filename_slideid_regex=r'[a-z]+')
  def test_get_metadata_free_slide_id_failure(
      self,
      filename_path,
      test_whole_filename_as_slideid,
      candidate_barcode_values,
      expected_error_msg,
  ):
    with self.assertRaisesRegex(
        decode_slideid.SlideIdIdentificationError,
        expected_error_msg,
    ):
      with mock.patch.object(
          barcode_reader,
          'read_barcode_in_files',
          autospec=True,
          return_value={
              bar_code: {'unused'} for bar_code in candidate_barcode_values
          },
      ):
        with flagsaver.flagsaver(
            test_whole_filename_as_slideid=test_whole_filename_as_slideid
        ):
          decode_slideid.get_metadata_free_slide_id(
              filename_path,
              [ancillary_image_extractor.AncillaryImage('foo.png', 'RGB', True)]
              * len(candidate_barcode_values),
              '',
          )

  @parameterized.named_parameters([
      dict(
          testcase_name='too_short',
          filename_path='_.svs',
          expected_error_msg=ingest_const.ErrorMsgs.SLIDE_ID_MISSING,
      ),
      dict(
          testcase_name='too_long',
          filename_path=f'{"a" * 65}_.svs',
          expected_error_msg=ingest_const.ErrorMsgs.INVALID_SLIDE_ID_LENGTH,
      ),
  ])
  @flagsaver.flagsaver(
      wsi2dcm_filename_slideid_regex=r'.*', test_whole_filename_as_slideid=False
  )
  def test_get_metadata_free_slide_id_length_failure(
      self, filename_path, expected_error_msg
  ):
    with self.assertRaisesRegex(
        decode_slideid.SlideIdIdentificationError, expected_error_msg
    ):
      with mock.patch.object(
          barcode_reader,
          'read_barcode_in_files',
          autospec=True,
          return_value={},
      ):
        decode_slideid.get_metadata_free_slide_id(filename_path, [], '')


if __name__ == '__main__':
  absltest.main()
