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

from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_image_extractor
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import barcode_reader
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import decode_slideid
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client


FLAGS = flags.FLAGS

DEFAULT_SLIDE_ID_REGEX = '^[a-zA-Z0-9]+-[a-zA-Z0-9]+(-[a-zA-Z0-9]+)+'


def _g3_zxing_cli_path() -> str:
  return os.path.join(FLAGS.test_srcdir, 'third_party/zxing/zxing_cli')


class DecodeSlideIdTest(parameterized.TestCase):

  @parameterized.named_parameters([
      dict(
          testcase_name='uri_none',
          gen_dicom=abstract_dicom_generation.GeneratedDicomFiles(
              'foo1.svs', None
          ),
          expected='foo1',
      ),
      dict(
          testcase_name='uri_empty',
          gen_dicom=abstract_dicom_generation.GeneratedDicomFiles(
              'foo2.tiff', ''
          ),
          expected='foo2',
      ),
      dict(
          testcase_name='uri_defined',
          gen_dicom=abstract_dicom_generation.GeneratedDicomFiles(
              'foo.svs', 'gs://b/bar/foo.svs'
          ),
          expected='foo',
      ),
  ])
  def test_get_whole_filename_to_test(self, gen_dicom, expected):
    self.assertEqual(
        decode_slideid._get_whole_filename_to_test(gen_dicom), expected
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='uri_none',
          gen_dicom=abstract_dicom_generation.GeneratedDicomFiles(
              '/tmp/foo1.svs', None
          ),
          expected='foo1',
      ),
      dict(
          testcase_name='uri_empty',
          gen_dicom=abstract_dicom_generation.GeneratedDicomFiles(
              '/tmp/foo2.svs', ''
          ),
          expected='foo2',
      ),
      dict(
          testcase_name='uri_not_gs_format',
          gen_dicom=abstract_dicom_generation.GeneratedDicomFiles(
              '/tmp/foo3.svs', 'unexpected'
          ),
          expected='foo3',
      ),
      dict(
          testcase_name='uri_no_bucket',
          gen_dicom=abstract_dicom_generation.GeneratedDicomFiles(
              '/tmp/foo4.svs', 'gs:///foo4.svs'
          ),
          expected='foo4',
      ),
      dict(
          testcase_name='uri_bucket_no_path',
          gen_dicom=abstract_dicom_generation.GeneratedDicomFiles(
              '/tmp/foo5.svs', 'gs://b/foo5.svs'
          ),
          expected='foo5',
      ),
      dict(
          testcase_name='uri_bucket_with_path',
          gen_dicom=abstract_dicom_generation.GeneratedDicomFiles(
              '/tmp/foo.svs', 'gs://b/bar/foo.svs'
          ),
          expected='bar/foo',
      ),
  ])
  @flagsaver.flagsaver(
      include_upload_bucket_path_in_whole_filename_slideid=True
  )
  def test_get_whole_filename_to_test_include_upload_bucket_path(
      self, gen_dicom, expected
  ):
    self.assertEqual(
        decode_slideid._get_whole_filename_to_test(gen_dicom), expected
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='split_filename',
          filename='SR-21-2-B1-5_test_img.svs',
          flgs={},
          expected=['SR-21-2-B1-5'],
      ),
      dict(
          testcase_name='filename_not_match_regex_and_include_whole_filename',
          filename='SR_21_2_B1-5&test_img.svs',
          flgs=dict(
              filename_slideid_split_str='&',
              test_whole_filename_as_slideid=True,
          ),
          expected=['SR_21_2_B1-5&test_img'],
      ),
      dict(
          testcase_name='split_filename_with_wholefilename',
          filename='SR-21-2-B1-5_test_img.svs',
          flgs=dict(test_whole_filename_as_slideid=True),
          expected=['SR-21-2-B1-5_test_img', 'SR-21-2-B1-5'],
      ),
      dict(
          testcase_name='split_filename_with_wholefilename_with_uploadpath',
          filename='SR-21-2-B1-5_test_img.svs',
          flgs=dict(
              test_whole_filename_as_slideid=True,
              include_upload_bucket_path_in_whole_filename_slideid=True,
          ),
          expected=['foo/SR-21-2-B1-5_test_img', 'SR-21-2-B1-5'],
      ),
      dict(
          testcase_name='filename_split_alternative_split_str',
          filename='SR-21-2-B1-5&test_img.svs',
          flgs=dict(filename_slideid_split_str='&'),
          expected=['SR-21-2-B1-5'],
      ),
      dict(
          testcase_name='filename_split_not_found_whole_filename',
          filename='SR-21-2-B1-5_test_img.svs',
          flgs=dict(
              filename_slideid_split_str='&',
              test_whole_filename_as_slideid=True,
          ),
          expected=['SR-21-2-B1-5_test_img'],
      ),
      dict(
          testcase_name='filename_split_not_found_whole_filename_with_path',
          filename='SR-21-2-B1-5.svs',
          flgs=dict(
              filename_slideid_split_str='&',
              test_whole_filename_as_slideid=True,
              include_upload_bucket_path_in_whole_filename_slideid=True,
          ),
          expected=['foo/SR-21-2-B1-5', 'SR-21-2-B1-5'],
      ),
      dict(
          testcase_name='filename_split_undefined',
          filename='SR-21-2-B1-5.svs',
          flgs=dict(
              filename_slideid_split_str='',
          ),
          expected=['SR-21-2-B1-5'],
      ),
      dict(
          testcase_name='skip_duplicates',
          filename='SR-21-2-B1-5__SR-21-2-B1-5.svs',
          flgs={},
          expected=['SR-21-2-B1-5'],
      ),
  ])
  def test_get_candidate_slide_ids_from_filename(
      self,
      filename,
      flgs,
      expected,
  ):
    """Tests identifying long slideid in filename path."""
    with flagsaver.flagsaver(
        wsi2dcm_filename_slideid_regex=DEFAULT_SLIDE_ID_REGEX, **flgs
    ):
      self.assertEqual(
          decode_slideid._get_candidate_slide_ids_from_filename(
              abstract_dicom_generation.GeneratedDicomFiles(
                  filename, f'gs://tst/foo/{filename}'
              )
          ),
          expected,
      )

  @parameterized.named_parameters([
      dict(
          testcase_name='filename_empty',
          filename='',
          flgs={},
      ),
      dict(
          testcase_name='filename_not_match_regex',
          filename='SR_21_2_B1-5&test_img.svs',
          flgs=dict(filename_slideid_split_str='&'),
      ),
  ])
  def test_get_candidate_slide_ids_from_filename_raises_if_empty(
      self,
      filename,
      flgs,
  ):
    """Tests identifying long slideid in filename path."""
    with flagsaver.flagsaver(
        wsi2dcm_filename_slideid_regex=DEFAULT_SLIDE_ID_REGEX, **flgs
    ):
      with self.assertRaisesRegex(
          decode_slideid.SlideIdIdentificationError,
          ingest_const.ErrorMsgs.FILENAME_MISSING_SLIDE_METADATA_PRIMARY_KEY,
      ):
        decode_slideid._get_candidate_slide_ids_from_filename(
            abstract_dicom_generation.GeneratedDicomFiles(
                filename, f'gs://tst/foo/{filename}'
            )
        )

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

  @parameterized.named_parameters([
      dict(
          testcase_name='none_barcode',
          error_msg=ingest_const.ErrorMsgs.CANDIDATE_SLIDE_ID_MISSING,
          slide_id=None,
      ),
      dict(
          testcase_name='empty_barcode',
          error_msg=ingest_const.ErrorMsgs.CANDIDATE_SLIDE_ID_MISSING,
          slide_id='',
      ),
      dict(
          testcase_name='missing_slideid',
          error_msg=ingest_const.ErrorMsgs.SLIDE_ID_MISSING_FROM_CSV_METADATA,
          slide_id='abc',
      ),
      dict(
          testcase_name='defined_on_multiple_rows',
          error_msg=ingest_const.ErrorMsgs.SLIDE_ID_DEFINED_ON_MULTIPLE_ROWS,
          slide_id='MD-03-3-A1-2',
      ),
  ])
  @flagsaver.flagsaver(metadata_bucket='test')
  def test_find_barcode_in_metadata_raises(self, slide_id, error_msg):
    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata(
        [gen_test_util.test_file_path('metadata_duplicate.csv')]
    )
    with self.assertRaisesRegex(
        decode_slideid.SlideIdIdentificationError, error_msg
    ):
      decode_slideid.find_slide_id_in_metadata(slide_id, meta_client)

  @parameterized.named_parameters([
      dict(
          testcase_name='filename_duplicate_in_metadata',
          filename='MD-03-3-A1-2_BF-MD.zip',
          error_msg=ingest_const.ErrorMsgs.SLIDE_ID_DEFINED_ON_MULTIPLE_ROWS,
      ),
      dict(
          testcase_name='filename_not_in_metadata',
          filename='NF-03-3-A1-2_BF-MD.zip',
          error_msg=ingest_const.ErrorMsgs.SLIDE_ID_MISSING_FROM_CSV_METADATA,
      ),
      dict(
          testcase_name='empty_filename_no_match',
          filename='',
          error_msg=ingest_const.ErrorMsgs.FILENAME_MISSING_SLIDE_METADATA_PRIMARY_KEY,
      ),
      dict(
          testcase_name='no_candidate_slide_id',
          filename='BF-MD.zip',
          error_msg=ingest_const.ErrorMsgs.FILENAME_MISSING_SLIDE_METADATA_PRIMARY_KEY,
      ),
  ])
  @flagsaver.flagsaver(metadata_bucket='test')
  def test_find_slideid_raises(self, filename: str, error_msg: str):
    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata(
        [gen_test_util.test_file_path('metadata_duplicate.csv')]
    )
    with self.assertRaisesRegex(
        decode_slideid.SlideIdIdentificationError, error_msg
    ):
      decode_slideid.get_slide_id_from_filename(
          abstract_dicom_generation.GeneratedDicomFiles(filename, 'tst'),
          meta_client,
      )

  @flagsaver.flagsaver(metadata_bucket='test')
  def test_find_slideid_in_filename_in_metadata(self):
    meta_client = metadata_storage_client.MetadataStorageClient()
    meta_client.set_debug_metadata(
        [gen_test_util.test_file_path('metadata_duplicate.csv')]
    )
    self.assertEqual(
        decode_slideid.get_slide_id_from_filename(
            abstract_dicom_generation.GeneratedDicomFiles(
                'SR-21-2-A1-3_BF-MD.zip', 'tst'
            ),
            meta_client,
        ),
        'SR-21-2-A1-3',
    )

  @flagsaver.flagsaver(metadata_bucket='test')
  @flagsaver.flagsaver(testing_disable_cloudvision=True)
  @flagsaver.flagsaver(wsi2dcm_filename_slideid_regex='^.*')
  def test_find_slide_id_in_barcode_in_metadata(self):
    with flagsaver.flagsaver(zxing_cli=_g3_zxing_cli_path()):
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

  @flagsaver.flagsaver(metadata_bucket='test')
  @flagsaver.flagsaver(testing_disable_cloudvision=True)
  @flagsaver.flagsaver(wsi2dcm_filename_slideid_regex='^.*')
  def test_find_slide_id_in_barcode_not_in_metadata(self):
    with flagsaver.flagsaver(zxing_cli=_g3_zxing_cli_path()):
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
    with flagsaver.flagsaver(zxing_cli=_g3_zxing_cli_path()):
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
          testcase_name='basic_filename',
          filename_path='abc_foo.svs',
          include_bucket=False,
          expected_slide_id='abc_foo',
      ),
      dict(
          testcase_name='filename_starts_with_peroid',
          filename_path='.bc1_.svs',
          include_bucket=False,
          expected_slide_id='.bc1_',
      ),
      dict(
          testcase_name='filename_has_single_peroid',
          filename_path='.svs',
          include_bucket=False,
          expected_slide_id='.svs',
      ),
      dict(
          testcase_name='basic_filename_with_bucket_path',
          filename_path='abc_foo.svs',
          include_bucket=True,
          expected_slide_id='foo/abc_foo',
      ),
      dict(
          testcase_name='peroid_filename_with_bucket_path',
          filename_path='.bc1_.svs',
          include_bucket=True,
          expected_slide_id='foo/.bc1_',
      ),
  ])
  @flagsaver.flagsaver(enable_metadata_free_ingestion=True)
  def test_get_metadata_free_slide_id_success(
      self,
      filename_path,
      include_bucket,
      expected_slide_id,
  ):
    with flagsaver.flagsaver(
        include_upload_bucket_path_in_whole_filename_slideid=include_bucket
    ):
      self.assertEqual(
          decode_slideid.get_metadata_free_slide_id(
              abstract_dicom_generation.GeneratedDicomFiles(
                  filename_path, f'gs://tst/foo/{filename_path}'
              ),
          ),
          expected_slide_id,
      )

  @flagsaver.flagsaver(
      enable_metadata_free_ingestion=True,
      metadata_tag_length_validation=ingest_flags.MetadataTagLengthValidation.ERROR,
  )
  def test_get_metadata_free_slide_id_length_failure(self):
    filename_path = f'{"a" * 65}_.svs'
    with self.assertRaisesRegex(
        decode_slideid.SlideIdIdentificationError,
        ingest_const.ErrorMsgs.INVALID_SLIDE_ID_LENGTH,
    ):
      with mock.patch.object(
          barcode_reader,
          'read_barcode_in_files',
          autospec=True,
          return_value={},
      ):
        decode_slideid.get_metadata_free_slide_id(
            abstract_dicom_generation.GeneratedDicomFiles(
                filename_path, f'gs://tst/foo/{filename_path}'
            ),
        )

  @parameterized.parameters([
      ingest_flags.MetadataTagLengthValidation.NONE,
      ingest_flags.MetadataTagLengthValidation.LOG_WARNING,
      ingest_flags.MetadataTagLengthValidation.LOG_WARNING_AND_CLIP,
      ingest_flags.MetadataTagLengthValidation.ERROR,
  ])
  @flagsaver.flagsaver(
      enable_metadata_free_ingestion=True,
  )
  def test_get_metadata_free_slide_id_empty_filename_raises(self, flg):
    filename_path = ''
    with flagsaver.flagsaver(metadata_tag_length_validation=flg):
      with self.assertRaisesRegex(
          decode_slideid.SlideIdIdentificationError,
          ingest_const.ErrorMsgs.INVALID_SLIDE_ID_LENGTH,
      ):
        with mock.patch.object(
            barcode_reader,
            'read_barcode_in_files',
            autospec=True,
            return_value={},
        ):
          decode_slideid.get_metadata_free_slide_id(
              abstract_dicom_generation.GeneratedDicomFiles(
                  filename_path, f'gs://tst/foo/{filename_path}'
              ),
          )

  @parameterized.parameters([
      ingest_flags.MetadataTagLengthValidation.NONE,
      ingest_flags.MetadataTagLengthValidation.LOG_WARNING,
      ingest_flags.MetadataTagLengthValidation.LOG_WARNING_AND_CLIP,
  ])
  @flagsaver.flagsaver(enable_metadata_free_ingestion=True)
  def test_get_metadata_free_slide_id_length_exceeds_limits(self, flg):
    base_filename = f'{"a" * 65}_'
    filename_path = f'{base_filename}.svs'
    with flagsaver.flagsaver(metadata_tag_length_validation=flg):
      with mock.patch.object(
          barcode_reader,
          'read_barcode_in_files',
          autospec=True,
          return_value={},
      ):
        self.assertEqual(
            decode_slideid.get_metadata_free_slide_id(
                abstract_dicom_generation.GeneratedDicomFiles(
                    filename_path, f'gs://tst/foo/{filename_path}'
                ),
            ),
            base_filename,
        )


if __name__ == '__main__':
  absltest.main()
