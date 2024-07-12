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
"""Tests for dicom_store_client."""

import dataclasses
import http
import json
import os
from typing import Tuple
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
import pydicom
import requests
import requests_mock

from shared_libs.flags import flag_utils
from shared_libs.logging_lib import cloud_logging_client
from shared_libs.test_utils.dicom_store_mock import dicom_store_mock
from transformation_pipeline.ingestion_lib import gen_test_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen import wsi_dicom_file_ref
from transformation_pipeline.ingestion_lib.dicom_util import dicom_test_util


_PROJECT_ID = '123'
_DICOM_WEBPATH = 'http://healthcare/mock/dicomWeb'


@dataclasses.dataclass(frozen=True)
class MockInstance:
  study_instance_uid: str
  series_instance_uid: str
  sop_instance_uid: str
  accession_number: str
  patient_id: str

  def pydicom_file_dataset(self) -> pydicom.FileDataset:
    ds = pydicom.Dataset()
    ds.StudyInstanceUID = self.study_instance_uid
    ds.SeriesInstanceUID = self.series_instance_uid
    ds.SOPInstanceUID = self.sop_instance_uid
    ds.AccessionNumber = self.accession_number
    if self.patient_id:
      ds.PatientID = self.patient_id
    ds = pydicom.FileDataset(
        filename_or_obj='',
        dataset=ds,
        file_meta=pydicom.dataset.FileMetaDataset(),
        preamble=b'\0' * 128,
    )
    ds.is_little_endian = True
    ds.is_implicit_VR = False
    return ds


def _create_mock_dataset() -> list[pydicom.FileDataset]:
  mock_dataset = []
  for instance in range(1, 4):
    mock_dataset.append(
        MockInstance(
            '1.1', '1.1.1', f'1.1.1.{instance}', 'a1', 'p1'
        ).pydicom_file_dataset()
    )
    mock_dataset.append(
        MockInstance(
            '1.2', '1.2.1', f'1.2.1.{instance}', 'a2', 'p1'
        ).pydicom_file_dataset()
    )
    mock_dataset.append(
        MockInstance(
            '1.3', '1.3.1', f'1.3.1.{instance}', 'a3', 'p2'
        ).pydicom_file_dataset()
    )
    mock_dataset.append(
        MockInstance(
            '1.4', '1.4.1', f'1.4.1.{instance}', 'a4', 'p3'
        ).pydicom_file_dataset()
    )
    mock_dataset.append(
        MockInstance(
            '1.5', '1.5.1', f'1.5.1.{instance}', 'a5', ''
        ).pydicom_file_dataset()
    )
    mock_dataset.append(
        MockInstance(
            '1.6', '1.6.1', f'1.6.1.{instance}', 'a5', 'p4'
        ).pydicom_file_dataset()
    )
  return mock_dataset


def _dicom_ref_dim_metadata(
    physical_width: str = '',
    physical_height: str = '',
    image_type: str = ingest_const.ORIGINAL_PRIMARY_VOLUME,
) -> wsi_dicom_file_ref.WSIDicomFileRef:
  return dicom_test_util.create_mock_non_dpas_generated_wsi_fref({
      ingest_const.DICOMTagKeywords.IMAGED_VOLUME_HEIGHT: physical_height,
      ingest_const.DICOMTagKeywords.IMAGED_VOLUME_WIDTH: physical_width,
      ingest_const.DICOMTagKeywords.IMAGE_TYPE: image_type,
      ingest_const.DICOMTagKeywords.SOP_CLASS_UID: (
          ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
      ),
  })


class DicomStoreClientTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self._dcm_path = gen_test_util.test_file_path('test_jpeg_dicom.dcm')

  def _create_upload_store_results(
      self,
  ) -> Tuple[
      dicom_store_client.UploadSlideToDicomStoreResults,
      wsi_dicom_file_ref.WSIDicomFileRef,
      wsi_dicom_file_ref.WSIDicomFileRef,
      wsi_dicom_file_ref.WSIDicomFileRef,
      wsi_dicom_file_ref.WSIDicomFileRef,
  ]:
    m1 = dicom_test_util.create_mock_dpas_generated_dicom_fref(
        {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '1'}
    )
    m2 = dicom_test_util.create_mock_dpas_generated_dicom_fref(
        {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '2'}
    )
    m3 = dicom_test_util.create_mock_dpas_generated_dicom_fref(
        {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '3'}
    )
    m4 = dicom_test_util.create_mock_dpas_generated_dicom_fref(
        {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '4'}
    )
    results = dicom_store_client.UploadSlideToDicomStoreResults(
        [m1, m2], [m3, m4]
    )
    return (results, m1, m2, m3, m4)

  def test_upload_store_results_constructor(self):
    results, m1, m2, m3, m4 = self._create_upload_store_results()
    self.assertEqual(results.ingested, [m1, m2])
    self.assertEqual(results.previously_ingested, [m3, m4])

  @parameterized.parameters([
      (
          [
              dicom_test_util.create_mock_dpas_generated_dicom_fref(
                  {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '1'}
              )
          ],
          [],
      ),
      (
          [],
          [
              dicom_test_util.create_mock_dpas_generated_dicom_fref(
                  {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '2'}
              )
          ],
      ),
      (
          [
              dicom_test_util.create_mock_dpas_generated_dicom_fref(
                  {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '3'}
              )
          ],
          [
              dicom_test_util.create_mock_dpas_generated_dicom_fref(
                  {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '4'}
              )
          ],
      ),
  ])
  def test_upload_store_results_slide_has_instances_in_dicom_store(
      self, ingested, prev_ingest
  ):
    results = dicom_store_client.UploadSlideToDicomStoreResults(
        ingested, prev_ingest
    )
    self.assertTrue(results.slide_has_instances_in_dicom_store())

  def test_upload_store_results_slide_has_no_instances_in_dicom_store(self):
    results = dicom_store_client.UploadSlideToDicomStoreResults([], [])
    self.assertFalse(results.slide_has_instances_in_dicom_store())

  def test_upload_store_results_slide_instances_in_dicom_store(self):
    results, m1, m2, m3, m4 = self._create_upload_store_results()
    self.assertEqual(results.slide_instances_in_dicom_store, [m1, m2, m3, m4])

  @parameterized.parameters([
      (
          [
              dicom_test_util.create_mock_dpas_generated_dicom_fref(
                  {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '1'}
              )
          ],
          [],
          '1',
      ),
      (
          [],
          [
              dicom_test_util.create_mock_dpas_generated_dicom_fref(
                  {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '2'}
              )
          ],
          '2',
      ),
      (
          [
              dicom_test_util.create_mock_dpas_generated_dicom_fref(
                  {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '3'}
              )
          ],
          [
              dicom_test_util.create_mock_dpas_generated_dicom_fref(
                  {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '4'}
              )
          ],
          '3',
      ),
  ])
  def test_upload_store_results_slide_series_instance_uid_in_dicom_store(
      self, ingested, prev_ingest, expected_series_instance_uid
  ):
    results = dicom_store_client.UploadSlideToDicomStoreResults(
        ingested, prev_ingest
    )
    self.assertEqual(
        results.slide_series_instance_uid_in_dicom_store,
        expected_series_instance_uid,
    )

  def test_get_existing_dicom_seriesuid(self):
    fref_list = [dicom_test_util.create_mock_non_dpas_generated_wsi_fref()]
    self.assertEqual(
        dicom_store_client._get_existing_dicom_seriesuid(fref_list), '1.2.3.4'
    )

  def test_get_existing_dicom_seriesuid_empty_wsidcmref_list(self):
    self.assertIsNone(dicom_store_client._get_existing_dicom_seriesuid([]))

  def test_http_get(self):
    """Tests HTTP Get."""
    # Unit test is not compatible with Forge.
    # Due to network access requirement. Test is NOP on Forge.
    if flag_utils.env_value_to_bool('UNITTEST_ON_FORGE'):
      cloud_logging_client.info(
          'Test_http_get is disabled Forge always passes. Run locally to test.'
      )
      return
    path = self.create_tempdir()
    tempfile = os.path.join(path, 'website_text.txt')
    self.assertIsNone(
        dicom_store_client._http_streaming_get(
            'https://www.google.com', {}, tempfile
        )
    )
    self.assertGreater(os.path.getsize(tempfile), 0)

  @parameterized.named_parameters([
      dict(
          testcase_name='mock_dpas_gen_ref',
          wsi_fref=dicom_test_util.create_mock_dpas_generated_dicom_fref(),
          expected_result=True,
      ),
      dict(
          testcase_name='adding_hash_and_uid_prefix_to_dicom_makes_it_dpas',
          wsi_fref=dicom_test_util.create_mock_non_dpas_generated_wsi_fref({
              ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: 'ZQRXL',
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
                  f'{ingest_const.DPAS_UID_PREFIX}.3'
              ),
          }),
          expected_result=True,
      ),
      dict(
          testcase_name='adding_uid_prefix_to_dicom_makes_it_dpas',
          wsi_fref=dicom_test_util.create_mock_non_dpas_generated_wsi_fref({
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
                  f'{ingest_const.DPAS_UID_PREFIX}.3'
              )
          }),
          expected_result=True,
      ),
      dict(
          testcase_name='adding_hash_to_dicom_makes_it_dpas',
          wsi_fref=dicom_test_util.create_mock_non_dpas_generated_wsi_fref(
              {ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: 'ZQRXL'}
          ),
          expected_result=True,
      ),
      dict(
          testcase_name='non_dpas_dicom',
          wsi_fref=dicom_test_util.create_mock_non_dpas_generated_wsi_fref(),
          expected_result=False,
      ),
  ])
  def test_is_dpas_generated_dicom(self, wsi_fref, expected_result):
    self.assertEqual(
        dicom_store_client._is_dpas_generated_dicom(wsi_fref), expected_result
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='hash_tag_differs',
          wsi_fref_1=dicom_test_util.create_mock_dpas_generated_dicom_fref(
              {ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: 'ABC'}
          ),
          wsi_fref_2=dicom_test_util.create_mock_dpas_generated_dicom_fref(
              {ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: 'ZQRXL'}
          ),
          dpas_dicom_wsidcmref_in_list_result=False,
      ),
      dict(
          testcase_name='test_mock_dpas_generated_dicom_are_same',
          wsi_fref_1=dicom_test_util.create_mock_dpas_generated_dicom_fref(),
          wsi_fref_2=dicom_test_util.create_mock_dpas_generated_dicom_fref(),
          dpas_dicom_wsidcmref_in_list_result=True,
      ),
      dict(
          testcase_name='test_mock_non_dpas_generated_dicom_are_same',
          wsi_fref_1=dicom_test_util.create_mock_non_dpas_generated_wsi_fref(),
          wsi_fref_2=dicom_test_util.create_mock_non_dpas_generated_wsi_fref(),
          dpas_dicom_wsidcmref_in_list_result=True,
      ),
      dict(
          testcase_name='study_instance_uid_ignored',
          wsi_fref_1=dicom_test_util.create_mock_non_dpas_generated_wsi_fref(),
          wsi_fref_2=dicom_test_util.create_mock_non_dpas_generated_wsi_fref(
              {ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: '5.6.7'}
          ),
          dpas_dicom_wsidcmref_in_list_result=False,
      ),
      dict(
          testcase_name='dpas_dicom_and_non_dpas_dicom_differ',
          wsi_fref_1=dicom_test_util.create_mock_non_dpas_generated_wsi_fref(),
          wsi_fref_2=dicom_test_util.create_mock_dpas_generated_dicom_fref(),
          dpas_dicom_wsidcmref_in_list_result=False,
      ),
      dict(
          testcase_name='ignore_sop_instance_uid',
          wsi_fref_1=dicom_test_util.create_mock_dpas_generated_dicom_fref({
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
                  f'{ingest_const.DPAS_UID_PREFIX}.1'
              )
          }),
          wsi_fref_2=dicom_test_util.create_mock_dpas_generated_dicom_fref({
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
                  f'{ingest_const.DPAS_UID_PREFIX}.2'
              )
          }),
          dpas_dicom_wsidcmref_in_list_result=True,
      ),
      dict(
          testcase_name='one_hash_missing',
          wsi_fref_1=dicom_test_util.create_mock_dpas_generated_dicom_fref(),
          wsi_fref_2=dicom_test_util.create_mock_dpas_generated_dicom_fref({
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: '1.2.3',
              ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: '',
          }),
          dpas_dicom_wsidcmref_in_list_result=True,
      ),
      dict(
          testcase_name='both_hash_missing',
          wsi_fref_1=dicom_test_util.create_mock_dpas_generated_dicom_fref({
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: '1.2.4',
              ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: '',
          }),
          wsi_fref_2=dicom_test_util.create_mock_dpas_generated_dicom_fref({
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: '1.2.3',
              ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: '',
          }),
          dpas_dicom_wsidcmref_in_list_result=True,
      ),
      dict(
          testcase_name='different_image_types',
          wsi_fref_1=dicom_test_util.create_mock_dpas_generated_dicom_fref({
              ingest_const.DICOMTagKeywords.IMAGE_TYPE: (
                  ingest_const.ORIGINAL_PRIMARY_VOLUME_RESAMPLED
              ),
          }),
          wsi_fref_2=dicom_test_util.create_mock_dpas_generated_dicom_fref({
              ingest_const.DICOMTagKeywords.IMAGE_TYPE: (
                  ingest_const.ORIGINAL_PRIMARY_VOLUME
              ),
          }),
          dpas_dicom_wsidcmref_in_list_result=False,
      ),
  ])
  def test_is_dpas_dicom_wsidcmref_in_list_base(
      self,
      wsi_fref_1,
      wsi_fref_2,
      dpas_dicom_wsidcmref_in_list_result,
  ):
    """Tests _is_wsidcmref_in_list and _is_dpas_dicom_wsidcmref_in_list."""
    result_1 = dicom_store_client.is_dpas_dicom_wsidcmref_in_list(
        wsi_fref_1, [wsi_fref_2]
    )
    result_2 = dicom_store_client.is_dpas_dicom_wsidcmref_in_list(
        wsi_fref_2, [wsi_fref_1]
    )
    # test comparison direction does not matter.
    self.assertEqual(result_1, result_2)
    self.assertEqual(result_1, dpas_dicom_wsidcmref_in_list_result)

  def test_is_dpas_dicom_wsidcmref_in_list_ignores_defined_tags(self):
    """Tests _is_wsidcmref_in_list and _is_dpas_dicom_wsidcmref_in_list."""
    wsi_fref_1 = dicom_test_util.create_mock_dpas_generated_dicom_fref({
        ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: '1.2.3.4',
        ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
            f'{ingest_const.DPAS_UID_PREFIX}.4'
        ),
        ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: '',
    })
    wsi_fref_2 = dicom_test_util.create_mock_dpas_generated_dicom_fref({
        ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: '1.2.3.5',
        ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
            f'{ingest_const.DPAS_UID_PREFIX}.3'
        ),
        ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: '',
    })
    ignore_tags = [ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID]
    result_1 = dicom_store_client.is_dpas_dicom_wsidcmref_in_list(
        wsi_fref_1, [wsi_fref_2], ignore_tags=ignore_tags
    )
    result_2 = dicom_store_client.is_dpas_dicom_wsidcmref_in_list(
        wsi_fref_2, [wsi_fref_1], ignore_tags=ignore_tags
    )
    # test comparison direction does not matter.
    self.assertEqual(result_1, result_2)
    self.assertTrue(result_1)

  def test_is_dpas_dicom_wsidcmref_in_list_ignore_hash(self):
    """Tests _is_wsidcmref_in_list and _is_dpas_dicom_wsidcmref_in_list."""
    wsi_fref_1 = dicom_test_util.create_mock_dpas_generated_dicom_fref(
        {ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: 'ABC'}
    )
    wsi_fref_2 = dicom_test_util.create_mock_dpas_generated_dicom_fref(
        {ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: 'ZQRXL'}
    )

    result_1 = dicom_store_client.is_dpas_dicom_wsidcmref_in_list(
        wsi_fref_1, [wsi_fref_2], ignore_source_image_hash_tag=True
    )
    result_2 = dicom_store_client.is_dpas_dicom_wsidcmref_in_list(
        wsi_fref_2, [wsi_fref_1], ignore_source_image_hash_tag=True
    )
    # test comparison direction does not matter.
    self.assertEqual(result_1, result_2)
    self.assertTrue(result_1)

  @parameterized.named_parameters([
      dict(
          testcase_name='hash_tag_differs',
          wsi_fref_1=dicom_test_util.create_mock_dpas_generated_dicom_fref(
              {ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: 'ABC'}
          ),
          wsi_fref_2=dicom_test_util.create_mock_dpas_generated_dicom_fref(
              {ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: 'ZQRXL'}
          ),
          wsidcmref_in_list_result=False,
      ),
      dict(
          testcase_name='test_mock_dpas_generated_dicom_are_same',
          wsi_fref_1=dicom_test_util.create_mock_dpas_generated_dicom_fref(),
          wsi_fref_2=dicom_test_util.create_mock_dpas_generated_dicom_fref(),
          wsidcmref_in_list_result=True,
      ),
      dict(
          testcase_name='test_mock_non_dpas_generated_dicom_are_same',
          wsi_fref_1=dicom_test_util.create_mock_non_dpas_generated_wsi_fref(),
          wsi_fref_2=dicom_test_util.create_mock_non_dpas_generated_wsi_fref(),
          wsidcmref_in_list_result=True,
      ),
      dict(
          testcase_name='study_instance_uid_different',
          wsi_fref_1=dicom_test_util.create_mock_non_dpas_generated_wsi_fref(),
          wsi_fref_2=dicom_test_util.create_mock_non_dpas_generated_wsi_fref(
              {ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: '5.6.7'}
          ),
          wsidcmref_in_list_result=False,
      ),
      dict(
          testcase_name='series_instance_uid_different',
          wsi_fref_1=dicom_test_util.create_mock_non_dpas_generated_wsi_fref(),
          wsi_fref_2=dicom_test_util.create_mock_non_dpas_generated_wsi_fref(
              {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '5.6.7'}
          ),
          wsidcmref_in_list_result=False,
      ),
      dict(
          testcase_name='series_instance_uid_different_dpas_dicom_different',
          wsi_fref_1=dicom_test_util.create_mock_dpas_generated_dicom_fref(),
          wsi_fref_2=dicom_test_util.create_mock_dpas_generated_dicom_fref(
              {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '5.6.7'}
          ),
          wsidcmref_in_list_result=False,
      ),
      dict(
          testcase_name='series_instance_uid_different_non_dpas_dicom_diff',
          wsi_fref_1=dicom_test_util.create_mock_non_dpas_generated_wsi_fref(),
          wsi_fref_2=dicom_test_util.create_mock_non_dpas_generated_wsi_fref(
              {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '5.6.7'}
          ),
          wsidcmref_in_list_result=False,
      ),
      dict(
          testcase_name='dpas_dicom_and_non_dpas_dicom_differ',
          wsi_fref_1=dicom_test_util.create_mock_non_dpas_generated_wsi_fref(),
          wsi_fref_2=dicom_test_util.create_mock_dpas_generated_dicom_fref(),
          wsidcmref_in_list_result=False,
      ),
      dict(
          testcase_name='dpas_dicom_tests_ignore_sop_instance_uid',
          wsi_fref_1=dicom_test_util.create_mock_dpas_generated_dicom_fref({
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
                  f'{ingest_const.DPAS_UID_PREFIX}.1'
              )
          }),
          wsi_fref_2=dicom_test_util.create_mock_dpas_generated_dicom_fref({
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
                  f'{ingest_const.DPAS_UID_PREFIX}.2'
              )
          }),
          wsidcmref_in_list_result=True,
      ),
      dict(
          testcase_name='dpas_dicom_test_does_not_test_hash_if_one_missing',
          wsi_fref_1=dicom_test_util.create_mock_dpas_generated_dicom_fref(),
          wsi_fref_2=dicom_test_util.create_mock_dpas_generated_dicom_fref({
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: '1.2.3',
              ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: '',
          }),
          wsidcmref_in_list_result=True,
      ),
  ])
  def test_is_dicomref_in_is_wsidcmref(
      self,
      wsi_fref_1,
      wsi_fref_2,
      wsidcmref_in_list_result,
  ):
    """Tests _is_wsidcmref_in_list and _is_dpas_dicom_wsidcmref_in_list."""
    result_1 = dicom_store_client.is_wsidcmref_in_list(wsi_fref_1, [wsi_fref_2])
    result_2 = dicom_store_client.is_wsidcmref_in_list(wsi_fref_2, [wsi_fref_1])
    # test comparison direction does not matter.
    self.assertEqual(result_1, result_2)
    self.assertEqual(result_1, wsidcmref_in_list_result)

  @parameterized.parameters([
      ('ORIGINAL\\PRIMARY\\LABEL\\None', 'ORIGINAL\\PRIMARY\\LABEL'),
      ('THUMBNAIL', 'THUMBNAIL'),
      ('LABEL', 'LABEL\\RESAMPLED'),
      ('ORIGINAL\\OVERVIEW', 'OVERVIEW\\RESAMPLED'),
      (
          'ORIGINAL\\PRIMARY\\VOLUME\\RESAMPLED',
          'ORIGINAL\\PRIMARY\\VOLUME\\RESAMPLED',
      ),
      (
          'ORIGINAL\\PRIMARY\\VOLUME\\RESAMPLED',
          'DERIVED\\PRIMARY\\VOLUME\\RESAMPLED',
      ),
      (
          'ORIGINAL\\PRIMARY\\VOLUME',
          'DERIVED\\PRIMARY\\VOLUME',
      ),
      (
          'DERIVED\\PRIMARY\\VOLUME\\NONE',
          'ORIGINAL\\PRIMARY\\VOLUME',
      ),
      (
          'DERIVED\\PRIMARY\\VOLUME',
          'ORIGINAL\\PRIMARY\\VOLUME\\NONE',
      ),
  ])
  def test_do_image_types_match_true(self, im_type1, im_type2):
    sop_class_uid = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    self.assertTrue(
        dicom_store_client._do_image_types_match(
            dicom_test_util.create_mock_non_dpas_generated_wsi_fref({
                ingest_const.DICOMTagKeywords.SOP_CLASS_UID: sop_class_uid,
                ingest_const.DICOMTagKeywords.IMAGE_TYPE: im_type1,
            }),
            dicom_test_util.create_mock_dpas_generated_dicom_fref({
                ingest_const.DICOMTagKeywords.SOP_CLASS_UID: sop_class_uid,
                ingest_const.DICOMTagKeywords.IMAGE_TYPE: im_type2,
            }),
        )
    )

  @parameterized.parameters([
      ('LABEL', 'THUMBNAIL'),
      ('ORIGINAL\\PRIMARY\\LABEL\\NONE', 'DERIVED\\PRIMARY\\OVERVIEW'),
      ('ORIGINAL\\PRIMARY\\THUMBNAIL\\NONE', 'ORIGINAL\\PRIMARY\\LABEL'),
      (
          'ORIGINAL\\PRIMARY\\VOLUME\\NONE',
          'ORIGINAL\\PRIMARY\\VOLUME\\RESAMPLED',
      ),
      (
          'DERIVED\\PRIMARY\\VOLUME\\NONE',
          'ORIGINAL\\PRIMARY\\VOLUME\\RESAMPLED',
      ),
  ])
  def test_do_image_types_match_false(self, im_type1, im_type2):
    sop_class_uid = ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
    self.assertFalse(
        dicom_store_client._do_image_types_match(
            dicom_test_util.create_mock_non_dpas_generated_wsi_fref({
                ingest_const.DICOMTagKeywords.SOP_CLASS_UID: sop_class_uid,
                ingest_const.DICOMTagKeywords.IMAGE_TYPE: im_type1,
            }),
            dicom_test_util.create_mock_dpas_generated_dicom_fref({
                ingest_const.DICOMTagKeywords.SOP_CLASS_UID: sop_class_uid,
                ingest_const.DICOMTagKeywords.IMAGE_TYPE: im_type2,
            }),
        )
    )

  @parameterized.parameters([
      ('ABC\\NONE', 'ABC', True),
      ('ABC', 'ABC', True),
      ('ABC\\EFG', 'ABC', False),
      ('ABC', 'EFG', False),
  ])
  def test_imagetype_(self, im_type1, im_type2, expected):
    sop_class_uid = '1.2.3'
    self.assertEqual(
        dicom_store_client._do_image_types_match(
            dicom_test_util.create_mock_non_dpas_generated_wsi_fref({
                ingest_const.DICOMTagKeywords.SOP_CLASS_UID: sop_class_uid,
                ingest_const.DICOMTagKeywords.IMAGE_TYPE: im_type1,
            }),
            dicom_test_util.create_mock_non_dpas_generated_wsi_fref({
                ingest_const.DICOMTagKeywords.SOP_CLASS_UID: sop_class_uid,
                ingest_const.DICOMTagKeywords.IMAGE_TYPE: im_type2,
            }),
        ),
        expected,
    )

  @parameterized.parameters([
      (ingest_const.THUMBNAIL, ingest_const.THUMBNAIL, True),
      (ingest_const.OVERVIEW, ingest_const.OVERVIEW, True),
      (ingest_const.LABEL, ingest_const.LABEL, True),
      (ingest_const.LABEL, ingest_const.OVERVIEW, False),
      (ingest_const.THUMBNAIL, ingest_const.ORIGINAL_PRIMARY_VOLUME, False),
  ])
  def test_is_wsi_ancillary_image_type(self, img1, img2, expected):
    img1 = dicom_test_util.create_mock_non_dpas_generated_wsi_fref({
        ingest_const.DICOMTagKeywords.IMAGE_TYPE: img1,
        ingest_const.DICOMTagKeywords.SOP_CLASS_UID: (
            ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
        ),
    })
    img2 = dicom_test_util.create_mock_non_dpas_generated_wsi_fref({
        ingest_const.DICOMTagKeywords.IMAGE_TYPE: img2,
        ingest_const.DICOMTagKeywords.SOP_CLASS_UID: (
            ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
        ),
    })
    self.assertEqual(
        dicom_store_client._is_wsi_ancillary_image_type(img1, img2), expected
    )

  @parameterized.parameters([
      (
          ingest_const.DicomSopClasses.SECONDARY_CAPTURE_IMAGE.uid,
          ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid,
      ),
      (
          ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid,
          ingest_const.DicomSopClasses.SECONDARY_CAPTURE_IMAGE.uid,
      ),
  ])
  def test_is_wsi_ancillary_image_type_returns_false__wrong_iod(
      self, iod1, iod2
  ):
    img1 = dicom_test_util.create_mock_non_dpas_generated_wsi_fref({
        ingest_const.DICOMTagKeywords.IMAGE_TYPE: ingest_const.THUMBNAIL,
        ingest_const.DICOMTagKeywords.SOP_CLASS_UID: iod1,
    })
    img2 = dicom_test_util.create_mock_non_dpas_generated_wsi_fref({
        ingest_const.DICOMTagKeywords.IMAGE_TYPE: ingest_const.THUMBNAIL,
        ingest_const.DICOMTagKeywords.SOP_CLASS_UID: iod2,
    })
    self.assertFalse(
        dicom_store_client._is_wsi_ancillary_image_type(img1, img2)
    )

  @parameterized.parameters([
      (ingest_const.THUMBNAIL, False),
      (ingest_const.OVERVIEW, False),
      (ingest_const.LABEL, False),
      (ingest_const.ORIGINAL_PRIMARY_VOLUME, True),
      (ingest_const.DERIVED_PRIMARY_VOLUME, True),
      (f'{ingest_const.DERIVED_PRIMARY_VOLUME}\\{ingest_const.NONE}', True),
      (
          f'{ingest_const.DERIVED_PRIMARY_VOLUME}\\{ingest_const.RESAMPLED}',
          True,
      ),
  ])
  def test_is_original_or_derived(self, val, expected):
    self.assertEqual(dicom_store_client._is_original_or_derived(val), expected)

  @parameterized.parameters([
      (ingest_const.THUMBNAIL, False),
      (ingest_const.OVERVIEW, False),
      (ingest_const.LABEL, False),
      (ingest_const.ORIGINAL_PRIMARY_VOLUME, False),
      (ingest_const.DERIVED_PRIMARY_VOLUME, False),
      (f'{ingest_const.DERIVED_PRIMARY_VOLUME}\\{ingest_const.NONE}', False),
      (
          f'{ingest_const.DERIVED_PRIMARY_VOLUME}\\{ingest_const.RESAMPLED}',
          True,
      ),
  ])
  def test_is_resampled_image_type(self, val, expected):
    self.assertEqual(dicom_store_client._is_resampled_image_type(val), expected)

  @parameterized.named_parameters([
      dict(
          testcase_name='both_instances_lack_dim_metadata',
          dcm_file=_dicom_ref_dim_metadata(),
          existing_dcm=_dicom_ref_dim_metadata(),
          expected=True,
      ),
      dict(
          testcase_name='image_one_has_dim_second_image_does_not',
          dcm_file=_dicom_ref_dim_metadata('10.5', '5.5'),
          existing_dcm=_dicom_ref_dim_metadata(),
          expected=False,
      ),
      dict(
          testcase_name='images_have_same_characters_in_physical_dim',
          dcm_file=_dicom_ref_dim_metadata('ABC', 'EFG'),
          existing_dcm=_dicom_ref_dim_metadata('ABC', 'EFG'),
          expected=True,
      ),
      dict(
          testcase_name='images_have_same_physical_dim',
          dcm_file=_dicom_ref_dim_metadata(
              '36.01760482788086', '7.364952087402344'
          ),
          existing_dcm=_dicom_ref_dim_metadata(
              '36.01760482798086', '7.3512492179870605'
          ),
          expected=True,
      ),
      dict(
          testcase_name='images_have_different_physical_dim',
          dcm_file=_dicom_ref_dim_metadata(
              '36.01760482788086', '7.364952087402344'
          ),
          existing_dcm=_dicom_ref_dim_metadata('36.1', '7.3512492179870605'),
          expected=False,
      ),
      dict(
          testcase_name='images_have_same_ancillary_type_dimension_is_ignored',
          dcm_file=_dicom_ref_dim_metadata('5', '7', ingest_const.THUMBNAIL),
          existing_dcm=_dicom_ref_dim_metadata(
              '1', '1', ingest_const.THUMBNAIL
          ),
          expected=True,
      ),
      dict(
          testcase_name='images_do_not_have_matching_ancillary_type_dimension_is_not_ignored',
          dcm_file=_dicom_ref_dim_metadata('5', '7', ingest_const.THUMBNAIL),
          existing_dcm=_dicom_ref_dim_metadata('1', '1', ingest_const.OVERVIEW),
          expected=False,
      ),
  ])
  def test_do_images_have_similar_dimensions(
      self, dcm_file, existing_dcm, expected
  ):
    self.assertEqual(
        dicom_store_client._do_images_have_similar_dimensions(
            dcm_file, existing_dcm
        ),
        expected,
    )

  @parameterized.parameters(
      [http.HTTPStatus.NOT_FOUND, http.HTTPStatus.TOO_MANY_REQUESTS]
  )
  @requests_mock.mock()
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_download_instance_from_uri_fails(self, error_code, mock_requests, _):
    uri = 'http://healthcare/mock'
    dicom_file_path = os.path.join(self.create_tempdir(), 'test.dcm')
    mock_response = mock.Mock()
    mock_response.status_code = error_code
    http_error = requests.HTTPError(
        f'{error_code} error', response=mock_response
    )
    mock_requests.get(uri, exc=http_error)

    client = dicom_store_client.DicomStoreClient(_DICOM_WEBPATH)
    with self.assertRaises(requests.HTTPError):
      client.download_instance_from_uri(uri, dicom_file_path)

  @requests_mock.mock()
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_download_instance_from_uri_succeeds(self, mock_requests, _):
    uri = 'http://healthcare/mock'
    dicom_file_path = os.path.join(self.create_tempdir(), 'test.dcm')
    with open(self._dcm_path, 'rb') as f:
      dcm_data = f.read()
    mock_requests.get(uri, content=dcm_data)

    client = dicom_store_client.DicomStoreClient(_DICOM_WEBPATH)
    client.download_instance_from_uri(uri, dicom_file_path)
    with open(dicom_file_path, 'rb') as f:
      self.assertEqual(f.read(), dcm_data)

  @requests_mock.mock()
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_download_instance_succeeds(self, mock_requests, _):
    url = 'http://healthcare/mock/dicomWeb/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5'
    dicom_file_path = os.path.join(self.create_tempdir(), 'test.dcm')
    with open(self._dcm_path, 'rb') as f:
      dcm_data = f.read()
    mock_requests.get(url, content=dcm_data)

    client = dicom_store_client.DicomStoreClient(_DICOM_WEBPATH)
    client.download_instance(
        study_uid='1.2.3',
        series_uid='1.2.3.4',
        sop_instance_uid='1.2.3.4.5',
        dicom_file_path=dicom_file_path,
    )
    with open(dicom_file_path, 'rb') as f:
      self.assertEqual(f.read(), dcm_data)

  @parameterized.parameters(
      [http.HTTPStatus.NOT_FOUND, http.HTTPStatus.TOO_MANY_REQUESTS]
  )
  @requests_mock.mock()
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_delete_resource_from_dicom_store_fails(
      self, error_code, mock_requests, _
  ):
    uri = 'http://healthcare/mock'
    mock_response = mock.Mock()
    mock_response.status_code = error_code
    http_error = requests.HTTPError(
        f'{error_code} error', response=mock_response
    )
    mock_requests.delete(uri, exc=http_error)

    client = dicom_store_client.DicomStoreClient(_DICOM_WEBPATH)
    self.assertFalse(client.delete_resource_from_dicom_store(uri))

  @requests_mock.mock()
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_delete_resource_from_dicom_store_succeeds(self, mock_requests, _):
    uri = 'http://healthcare/mock'
    mock_requests.delete(uri)

    client = dicom_store_client.DicomStoreClient(_DICOM_WEBPATH)
    self.assertTrue(client.delete_resource_from_dicom_store(uri))

  @requests_mock.mock()
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_delete_series_from_dicom_store_succeeds(self, mock_requests, _):
    url = 'http://healthcare/mock/dicomWeb/studies/1.2.3/series/1.2.3.4'
    mock_requests.delete(url)

    client = dicom_store_client.DicomStoreClient(_DICOM_WEBPATH)
    self.assertTrue(
        client.delete_series_from_dicom_store(
            study_uid='1.2.3', series_uid='1.2.3.4'
        )
    )

  @parameterized.parameters(['', None])
  def test_set_dicom_series_uid_undefined_series(self, series_uid_val):
    dicom_list = []
    for idx in range(3):
      dicom_list.append(
          dicom_test_util.create_test_dicom_instance(
              temp_dir=self.create_tempdir(),
              study='1.2.3',
              series=f'1.2.3.{idx}',
              sop_instance_uid=f'1.2.3.4.{idx}',
          )
      )
    self.assertFalse(
        dicom_store_client.set_dicom_series_uid(dicom_list, series_uid_val)
    )
    for idx, dcm_file in enumerate(dicom_list):
      with pydicom.dcmread(dcm_file) as dcm:
        self.assertEqual(dcm.SeriesInstanceUID, f'1.2.3.{idx}')

  def test_set_dicom_series_uid_series_matches(self):
    series_uid_val = '1.2.3.5'
    dicom_list = []
    for idx in range(3):
      dicom_list.append(
          dicom_test_util.create_test_dicom_instance(
              temp_dir=self.create_tempdir().full_path,
              study='1.2.3',
              series=series_uid_val,
              sop_instance_uid=f'1.2.3.4.{idx}',
          )
      )
    self.assertFalse(
        dicom_store_client.set_dicom_series_uid(dicom_list, series_uid_val)
    )
    for dcm_file in dicom_list:
      with pydicom.dcmread(dcm_file) as dcm:
        self.assertEqual(dcm.SeriesInstanceUID, series_uid_val)

  def test_set_dicom_series_uid_series_changes(self):
    series_uid_val = '1.2.99'
    dicom_list = []
    for idx in range(3):
      dicom_list.append(
          dicom_test_util.create_test_dicom_instance(
              temp_dir=self.create_tempdir().full_path,
              study='1.2.3',
              series=f'1.2.3.{idx}',
              sop_instance_uid=f'1.2.3.4.{idx}',
          )
      )
    self.assertTrue(
        dicom_store_client.set_dicom_series_uid(dicom_list, series_uid_val)
    )
    for dcm_file in dicom_list:
      with pydicom.dcmread(dcm_file) as dcm:
        self.assertEqual(dcm.SeriesInstanceUID, series_uid_val)

  @parameterized.parameters(
      [http.HTTPStatus.NOT_FOUND, http.HTTPStatus.TOO_MANY_REQUESTS]
  )
  @requests_mock.mock()
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_upload_single_instance_to_dicom_store_fails(
      self, error_code, mock_requests, _
  ):
    dcm_path = dicom_test_util.create_test_dicom_instance(self.create_tempdir())
    mock_response = mock.Mock()
    mock_response.status_code = error_code
    http_error = requests.HTTPError(
        f'{error_code} error', response=mock_response
    )
    mock_requests.post(
        'http://healthcare/mock/dicomWeb/studies', exc=http_error
    )

    client = dicom_store_client.DicomStoreClient(_DICOM_WEBPATH)
    with self.assertRaises(requests.HTTPError):
      _ = client._upload_single_instance_to_dicom_store(dcm_path)

  @requests_mock.mock()
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_upload_single_instance_to_dicom_store_is_duplicate_succeeds(
      self, mock_requests, _
  ):
    dcm_path = dicom_test_util.create_test_dicom_instance(self.create_tempdir())
    mock_response = mock.Mock()
    mock_response.status_code = 409
    http_error = requests.HTTPError('409 error', response=mock_response)
    mock_requests.post(
        'http://healthcare/mock/dicomWeb/studies', exc=http_error
    )

    client = dicom_store_client.DicomStoreClient(_DICOM_WEBPATH)
    dcm_save_result = client._upload_single_instance_to_dicom_store(dcm_path)
    self.assertTrue(dcm_save_result.is_duplicate)

  @requests_mock.mock()
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_upload_single_instance_to_dicom_store_existing_is_duplicate_succeeds(
      self, mock_requests, _
  ):
    dcm_path = dicom_test_util.create_test_dicom_instance(self.create_tempdir())
    existing_dicoms = [
        wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_file(
            dicom_test_util.create_test_dicom_instance(
                self.create_tempdir(), series='4.5.6'
            )
        ),
        wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_file(
            dicom_test_util.create_test_dicom_instance(self.create_tempdir())
        ),
    ]
    mock_requests.post('http://healthcare/mock/dicomWeb/studies')

    client = dicom_store_client.DicomStoreClient(_DICOM_WEBPATH)
    dcm_save_result = client._upload_single_instance_to_dicom_store(
        dcm_path, existing_dicoms
    )
    self.assertTrue(dcm_save_result.is_duplicate)

  @requests_mock.mock()
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_upload_single_instance_to_dicom_store_no_existing_succeeds(
      self, mock_requests, _
  ):
    dcm_path = dicom_test_util.create_test_dicom_instance(self.create_tempdir())
    mock_requests.post('http://healthcare/mock/dicomWeb/studies')

    client = dicom_store_client.DicomStoreClient(_DICOM_WEBPATH)
    dcm_save_result = client._upload_single_instance_to_dicom_store(dcm_path)
    self.assertFalse(dcm_save_result.is_duplicate)

  @requests_mock.mock()
  @mock.patch(
      'google.auth.default',
      autospec=True,
      return_value=(mock.Mock(), _PROJECT_ID),
  )
  def test_upload_single_instance_to_dicom_store_existing_not_duplicate_succeeds(
      self, mock_requests, _
  ):
    dcm_path = dicom_test_util.create_test_dicom_instance(self.create_tempdir())
    existing_dicoms = [
        wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_file(
            dicom_test_util.create_test_dicom_instance(
                self.create_tempdir(), series='4.5.6'
            )
        ),
        wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_file(
            dicom_test_util.create_test_dicom_instance(
                self.create_tempdir(), series='7.8.9'
            )
        ),
    ]
    mock_requests.post('http://healthcare/mock/dicomWeb/studies')

    client = dicom_store_client.DicomStoreClient(_DICOM_WEBPATH)
    dcm_save_result = client._upload_single_instance_to_dicom_store(
        dcm_path, existing_dicoms
    )
    self.assertFalse(dcm_save_result.is_duplicate)

  @parameterized.parameters([
      dicom_store_client.DiscoverExistingSeriesOptions.IGNORE,
      dicom_store_client.DiscoverExistingSeriesOptions.USE_HASH,
      dicom_store_client.DiscoverExistingSeriesOptions.USE_STUDY_AND_SERIES,
  ])
  def test_upload_to_dicom_store_no_files(
      self, discover_existing_series_option
  ):
    client = dicom_store_client.DicomStoreClient(_DICOM_WEBPATH)
    result = client.upload_to_dicom_store([], discover_existing_series_option)
    self.assertEqual(result.ingested, [])
    self.assertEqual(result.previously_ingested, [])

  @parameterized.parameters([True, False])
  @mock.patch.object(
      dicom_store_client.DicomStoreClient, '_add_auth_to_header', autospec=True
  )
  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      '_upload_single_instance_to_dicom_store',
  )
  @mock.patch.object(
      dicom_store_client.DicomStoreClient,
      '_copy_dicom_to_bucket',
      autospec=True,
  )
  def test_upload_to_dicom_store_copy_to_bucket_enabled(
      self,
      copy_to_bucket_enabled,
      mk_copy_to_bucket,
      mk_upload,
      unused_mk_add_auth,
  ):
    client = dicom_store_client.DicomStoreClient(_DICOM_WEBPATH)
    client.upload_to_dicom_store(
        ['test.dcm'],
        dicom_store_client.DiscoverExistingSeriesOptions.IGNORE,
        copy_to_bucket_enabled=copy_to_bucket_enabled,
    )

    mk_upload.assert_called_once_with('test.dcm', mock.ANY)
    if copy_to_bucket_enabled:
      mk_copy_to_bucket.assert_called_once()
    else:
      mk_copy_to_bucket.assert_not_called()

  @parameterized.named_parameters([
      dict(
          testcase_name='accession_search',
          accession_number='a1',
          patient_id='',
          undefined_pid=False,
          expected={'1.1'},
      ),
      dict(
          testcase_name='patient_id_search_single',
          accession_number='',
          patient_id='p2',
          undefined_pid=False,
          expected={'1.3'},
      ),
      dict(
          testcase_name='patient_id_search_multiple',
          accession_number='',
          patient_id='p1',
          undefined_pid=False,
          expected={'1.1', '1.2'},
      ),
      dict(
          testcase_name='acession_and_patient_id_search',
          accession_number='a1',
          patient_id='p1',
          undefined_pid=False,
          expected={'1.1'},
      ),
      dict(
          testcase_name='not_found_accession',
          accession_number='a5',
          patient_id='p1',
          undefined_pid=False,
          expected=set(),
      ),
      dict(
          testcase_name='not_found_patient',
          accession_number='1.1',
          patient_id='p4',
          undefined_pid=False,
          expected=set(),
      ),
      dict(
          testcase_name='not_found_patient_and_accession',
          accession_number='a5',
          patient_id='p5',
          undefined_pid=False,
          expected=set(),
      ),
      dict(
          testcase_name='empty',
          accession_number='',
          patient_id='',
          undefined_pid=False,
          expected=set(),
      ),
      dict(
          testcase_name='studies_with_no_pateint_id',
          accession_number='a5',
          patient_id='',
          undefined_pid=True,
          expected={'1.5'},
      ),
      dict(
          testcase_name='studies_with_or_without_no_pateint_id',
          accession_number='a5',
          patient_id='',
          undefined_pid=False,
          expected={'1.5', '1.6'},
      ),
      dict(
          testcase_name='studies_with_pateint_id',
          accession_number='a5',
          patient_id='p4',
          undefined_pid=False,
          expected={'1.6'},
      ),
  ])
  def test_dicom_store_study_instance_uid_search(
      self,
      accession_number,
      patient_id,
      undefined_pid,
      expected,
  ) -> None:
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url) as mk_store:
      for ds in _create_mock_dataset():
        mk_store[mock_dicomweb_url].add_instance(ds)
      ds_client = dicom_store_client.DicomStoreClient(mock_dicomweb_url)
      self.assertEqual(
          set(
              ds_client.study_instance_uid_search(
                  accession_number=accession_number,
                  patient_id=patient_id,
                  find_studies_with_undefined_patient_id=undefined_pid,
              )
          ),
          expected,
      )

  @parameterized.named_parameters([
      dict(testcase_name='return_all', limit=None, expected_len=2),
      dict(testcase_name='limit_1', limit=1, expected_len=1),
      dict(testcase_name='limit_2', limit=2, expected_len=2),
  ])
  def test_dicom_store_study_instance_uid_search_limit(
      self, limit, expected_len
  ) -> None:
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url) as mk_store:
      for ds in _create_mock_dataset():
        mk_store[mock_dicomweb_url].add_instance(ds)
      ds_client = dicom_store_client.DicomStoreClient(mock_dicomweb_url)
      self.assertLen(
          ds_client.study_instance_uid_search(patient_id='p1', limit=limit),
          expected_len,
      )

  def test_dicom_store_study_instance_uid_search_invalid_limit_raises(
      self,
  ) -> None:
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url) as mk_store:
      for ds in _create_mock_dataset():
        mk_store[mock_dicomweb_url].add_instance(ds)
      ds_client = dicom_store_client.DicomStoreClient(mock_dicomweb_url)
      with self.assertRaises(ValueError):
        ds_client.study_instance_uid_search(patient_id='p1', limit=0)

  @parameterized.named_parameters([
      dict(
          testcase_name='missing_study_instance_uid_raises',
          http_status_code=http.HTTPStatus.OK,
          http_response='[{}]',
          expected_exception=dicom_store_client.StudyInstanceUIDSearchError,
      ),
      dict(
          testcase_name='json_reponse_raises',
          http_status_code=http.HTTPStatus.OK,
          http_response='{a:1}',
          expected_exception=dicom_store_client.StudyInstanceUIDSearchError,
      ),
      dict(
          testcase_name='httperror_reponse_raises',
          http_status_code=http.HTTPStatus.BAD_REQUEST,
          http_response='BadRequest',
          expected_exception=requests.HTTPError,
      ),
  ])
  def test_dicom_store_study_instance_invalid(
      self, http_status_code, http_response, expected_exception
  ):
    mock_response = dicom_store_mock.MockHttpResponse(
        r'/studies(\?.*)?',
        dicom_store_mock.RequestMethod.GET,
        http_status_code,
        http_response,
        dicom_store_mock.ContentType.APPLICATION_DICOM_JSON,
    )
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url) as mk_store:
      mk_store[mock_dicomweb_url].set_mock_response(mock_response)
      ds_client = dicom_store_client.DicomStoreClient(mock_dicomweb_url)
      with self.assertRaises(expected_exception):
        ds_client.study_instance_uid_search(patient_id='p1')

  @parameterized.named_parameters(
      dict(
          testcase_name='study_not_found',
          study_instance_uid='99',
          series_instance_uid=None,
          sop_instance_uid=None,
          exp_len=0,
      ),
      dict(
          testcase_name='study_search',
          study_instance_uid='1.2',
          series_instance_uid=None,
          sop_instance_uid=None,
          exp_len=3,
      ),
      dict(
          testcase_name='series_search_not_found',
          study_instance_uid='1.3',
          series_instance_uid='1.3.2',
          sop_instance_uid=None,
          exp_len=0,
      ),
      dict(
          testcase_name='series_search_found',
          study_instance_uid='1.3',
          series_instance_uid='1.3.1',
          sop_instance_uid=None,
          exp_len=3,
      ),
      dict(
          testcase_name='instance_search_found',
          study_instance_uid='1.4',
          series_instance_uid='1.4.1',
          sop_instance_uid='1.4.1.1',
          exp_len=1,
      ),
      dict(
          testcase_name='instance_search_not_found',
          study_instance_uid='1.4',
          series_instance_uid='1.4.1',
          sop_instance_uid='1.4.1.6',
          exp_len=0,
      ),
  )
  def test_get_instance_tags_json(
      self, study_instance_uid, series_instance_uid, sop_instance_uid, exp_len
  ):
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url) as mk_store:
      for ds in _create_mock_dataset():
        mk_store[mock_dicomweb_url].add_instance(ds)
      ds_client = dicom_store_client.DicomStoreClient(mock_dicomweb_url)
      results = ds_client.get_instance_tags_json(
          study_instance_uid, series_instance_uid, sop_instance_uid
      )
      self.assertLen(results, exp_len)

  @parameterized.named_parameters([
      dict(
          testcase_name='http',
          http_status_code=http.HTTPStatus.BAD_REQUEST,
          http_response='BadRequest',
          expected_exception=requests.HTTPError,
      ),
      dict(
          testcase_name='json',
          http_status_code=http.HTTPStatus.OK,
          http_response='{a:1}',
          expected_exception=json.JSONDecodeError,
      ),
  ])
  def test_get_instance_tags_bad_response_raise_error(
      self, http_status_code, http_response, expected_exception
  ):
    study_instance_uid = '1.1'
    series_instance_uid = '1.1.2'
    sop_instance_uid = '1.1.2.3'
    mock_response = dicom_store_mock.MockHttpResponse(
        r'/studies/1.1/series/1.1.2/instances\?SOPInstanceUID=1.1.2.3',
        dicom_store_mock.RequestMethod.GET,
        http_status_code,
        http_response,
        dicom_store_mock.ContentType.APPLICATION_DICOM_JSON,
    )
    mock_dicomweb_url = 'https://mock.dicomstore.com/dicomWeb'
    with dicom_store_mock.MockDicomStores(mock_dicomweb_url) as mk_store:
      mk_store[mock_dicomweb_url].set_mock_response(mock_response)
      ds_client = dicom_store_client.DicomStoreClient(mock_dicomweb_url)
      with self.assertRaises(expected_exception):
        ds_client.get_instance_tags_json(
            study_instance_uid, series_instance_uid, sop_instance_uid
        )


if __name__ == '__main__':
  absltest.main()
