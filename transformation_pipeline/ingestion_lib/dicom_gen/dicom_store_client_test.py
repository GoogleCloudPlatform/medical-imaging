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
    m1 = dicom_test_util.create_mock_dicom_fref(
        {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '1'}
    )
    m2 = dicom_test_util.create_mock_dicom_fref(
        {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '2'}
    )
    m3 = dicom_test_util.create_mock_dicom_fref(
        {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '3'}
    )
    m4 = dicom_test_util.create_mock_dicom_fref(
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
              dicom_test_util.create_mock_dicom_fref(
                  {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '1'}
              )
          ],
          [],
      ),
      (
          [],
          [
              dicom_test_util.create_mock_dicom_fref(
                  {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '2'}
              )
          ],
      ),
      (
          [
              dicom_test_util.create_mock_dicom_fref(
                  {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '3'}
              )
          ],
          [
              dicom_test_util.create_mock_dicom_fref(
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
              dicom_test_util.create_mock_dicom_fref(
                  {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '1'}
              )
          ],
          [],
          '1',
      ),
      (
          [],
          [
              dicom_test_util.create_mock_dicom_fref(
                  {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '2'}
              )
          ],
          '2',
      ),
      (
          [
              dicom_test_util.create_mock_dicom_fref(
                  {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: '3'}
              )
          ],
          [
              dicom_test_util.create_mock_dicom_fref(
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
    fref_list = [dicom_test_util.create_wsi_fref()]
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
      cloud_logging_client.logger().info(
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

  @parameterized.parameters([
      (dicom_test_util.create_mock_dicom_fref(), True),
      (
          dicom_test_util.create_wsi_fref({
              ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: 'ZQRXL',
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
                  f'{ingest_const.DPAS_UID_PREFIX}.3'
              ),
          }),
          True,
      ),
      (
          dicom_test_util.create_wsi_fref(
              {
                  ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
                      f'{ingest_const.DPAS_UID_PREFIX}.3'
                  )
              }
          ),
          True,
      ),
      (
          dicom_test_util.create_wsi_fref(
              {ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: 'ZQRXL'}
          ),
          True,
      ),
      (dicom_test_util.create_wsi_fref(), False),
  ])
  def test_is_dpas_generated_dicom(self, wsi_fref, expected_result):
    self.assertEqual(
        dicom_store_client._is_dpas_generated_dicom(wsi_fref), expected_result
    )

  def _is_wsidcmref_in_list_test_wrapper(
      self, wsi_ref1, wsi_ref2, ignore_source_image_hash=False
  ) -> bool:
    """Tests equality is by bidirectional and default and explicit are same."""
    result_1 = dicom_store_client.is_wsidcmref_in_list(
        wsi_ref1, [wsi_ref2], ignore_source_image_hash
    )
    result_2 = dicom_store_client.is_wsidcmref_in_list(
        wsi_ref2, [wsi_ref1], ignore_source_image_hash
    )
    # test comparison direction does not matter.
    self.assertEqual(result_1, result_2)
    if not ignore_source_image_hash:
      # test default parameter
      result_3 = dicom_store_client.is_wsidcmref_in_list(wsi_ref2, [wsi_ref1])
      self.assertEqual(result_1, result_3)
    return result_1

  def _is_dpas_dicom_wsidcmref_in_list_wrapper(
      self,
      wsi_ref1,
      wsi_ref2,
      ignore_source_image_hash=False,
      ignore_tag_list=None,
  ) -> bool:
    """Tests equality is by bidirectional and default and explicit are same."""
    result_1 = dicom_store_client.is_dpas_dicom_wsidcmref_in_list(
        wsi_ref1, [wsi_ref2], ignore_source_image_hash, ignore_tag_list
    )
    result_2 = dicom_store_client.is_dpas_dicom_wsidcmref_in_list(
        wsi_ref2, [wsi_ref1], ignore_source_image_hash, ignore_tag_list
    )
    # test comparison direction does not matter.
    self.assertEqual(result_1, result_2)
    if not ignore_source_image_hash:
      # test default parameter
      result_3 = dicom_store_client.is_dpas_dicom_wsidcmref_in_list(
          wsi_ref2, [wsi_ref1], ignore_tags=ignore_tag_list
      )
      self.assertEqual(result_1, result_3)
    return result_1

  @parameterized.parameters([
      (
          dicom_test_util.create_mock_dicom_fref(
              {ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: 'ABC'}
          ),
          dicom_test_util.create_mock_dicom_fref(
              {ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: 'ZQRXL'}
          ),
          False,
          False,
          False,
          None,
      ),
      (
          dicom_test_util.create_mock_dicom_fref(
              {ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: 'ABC'}
          ),
          dicom_test_util.create_mock_dicom_fref(
              {ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: 'ZQRXL'}
          ),
          True,
          True,
          True,
          None,
      ),
      (
          dicom_test_util.create_mock_dicom_fref(),
          dicom_test_util.create_mock_dicom_fref(),
          False,
          True,
          True,
          None,
      ),
      (
          dicom_test_util.create_wsi_fref(),
          dicom_test_util.create_wsi_fref(),
          False,
          True,
          True,
          None,
      ),
      (
          dicom_test_util.create_wsi_fref(),
          dicom_test_util.create_wsi_fref(
              {ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: '5.6.7'}
          ),
          False,
          False,
          False,
          None,
      ),
      (
          dicom_test_util.create_wsi_fref(),
          dicom_test_util.create_mock_dicom_fref(),
          False,
          False,
          False,
          None,
      ),
      (
          dicom_test_util.create_mock_dicom_fref(
              {
                  ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
                      f'{ingest_const.DPAS_UID_PREFIX}.1'
                  )
              }
          ),
          dicom_test_util.create_mock_dicom_fref(
              {
                  ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
                      f'{ingest_const.DPAS_UID_PREFIX}.2'
                  )
              }
          ),
          False,
          True,
          True,
          None,
      ),
      (
          dicom_test_util.create_mock_dicom_fref(),
          dicom_test_util.create_mock_dicom_fref({
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: '1.2.3',
              ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: '',
          }),
          False,
          True,
          True,
          None,
      ),
      (
          dicom_test_util.create_mock_dicom_fref({
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: '1.2.4',
              ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: '',
          }),
          dicom_test_util.create_mock_dicom_fref({
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: '1.2.3',
              ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: '',
          }),
          False,
          False,
          True,
          None,
      ),
      (
          dicom_test_util.create_mock_dicom_fref({
              ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: '1.2.3.4',
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
                  f'{ingest_const.DPAS_UID_PREFIX}.4'
              ),
              ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: '',
          }),
          dicom_test_util.create_mock_dicom_fref({
              ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: '1.2.3.5',
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
                  f'{ingest_const.DPAS_UID_PREFIX}.3'
              ),
              ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: '',
          }),
          False,
          False,
          False,
          None,
      ),
      (
          dicom_test_util.create_mock_dicom_fref({
              ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: '1.2.3.4',
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
                  f'{ingest_const.DPAS_UID_PREFIX}.4'
              ),
              ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: '',
          }),
          dicom_test_util.create_mock_dicom_fref({
              ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: '1.2.3.5',
              ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: (
                  f'{ingest_const.DPAS_UID_PREFIX}.3'
              ),
              ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG: '',
          }),
          False,
          False,
          True,
          [ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID],
      ),
  ])
  def test_is_dicomref_in_list(
      self,
      wsi_fref_1,
      wsi_fref_2,
      ignore_source_image_hash,
      wsidcmref_in_list_result,
      dpas_dicom_wsidcmref_in_list_result,
      ignore_tag_list,
  ):
    """Tests _is_wsidcmref_in_list and _is_dpas_dicom_wsidcmref_in_list."""
    self.assertEqual(
        (
            self._is_wsidcmref_in_list_test_wrapper(
                wsi_fref_1, wsi_fref_2, ignore_source_image_hash
            ),
            self._is_dpas_dicom_wsidcmref_in_list_wrapper(
                wsi_fref_1,
                wsi_fref_2,
                ignore_source_image_hash,
                ignore_tag_list,
            ),
        ),
        (wsidcmref_in_list_result, dpas_dicom_wsidcmref_in_list_result),
    )

  @parameterized.parameters([
      ('ORIGINAL\\PRIMARY\\LABEL\\None', 'ORIGINAL\\PRIMARY\\LABEL'),
      ('THUMBNAIL', 'THUMBNAIL'),
      (
          'ORIGINAL\\PRIMARY\\VOLUME\\RESAMPLED',
          'ORIGINAL\\PRIMARY\\VOLUME\\RESAMPLED',
      ),
  ])
  def test_imagetype_match(self, im_type1, im_type2):
    self.assertTrue(dicom_store_client._imagetype_match(im_type1, im_type2))

  @parameterized.parameters([
      ('LABEL', 'THUMBNAIL'),
      ('ORIGINAL\\PRIMARY\\LABEL\\NONE', 'DERIVED\\PRIMARY\\LABEL'),
      ('ORIGINAL\\PRIMARY\\THUMBNAIL\\NONE', 'ORIGINAL\\PRIMARY\\LABEL'),
      (
          'ORIGINAL\\PRIMARY\\VOLUME\\NONE',
          'ORIGINAL\\PRIMARY\\VOLUME\\RESAMPLED',
      ),
  ])
  def test_imagetype_do_not_match(self, im_type1, im_type2):
    self.assertFalse(dicom_store_client._imagetype_match(im_type1, im_type2))

  @parameterized.parameters([404, 429])
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

  @parameterized.parameters([404, 429])
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

  @parameterized.parameters([404, 429])
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
                  find_only_studies_with_undefined_patient_id=undefined_pid,
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
