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
"""Tests for sparse dicom util test."""

import json
from typing import Any, Mapping, MutableMapping, Optional
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import pydicom

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import dicom_store_util
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import flask_util
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import shared_test_util
from pathology.dicom_proxy import sparse_dicom_util
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.test_utils.dicom_store_mock import dicom_store_mock


_MOCK_BASE_URL = dicom_url_util.DicomWebBaseURL(
    'v1', 'project', 'location', 'dataset', 'dicomstore'
)


def _mock_sparse_dicom() -> pydicom.FileDataset:
  dcm = shared_test_util.jpeg_encoded_dicom_instance()
  dcm.DimensionOrganizationType = 'TILED_SPARSE'
  ds1 = pydicom.Dataset()
  ds1.StudyID = '1'
  ds2 = pydicom.Dataset()
  ds2.StudyID = '2'
  dcm.PerFrameFunctionalGroupsSequence = [ds1, ds2]
  return dcm


def _dicom_to_json(dcm: pydicom.FileDataset) -> MutableMapping[str, Any]:
  metadata = dcm.to_json_dict()
  metadata.update(dcm.file_meta.to_json_dict())
  return metadata


@flagsaver.flagsaver(validate_iap=False)
@mock.patch.object(
    user_auth_util,
    '_get_email_from_bearer_token',
    autospec=True,
    return_value='mock@email.com',
)
@mock.patch.object(
    flask_util,
    'get_headers',
    autospec=True,
    return_value={
        proxy_const.HeaderKeywords.AUTH_HEADER_KEY: 'bearer mock_token',
    },
)
@mock.patch.object(
    flask_util,
    'get_first_key_args',
    autospec=True,
    return_value={},
)
@mock.patch.object(
    flask_util,
    'get_key_args_list',
    autospec=True,
    return_value={},
)
@mock.patch.object(
    flask_util,
    'get_method',
    autospec=True,
    return_value='GET',
)
def _download_and_return_sparse_dicom_metadata(
    mgr: dicom_store_util.MetadataThreadPoolDownloadManager,
    metadata: Mapping[str, Any],
    *unused_mocks,
    dcm: Optional[pydicom.Dataset] = None,
) -> str:
  result = sparse_dicom_util.download_and_return_sparse_dicom_metadata(
      _MOCK_BASE_URL,
      dicom_url_util.StudyInstanceUID(
          dcm.StudyInstanceUID if dcm is not None else ''
      ),
      dicom_url_util.SeriesInstanceUID(
          dcm.SeriesInstanceUID if dcm is not None else ''
      ),
      dicom_url_util.SOPInstanceUID(
          dcm.SOPInstanceUID if dcm is not None else ''
      ),
      dict(metadata),
      cache_enabled_type.CachingEnabled(True),
      mgr,
  )
  if isinstance(result, str):
    return result
  return result.result()


class SparseDicomUtilTest(parameterized.TestCase):

  @parameterized.parameters(['All', '00209311', 'DimensionOrganizationType'])
  @mock.patch.object(flask_util, 'get_includefields', autospec=True)
  def test_do_includefields_request_dimensional_organization_type_true(
      self, val, mk_include
  ):
    mk_include.return_value = set([val])
    self.assertTrue(
        sparse_dicom_util.do_includefields_request_dimensional_organization_type()
    )

  @parameterized.parameters(
      ['All', '52009230', 'PerFrameFunctionalGroupsSequence']
  )
  @mock.patch.object(flask_util, 'get_includefields', autospec=True)
  def test_do_includefields_request_perframe_functional_group_seq_true(
      self, val, mk_include
  ):
    mk_include.return_value = set([val])
    self.assertTrue(
        sparse_dicom_util.do_includefields_request_perframe_functional_group_seq()
    )

  @parameterized.parameters(
      ['', '52009230', 'PerFrameFunctionalGroupsSequence']
  )
  @mock.patch.object(flask_util, 'get_includefields', autospec=True)
  def test_do_includefields_request_dimensional_organization_type_false(
      self, val, mk_include
  ):
    mk_include.return_value = set([val])
    self.assertFalse(
        sparse_dicom_util.do_includefields_request_dimensional_organization_type()
    )

  @parameterized.parameters(['', '00209311', 'DimensionOrganizationType'])
  @mock.patch.object(flask_util, 'get_includefields', autospec=True)
  def test_do_includefields_request_perframe_functional_group_seq_false(
      self, val, mk_include
  ):
    mk_include.return_value = set([val])
    self.assertFalse(
        sparse_dicom_util.do_includefields_request_perframe_functional_group_seq()
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='empty_sq',
          metadata={'52009230': {'vr': 'SQ', 'Value': []}},
      ),
      dict(testcase_name='null_sq', metadata={'52009230': {'vr': 'SQ'}}),
      dict(testcase_name='missing_tag', metadata={}),
      dict(
          testcase_name='other_tag',
          metadata={'00209311': {'vr': 'SQ', 'Value': [{}]}},
      ),
  ])
  def test_is_missing_per_frame_functional_groups_sequence_true(self, metadata):
    self.assertTrue(
        sparse_dicom_util._is_missing_per_frame_functional_groups_sequence(
            metadata
        )
    )

  def test_is_missing_per_frame_functional_groups_sequence_false(self):
    self.assertFalse(
        sparse_dicom_util._is_missing_per_frame_functional_groups_sequence(
            {'52009230': {'vr': 'SQ', 'Value': [{}]}}
        )
    )

  def test_download_and_return_sparse_dicom_metadata_nop_if_sparse_dicom_has_data(
      self,
  ):
    dcm = _mock_sparse_dicom()
    metadata = _dicom_to_json(dcm)
    expected_result = json.dumps(metadata)
    with dicom_store_util.MetadataThreadPoolDownloadManager(
        1, dicom_proxy_flags.MAX_AUGMENTED_METADATA_DOWNLOAD_SIZE_FLG.value
    ) as mgr:
      self.assertEqual(
          _download_and_return_sparse_dicom_metadata(mgr, metadata, dcm=dcm),
          expected_result,
      )

  @parameterized.parameters(['0020000D', '0020000E', '00080018'])
  def test_download_and_return_sparse_dicom_metadata_nop_if_missing_uid(
      self, metadata_update
  ):
    dcm = _mock_sparse_dicom()
    dcm[metadata_update].value = ''
    metadata = _dicom_to_json(dcm)
    expected_result = json.dumps(metadata)
    with dicom_store_util.MetadataThreadPoolDownloadManager(
        1, dicom_proxy_flags.MAX_AUGMENTED_METADATA_DOWNLOAD_SIZE_FLG.value
    ) as mgr:
      self.assertEqual(
          _download_and_return_sparse_dicom_metadata(mgr, metadata, dcm=dcm),
          expected_result,
      )

  @mock.patch(
      'redis.Redis', autospec=True, return_value=shared_test_util.RedisMock()
  )
  def test_download_and_return_sparse_dicom_metadata(self, *unused_mocks):
    dcm = _mock_sparse_dicom()
    metadata = _dicom_to_json(dcm)
    expected_result = json.dumps(metadata)
    del metadata['52009230']
    with dicom_store_util.MetadataThreadPoolDownloadManager(
        1, dicom_proxy_flags.MAX_AUGMENTED_METADATA_DOWNLOAD_SIZE_FLG.value
    ) as mgr:
      with dicom_store_mock.MockDicomStores(
          _MOCK_BASE_URL.full_url
      ) as mk_stores:
        mk_stores[_MOCK_BASE_URL.full_url].add_instance(dcm)
        result = _download_and_return_sparse_dicom_metadata(mgr, metadata)
        self.assertEqual(json.loads(result), json.loads(expected_result))

  @mock.patch(
      'redis.Redis', autospec=True, return_value=shared_test_util.RedisMock()
  )
  def test_2nd_download_and_return_sparse_dicom_metadata_reads_from_cache(
      self, *unused_mocks
  ):
    dcm = _mock_sparse_dicom()
    metadata = _dicom_to_json(dcm)
    expected_result = json.dumps(metadata)
    del metadata['52009230']
    with dicom_store_util.MetadataThreadPoolDownloadManager(
        1, dicom_proxy_flags.MAX_AUGMENTED_METADATA_DOWNLOAD_SIZE_FLG.value
    ) as mgr:
      with dicom_store_mock.MockDicomStores(
          _MOCK_BASE_URL.full_url
      ) as mk_stores:
        mk_stores[_MOCK_BASE_URL.full_url].add_instance(dcm)
        _download_and_return_sparse_dicom_metadata(mgr, metadata)
      result = _download_and_return_sparse_dicom_metadata(mgr, metadata)
      self.assertEqual(json.loads(result), json.loads(expected_result))

  @mock.patch(
      'redis.Redis', autospec=True, return_value=shared_test_util.RedisMock()
  )
  def test_download_and_return_sparse_dicom_metadata_missing_sq(
      self, *unused_mocks
  ):
    dcm = _mock_sparse_dicom()
    del dcm['52009230']
    metadata = _dicom_to_json(dcm)
    expected_result = json.dumps(metadata)
    with dicom_store_util.MetadataThreadPoolDownloadManager(
        1, dicom_proxy_flags.MAX_AUGMENTED_METADATA_DOWNLOAD_SIZE_FLG.value
    ) as mgr:
      with dicom_store_mock.MockDicomStores(
          _MOCK_BASE_URL.full_url
      ) as mk_stores:
        mk_stores[_MOCK_BASE_URL.full_url].add_instance(dcm)

        _download_and_return_sparse_dicom_metadata(mgr, metadata)
      # read second request outside of dicom store mock context to force code
      # only succeed if it reads value from cache.
      result = _download_and_return_sparse_dicom_metadata(mgr, metadata)
      self.assertEqual(json.loads(result), json.loads(expected_result))


if __name__ == '__main__':
  absltest.main()
