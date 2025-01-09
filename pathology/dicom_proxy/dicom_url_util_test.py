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
"""Tests for dicom url util."""

from absl.testing import absltest
from absl.testing import parameterized

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import render_frame_params
from pathology.dicom_proxy import user_auth_util


_UID_123 = '1.2.3'
_UID_124 = '1.2.4'
_UID_125 = '1.2.5'
_ACCEPT = 'Accept'
_JPEG_MIME_TYPE = 'image/jpeg'
_PNG_MIME_TYPE = 'image/png'

_DICOM_WEB_URL = dicom_url_util.DicomWebBaseURL('v1', 'p', 'l', 'data', 'dicom')
_STUDY_INSTANCE_UID = dicom_url_util.StudyInstanceUID(_UID_123)
_SERIES_INSTANCE_UID = dicom_url_util.SeriesInstanceUID(_UID_124)
_SOP_INSTANCE_UID = dicom_url_util.SOPInstanceUID(_UID_125)

_DICOM_STORE_SERIES_URL = (
    'https://healthcare.googleapis.com/v1/projects/p/'
    'locations/l/datasets/data/dicomStores/dicom/dicomWeb'
    f'/studies/{_UID_123}/series/{_UID_124}'
)

_AuthSession = user_auth_util.AuthSession


class DicomUrlUtilTest(parameterized.TestCase):

  def test_dicom_webbaseurl(self):
    self.assertEqual(
        str(_DICOM_WEB_URL),
        'v1/projects/p/locations/l/datasets/data/dicomStores/dicom/dicomWeb',
    )

  def test_dicom_webbaseurl_full_url(self):
    self.assertEqual(
        _DICOM_WEB_URL.full_url,
        'https://healthcare.googleapis.com/v1/projects/p/locations/l/datasets/data/dicomStores/dicom/dicomWeb',
    )

  def test_study_instance_uid(self):
    self.assertEqual(str(_STUDY_INSTANCE_UID), f'studies/{_UID_123}')

  def test_series_instance_uid(self):
    self.assertEqual(str(_SERIES_INSTANCE_UID), f'series/{_UID_124}')

  def test_sop_instance_uid(self):
    self.assertEqual(str(_SOP_INSTANCE_UID), f'instances/{_UID_125}')

  def test_base_dicom_series_url(self):
    result = dicom_url_util.base_dicom_series_url(
        _DICOM_WEB_URL, _STUDY_INSTANCE_UID, _SERIES_INSTANCE_UID
    )
    self.assertEqual(result, _DICOM_STORE_SERIES_URL)

  def test_base_dicom_instance_url(self):
    result = dicom_url_util.base_dicom_instance_url(
        _DICOM_WEB_URL,
        _STUDY_INSTANCE_UID,
        _SERIES_INSTANCE_UID,
        _SOP_INSTANCE_UID,
    )
    self.assertEqual(result, f'{_DICOM_STORE_SERIES_URL}/instances/{_UID_125}')

  def test_series_dicom_instance_url(self):
    series_url = dicom_url_util.base_dicom_series_url(
        _DICOM_WEB_URL, _STUDY_INSTANCE_UID, _SERIES_INSTANCE_UID
    )

    result = dicom_url_util.series_dicom_instance_url(
        series_url, _SOP_INSTANCE_UID
    )

    self.assertEqual(result, f'{_DICOM_STORE_SERIES_URL}/instances/{_UID_125}')

  def test_download_dicom_instance_not_transcoded(self):
    series_url = dicom_url_util.base_dicom_series_url(
        _DICOM_WEB_URL, _STUDY_INSTANCE_UID, _SERIES_INSTANCE_UID
    )

    result = dicom_url_util.download_dicom_instance_not_transcoded(
        _AuthSession(None),
        dicom_url_util.series_dicom_instance_url(series_url, _SOP_INSTANCE_UID),
    )

    self.assertEqual(
        (result.url, result.headers),
        (
            f'{_DICOM_STORE_SERIES_URL}/instances/{_UID_125}',
            {_ACCEPT: 'application/dicom; transfer-syntax=*'},
        ),
    )

  def test_download_dicom_raw_frame(self):
    series_url = dicom_url_util.base_dicom_series_url(
        _DICOM_WEB_URL, _STUDY_INSTANCE_UID, _SERIES_INSTANCE_UID
    )
    frame_number = 4
    params = render_frame_params.RenderFrameParams(
        enable_caching=cache_enabled_type.CachingEnabled(False)
    )

    result = dicom_url_util.download_dicom_raw_frame(
        _AuthSession(None),
        dicom_url_util.series_dicom_instance_url(series_url, _SOP_INSTANCE_UID),
        [frame_number],
        params,
    )

    self.assertEqual(
        (result.url, result.headers, result.enable_caching),
        (
            f'{_DICOM_STORE_SERIES_URL}/instances/{_UID_125}/frames/{frame_number}',
            {
                _ACCEPT: (
                    'multipart/related; type="application/octet-stream"; '
                    'transfer-syntax=*'
                )
            },
            cache_enabled_type.CachingEnabled(False),
        ),
    )

  @parameterized.parameters([
      (enum_types.Compression.JPEG, _JPEG_MIME_TYPE),
      (enum_types.Compression.PNG, _PNG_MIME_TYPE),
  ])
  def test_rendered_frame_accept(self, compression, mime_type):
    self.assertEqual(
        dicom_url_util._rendered_frame_accept(compression), {_ACCEPT: mime_type}
    )

  @parameterized.parameters([
      (enum_types.Compression.JPEG, _JPEG_MIME_TYPE),
      (enum_types.Compression.PNG, _PNG_MIME_TYPE),
  ])
  def test_download_rendered_dicom_frame(self, compression, mime_type):
    series_url = dicom_url_util.base_dicom_series_url(
        _DICOM_WEB_URL, _STUDY_INSTANCE_UID, _SERIES_INSTANCE_UID
    )
    frame_number = 4
    params = render_frame_params.RenderFrameParams(
        compression=compression,
        enable_caching=cache_enabled_type.CachingEnabled(True),
    )

    result = dicom_url_util.download_rendered_dicom_frame(
        _AuthSession(None),
        dicom_url_util.series_dicom_instance_url(series_url, _SOP_INSTANCE_UID),
        frame_number,
        params,
    )

    self.assertEqual(
        (result.url, result.headers, result.enable_caching),
        (
            (
                f'{_DICOM_STORE_SERIES_URL}/instances/{_UID_125}/frames/'
                f'{frame_number}/rendered'
            ),
            {_ACCEPT: mime_type},
            cache_enabled_type.CachingEnabled(True),
        ),
    )

  @parameterized.parameters([
      (enum_types.Compression.JPEG, enum_types.Compression.JPEG),
      (enum_types.Compression.PNG, enum_types.Compression.PNG),
      (enum_types.Compression.WEBP, enum_types.Compression.PNG),
      (enum_types.Compression.GIF, enum_types.Compression.PNG),
      (enum_types.Compression.RAW, enum_types.Compression.PNG),
  ])
  def test_get_rendered_frame_compression(self, compression, expected):
    result = dicom_url_util.get_rendered_frame_compression(compression)
    self.assertEqual(result, expected)

  @parameterized.parameters(
      ['/v1/projects', 'v1/projects', '/projects', 'projects']
  )
  def test_get_dicom_store_url(self, test_path):
    original = dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION
    try:
      dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION = 'v1'
      self.assertEqual(
          dicom_url_util.get_dicom_store_url(test_path),
          'https://healthcare.googleapis.com/v1/projects',
      )
    finally:
      dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION = original

  @parameterized.named_parameters([
      dict(
          testcase_name='series_level',
          series_url='http://dicomstore.com/dicomweb/series/1.2.3',
          instance=None,
          expected_url='http://dicomstore.com/dicomweb/series/1.2.3/metadata',
      ),
      dict(
          testcase_name='instance_level',
          series_url='http://dicomstore.com/dicomweb/series/1.2.3',
          instance=dicom_url_util.SOPInstanceUID('4.5.6'),
          expected_url='http://dicomstore.com/dicomweb/series/1.2.3/instances/4.5.6/metadata',
      ),
  ])
  def test_dicom_instance_metadata_query(
      self, series_url, instance, expected_url
  ):
    result = dicom_url_util.dicom_instance_metadata_query(
        _AuthSession(None), series_url, instance
    )
    self.assertEqual(result.url, expected_url)
    self.assertEqual(result.headers, {'Accept': 'application/dicom+json'})

  @parameterized.named_parameters([
      dict(
          testcase_name='series_level',
          series_url='http://dicomstore.com/dicomweb/series/1.2.3',
          instance=None,
          additional_tags=None,
          expected_url='http://dicomstore.com/dicomweb/series/1.2.3/instances',
      ),
      dict(
          testcase_name='instance_level',
          series_url='http://dicomstore.com/dicomweb/series/1.2.3',
          instance=dicom_url_util.SOPInstanceUID('4.5.6'),
          additional_tags=None,
          expected_url='http://dicomstore.com/dicomweb/series/1.2.3/instances?SOPInstanceUID=4.5.6',
      ),
      dict(
          testcase_name='series_level_with_additional_tags',
          series_url='http://dicomstore.com/dicomweb/series/1.2.3',
          instance=None,
          additional_tags=['one', 'two'],
          expected_url='http://dicomstore.com/dicomweb/series/1.2.3/instances?includefield=one&includefield=two',
      ),
      dict(
          testcase_name='instance_level_with_additional_tags',
          series_url='http://dicomstore.com/dicomweb/series/1.2.3',
          instance=dicom_url_util.SOPInstanceUID('4.5.6'),
          additional_tags=['one', 'two'],
          expected_url='http://dicomstore.com/dicomweb/series/1.2.3/instances?SOPInstanceUID=4.5.6&includefield=one&includefield=two',
      ),
  ])
  def test_dicom_instance_tag_query(
      self, series_url, instance, additional_tags, expected_url
  ):
    result = dicom_url_util.dicom_instance_tag_query(
        _AuthSession(None),
        series_url,
        instance,
        additional_tags,
    )
    self.assertEqual(result.url, expected_url)
    self.assertEqual(result.headers, {'Accept': 'application/dicom+json'})

  def test_base_dicom_study_url(self):
    self.assertEqual(
        dicom_url_util.base_dicom_study_url(
            _DICOM_WEB_URL, dicom_url_util.StudyInstanceUID('1.2.3')
        ),
        'https://healthcare.googleapis.com/v1/projects/p/locations/l/datasets/data/dicomStores/dicom/dicomWeb/studies/1.2.3',
    )


if __name__ == '__main__':
  absltest.main()
