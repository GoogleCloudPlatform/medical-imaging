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
"""Tests for parameters exceptions and return types."""
import dataclasses
import filecmp
import os

from absl.testing import absltest
from absl.testing import parameterized

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import dicom_instance_request
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import frame_retrieval_util
from pathology.dicom_proxy import parameters_exceptions_and_return_types
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import pydicom_single_instance_read_cache
from pathology.dicom_proxy import render_frame_params
from pathology.dicom_proxy import shared_test_util
from pathology.dicom_proxy import user_auth_util


# Types
_AuthSession = user_auth_util.AuthSession
_Compression = enum_types.Compression
_Interpolation = enum_types.Interpolation
_PyDicomFilePath = pydicom_single_instance_read_cache.PyDicomFilePath
_PyDicomSingleInstanceCache = (
    pydicom_single_instance_read_cache.PyDicomSingleInstanceCache
)
_Metrics = parameters_exceptions_and_return_types.Metrics
_RenderFrameParams = render_frame_params.RenderFrameParams
_MOCK_DICOM_STORE_API_VERSION = 'v1'

_EXPECTED_METADATA = {
    'bits_allocated': 8,
    'bits_stored': 8,
    'columns': 256,
    'high_bit': 7,
    'lossy_compression_method': 'ISO_10918_1',
    'lossy_image_compression': '01',
    'number_of_frames': 15,
    'planar_configuration': 0,
    'rows': 256,
    'samples_per_pixel': 3,
    'sop_class_uid': '1.2.840.10008.5.1.4.1.1.77.1.6',
    'total_pixel_matrix_columns': 1152,
    'total_pixel_matrix_rows': 700,
    'dimension_organization_type': 'TILED_FULL',
    'dicom_transfer_syntax': '1.2.840.10008.1.2.4.50',
    'icc_profile_colorspace': '',
    'icc_profile_bulkdata_uri': '',
    'metadata_source': {
        'store_url': 'localhost',
        'sop_instance_uid': {
            'sop_instance_uid': (
                '1.2.276.0.7230010.3.1.4.296485376.89.1688794081.412405'
            )
        },
        'caching_enabled': False,
    },
}


class ParametersExceptionsAndReturnTypesTest(parameterized.TestCase):

  def test_metrics_defaults(self):
    expected = dict(
        mini_batch_requests=0,
        frame_requests=0,
        images_transcoded=False,
        number_of_frames_downloaded_from_store=0,
    )

    metric = _Metrics()

    self.assertEqual(dataclasses.asdict(metric), expected)

  def test_metrics_params(self):
    mini_batch_requests = 5
    frame_requests = 6
    images_transcoded = True
    number_of_frames_downloaded_from_store = 10
    expected = dict(
        mini_batch_requests=mini_batch_requests,
        frame_requests=frame_requests,
        images_transcoded=images_transcoded,
        number_of_frames_downloaded_from_store=number_of_frames_downloaded_from_store,
    )

    metric = _Metrics(
        mini_batch_requests,
        frame_requests,
        True,
        number_of_frames_downloaded_from_store,
    )

    self.assertEqual(dataclasses.asdict(metric), expected)

  @parameterized.parameters([
      (_Metrics(3, 3, False, 4), _Metrics(5, 6, True, 6)),
      (_Metrics(5, 6, True, 3), _Metrics(3, 3, False, 7)),
  ])
  def test_add_metrics(self, metric1, metric2):
    expected = dict(
        mini_batch_requests=8,
        frame_requests=9,
        images_transcoded=True,
        number_of_frames_downloaded_from_store=10,
    )

    metric1.add_metrics(metric2)
    self.assertEqual(dataclasses.asdict(metric1), expected)

  def test_metrics_str_dict(self):
    mini_batch_requests = 5
    frame_requests = 6
    images_transcoded = True
    number_of_frames_downloaded_from_store = 10
    metric = _Metrics(
        mini_batch_requests,
        frame_requests,
        images_transcoded,
        number_of_frames_downloaded_from_store,
    )
    self.assertEqual(
        metric.str_dict(),
        {
            'mini_batch_requests': str(mini_batch_requests),
            'frame_requests': str(frame_requests),
            'images_transcoded': str(images_transcoded),
            'number_of_frames_downloaded_from_store': str(
                number_of_frames_downloaded_from_store
            ),
        },
    )

  def test_defines_downsampling_frame_request_exception(self):
    with self.assertRaises(
        parameters_exceptions_and_return_types.DownsamplingFrameRequestError
    ):
      raise parameters_exceptions_and_return_types.DownsamplingFrameRequestError()

  def test_rendered_frame_prams(self):
    param = _RenderFrameParams(
        2.5,
        _Interpolation.CUBIC,
        _Compression.PNG,
        85,
        proxy_const.ICCProfile.SRGB,
    )

    self.assertEqual(param.downsample, 2.5)
    self.assertEqual(param.interpolation, _Interpolation.CUBIC)
    self.assertEqual(param.compression, _Compression.PNG)
    self.assertEqual(param.quality, 85)
    self.assertEqual(param.icc_profile, proxy_const.ICCProfile.SRGB)

  def test_dicom_instance_web_request(self):
    auth = _AuthSession(None)
    project = 'test_project'
    region = 'us-central1'
    dataset = 'dataset'
    dicom_store = 'dicom_store'
    study = '1.2'
    series = f'{study}.3'
    instance = f'{series}.4'
    series_dicom_url = (
        'https://healthcare.googleapis.com/v1/projects/'
        f'{project}/locations/{region}/datasets/{dataset}/'
        f'dicomStores/{dicom_store}/dicomWeb/studies/{study}/'
        f'series/{series}'
    )
    url_args = {}
    url_header = {'accept': 'image/jpeg'}
    metadata = shared_test_util.jpeg_encoded_dicom_instance_metadata()

    web_request = (
        parameters_exceptions_and_return_types.DicomInstanceWebRequest(
            auth,
            dicom_url_util.DicomWebBaseURL(
                _MOCK_DICOM_STORE_API_VERSION,
                project,
                region,
                dataset,
                dicom_store,
            ),
            dicom_url_util.StudyInstanceUID(study),
            dicom_url_util.SeriesInstanceUID(series),
            dicom_url_util.SOPInstanceUID(instance),
            cache_enabled_type.CachingEnabled(True),
            url_args,
            url_header,
            metadata,
        )
    )

    self.assertIs(web_request._user_auth, auth)
    self.assertEqual(str(web_request._instance), f'instances/{instance}')
    self.assertEqual(str(web_request._dicom_series_url), series_dicom_url)
    self.assertIs(web_request.user_auth, auth)
    self.assertEqual(
        web_request._enable_caching, cache_enabled_type.CachingEnabled(True)
    )
    self.assertEqual(
        web_request.dicom_sop_instance_url,
        f'{series_dicom_url}/instances/{instance}',
    )
    self.assertIsInstance(
        web_request, dicom_instance_request.DicomInstanceRequest
    )
    self.assertIs(web_request.metadata, metadata)
    self.assertEqual(web_request.url_args, url_args)

  @parameterized.parameters(['foo', dicom_url_util.DicomSopInstanceUrl('bar')])
  def test_local_dicom_instance_set_dicom_sop_instance_url_str(self, val):
    path = shared_test_util.jpeg_encoded_dicom_instance_test_path()
    request = parameters_exceptions_and_return_types.LocalDicomInstance(
        _PyDicomFilePath(path)
    )
    self.assertEqual(request.dicom_sop_instance_url, path)
    request.dicom_sop_instance_url = val
    self.assertEqual(request.dicom_sop_instance_url, val)
    self.assertEmpty(request.url_args)

  def test_local_dicom_instance_from_str(self):
    path = shared_test_util.jpeg_encoded_dicom_instance_test_path()
    request = parameters_exceptions_and_return_types.LocalDicomInstance(
        _PyDicomFilePath(path)
    )
    expected_auth = _AuthSession(None)
    self.assertEqual(request.dicom_sop_instance_url, path)
    self.assertEqual(request.cache.path, path)
    self.assertEqual(
        (request.user_auth.authorization, request.user_auth.authority),
        (expected_auth.authorization, expected_auth.authority),
    )
    self.assertEqual(dataclasses.asdict(request.metadata), _EXPECTED_METADATA)
    self.assertEmpty(request.url_args)

  def test_local_dicom_instance_from_cache(self):
    path = shared_test_util.jpeg_encoded_dicom_instance_test_path()
    cache = _PyDicomSingleInstanceCache(_PyDicomFilePath(path))
    expected_auth = _AuthSession(None)
    request = parameters_exceptions_and_return_types.LocalDicomInstance(
        cache, url_args={'downsample': '2.0'}
    )
    self.assertEqual(request.dicom_sop_instance_url, path)
    self.assertEqual(
        (request.user_auth.authorization, request.user_auth.authority),
        (expected_auth.authorization, expected_auth.authority),
    )
    self.assertIs(request.cache, cache)
    self.assertEqual(dataclasses.asdict(request.metadata), _EXPECTED_METADATA)
    self.assertEqual(request.url_args, {'downsample': '2.0'})

  def test_local_dicom_instance_download(self):
    path = shared_test_util.jpeg_encoded_dicom_instance_test_path()
    cache = _PyDicomSingleInstanceCache(_PyDicomFilePath(path))
    local = parameters_exceptions_and_return_types.LocalDicomInstance(cache)
    temp_dir = self.create_tempdir()
    temp_path = os.path.join(temp_dir, 'temp.dcm')

    local.download_instance(temp_path)

    self.assertTrue(filecmp.cmp(temp_path, cache.path, shallow=False))

  def test_get_dicom_frames(self):
    request = parameters_exceptions_and_return_types.LocalDicomInstance(
        shared_test_util.jpeg_encoded_pydicom_instance_cache()
    )
    frame_images = request.get_dicom_frames(
        _RenderFrameParams(compression=_Compression.PNG), [1]
    )

    self.assertLen(frame_images.images, 1)
    self.assertEqual(frame_images.compression, _Compression.JPEG)
    with open(
        shared_test_util.get_testdir_path(
            'parameters_exceptions_and_return_types_test', 'frame.jpeg'
        ),
        'rb',
    ) as infile:
      self.assertTrue(
          shared_test_util.rgb_image_almost_equal(
              frame_images.images[1], infile.read()
          )
      )

  def test_return_frame_bytes_throws_if_frame_data_list_contains_none(self):
    with self.assertRaisesRegex(
        frame_retrieval_util.BaseFrameRetrievalError,
        'Frame image bytes is none',
    ):
      parameters_exceptions_and_return_types._return_frame_bytes(
          [frame_retrieval_util.FrameData(None, True)]
      )

  def test_local_dicom_instance_get_raw_dicom_frames(self):
    request = parameters_exceptions_and_return_types.LocalDicomInstance(
        shared_test_util.jpeg_encoded_pydicom_instance_cache()
    )
    with open(
        shared_test_util.get_testdir_path(
            'parameters_exceptions_and_return_types_test', 'frame.jpeg'
        ),
        'rb',
    ) as infile:
      self.assertTrue(
          shared_test_util.rgb_image_almost_equal(
              request.get_raw_dicom_frames(_RenderFrameParams(), [2])[0],
              infile.read(),
          )
      )

  def test_rendered_dicom_frames(self):
    byte_list = [b'1234', b'5678']
    params = _RenderFrameParams()

    rf = parameters_exceptions_and_return_types.RenderedDicomFrames(
        byte_list, params, _Metrics(6, 4)
    )

    self.assertEqual(rf.images, byte_list)
    self.assertEqual(rf.compression, params.compression)
    self.assertEqual(rf.metrics.mini_batch_requests, 6)
    self.assertEqual(rf.metrics.frame_requests, 4)

  @parameterized.parameters([
      (_Compression.JPEG, 'image/jpeg'),
      (_Compression.PNG, 'image/png'),
      (_Compression.WEBP, 'image/webp'),
      (_Compression.RAW, 'image/raw'),
      (_Compression.GIF, 'image/gif'),
      (_Compression.JPEG_TRANSCODED_TO_JPEGXL, 'image/jxl'),
      (_Compression.JPEGXL, 'image/jxl'),
      (999, None),
  ])
  def test_rendered_dicom_frame_content_type(self, compression, expected):
    params = _RenderFrameParams()
    params.compression = compression
    rf = parameters_exceptions_and_return_types.RenderedDicomFrames(
        [b'1234', b'5678'], params, _Metrics(6, 4)
    )

    if expected is None:
      with self.assertRaises(ValueError):
        _ = rf.content_type
    else:
      self.assertEqual(rf.content_type, expected)

  @parameterized.parameters([
      (_Compression.JPEG, 'image/jpeg; transfer-syntax=1.2.840.10008.1.2.4.50'),
      (_Compression.PNG, 'image/png'),
      (_Compression.WEBP, 'image/webp'),
      (
          _Compression.RAW,
          'application/octet-stream; transfer-syntax=1.2.840.10008.1.2.1',
      ),
      (
          _Compression.JPEG_TRANSCODED_TO_JPEGXL,
          'image/jxl; transfer-syntax=1.2.840.10008.1.2.4.111',
      ),
      (
          _Compression.JPEGXL,
          'image/jxl; transfer-syntax=1.2.840.10008.1.2.4.112',
      ),
      (_Compression.GIF, 'image/gif'),
      (999, None),
  ])
  def test_rendered_dicom_frame_multipart_content_type(
      self, compression, expected
  ):
    params = _RenderFrameParams()
    params.compression = compression
    rf = parameters_exceptions_and_return_types.RenderedDicomFrames(
        [b'1234', b'5678'], params, _Metrics(6, 4)
    )

    if expected is None:
      with self.assertRaises(ValueError):
        _ = rf.multipart_content_type
    else:
      self.assertEqual(rf.multipart_content_type, expected)

  def test_dicom_instance_web_request_as_dict(self):
    project = 'cf-dp-dev'
    location = 'us-west1'
    dataset = 'bigdata'
    dicom_store = 'bigdicomstore'
    study = '1.2.3'
    series = '1.2.3.4'
    instance = '1.2.3.4.5'
    authorization = 'test_auth'
    authority = 'test_authority'
    enable_caching = True
    url_args = {}
    url_header = {'accept': 'image/jpeg'}
    expected_dict = {
        'dicom_instance': instance,
        'dicom_series_url': (
            f'https://healthcare.googleapis.com/v1/projects/{project}/locations'
            f'/{location}/datasets/{dataset}/dicomStores/{dicom_store}/'
            f'dicomWeb/studies/{study}/series/{series}'
        ),
        'source_instance_metadata': None,
        'user_auth': {'authorization': authorization, 'authority': authority},
        'enable_caching': enable_caching,
        'url_args': url_args,
        'url_header': url_header,
    }

    result = parameters_exceptions_and_return_types.DicomInstanceWebRequest(
        _AuthSession({'authorization': authorization, 'authority': authority}),
        dicom_url_util.DicomWebBaseURL(
            _MOCK_DICOM_STORE_API_VERSION,
            project,
            location,
            dataset,
            dicom_store,
        ),
        dicom_url_util.StudyInstanceUID(study),
        dicom_url_util.SeriesInstanceUID(series),
        dicom_url_util.SOPInstanceUID(instance),
        cache_enabled_type.CachingEnabled(enable_caching),
        url_args,
        url_header,
    )

    self.assertEqual(result.as_dict(), expected_dict)


if __name__ == '__main__':
  absltest.main()
