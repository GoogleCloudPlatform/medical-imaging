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
"""Tests for downsample util."""

import dataclasses
import io
import os
from typing import Any, Mapping, Optional, Tuple
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import PIL
from PIL import ImageCms
import pydicom

from pathology.dicom_proxy import color_conversion_util
from pathology.dicom_proxy import downsample_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import icc_color_transform
from pathology.dicom_proxy import parameters_exceptions_and_return_types
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import pydicom_single_instance_read_cache
from pathology.dicom_proxy import render_frame_params
from pathology.dicom_proxy import shared_test_util


_LocalDicomInstance = parameters_exceptions_and_return_types.LocalDicomInstance
_RenderFrameParams = render_frame_params.RenderFrameParams
_DownsamplingFrameRequestError = (
    parameters_exceptions_and_return_types.DownsamplingFrameRequestError
)
_Compression = enum_types.Compression
_Metrics = parameters_exceptions_and_return_types.Metrics


def _compression_ext(compression: _Compression) -> str:
  if compression == _Compression.JPEG:
    return 'jpeg'
  if compression == _Compression.WEBP:
    return 'webp'
  if compression == _Compression.GIF:
    return 'gif'
  if compression == _Compression.PNG:
    return 'png'
  raise ValueError('unexpected')


def _interpolation_str(interpolation: enum_types.Interpolation) -> str:
  if interpolation == enum_types.Interpolation.NEAREST:
    return 'nearest'
  if interpolation == enum_types.Interpolation.LINEAR:
    return 'linear'
  if interpolation == enum_types.Interpolation.CUBIC:
    return 'cubic'
  if interpolation == enum_types.Interpolation.AREA:
    return 'area'
  if interpolation == enum_types.Interpolation.LANCZOS4:
    return 'lanczos'
  raise ValueError('unexpected')


def _get_downsample_util_path(filename: str) -> str:
  return shared_test_util.get_testdir_path('downsample_util', filename)


def _get_dicom_instance_path(filename: str) -> str:
  return shared_test_util.get_testdir_path('dicom_instance', filename)


def _get_local_dicom_instance_and_params(
    path: str, params: Optional[Mapping[str, Any]] = None
) -> Tuple[_LocalDicomInstance, _RenderFrameParams]:
  path = pydicom_single_instance_read_cache.PyDicomFilePath(path)
  cache = pydicom_single_instance_read_cache.PyDicomSingleInstanceCache(path)
  if params is None:
    params = {}
  params = _RenderFrameParams(**params)
  return (_LocalDicomInstance(cache), params)


def _get_jpeg_encoded_dicom_local_instance_and_params(
    params: Optional[Mapping[str, Any]] = None,
) -> Tuple[_LocalDicomInstance, _RenderFrameParams]:
  return _get_local_dicom_instance_and_params(
      shared_test_util.jpeg_encoded_dicom_instance_test_path(), params
  )


class DownsampleUtilTest(parameterized.TestCase):

  def _rgb_image_almost_equal(
      self, test_bytes: bytes, expected_path: str
  ) -> None:
    with open(expected_path, 'rb') as infile:
      self.assertTrue(
          shared_test_util.rgb_image_almost_equal(test_bytes, infile.read())
      )

  @parameterized.parameters([
      (2.0, []),  #   def test_get_rendered_dicom_frames_
      (2.0, 1000 * [1]),  # to_many_rendered_frame_requests
      (0.5, [1, 1]),  # downsample_less_than_one
  ])
  @flagsaver.flagsaver(max_number_of_frame_per_request=20)
  def test_get_rendered_dicom_frames_throws(
      self, downsample, frames_indexes_requested
  ):
    instance, params = _get_jpeg_encoded_dicom_local_instance_and_params()
    params.downsample = downsample

    with self.assertRaises(_DownsamplingFrameRequestError):
      downsample_util.get_rendered_dicom_frames(
          instance, params, frames_indexes_requested
      )

  def test_get_max_downsample(self):
    instance = shared_test_util.jpeg_encoded_dicom_local_instance()

    self.assertEqual(
        downsample_util._get_max_downsample(instance.metadata),
        max(
            instance.metadata.total_pixel_matrix_columns,
            instance.metadata.total_pixel_matrix_rows,
        ),
    )

  @flagsaver.flagsaver(max_number_of_frame_per_request=5)
  def test_invalid_frame_index_too_small(self):
    instance, params = _get_jpeg_encoded_dicom_local_instance_and_params()

    with self.assertRaises(_DownsamplingFrameRequestError):
      downsample_util.get_rendered_dicom_frames(instance, params, [0])

  def test_invalid_frame_index_too_large(self):
    instance, params = _get_jpeg_encoded_dicom_local_instance_and_params()

    with self.assertRaises(_DownsamplingFrameRequestError):
      downsample_util.get_rendered_dicom_frames(instance, params, [50])

  @parameterized.parameters([
      _Compression.JPEG,
      _Compression.PNG,
      _Compression.WEBP,
      _Compression.GIF,
  ])
  def test_get_rendered_dicom_frames_no_downsample(self, compression):
    instance, params = _get_jpeg_encoded_dicom_local_instance_and_params(
        dict(downsample=1.0, compression=compression)
    )
    ext = _compression_ext(params.compression)
    path = _get_downsample_util_path(
        f'test_get_rendered_dicom_frames_no_downsample.{ext}'
    )
    rendered_frames = downsample_util.get_rendered_dicom_frames(
        instance, params, [10]
    )

    self.assertEqual(rendered_frames.compression, compression)
    self.assertEqual(rendered_frames.metrics.mini_batch_requests, 0)
    self.assertEqual(rendered_frames.metrics.frame_requests, 1)
    self.assertEqual(
        rendered_frames.metrics.images_transcoded,
        compression != _Compression.JPEG,
    )
    self.assertEqual(
        rendered_frames.metrics.number_of_frames_downloaded_from_store, 0
    )
    self.assertLen(rendered_frames.images, 1)
    self._rgb_image_almost_equal(rendered_frames.images[0], path)

  @parameterized.parameters([
      _Compression.JPEG,
      _Compression.PNG,
      _Compression.WEBP,
      _Compression.GIF,
  ])
  @flagsaver.flagsaver(max_mini_batch_frame_request_size=500)
  def test_get_rendered_dicom_frames_single_mini_batch_request(
      self, compression
  ):
    instance, params = _get_jpeg_encoded_dicom_local_instance_and_params(
        dict(
            interpolation=enum_types.Interpolation.AREA,
            downsample=2.0,
            compression=compression,
        )
    )
    ext = _compression_ext(params.compression)
    downsample_image_index_list = [1, 2, 3]
    rendered_frames = downsample_util.get_rendered_dicom_frames(
        instance, params, downsample_image_index_list
    )

    self.assertEqual(rendered_frames.metrics.mini_batch_requests, 1)
    self.assertEqual(rendered_frames.metrics.frame_requests, 10)
    self.assertTrue(rendered_frames.metrics.images_transcoded)
    self.assertEqual(
        rendered_frames.metrics.number_of_frames_downloaded_from_store, 0
    )
    self.assertLen(rendered_frames.images, len(downsample_image_index_list))
    self.assertEqual(rendered_frames.compression, compression)
    for idx, img in zip(downsample_image_index_list, rendered_frames.images):
      path = _get_downsample_util_path(
          f'downsample_2_index_{idx}_interpolation_area.{ext}'
      )
      self._rgb_image_almost_equal(img, path)

  @flagsaver.flagsaver(max_mini_batch_frame_request_size=5)
  def test_get_rendered_dicom_frames_copy_prior_mini_batch(self):
    instance, params = _get_jpeg_encoded_dicom_local_instance_and_params(
        dict(
            interpolation=enum_types.Interpolation.AREA,
            downsample=2.0,
            compression=_Compression.JPEG,
        )
    )
    ext = _compression_ext(params.compression)
    index = 2
    downsample_image_index_list = [index, index]
    path = _get_downsample_util_path(
        f'downsample_2_index_{index}_interpolation_area.{ext}'
    )

    rendered_frames = downsample_util.get_rendered_dicom_frames(
        instance, params, [index, index]
    )

    self.assertEqual(rendered_frames.metrics.mini_batch_requests, 2)
    self.assertEqual(rendered_frames.metrics.frame_requests, 4)
    self.assertTrue(rendered_frames.metrics.images_transcoded)
    self.assertEqual(
        rendered_frames.metrics.number_of_frames_downloaded_from_store, 0
    )
    self.assertLen(rendered_frames.images, len(downsample_image_index_list))
    self.assertEqual(rendered_frames.compression, _Compression.JPEG)
    self._rgb_image_almost_equal(rendered_frames.images[0], path)

  @flagsaver.flagsaver(max_mini_batch_frame_request_size=5)
  def test_get_rendered_dicom_frames_split_across_mini_batches(self):
    instance, params = _get_jpeg_encoded_dicom_local_instance_and_params(
        dict(
            interpolation=enum_types.Interpolation.AREA,
            downsample=2.0,
            compression=_Compression.JPEG,
        )
    )
    ext = _compression_ext(params.compression)
    downsample_image_index_list = [1, 2, 3]
    rendered_frames = downsample_util.get_rendered_dicom_frames(
        instance, params, downsample_image_index_list
    )

    self.assertEqual(rendered_frames.metrics.mini_batch_requests, 3)
    self.assertEqual(rendered_frames.metrics.frame_requests, 10)
    self.assertTrue(rendered_frames.metrics.images_transcoded)
    self.assertEqual(
        rendered_frames.metrics.number_of_frames_downloaded_from_store, 0
    )
    self.assertLen(rendered_frames.images, len(downsample_image_index_list))
    self.assertEqual(rendered_frames.compression, _Compression.JPEG)
    for idx, img in zip(downsample_image_index_list, rendered_frames.images):
      self._rgb_image_almost_equal(
          img,
          _get_downsample_util_path(
              f'downsample_2_index_{idx}_interpolation_area.{ext}'
          ),
      )

  @parameterized.parameters([
      enum_types.Interpolation.NEAREST,
      enum_types.Interpolation.LINEAR,
      enum_types.Interpolation.CUBIC,
      enum_types.Interpolation.AREA,
      enum_types.Interpolation.LANCZOS4,
  ])
  @flagsaver.flagsaver(max_mini_batch_frame_request_size=500)
  def test_get_rendered_dicom_frames_test_interpolation(self, interpolation):
    instance, params = _get_jpeg_encoded_dicom_local_instance_and_params(
        dict(
            interpolation=interpolation,
            downsample=2.0,
            compression=_Compression.PNG,
        )
    )
    interp_string = _interpolation_str(params.interpolation)
    ext = _compression_ext(params.compression)
    downsample_image_index_list = [2, 3]
    rendered_frames = downsample_util.get_rendered_dicom_frames(
        instance, params, downsample_image_index_list
    )

    self.assertEqual(rendered_frames.metrics.mini_batch_requests, 1)
    if interpolation == enum_types.Interpolation.AREA:
      self.assertEqual(rendered_frames.metrics.frame_requests, 6)
    else:
      self.assertEqual(rendered_frames.metrics.frame_requests, 12)
    self.assertTrue(rendered_frames.metrics.images_transcoded)
    self.assertEqual(
        rendered_frames.metrics.number_of_frames_downloaded_from_store, 0
    )
    self.assertLen(rendered_frames.images, len(downsample_image_index_list))
    self.assertEqual(rendered_frames.compression, _Compression.PNG)
    for idx, img in zip(downsample_image_index_list, rendered_frames.images):
      self._rgb_image_almost_equal(
          img,
          _get_downsample_util_path(
              f'downsample_2_index_{idx}_interpolation_{interp_string}.{ext}'
          ),
      )

  @flagsaver.flagsaver(max_mini_batch_frame_request_size=500)
  def test_get_rendered_dicom_frames_invalid_icc_profile_raises(self):
    icc_profile_bytes = color_conversion_util._get_srgb_iccprofile()
    dcm_path = os.path.join(self.create_tempdir(), 'mock_dicom.dcm')
    with pydicom.dcmread(
        shared_test_util.jpeg_encoded_dicom_instance_test_path()
    ) as dcm:
      dcm.ICCProfile = icc_profile_bytes
      dcm.save_as(dcm_path)
    instance, params = _get_local_dicom_instance_and_params(
        dcm_path,
        dict(
            interpolation=enum_types.Interpolation.AREA,
            downsample=2.0,
            compression=_Compression.PNG,
            icc_profile=enum_types.ICCProfile('foo'),
        ),
    )
    downsample_image_index_list = [2, 3]
    with self.assertRaises(_DownsamplingFrameRequestError):
      downsample_util.get_rendered_dicom_frames(
          instance, params, downsample_image_index_list
      )

  @parameterized.parameters([
      (True, True, _Compression.PNG),
      (False, True, _Compression.PNG),
      (False, False, _Compression.PNG),
      (True, False, _Compression.PNG),
      (True, True, _Compression.JPEG),
      (False, True, _Compression.JPEG),
      (False, False, _Compression.JPEG),
      (True, False, _Compression.JPEG),
  ])
  def test_downsample_1x_instance(
      self, batch_mode, decode_image_as_numpy, compression
  ):
    instance, params = _get_jpeg_encoded_dicom_local_instance_and_params(
        dict(downsample=1.0, compression=compression)
    )
    ext = _compression_ext(params.compression)
    clip_image = True
    expected_metrics = dict(
        frame_requests=instance.metadata.number_of_frames,  # process all frames
        images_transcoded=True,  # full image built from frames and recompressed
        mini_batch_requests=0,  # 1x downsample does not call mini-batch code.
        number_of_frames_downloaded_from_store=0,
    )
    orig_params = params.copy()
    filename = f'downsample1.{ext}'

    returned_images = downsample_util.downsample_dicom_instance(
        instance, params, batch_mode, decode_image_as_numpy, clip_image
    )

    self.assertEqual(
        dataclasses.asdict(orig_params), dataclasses.asdict(params)
    )
    self.assertEqual(returned_images.compression, params.compression)
    self.assertEqual(
        dataclasses.asdict(returned_images.metrics), expected_metrics
    )
    self.assertLen(returned_images.images, 1)
    self._rgb_image_almost_equal(
        returned_images.images[0], _get_dicom_instance_path(filename)
    )

  def test_downsample_1x_instance_raises_if_invalid_icc_profile(self):
    batch_mode = False
    decode_image_as_numpy = False
    icc_profile_bytes = color_conversion_util._get_srgb_iccprofile()
    dcm_path = os.path.join(self.create_tempdir(), 'mock_dicom.dcm')
    with pydicom.dcmread(
        shared_test_util.jpeg_encoded_dicom_instance_test_path()
    ) as dcm:
      dcm.ICCProfile = icc_profile_bytes
      dcm.save_as(dcm_path)
    instance, params = _get_local_dicom_instance_and_params(
        dcm_path,
        dict(
            interpolation=enum_types.Interpolation.AREA,
            downsample=1.0,
            compression=_Compression.PNG,
            icc_profile=enum_types.ICCProfile('foo'),
        ),
    )
    clip_image = True
    with self.assertRaises(_DownsamplingFrameRequestError):
      downsample_util.downsample_dicom_instance(
          instance, params, batch_mode, decode_image_as_numpy, clip_image
      )

  @parameterized.named_parameters([
      dict(
          testcase_name='embed_iccprofile_true',
          embed_iccprofile=True,
      ),
      dict(
          testcase_name='embed_iccprofile_false',
          embed_iccprofile=False,
      ),
  ])
  def test_downsample_1x_instance_with_embedded_icc_profile(
      self,
      embed_iccprofile: bool,
  ):
    expected_embedded_icc_profile = (
        color_conversion_util._get_srgb_iccprofile()
        if embed_iccprofile
        else None
    )
    dcm_path = os.path.join(self.create_tempdir(), 'mock_dicom.dcm')
    icc_profile_bytes = color_conversion_util._get_srgb_iccprofile()
    with pydicom.dcmread(
        shared_test_util.jpeg_encoded_dicom_instance_test_path()
    ) as dcm:
      dcm.ICCProfile = icc_profile_bytes
      dcm.save_as(dcm_path)
    instance, params = _get_local_dicom_instance_and_params(
        dcm_path,
        dict(
            downsample=1.0,
            compression=_Compression.JPEG,
            icc_profile=proxy_const.ICCProfile.YES,
            embed_iccprofile=embed_iccprofile,
        ),
    )

    batch_mode = True
    decode_image_as_numpy = True
    clip_image = True
    expected_metrics = dict(
        frame_requests=instance.metadata.number_of_frames,  # process all frames
        images_transcoded=True,  # full image built from frames and recompressed
        mini_batch_requests=0,  # 1x downsample does not call mini-batch code.
        number_of_frames_downloaded_from_store=0,
    )
    orig_params = params.copy()

    returned_images = downsample_util.downsample_dicom_instance(
        instance, params, batch_mode, decode_image_as_numpy, clip_image
    )

    self.assertEqual(
        dataclasses.asdict(orig_params), dataclasses.asdict(params)
    )
    self.assertEqual(returned_images.compression, params.compression)
    self.assertEqual(
        dataclasses.asdict(returned_images.metrics), expected_metrics
    )
    self.assertLen(returned_images.images, 1)
    with PIL.Image.open(io.BytesIO(returned_images.images[0])) as img:
      self.assertEqual(
          img.info.get('icc_profile'), expected_embedded_icc_profile
      )

  @parameterized.parameters([
      (dict(downsample=2.0, compression=_Compression.JPEG, quality=95), True),
      (dict(downsample=4.0, compression=_Compression.JPEG, quality=95), True),
      (dict(downsample=2.0, compression=_Compression.JPEG, quality=35), True),
      (dict(downsample=4.0, compression=_Compression.JPEG, quality=35), True),
      (dict(downsample=2.0, compression=_Compression.PNG, quality=95), True),
      (dict(downsample=4.0, compression=_Compression.PNG, quality=95), True),
      (dict(downsample=256.0, compression=_Compression.PNG, quality=95), True),
      (dict(downsample=256.0, compression=_Compression.PNG, quality=95), False),
  ])
  def test_downsample_instance(self, params, clip_image):
    instance, params = _get_jpeg_encoded_dicom_local_instance_and_params(params)
    ext = _compression_ext(params.compression)
    batch_mode = True
    decode_image_as_numpy = True
    expected_metrics = dict(
        frame_requests=instance.metadata.number_of_frames,  # process all frames
        images_transcoded=True,  # full image built from frames and recompressed
        mini_batch_requests=1,  # processed in one mini-batch
        number_of_frames_downloaded_from_store=0,
    )
    orig_params = params.copy()
    filename = (
        f'downsample{params.downsample}_quality{params.quality}_clip'
        f'{clip_image}.{ext}'
    )

    returned_images = downsample_util.downsample_dicom_instance(
        instance, params, batch_mode, decode_image_as_numpy, clip_image
    )

    self.assertEqual(
        dataclasses.asdict(orig_params), dataclasses.asdict(params)
    )
    self.assertEqual(returned_images.compression, params.compression)
    self.assertEqual(
        dataclasses.asdict(returned_images.metrics), expected_metrics
    )
    self.assertLen(returned_images.images, 1)
    self._rgb_image_almost_equal(
        returned_images.images[0], _get_dicom_instance_path(filename)
    )

  @parameterized.parameters([
      enum_types.Interpolation.NEAREST,
      enum_types.Interpolation.LINEAR,
      enum_types.Interpolation.CUBIC,
      enum_types.Interpolation.AREA,
      enum_types.Interpolation.LANCZOS4,
  ])
  def test_downsample_instance_interpolation(self, interp):
    instance, params = _get_jpeg_encoded_dicom_local_instance_and_params(
        dict(
            interpolation=interp, compression=_Compression.PNG, downsample=11.0
        )
    )
    interp_str = _interpolation_str(params.interpolation)
    ext = _compression_ext(params.compression)
    batch_mode = True
    decode_image_as_numpy = True
    clip_image = True
    expected_metrics = dict(
        frame_requests=instance.metadata.number_of_frames,  # process all frames
        images_transcoded=True,  # full image built from frames and recompressed
        mini_batch_requests=1,  # processed in one mini-batch
        number_of_frames_downloaded_from_store=0,
    )
    orig_params = params.copy()
    filename = f'downsample11_interp_{interp_str}.{ext}'

    returned_images = downsample_util.downsample_dicom_instance(
        instance, params, batch_mode, decode_image_as_numpy, clip_image
    )

    self.assertEqual(
        dataclasses.asdict(orig_params), dataclasses.asdict(params)
    )
    self.assertEqual(returned_images.compression, params.compression)
    self.assertEqual(
        dataclasses.asdict(returned_images.metrics), expected_metrics
    )
    self.assertLen(returned_images.images, 1)
    self._rgb_image_almost_equal(
        returned_images.images[0], _get_dicom_instance_path(filename)
    )

  @parameterized.parameters([
      enum_types.Interpolation.CUBIC,
      enum_types.Interpolation.AREA,
      enum_types.Interpolation.LANCZOS4,
  ])
  @flagsaver.flagsaver(max_mini_batch_frame_request_size=30)
  def test_downsample_instance_batching(self, interpolation):
    instance, params = _get_jpeg_encoded_dicom_local_instance_and_params(
        dict(
            interpolation=interpolation,
            compression=_Compression.PNG,
            downsample=2.0,
        )
    )
    interp_str = _interpolation_str(params.interpolation)
    ext = _compression_ext(params.compression)
    batch_mode = True
    decode_image_as_numpy = True
    clip_image = True
    expected_metrics = dict(
        frame_requests=instance.metadata.number_of_frames,  # process all frames
        images_transcoded=True,  # full image built from frames and recompressed
        mini_batch_requests=1,
        number_of_frames_downloaded_from_store=0,
    )
    orig_params = params.copy()
    filename = f'downsample2_interp_{interp_str}.{ext}'

    returned_images = downsample_util.downsample_dicom_instance(
        instance, params, batch_mode, decode_image_as_numpy, clip_image
    )

    self.assertEqual(
        dataclasses.asdict(orig_params), dataclasses.asdict(params)
    )
    self.assertEqual(returned_images.compression, params.compression)
    self.assertEqual(
        dataclasses.asdict(returned_images.metrics), expected_metrics
    )
    self.assertLen(returned_images.images, 1)
    self._rgb_image_almost_equal(
        returned_images.images[0], _get_dicom_instance_path(filename)
    )

  def test_downsample_instance_invalid_downsample_throws(self):
    instance, params = _get_jpeg_encoded_dicom_local_instance_and_params(
        dict(downsample=-1.0)
    )
    batch_mode = True
    decode_image_as_numpy = True
    clip_image = True
    with self.assertRaises(_DownsamplingFrameRequestError):
      downsample_util.downsample_dicom_instance(
          instance, params, batch_mode, decode_image_as_numpy, clip_image
      )

  @parameterized.parameters([1.0, 2.0])
  @flagsaver.flagsaver(max_mini_batch_frame_request_size=5)
  def test_get_rendered_dicom_frames_throws_frame_to_large(self, downsample):
    instance, params = _get_jpeg_encoded_dicom_local_instance_and_params(
        dict(
            interpolation=enum_types.Interpolation.AREA,
            downsample=downsample,
            compression=_Compression.JPEG,
        )
    )
    with self.assertRaises(_DownsamplingFrameRequestError):
      downsample_util.get_rendered_dicom_frames(instance, params, [99999])

  @flagsaver.flagsaver(max_mini_batch_frame_request_size=15)
  def test_downsample_dicom_web_instance(self):
    instance, params = _get_jpeg_encoded_dicom_local_instance_and_params(
        dict(
            interpolation=enum_types.Interpolation.AREA,
            compression=_Compression.PNG,
            downsample=11.0,
        )
    )
    interp_str = _interpolation_str(params.interpolation)
    ext = _compression_ext(params.compression)
    batch_mode = True
    decode_image_as_numpy = True
    clip_image = True
    expected_metrics = dict(
        frame_requests=instance.metadata.number_of_frames,  # process all frames
        images_transcoded=True,  # full image built from frames and recompressed
        mini_batch_requests=1,
        number_of_frames_downloaded_from_store=0,
    )
    orig_params = params.copy()
    filename = f'downsample11_interp_{interp_str}.{ext}'

    returned_images = downsample_util.downsample_dicom_web_instance(
        instance, params, batch_mode, decode_image_as_numpy, clip_image
    )

    self.assertEqual(
        dataclasses.asdict(orig_params), dataclasses.asdict(params)
    )
    self.assertEqual(returned_images.compression, params.compression)
    self.assertEqual(
        dataclasses.asdict(returned_images.metrics), expected_metrics
    )
    self.assertLen(returned_images.images, 1)
    self._rgb_image_almost_equal(
        returned_images.images[0], _get_dicom_instance_path(filename)
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='embed_iccprofile_true',
          embed_iccprofile=True,
      ),
      dict(
          testcase_name='embed_iccprofile_false',
          embed_iccprofile=False,
      ),
  ])
  def test_transcoded_rendered_dicom_frames(
      self,
      embed_iccprofile: bool,
  ):
    expected_embedded_icc_profile = (
        color_conversion_util._get_srgb_iccprofile()
        if embed_iccprofile
        else None
    )
    fname = 'downsample_2_index_3_interpolation_area.png'
    test_file_path = _get_downsample_util_path(fname)
    icc_profile_transform = icc_color_transform.IccColorTransform(
        proxy_const.ICCProfile.SRGB,
        mock.Mock(),
        color_conversion_util._get_srgb_iccprofile(),
    )
    params = _RenderFrameParams()
    params.compression = _Compression.JPEG
    params.quality = 95
    params.embed_iccprofile = embed_iccprofile
    with PIL.Image.open(test_file_path) as img:
      with io.BytesIO() as buffer:
        img.save(
            buffer,
            format='JPEG',
            subsampling=0,
            quality=params.quality,
            icc_profile=expected_embedded_icc_profile,
        )
        expected_output = buffer.getvalue()

    with open(test_file_path, 'rb') as infile:
      with mock.patch.object(ImageCms, 'applyTransform', autospec=True):
        transcoded_img = downsample_util._TranscodedRenderedDicomFrames(
            [infile.read()],
            icc_profile_transform,
            params,
            None,
            _Metrics(),
            shared_test_util.create_mock_dicom_instance_metadata(
                '1.2.840.10008.1.2.4.50'
            ),
        )

    self.assertLen(transcoded_img.images, 1)
    with PIL.Image.open(io.BytesIO(transcoded_img.images[0])) as img:
      self.assertEqual(
          img.info.get('icc_profile'), expected_embedded_icc_profile
      )
    self.assertTrue(
        shared_test_util.rgb_image_almost_equal(
            transcoded_img.images[0], expected_output
        )
    )

  @parameterized.parameters([
      (proxy_const.ICCProfile.SRGB, 'SRGB', proxy_const.ICCProfile.YES),
      (proxy_const.ICCProfile.ADOBERGB, 'ADOBERGB', proxy_const.ICCProfile.YES),
      (proxy_const.ICCProfile.ROMMRGB, 'ROMMRGB', proxy_const.ICCProfile.YES),
      (proxy_const.ICCProfile.SRGB, 'ADOBERGB', proxy_const.ICCProfile.SRGB),
      (proxy_const.ICCProfile.SRGB, 'ROMMRGB', proxy_const.ICCProfile.SRGB),
      (proxy_const.ICCProfile.SRGB, 'Other', proxy_const.ICCProfile.SRGB),
      (proxy_const.ICCProfile.SRGB, '', proxy_const.ICCProfile.SRGB),
      (proxy_const.ICCProfile.ADOBERGB, 'ADOBERGB', proxy_const.ICCProfile.YES),
      (
          proxy_const.ICCProfile.ADOBERGB,
          'SRGB',
          proxy_const.ICCProfile.ADOBERGB,
      ),
      (
          proxy_const.ICCProfile.ADOBERGB,
          'ROMMRGB',
          proxy_const.ICCProfile.ADOBERGB,
      ),
      (
          proxy_const.ICCProfile.ADOBERGB,
          'Other',
          proxy_const.ICCProfile.ADOBERGB,
      ),
      (proxy_const.ICCProfile.ADOBERGB, '', proxy_const.ICCProfile.ADOBERGB),
      (proxy_const.ICCProfile.ROMMRGB, 'ROMMRGB', proxy_const.ICCProfile.YES),
      (
          proxy_const.ICCProfile.ROMMRGB,
          'ADOBERGB',
          proxy_const.ICCProfile.ROMMRGB,
      ),
      (proxy_const.ICCProfile.ROMMRGB, 'SRGB', proxy_const.ICCProfile.ROMMRGB),
      (proxy_const.ICCProfile.ROMMRGB, 'Other', proxy_const.ICCProfile.ROMMRGB),
      (proxy_const.ICCProfile.ROMMRGB, '', proxy_const.ICCProfile.ROMMRGB),
      (proxy_const.ICCProfile.YES, 'Other', proxy_const.ICCProfile.YES),
      (proxy_const.ICCProfile.YES, 'SRGB', proxy_const.ICCProfile.YES),
      (proxy_const.ICCProfile.YES, 'ROMMRGB', proxy_const.ICCProfile.YES),
      (proxy_const.ICCProfile.YES, 'ADOBERGB', proxy_const.ICCProfile.YES),
      (proxy_const.ICCProfile.YES, '', proxy_const.ICCProfile.YES),
      (proxy_const.ICCProfile.NO, 'Other', proxy_const.ICCProfile.NO),
      (proxy_const.ICCProfile.NO, 'SRGB', proxy_const.ICCProfile.NO),
      (proxy_const.ICCProfile.NO, 'ROMMRGB', proxy_const.ICCProfile.NO),
      (proxy_const.ICCProfile.NO, 'ADOBERGB', proxy_const.ICCProfile.NO),
      (proxy_const.ICCProfile.NO, '', proxy_const.ICCProfile.NO),
  ])
  def test_optimize_iccprofile_transform_colorspace(
      self, requested_iccprofile, dicom_colorspace, expected
  ):
    _, params = _get_jpeg_encoded_dicom_local_instance_and_params()
    params = dataclasses.replace(params, icc_profile=requested_iccprofile)
    result = downsample_util._optimize_iccprofile_transform_colorspace(
        params, dicom_colorspace
    )
    self.assertEqual(result.icc_profile, expected)

  @parameterized.parameters([
      ('SRGB IEC61966-2.', True),
      ('SRGB', True),
      ('', False),
      ('color', False),
  ])
  def test_metadata_colorspace_equals(self, colorspace_name, expected: bool):
    self.assertEqual(
        downsample_util._metadata_colorspace_equals(colorspace_name, 'SRGB'),
        expected,
    )


if __name__ == '__main__':
  absltest.main()
