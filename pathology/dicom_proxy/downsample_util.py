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
"""Util that generates transcoded and/or downsampled frame images."""
import collections
import dataclasses
import math
import os
import tempfile
import typing
from typing import Dict, Iterator, List, Optional, Union

import numpy as np

from pathology.dicom_proxy import color_conversion_util
from pathology.dicom_proxy import dicom_image_coordinate_util
from pathology.dicom_proxy import dicom_instance_frame_patch_util
from pathology.dicom_proxy import dicom_instance_request
from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import frame_retrieval_util
from pathology.dicom_proxy import icc_color_transform
from pathology.dicom_proxy import image_util
from pathology.dicom_proxy import metadata_util
from pathology.dicom_proxy import parameters_exceptions_and_return_types
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import render_frame_params
from pathology.shared_libs.logging_lib import cloud_logging_client

# Types
_DicomInstanceFramePatch = (
    dicom_instance_frame_patch_util.DicomInstanceFramePatch
)
_IccColorTransform = icc_color_transform.IccColorTransform
_RenderedDicomFrames = (
    parameters_exceptions_and_return_types.RenderedDicomFrames
)
_RenderFrameParams = render_frame_params.RenderFrameParams
_LocalDicomInstance = parameters_exceptions_and_return_types.LocalDicomInstance
_DicomInstanceRequest = dicom_instance_request.DicomInstanceRequest
_DownsamplingFrameRequestError = (
    parameters_exceptions_and_return_types.DownsamplingFrameRequestError
)
_Metrics = parameters_exceptions_and_return_types.Metrics
_FrameImages = frame_retrieval_util.FrameImages

_ERROR_RETRIEVING_DICOM_FRAMES = 'Error occurred retrieving DICOM frame(s).'


@dataclasses.dataclass
class _MiniBatch:
  """Represents one mini-batch frame request.

  Attributes:
    frame_patches: List of downsampled image patches (describes coordinates in
      and required frames in source imaging)
    required_frame_indexes: Dict frame indexes in source imaging that are
      required to generate all patches(images) in the mini-batch.
  """

  frame_patches: List[_DicomInstanceFramePatch]
  required_frame_indexes: Dict[int, None]


@dataclasses.dataclass
class _MiniBatchFrameImages:
  """DICOM frames retrieved and still needed to generate images in mini-batch.

  Attributes:
    frames_retrieved: Frame images retrieved.
    frame_indexes_to_get: List of frame indexes in source imaging not defined in
      frames_retrieved that need to be retrieved.
  """

  frames_retrieved: Optional[_FrameImages]
  frame_indexes_to_get: List[int]


class _TranscodedRenderedDicomFrames(_RenderedDicomFrames):
  """Return type for transcoded requests to get_rendered_dicom_frame."""

  def __init__(
      self,
      frames: List[Union[bytes, _DicomInstanceFramePatch]],
      icc_profile_transform: Optional[_IccColorTransform],
      params: _RenderFrameParams,
      source_frames: Optional[frame_retrieval_util.FrameImages],
      metrics: _Metrics,
      source_dicom_metadata: metadata_util.DicomInstanceMetadata,
  ):
    """Constructor.

    Args:
      frames: List of compressed images (bytes) or downsampled frame patches.
        Images in order requested.
      icc_profile_transform: color profile transform to apply to imaging.
      params: Parameters defining downsampled rendered frame requests.
      source_frames: Source frame imaging used to construct images from
        downsampled patches.  None of the frames is List[bytes].
      metrics: metrics to measure implementation
      source_dicom_metadata: Metadata for source DICOM.
    """
    if source_frames is None:
      is_frame_patch = False
      decoded_source_frames = None
    else:
      is_frame_patch = True
      # Images decoded using OpenCV into BGR ordering
      decoded_source_frames = {
          num: image_util.decode_image_bytes(
              img, source_dicom_metadata.dicom_transfer_syntax
          )
          for num, img in source_frames.images.items()
      }

    encoded_images = []
    for image in frames:
      if is_frame_patch:
        image = typing.cast(
            _DicomInstanceFramePatch, image
        ).get_downsampled_image(decoded_source_frames)
      elif (
          icc_profile_transform is None
          and source_dicom_metadata.is_jpg_transcoded_to_jpegxl
          and params.compression == enum_types.Compression.JPEG
      ):
        # transcoding JPEGXL derieved from JPEG to JPEG with no icc profile
        # correction
        try:
          encoded_images.append(image_util.transcode_jpxl_to_jpeg(image))
          continue
        except image_util.JpegxlToJpegTranscodeError:
          image = image_util.decode_image_bytes(
              image, source_dicom_metadata.dicom_transfer_syntax
          )
      elif (
          icc_profile_transform is None
          and source_dicom_metadata.is_baseline_jpeg
          and (
              params.compression == enum_types.Compression.JPEGXL
              or params.compression
              == enum_types.Compression.JPEG_TRANSCODED_TO_JPEGXL
          )
      ):
        # transcoding JPEG to JPEGXL with no icc profile correction
        encoded_images.append(image_util.transcode_jpeg_to_jpxl(image))
        continue
      else:
        image = image_util.decode_image_bytes(
            image, source_dicom_metadata.dicom_transfer_syntax
        )
      if icc_profile_transform is None:
        icc_profile = None
      else:
        image = color_conversion_util.transform_image_color(
            image, icc_profile_transform
        )
        icc_profile = icc_profile_transform.rendered_icc_profile
      encoded_images.append(
          image_util.encode_image(
              image,
              params.compression,
              params.quality,
              icc_profile if params.embed_iccprofile else None,
          )
      )
    super().__init__(encoded_images, params, metrics)


def _metadata_colorspace_equals(
    instance_icc_profile_color_space: str, name: str
) -> bool:
  return name in instance_icc_profile_color_space.strip().upper().split(' ')


def _optimize_iccprofile_transform_colorspace(
    params: _RenderFrameParams, instance_icc_profile_color_space: str
) -> _RenderFrameParams:
  """If DICOM allready in desired color space, skips transform.

  Args:
    params: Downsampling rendering parameters.
    instance_icc_profile_color_space: String representation if color space in
      DICOM instance metadata.

  Returns:
    Downsampling rendering parameters adjusted based on DICOM colorspace.
  """
  if _metadata_colorspace_equals(
      instance_icc_profile_color_space, params.icc_profile.upper()
  ):
    return dataclasses.replace(params, icc_profile=proxy_const.ICCProfile.YES)
  return params


def _get_minibatch(
    minibatch_index: int,
    dicom_frame_indexes: list[int],
    dicom_instance_source: _DicomInstanceRequest,
    params: _RenderFrameParams,
    img_filter: image_util.GaussianImageFilter,
    downsample_metadata: metadata_util.DicomInstanceMetadata,
) -> _MiniBatch:
  """Returns next mini-batch of images to downsample.

     Returns mini-batches of frames (N <=  _MAX_MINI_BATCH_FRAME_REQUEST_SIZE)

  Args:
    minibatch_index: Starting index in the frame list to begin generating the
      mini-batch.
    dicom_frame_indexes: Downsampled frame indexes to generate.
    dicom_instance_source: DICOM source for downsampled imaging.
    params: Downsampling parameters.
    img_filter: Image filter to apply before downsampling to reduce aliasing.
    downsample_metadata: Metadata for the downsampled instance.

  Returns:
    _MiniBatch

  Raises:
    DownsamplingFrameRequestError: Number of frames in source imaging
                                   required to generate a single downsampled
                                   image exceeds value of
                                   MAX_MINI_BATCH_FRAME_REQUEST_SIZE_FLG.
  """
  frame_patches = []
  required_frame_indexes = collections.OrderedDict()
  frame_index_count = len(dicom_frame_indexes)

  # scaling require to transform between
  # source dimensions and downsampled dimensions != params.downsample
  # if frame dimensions are not perfect multiples. Ensures source patches
  # fully sample source imaging
  downsampled_frame_coord_scale_factor = (
      dicom_instance_frame_patch_util.get_downsampled_frame_coord_scale_factor(
          dicom_instance_source.metadata, downsample_metadata
      )
  )

  while minibatch_index < frame_index_count:
    downsample_frame_pixel_coord = (
        dicom_image_coordinate_util.get_instance_frame_pixel_coordinates(
            downsample_metadata, dicom_frame_indexes[minibatch_index]
        )
    )
    frame_patch = _DicomInstanceFramePatch(
        downsample_frame_pixel_coord,
        params.downsample,
        downsampled_frame_coord_scale_factor,
        dicom_instance_source.metadata,
        params.interpolation,
        img_filter,
    )
    patch_frame_count = len(frame_patch.frame_indexes)
    if (
        patch_frame_count
        > dicom_proxy_flags.MAX_MINI_BATCH_FRAME_REQUEST_SIZE_FLG.value
    ):
      raise _DownsamplingFrameRequestError(
          'Number of frames required to create a single downsampled frame '
          'exceeds max number of frames supported by the DICOM Proxy. '
          'Reduce frame downsampling. Downsample requires '
          f'{patch_frame_count} source frames; Max mini-batch request size: '
          f'{dicom_proxy_flags.MAX_MINI_BATCH_FRAME_REQUEST_SIZE_FLG.value}.'
      )
    # if worst case estimate for total size of required frames exceeds
    # limit process existing batch
    if (
        patch_frame_count + len(required_frame_indexes)
        > dicom_proxy_flags.MAX_MINI_BATCH_FRAME_REQUEST_SIZE_FLG.value
    ):
      break
    frame_patches.append(frame_patch)

    # multiple frames are being requested only get ones which haven't
    # already been requested in mini-batch
    for fi in frame_patch.frame_indexes:
      required_frame_indexes[fi] = None
    minibatch_index += 1
  return _MiniBatch(frame_patches, required_frame_indexes)


def _create_minibatch_frame_request(
    required_frame_indexes: Iterator[int],
    previous_batch: Optional[_MiniBatchFrameImages],
) -> _MiniBatchFrameImages:
  """Creates a mini-batch frame request.

  Args:
    required_frame_indexes: List of frames required from source imaging to
      generate downsampled patches.
    previous_batch: Previous mini-batch. If defined will bring any previously
      retrieved required frames forward into next mini-batch to reduce the
      number of frames which need to be retrieved for the returned mini-batch.

  Returns:
    _MiniBatchFrameImages
  """

  if previous_batch is None or previous_batch.frames_retrieved is None:
    return _MiniBatchFrameImages(None, list(required_frame_indexes))

  # if frames previously retrieved in preceding mini-batch.
  # scan frames list for frames requested in mini-batch and carry previously
  # retrieved and required frames forward into next mini-batch.
  required_retrieved_images = {}
  indexes_required = []
  for fi in required_frame_indexes:
    frame_found = previous_batch.frames_retrieved.images.get(fi)
    if frame_found is None:
      indexes_required.append(fi)
    else:
      required_retrieved_images[fi] = frame_found
  previous_batch.frames_retrieved.images = required_retrieved_images
  previous_batch.frame_indexes_to_get = indexes_required
  return previous_batch


def _get_missing_minibatch_frames(
    dicom_instance_source: _DicomInstanceRequest,
    params: _RenderFrameParams,
    frame_request: _MiniBatchFrameImages,
    metrics: _Metrics,
) -> None:
  """Retrieves mini-batch frames that have not been retrieved.

  Args:
    dicom_instance_source: DICOM instance to retrieve frames from.
    params: Parameters to use.
    frame_request: Mini-batch frame request.
    metrics: Metrics to measure implementation.

  Returns:
    None

  Raises:
    DownsamplingFrameRequestError: Error occurred retrieving DICOM frame(s).
    DicomFrameRequestError: Error occurred retrieving DICOM frame(s).
  """
  if not frame_request.frame_indexes_to_get:
    return
  metrics.frame_requests += len(frame_request.frame_indexes_to_get)
  try:
    source_frames = dicom_instance_source.get_dicom_frames(
        params, frame_request.frame_indexes_to_get
    )
  except frame_retrieval_util.BaseFrameRetrievalError as exp:
    raise _DownsamplingFrameRequestError(
        _ERROR_RETRIEVING_DICOM_FRAMES
    ) from exp
  metrics.number_of_frames_downloaded_from_store += (
      source_frames.number_of_frames_downloaded_from_store
  )
  if frame_request.frames_retrieved is None:
    frame_request.frames_retrieved = source_frames
  else:
    frame_request.frames_retrieved.images.update(source_frames.images)
  frame_request.frame_indexes_to_get = []


def _get_max_downsample(metadata: metadata_util.DicomInstanceMetadata) -> float:
  """Returns maximum downsampling factor for DICOM instance metadata."""
  return float(
      max(metadata.total_pixel_matrix_columns, metadata.total_pixel_matrix_rows)
  )


def _get_frames_no_downsampling(
    frame_indexes: List[int],
    dicom_instance_source: _DicomInstanceRequest,
    params: _RenderFrameParams,
    icc_profile_transform: Optional[_IccColorTransform],
    metrics: _Metrics,
) -> _RenderedDicomFrames:
  """Returns DICOM frames without downsampling, optionally transcoded.

  Args:
    frame_indexes: List of frame indexes to retrieve from source instance.
    dicom_instance_source: DICOM instance to retrieve frames from.
    params: Parameters to use.
    icc_profile_transform: ICC profile transform to apply to frames.
    metrics: Metrics to measure implementation.

  Returns:
    _RenderedDicomFrames (encoded frame imaging bytes and description of
                          encoding)
  Raises:
    DownsamplingFrameRequestError: Error occurred retrieving DICOM frame(s).
    DicomFrameRequestError: Error occurred retrieving DICOM frame(s).
  """
  dedup_frame_indexes = list(
      collections.OrderedDict.fromkeys(frame_indexes).keys()
  )
  metrics.frame_requests += len(dedup_frame_indexes)
  try:
    source_frames = dicom_instance_source.get_dicom_frames(
        params, dedup_frame_indexes
    )
  except frame_retrieval_util.BaseFrameRetrievalError as exp:
    raise _DownsamplingFrameRequestError(
        _ERROR_RETRIEVING_DICOM_FRAMES
    ) from exp
  result = [
      source_frames.images[dicom_frame_index]
      for dicom_frame_index in frame_indexes
  ]
  metrics.number_of_frames_downloaded_from_store += (
      source_frames.number_of_frames_downloaded_from_store
  )
  if (
      params.compression == source_frames.compression
      and icc_profile_transform is None
  ):
    metrics.images_transcoded = False
    return _RenderedDicomFrames(result, params, metrics)
  # transcode images
  metrics.images_transcoded = True
  return _TranscodedRenderedDicomFrames(
      result,
      icc_profile_transform,
      params,
      None,
      metrics,
      dicom_instance_source.metadata,
  )


def get_rendered_dicom_frames(
    dicom_instance_source: _DicomInstanceRequest,
    params: _RenderFrameParams,
    dicom_frame_indexes: List[int],
) -> _RenderedDicomFrames:
  """Returns downsampled and/or transcoded WSI frame imaging.

  Function accepts a list of downsampled frames and performs downsampling using
  params defined parameters. Frame retrieval is conducted in parallel and
  utilizes extensive caching to reduce DICOM store transactions both within and
  across requests.

  RenderedFrameParams.downsample < 1 implies zooming into imaging and is not
  supported.

  If RenderedFrameParams.downsample == 1 frames will be retrieved in parallel,
  transcoded as needed, but not resized. Number of frames returned will equal
  the number of frames requested.

  If RenderedFrameParams.downsample > 1 implies imaging downsampling, e.g. 2 =
  50% reduction. Downsampled frame generation requires retrieval of multiple
  higher magnification frames (4 - 16). Higher level frames are first identified
  and de-duplicated.  The set of required frames is then retrieved. If the set
  of frames exceeds the servers _MAX_FRAME_BATCH_REQUEST_SIZE_FLG, then the
  request will be split into mini-batches. This may cause duplicate store
  transactions. Frame retrieval is cached (LRU_, so it is possible that frames
  across non-consecutive mini-batches may not require retrieval from the store.
  If this occurs the compressed frame imaging require duplicate decompression.
  The set of required frames not in memory is requested in parallel and
  decompressed to RGB for downsampled image generation.

  Transcoding
  Image transcoding is supported for (JPEG, WEBP, PNG, GIF). JPEG is a native
  format supported by DICOM. If the DICOM is encoded in JPEG and requested in
  JPEG then the image will be returned without transcoding for higher quality
  and performance.

  ICC_Color profile correction requires image transcoding.

  Security
  All requests perform an uncached metadata transaction against the DICOM store
  to retrieve the instance metadata required for downsampling. This operation
  also functions to validate that the requesting user has read access to the
  DICOM store. Optimally, if all other store requests are cached then this will
  be the only transaction the user performs against the store.

  Limitations

  * Total number of frames requests <= _MAX_NUMBER_OF_FRAMES_PER_REQUEST_FLG

    Ramifications
    It may require multiple tile-server transactions to request frames.
    Total number of frames requested in a mini-batch <=
    _MAX_MINI_BATCH_FRAME_REQUEST_SIZE_FLG

  * Total number of frames requested in a mini-batch <=
  _MAX_MINI_BATCH_FRAME_REQUEST_SIZE_FLG (Applies to requests with downsampling
  factor > 1.0)

    Ramifications
    1) By default _MAX_MINI_BATCH_FRAME_REQUEST_SIZE_FLG = 500 server should
    support downsamples up to ~16x for images of any size. By default, images
    with less than 500 will support downsampling by an factor, e.g. 32x or 100x.

    2) Performance will drop as the number of mini-batches required to fulfill
    the request increases.

  Assumptions

    The LRU caching and limits are based on frame numbers. It is assumed frame
    dimensions will be relatively consistent across the DICOM (200 - 512 pixels
    square). DICOM Proxy memory requirements will increase as frame size
    increases.

  Recommendations

    Source DICOM: Source DICOM instance for frame generation should optimally be
                   the instance which requires the least downsampling.  Example,
                   if the DICOM store has a 40x, and 10x instance stored and
                   tile representing a 5x instance is desired then 2x down
                   sample from the 10x should be requested.

    Frame Request: Batch frame requests will improve performance even if
                   the request is internally broken into multiple mini-batches.
    Compression: JPEG compression (Improved performance for JPEG encoded files)
    Downsampling Algorithm: AREA = Recommended for quality and speed.

  Args:
    dicom_instance_source: DICOM instance being returned (DICOMweb or Local).
    params: Parameters for downsampling and transcoding
    dicom_frame_indexes: List of frame indexs to return.  Frame index’s relative
      to downsampled imaging dimensions. First frame # = 1

  Returns:
    RenderedDicomFrames (list of generated frame imaging and compression format
    that the images are encoded in).

  Raises:
    DownsamplingFrameRequestError: Invalid downsampling frame request.
  """
  metrics = _Metrics()
  if not dicom_frame_indexes:
    raise _DownsamplingFrameRequestError('No frames requested.')
  requested_count = len(dicom_frame_indexes)
  if (
      dicom_proxy_flags.MAX_NUMBER_OF_FRAMES_PER_REQUEST_FLG.value
      < requested_count
  ):
    raise _DownsamplingFrameRequestError(
        'Number of frames requested exceeds max number of frames that can be'
        f' returned in one batch; Requested frame count: {requested_count}; Max'
        ' supported:'
        f' {dicom_proxy_flags.MAX_NUMBER_OF_FRAMES_PER_REQUEST_FLG.value}.'
    )
  if params.downsample < 1.0:
    raise _DownsamplingFrameRequestError(
        'DICOM Proxy does not support downsampling factors less than 1.0; '
        f'downsampling factor requested: {params.downsample}.'
    )

  if min(dicom_frame_indexes) <= 0:
    raise _DownsamplingFrameRequestError(
        'Requesting frame # < 1; '
        f'Frame numbers requested: {dicom_frame_indexes}'
    )

  # cannot downsample smaller than 1 x 1 pixel
  params.downsample = min(
      params.downsample, _get_max_downsample(dicom_instance_source.metadata)
  )

  #  Uncached metadata request
  #  functions as a test that a user has read access to dicom store.
  downsample_metadata = dicom_instance_source.metadata.downsample(
      params.downsample
  )

  if max(dicom_frame_indexes) > downsample_metadata.number_of_frames:
    msg = (
        'Requesting frame # > metadata number of frames; '
        f'Number of frames: {downsample_metadata.number_of_frames} '
        f'Frame numbers requested: {dicom_frame_indexes}'
    )
    cloud_logging_client.error(
        msg,
        {
            'downsample_factor': params.downsample,
            'source_instance_metadata': dataclasses.asdict(
                dicom_instance_source.metadata
            ),
            'downsampled_instance_metadata': dataclasses.asdict(
                downsample_metadata
            ),
        },
    )
    raise _DownsamplingFrameRequestError(msg)
  params = _optimize_iccprofile_transform_colorspace(
      params, dicom_instance_source.icc_profile_color_space()
  )
  try:
    icc_profile_transform = dicom_instance_source.icc_profile_transform(
        params.icc_profile
    )
  except color_conversion_util.UnableToLoadIccProfileError as exp:
    raise _DownsamplingFrameRequestError(str(exp)) from exp
  dicom_frame_indexes = [index - 1 for index in dicom_frame_indexes]
  if params.downsample == 1.0:
    return _get_frames_no_downsampling(
        dicom_frame_indexes,
        dicom_instance_source,
        params,
        icc_profile_transform,
        metrics,
    )

  img_filter = image_util.GaussianImageFilter(
      params.downsample, params.interpolation
  )
  # if necessary batch frame requests into mini-batches to control total number
  # of source imaging frames in memory at one time.
  downsample_image_list = []
  minibatch_start_index = 0
  minibatch_frame_request: Optional[_MiniBatchFrameImages] = None
  while minibatch_start_index < len(dicom_frame_indexes):
    minibatch = _get_minibatch(
        minibatch_start_index,
        dicom_frame_indexes,
        dicom_instance_source,
        params,
        img_filter,
        downsample_metadata,
    )
    minibatch_start_index += len(minibatch.frame_patches)

    minibatch_frame_request = _create_minibatch_frame_request(
        iter(minibatch.required_frame_indexes.keys()), minibatch_frame_request
    )
    _get_missing_minibatch_frames(
        dicom_instance_source, params, minibatch_frame_request, metrics
    )
    transcoded_frames = _TranscodedRenderedDicomFrames(
        minibatch.frame_patches,
        icc_profile_transform,
        params,
        minibatch_frame_request.frames_retrieved,
        metrics,
        dicom_instance_source.metadata,
    )

    metrics.mini_batch_requests += 1
    downsample_image_list.extend(transcoded_frames.images)
  metrics.images_transcoded = True
  return _RenderedDicomFrames(downsample_image_list, params, metrics)


def downsample_dicom_instance(
    dicom_instance: _DicomInstanceRequest,
    params: _RenderFrameParams,
    batch_mode: bool = False,
    decode_image_as_numpy: bool = True,
    clip_image_dim: bool = True,
) -> _RenderedDicomFrames:
  """Downsample the entire DICOM instance and return as image.

  Function will require large amounts of memory for high magnification imaging.

  Args:
    dicom_instance: DICOM instance to downsample.
    params: Parameters to use.
    batch_mode: Flag indicates if all imaging frames should be requested in one
      batched transaction. Minimizes duplication of frame decoding computational
      effort at the expense of memory. False, recommend for instances with large
      frame numbers.
    decode_image_as_numpy: If true imaging is not encoded at the end of
      get_rendered_dicom_frames (True = optimization). False models calls to
      get_rendered_dicom_frames.
    clip_image_dim: Clip dimensions to dimensions of downsampled image.

  Returns:
    _RenderedDicomFrames encoding whole instance as a single frame image.
  """
  if params.downsample < 1.0:
    raise _DownsamplingFrameRequestError(
        'DICOM Proxy does not support downsampling factors less than 1.0; '
        f'downsampling factor requested: {params.downsample}.'
    )
  params = _optimize_iccprofile_transform_colorspace(
      params, dicom_instance.icc_profile_color_space()
  )
  orig_params = params  # original unmodified params.
  if decode_image_as_numpy:
    params = params.copy()  # copy params to preserve parameter values
    params.compression = enum_types.Compression.NUMPY
  ds_meta = dicom_instance.metadata.downsample(params.downsample)
  width = ds_meta.total_pixel_matrix_columns
  height = ds_meta.total_pixel_matrix_rows
  tile_width = ds_meta.columns
  tile_height = ds_meta.rows
  frame_width_count = int(math.ceil(float(width) / float(tile_width)))
  frame_height_count = int(math.ceil(float(height) / float(tile_height)))
  ds_img_width = frame_width_count * tile_width
  ds_img_height = frame_height_count * tile_height
  instance_image = None

  image_list = None
  if batch_mode:
    batch_frame_block = []
    max_frame_count = frame_height_count * frame_width_count
  else:
    max_frame_count = None
    batch_frame_block = None

  framenumber = 1
  py = 0
  py_end = tile_height
  return_metrics = _Metrics()
  for _ in range(frame_height_count):
    px = 0
    px_end = tile_width
    for _ in range(frame_width_count):
      if batch_mode:
        if (
            not batch_frame_block
            or framenumber < batch_frame_block[0]
            or framenumber > batch_frame_block[-1]
        ):
          batch_frame_block = list(
              range(
                  framenumber,
                  min(
                      framenumber
                      + dicom_proxy_flags.MAX_NUMBER_OF_FRAMES_PER_REQUEST_FLG.value,
                      max_frame_count + 1,
                  ),
              )
          )
          rendered_frames = get_rendered_dicom_frames(
              dicom_instance, params, batch_frame_block
          )
          return_metrics.add_metrics(rendered_frames.metrics)
          image_list = rendered_frames.images
          # request frames in reverse order.  Rendered frames poped from list.
          image_list.reverse()
          del rendered_frames
        undecoded_image = image_list.pop()  # pytype: disable=attribute-error
      else:
        rendered_frame = get_rendered_dicom_frames(
            dicom_instance, params, [framenumber]
        )
        return_metrics.add_metrics(rendered_frame.metrics)
        undecoded_image = rendered_frame.images[0]
        del rendered_frame
      if params.compression == enum_types.Compression.NUMPY:
        decoded_image = typing.cast(np.ndarray, undecoded_image)
      elif params.compression == enum_types.Compression.RAW:
        decoded_image = np.ndarray(
            (tile_height, tile_width, 3), dtype=np.uint8, buffer=undecoded_image
        )
      else:
        decoded_image = image_util.decode_image_bytes(
            undecoded_image, dicom_instance.metadata
        )
      if instance_image is None:
        instance_image = np.zeros(
            (
                ds_img_height,
                ds_img_width,
                decoded_image.shape[-1],
            ),  # type: ignore
            dtype=np.uint8,
        )
      instance_image[py:py_end, px:px_end] = decoded_image[...]
      del undecoded_image, decoded_image
      framenumber += 1
      px = px_end
      px_end += tile_width
    py = py_end
    py_end += tile_height
  if clip_image_dim:
    # crop image to actual image dimensions
    instance_image = instance_image[:height, :width, ...]
  icc_profile_transform = dicom_instance.icc_profile_transform(
      orig_params.icc_profile
  )
  if icc_profile_transform is None:
    icc_profile = None
  else:
    icc_profile = icc_profile_transform.rendered_icc_profile
  return_metrics.images_transcoded = True  # all images re-compressed here.
  return _RenderedDicomFrames(
      [
          image_util.encode_image(
              instance_image,
              orig_params.compression,  # type: ignore
              orig_params.quality,
              icc_profile if orig_params.embed_iccprofile else None,
          )
      ],
      orig_params,
      return_metrics,
  )


def downsample_dicom_web_instance(
    dicom_instance: _DicomInstanceRequest,
    params: _RenderFrameParams,
    batch_mode: bool = False,
    decode_image_as_numpy: bool = True,
    clip_image_dim: bool = True,
) -> _RenderedDicomFrames:
  """Downsample the entire DICOM instance and return as image.

  Tests metadata to determine if DICOM appears to be encoded as Baseline JPG
    Yes: Downloads instance to container and generates image locally.
    No:  Generates downsampled frames via rendered frame requests against store.

  Args:
    dicom_instance: DICOM Store instance to downsample.
    params: Parameters to use.
    batch_mode: Flag indicates if all imaging frames should be requested in one
      batched transaction. Minimizes duplication of frame decoding computational
      effort at the expense of memory. False, recommend for instances with large
      frame numbers.
    decode_image_as_numpy: If true imaging is not encoded at the end of
      get_rendered_dicom_frames (True = optimization). False models calls to
      get_rendered_dicom_frames.
    clip_image_dim: Clip dimensions to dimensions of downsampled image.

  Returns:
    _RenderedDicomFrames encoding whole instance as a single frame image.
  """
  if not (
      dicom_instance.metadata.is_baseline_jpeg
      or dicom_instance.metadata.is_jpeg2000
      or dicom_instance.metadata.is_jpegxl
  ):
    return downsample_dicom_instance(
        dicom_instance,
        params,
        batch_mode,
        decode_image_as_numpy,
        clip_image_dim,
    )
  with tempfile.TemporaryDirectory() as temp_dir:
    path = os.path.join(temp_dir, 'instance.dcm')
    dicom_instance.download_instance(path)
    return downsample_dicom_instance(
        _LocalDicomInstance(path),
        params,
        batch_mode,
        decode_image_as_numpy,
        clip_image_dim,
    )
