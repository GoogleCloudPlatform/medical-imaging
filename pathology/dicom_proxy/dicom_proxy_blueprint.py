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
"""Flask blueprint for DICOM Store DICOM Proxy."""

import dataclasses
import http
import os
import re
from typing import List, Mapping, Optional

import flask
import flask_compress
import requests_toolbelt

from pathology.dicom_proxy import annotations_util
from pathology.dicom_proxy import base_dicom_request_error
from pathology.dicom_proxy import bulkdata_util
from pathology.dicom_proxy import dicom_instance_request
from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import dicom_store_util
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import downsample_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import execution_timer
from pathology.dicom_proxy import flask_util
from pathology.dicom_proxy import frame_caching_util
from pathology.dicom_proxy import iccprofile_bulk_metadata_util
from pathology.dicom_proxy import logging_util
from pathology.dicom_proxy import metadata_augmentation
from pathology.dicom_proxy import metadata_util
from pathology.dicom_proxy import parameters_exceptions_and_return_types
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import render_frame_params
from pathology.dicom_proxy import sparse_dicom_util
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.build_version import build_version
from pathology.shared_libs.flags import flag_utils
from pathology.shared_libs.iap_auth_lib import auth
from pathology.shared_libs.logging_lib import cloud_logging_client


# Types
_Compression = enum_types.Compression
_DicomInstanceRequest = dicom_instance_request.DicomInstanceRequest
_DicomInstanceWebRequest = (
    parameters_exceptions_and_return_types.DicomInstanceWebRequest
)
_DownsamplingFrameRequestError = (
    parameters_exceptions_and_return_types.DownsamplingFrameRequestError
)
_ICCProfile = enum_types.ICCProfile
_Interpolation = enum_types.Interpolation
_RenderFrameParams = render_frame_params.RenderFrameParams

# Constants
_DIMENSION_ORG_ERR = (
    'The DICOM proxy does not support returning frames from '
    'DICOM instances that have DimensionalOrganizationType '
    'tag (0020,9311) != TILED_FULL'
)
_INSTANCE_IS_NOT_FULLY_TILED = 'Instance is not fully tiled.'

# Threshold for initation of preemptive DICOM instance caching.
# percentage of requested frames which were not in cache and had to be feteched
# from dicom store. Calculated as total frames retrieved from store / total
# frames requested. If this is greater than or equal to this threshold then
# frame caching is initated. This threshold only has real value if frames are
# requested in batches. Requests which get a single frame, will have
# proportions which are either 0, the frame was was returned from cache or 1
# frame was fetched from store.
_PREEMPTIVE_DICOM_INSTANCE_CACHING_THRESHOLD = 0.5


# Global
dicom_proxy = flask.Blueprint(
    'dicom_proxy',
    __name__,
    url_prefix=dicom_proxy_flags.PROXY_SERVER_URL_PATH_PREFIX
    if dicom_proxy_flags.PROXY_SERVER_URL_PATH_PREFIX
    else None,
)
compress = flask_compress.Compress()
_VALID_FRAME_CHARS = re.compile('[^0-9, ]')


def _parse_interpolation(args: Mapping[str, str]) -> _Interpolation:
  """Parses interpolation algorithm from  request args.

  Args:
    args: HTTP request args.

  Returns:
    _Interpolation

  Raises:
    ValueError: Unsupported interpolation algorithm
  """
  arg = flask_util.get_parameter_value(
      args, enum_types.TileServerHttpParams.INTERPOLATION
  )
  if arg == 'area':
    return _Interpolation.AREA  # Recommended
  if arg == 'cubic':
    return _Interpolation.CUBIC
  if arg == 'lanczos4':
    return _Interpolation.LANCZOS4
  if arg == 'linear':
    return _Interpolation.LINEAR
  if arg == 'nearest':
    return _Interpolation.NEAREST
  raise ValueError('Unsupported interpolation algorithm')


def _parse_frame_list(frames: str) -> List[int]:
  """Parses frame string into list of indexs.

  Args:
    frames: String containing comma separated values.

  Returns:
    List of frame indexes.

  Raises:
    ValueError: list contains non-int values or invalid chars.
    IndexError: list contains invalid indexes.
  """
  if _VALID_FRAME_CHARS.search(frames) is not None:
    raise ValueError(f'Frame string contains invalid chars Frames:{frames}')
  frame_indexes = [int(arg) for arg in frames.split(',')]
  if not frame_indexes:
    raise IndexError('No frame indexes.')
  if min(frame_indexes) <= 0:
    raise IndexError('Frame index <= 0 are invalid')
  return frame_indexes


def _parse_compression_quality(args: Mapping[str, str]) -> int:
  """Returns request image compression quality.

  Args:
    args: HTTP Request Args.

  Raise:
    ValueError: Invalid quality value or cannot parse quality to int.
  """
  quality = int(
      flask_util.get_parameter_value(
          args, enum_types.TileServerHttpParams.QUALITY
      )
  )
  if quality < 1 or quality > 100:
    raise ValueError('Invalid quality value')
  return quality


def _parse_iccprofile(args: Mapping[str, str]) -> _ICCProfile:
  """Returns request image iccprofile color correction.

  https://dicom.nema.org/medical/dicom/2019a/output/chtml/part18/sect_6.5.8.html

  Args:
    args: HTTP Request Args.

  Raises:
    ValueError: request is not supported.
  """
  if dicom_proxy_flags.DISABLE_ICC_PROFILE_CORRECTION_FLG.value:
    return _ICCProfile(proxy_const.ICCProfile.NO)
  return _ICCProfile(
      flask_util.get_parameter_value(
          args, enum_types.TileServerHttpParams.ICCPROFILE
      ).upper()
  )


def _get_request_compression(
    accept_header: Optional[str], instance_compression: _Compression
) -> _Compression:
  """Returns the image compression format specified in the request.

  Args:
    accept_header: Request http accept header value.
    instance_compression: Compression format of the DICOM instance.

  Returns:
    Requested compression format.

  Raises:
    ValueError: requested ccompression format is not supported.
  """
  if accept_header is None:
    return _Compression.JPEG
  accept_header = accept_header.strip().lower()
  if not accept_header or 'image/jpeg' in accept_header:
    return _Compression.JPEG
  if 'image/png' in accept_header:
    return _Compression.PNG
  if 'image/webp' in accept_header:
    return _Compression.WEBP
  if 'image/gif' in accept_header:
    return _Compression.GIF
  if '*/*' in accept_header:
    return _Compression.JPEG
  if 'image/jxl' in accept_header:
    if instance_compression == _Compression.JPEG:
      return _Compression.JPEG_TRANSCODED_TO_JPEGXL
    else:
      return _Compression.JPEGXL
  raise ValueError('Unsupported compression format.')


def _parse_embed_icc_profile(args: Mapping[str, str]) -> bool:
  """Returns False if icc_profile should not be embedded in returned images.

  Args:
    args: HTTP Request Args.

  Raises:
    Value error if parameter cannot be converted to bool.
  """
  result = flag_utils.str_to_bool(
      flask_util.get_parameter_value(
          args, enum_types.TileServerHttpParams.EMBED_ICCPROFILE
      )
  )
  return result


def _parse_request_params(
    args: Mapping[str, str],
    header: Mapping[str, str],
    instance_compression: _Compression,
) -> _RenderFrameParams:
  """Returns RenderedFrameParams initialized from HTTP request arguments.

  Args:
    args: Argument key/value mapping passed to url.  Returns initialized
      RenderedFrameParams class.
    header: Passed to HTTP request.
    instance_compression: Compression format of the DICOM instance.

  Raises:
    ValueError: Error in parameter definition.
  """
  downsample = flask_util.parse_downsample(args)
  # https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_8.3.5.html#sect_8.3.5.1.2
  jpeg_quality = _parse_compression_quality(args)
  interpolation = _parse_interpolation(args)
  # https://dicom.nema.org/medical/dicom/2019a/output/chtml/part18/sect_6.5.8.html#sect_6.5.8.1.2.5
  iccprofile = _parse_iccprofile(args)
  enable_caching = flask_util.parse_cache_enabled(args)
  embed_iccprofile = _parse_embed_icc_profile(args)
  result = _RenderFrameParams(
      downsample,
      interpolation,
      _get_request_compression(
          header.get(flask_util.ACCEPT_HEADER_KEY), instance_compression
      ),
      jpeg_quality,
      iccprofile,
      enable_caching,
      embed_iccprofile,
  )
  return result


@execution_timer.log_execution_time('_create_dicom_instance_web_request')
def _create_dicom_instance_web_request(
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL,
    study: str,
    series: str,
    instance: str,
) -> _DicomInstanceWebRequest:
  """Creates a DICOM instance web request from parsed url parameters.

  Args:
    dicom_web_base_url: Base DICOMweb URL for store.
    study: DICOM StudyInstanceUID.
    series: DICOM SeriesInstanceUID.
    instance: DICOM SOPInstanceUID.

  Returns:
    DicomInstanceWebRequest
  """
  fl_request_args = flask_util.get_dicom_proxy_request_args()
  http_header = flask_util.norm_dict_keys(
      flask_util.get_headers(), [flask_util.ACCEPT_HEADER_KEY]
  )
  return _DicomInstanceWebRequest(
      user_auth_util.AuthSession(flask_util.get_headers()),
      dicom_web_base_url,
      dicom_url_util.StudyInstanceUID(study),
      dicom_url_util.SeriesInstanceUID(series),
      dicom_url_util.SOPInstanceUID(instance),
      flask_util.parse_cache_enabled(fl_request_args),
      fl_request_args,
      http_header,
  )


def _get_rendered_frames(
    instance_request: _DicomInstanceRequest, frames: str, rendered_request: bool
) -> flask.Response:
  """Returns downsampled and/or transcoded frames from WSI DICOM instance.

  Args:
    instance_request: DicomInstanceRequest
    frames: String encoding list of frames to render.
    rendered_request: Is this a DICOM rendered frames request or frames request.

  Returns:
    Requested DICOM onstance frame images (flask.Response)
  """
  try:
    dicom_frame_indexes = _parse_frame_list(frames)
  except (ValueError, IndexError) as exp:
    cloud_logging_client.error('Exception parsing frame list.', exp)
    return flask_util.exception_flask_response(exp)
  try:
    params = _parse_request_params(
        instance_request.url_args,
        instance_request.url_header,
        instance_request.metadata.image_compression,
    )
  except ValueError as exp:
    cloud_logging_client.error('Exception parsing request params.', exp)
    return flask_util.exception_flask_response(exp)

  if (
      not instance_request.metadata.is_tiled_full
      and instance_request.metadata.number_of_frames > 1
      and params.downsample > 1.0
  ):
    cloud_logging_client.error(
        _INSTANCE_IS_NOT_FULLY_TILED,
        dataclasses.asdict(instance_request.metadata),
    )
    return flask_util.exception_flask_response(_DIMENSION_ORG_ERR)

  try:
    result = downsample_util.get_rendered_dicom_frames(
        instance_request, params, dicom_frame_indexes
    )
  except _DownsamplingFrameRequestError as exp:
    cloud_logging_client.error('Error occurred getting rendered frame.', exp)
    return flask_util.exception_flask_response(exp)
  except base_dicom_request_error.BaseDicomRequestError as exp:
    cloud_logging_client.error('Error occurred getting rendered frame.', exp)
    return exp.flask_response()

  request_metric_dict = result.metrics.str_dict()
  execution_timer.log_message_to_execution_time_stack(
      f'Request metrics: {request_metric_dict}'
  )

  frame_requests_from_store = float(
      result.metrics.number_of_frames_downloaded_from_store
  )
  total_frame_requests = float(result.metrics.frame_requests)
  if (
      frame_requests_from_store / total_frame_requests
  ) >= _PREEMPTIVE_DICOM_INSTANCE_CACHING_THRESHOLD:
    frame_caching_util.cache_instance_frames_in_background(
        instance_request, dicom_frame_indexes
    )
  else:
    frame_caching_util.cleanup_preemptive_cache_loading_set()
  if rendered_request and len(result.images) == 1:
    return flask.Response(
        response=result.images[0],
        headers={'Content-Type': result.content_type},
        direct_passthrough=True,
    )
  fields = []
  for index, frame_image_bytes in enumerate(result.images):
    frame_index = str(dicom_frame_indexes[index])
    frame_content_type = result.multipart_content_type
    fields.append((frame_index, (None, frame_image_bytes, frame_content_type)))
  multipart_data = requests_toolbelt.MultipartEncoder(fields=fields)
  return flask.Response(
      response=multipart_data.read(),
      headers={
          'Content-Type': (
              f'multipart/related; boundary={multipart_data.boundary_value}'
          )
      },
      direct_passthrough=True,
  )


def _missing_series_uid_redirect(
    url_base: dicom_url_util.DicomWebBaseURL,
    study_instance_uid: str,
    sop_instance_uid: str,
    prefix: str,
) -> flask.Response:
  """Redirects requests with study and sop instance uid and missing series uid.

  HTTP redirects were added to enable the proxy to correct non-standard DICOM
  rendered frame and rendered instance requests that are missing a series
  instance uid but have valid study instance uid and sop instance uid. Madcap
  generates malformed requests when a imaging is requested by study instance UID
  alone. The code here improves the behavior by enabling the the proxy to
  correctly handle these requests.

  Args:
    url_base: DicomWebBaseURL.
    study_instance_uid: DICOM instance study instance UID.
    sop_instance_uid: DICOM instance SOP instance UID.
    prefix: Prefix to append at end of redirect.

  Returns:
    Series Instance UID, and if series instance UID was discoverd then the
    DICOM web path to the instance, if the series instance uid

  Raises:
    metadata_util.GetSeriesInstanceUIDError: An error occured while searching
    for the Series Instance UID.
  """
  headers = user_auth_util.AuthSession(flask_util.get_headers())
  study_instance_uid = dicom_url_util.StudyInstanceUID(study_instance_uid)
  sop_instance_uid = dicom_url_util.SOPInstanceUID(sop_instance_uid)
  try:
    s_uid = metadata_util.get_series_instance_uid_for_study_and_instance_uid(
        headers, url_base, study_instance_uid, sop_instance_uid
    )
  except metadata_util.GetSeriesInstanceUIDError as exp:
    cloud_logging_client.debug(
        'Could not determine series instance uid; proxying request.',
        exp,
    )
    return dicom_store_util.dicom_store_proxy()
  url = bulkdata_util.get_bulk_data_base_url(url_base)
  params = flask_util.get_parameters()
  return flask.redirect(
      f'{url}/{study_instance_uid}/series/{s_uid}/{sop_instance_uid}'
      f'{prefix}{params}',
      http.HTTPStatus.MOVED_PERMANENTLY,
  )


def _get_frames_instance_generic(
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL,
    study_instance_uid: str,
    series_instance_uid: str,
    sop_instance_uid: str,
    framelist: str,
    is_rendered_request: bool,
) -> flask.Response:
  """Returns rendered (downsampled) frames from DICOM instance.

  Args:
    dicom_web_base_url: Base DICOMweb URL for store.
    study_instance_uid: DICOM study instance UID.
    series_instance_uid: DICOM series instance UID.
    sop_instance_uid: DICOM sop instance UID.
    framelist: Comma separated value string listing of frames to return.
    is_rendered_request: Is DICOMweb rendered request or frames request.

  Returns:
    flask.Response returning rendered frame request.
  """
  instance_request = _create_dicom_instance_web_request(
      dicom_web_base_url,
      study_instance_uid,
      series_instance_uid,
      sop_instance_uid,
  )
  try:
    if not instance_request.metadata.is_wsi_iod:
      cloud_logging_client.debug(
          'Proxying rendered instance request instance is not VL Whole Slide'
          ' Microscopy Image SOPClassUID.',
          dataclasses.asdict(instance_request.metadata),
      )
      return dicom_store_util.dicom_store_proxy()
    rendered_frame = _get_rendered_frames(
        instance_request, framelist, is_rendered_request
    )
    # Test if single frame is being requested, if yes tell client to cache
    # response.
    if ',' in framelist:
      rendered_frame.headers['Cache-Control'] = (
          f'max-age={dicom_proxy_flags.CACHE_CONTROL_TTL_RENDERED_FRAMES_FLG.value}'
      )
    return rendered_frame
  except metadata_util.ReadDicomMetadataError as exp:
    cloud_logging_client.error('Reading DICOM metadata.', exp)
    return flask_util.exception_flask_response(exp)


def _get_masked_flask_headers() -> Mapping[str, str]:
  return logging_util.mask_privileged_header_values(flask_util.get_headers())


@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_INSTANCE_URL_DEFAULT_VERSION}/frames/<string:framelist>/rendered',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_get_frame_instance',
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION,
        'is_rendered_request': True,
    },
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_INSTANCE_URL_DEFAULT_VERSION}/frames/<string:framelist>',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_get_frame_instance',
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION,
        'is_rendered_request': False,
    },
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_INSTANCE_URL}/frames/<string:framelist>/rendered',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_get_frame_instance',
    defaults={'is_rendered_request': True},
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_INSTANCE_URL}/frames/<string:framelist>',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_get_frame_instance',
    defaults={'is_rendered_request': False},
    strict_slashes=False,
)
@logging_util.log_exceptions
@execution_timer.log_execution_time('_get_frame_instance')
@auth.validate_iap
@metadata_util.ClearLocalMetadata()
def _get_frame_instance(
    store_api_version: str,
    projectid: str,
    location: str,
    datasetid: str,
    dicomstore: str,
    study_instance_uid: str,
    series_instance_uid: str,
    sop_instance_uid: str,
    framelist: str,
    is_rendered_request: bool,
) -> flask.Response:
  """Flask entry point for DICOMweb frame request that returns frame instances.

  Args:
    store_api_version: Healthcare API DICOM store version.
    projectid: GCP projectid.
    location: Location of healthcare api dataset.
    datasetid: Healthcare API dataset ID.
    dicomstore: DICOM store to connect to.
    study_instance_uid: DICOM study instance UID.
    series_instance_uid: DICOM series instance UID.
    sop_instance_uid: DICOM sop instance UID.
    framelist: Comma separated value string listing of frames to return.
    is_rendered_request: True if rendered frame request.

  Returns:
    Requested DICOM onstance frame images (flask.Response)
  """
  return _get_frames_instance_generic(
      dicom_url_util.DicomWebBaseURL(
          store_api_version, projectid, location, datasetid, dicomstore
      ),
      study_instance_uid,
      series_instance_uid,
      sop_instance_uid,
      framelist,
      is_rendered_request,
  )


@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_STUDY_URL_DEFAULT_VERSION}/instances/<string:sop_instance_uid>/frames/<string:framelist>/rendered',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_correct_frame_instance_missing_series_query',
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION,
        'is_rendered_request': True,
    },
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_STUDY_URL_DEFAULT_VERSION}/instances/<string:sop_instance_uid>/frames/<string:framelist>',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_correct_frame_instance_missing_series_query',
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION,
        'is_rendered_request': False,
    },
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_STUDY_URL}/instances/<string:sop_instance_uid>/frames/<string:framelist>/rendered',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_redirect_frame_instance_missing_series_query',
    defaults={'is_rendered_request': True},
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_STUDY_URL}/instances/<string:sop_instance_uid>/frames/<string:framelist>',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_redirect_frame_instance_missing_series_query',
    defaults={'is_rendered_request': False},
    strict_slashes=False,
)
@logging_util.log_exceptions
@execution_timer.log_execution_time(
    '_redirect_frame_instance_missing_series_query'
)
@auth.validate_iap
@metadata_util.ClearLocalMetadata()
def _redirect_frame_instance_missing_series_query(
    store_api_version: str,
    projectid: str,
    location: str,
    datasetid: str,
    dicomstore: str,
    study_instance_uid: str,
    sop_instance_uid: str,
    framelist: str,
    is_rendered_request: bool,
) -> flask.Response:
  """Flask entry point for DICOMweb frame request that returns frame instances.

  Args:
    store_api_version: Healthcare API DICOM store version.
    projectid: GCP projectid.
    location: Location of healthcare api dataset.
    datasetid: Healthcare API dataset ID.
    dicomstore: DICOM store to connect to.
    study_instance_uid: DICOM study instance UID.
    sop_instance_uid: DICOM sop instance UID.
    framelist: Comma separated value string listing of frames to return.
    is_rendered_request: True if rendered frame request.

  Returns:
    Requested DICOM onstance frame images (flask.Response)
  """
  prefix = f'/frames/{framelist}'
  if is_rendered_request:
    prefix = f'{prefix}/rendered'
  return _missing_series_uid_redirect(
      dicom_url_util.DicomWebBaseURL(
          store_api_version, projectid, location, datasetid, dicomstore
      ),
      study_instance_uid,
      sop_instance_uid,
      prefix,
  )


def _rendered_wsi_instance(
    instance_request: _DicomInstanceRequest,
) -> flask.Response:
  """Returns rendered WSI DICOM instance optionally downsampled.

  Args:
    instance_request: DicomInstanceRequest

  Returns:
    Requested DICOM onstance frame images (flask.Response)
  """
  if instance_request.metadata.number_of_frames == 1:
    return _get_rendered_frames(instance_request, '1', True)
  try:
    params = _parse_request_params(
        instance_request.url_args,
        instance_request.url_header,
        instance_request.metadata.image_compression,
    )
  except ValueError as exp:
    cloud_logging_client.error('Exception parsing request params.', exp)
    return flask_util.exception_flask_response(exp)
  try:
    result = downsample_util.downsample_dicom_web_instance(
        instance_request,
        params,
        batch_mode=True,
        decode_image_as_numpy=True,
        clip_image_dim=True,
    )
  except _DownsamplingFrameRequestError as exp:
    cloud_logging_client.error('Error occurred getting rendered instance.', exp)
    return flask_util.exception_flask_response(exp)
  except base_dicom_request_error.BaseDicomRequestError as exp:
    cloud_logging_client.error('Error occurred getting rendered instance.', exp)
    return exp.flask_response()
  return flask.Response(
      response=result.images[0],
      headers={
          'Content-Type': result.content_type,
          'Cache-Control': (
              f'max-age={dicom_proxy_flags.CACHE_CONTROL_TTL_RENDERED_FRAMES_FLG.value}'
          ),
      },
  )


@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_INSTANCE_URL_DEFAULT_VERSION}/rendered',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_rendered_instance',
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION
    },
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_INSTANCE_URL}/rendered',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_rendered_instance',
    strict_slashes=False,
)
@logging_util.log_exceptions
@execution_timer.log_execution_time('_rendered_instance')
@auth.validate_iap
@metadata_util.ClearLocalMetadata()
def _rendered_instance(
    store_api_version: str,
    projectid: str,
    location: str,
    datasetid: str,
    dicomstore: str,
    study_instance_uid: str,
    series_instance_uid: str,
    sop_instance_uid: str,
) -> flask.Response:
  """Flask entry point for DICOMweb rendered instance request.

  Args:
    store_api_version: Healthcare API DICOM store version.
    projectid: GCP projectid.
    location: Location of healthcare api dataset.
    datasetid: Healthcare API dataset ID.
    dicomstore: DICOM store to connect to.
    study_instance_uid: DICOM study instance UID.
    series_instance_uid: DICOM series instance UID.
    sop_instance_uid: DICOM sop instance UID.

  Returns:
    Requested DICOM instance frame images (flask.Response)
  """
  cloud_logging_client.info(
      'External DICOM rendered instance entrypoint',
      {
          proxy_const.LogKeywords.HEALTHCARE_API_DICOM_VERSION: (
              store_api_version
          ),
          proxy_const.LogKeywords.PROJECT_ID: projectid,
          proxy_const.LogKeywords.LOCATION: location,
          proxy_const.LogKeywords.DATASET_ID: datasetid,
          proxy_const.LogKeywords.DICOM_STORE: dicomstore,
          proxy_const.LogKeywords.STUDY_INSTANCE_UID: study_instance_uid,
          proxy_const.LogKeywords.SERIES_INSTANCE_UID: series_instance_uid,
          proxy_const.LogKeywords.SOP_INSTANCE_UID: sop_instance_uid,
      },
      _get_masked_flask_headers(),
  )
  try:
    instance_request = _create_dicom_instance_web_request(
        dicom_url_util.DicomWebBaseURL(
            store_api_version, projectid, location, datasetid, dicomstore
        ),
        study_instance_uid,
        series_instance_uid,
        sop_instance_uid,
    )
    if not instance_request.metadata.is_wsi_iod:
      cloud_logging_client.debug(
          'Proxying rendered instance request instance is not VL Whole Slide'
          ' Microscopy Image SOPClassUID.',
          dataclasses.asdict(instance_request.metadata),
      )
      return dicom_store_util.dicom_store_proxy()
    if (
        instance_request.metadata.is_tiled_sparse
        and (
            instance_request.metadata.total_pixel_matrix_columns
            != instance_request.metadata.columns
            or instance_request.metadata.total_pixel_matrix_rows
            != instance_request.metadata.rows
        )
    ) or (
        not instance_request.metadata.is_tiled_full
        and instance_request.metadata.number_of_frames > 1
    ):
      cloud_logging_client.warning(
          _INSTANCE_IS_NOT_FULLY_TILED,
          dataclasses.asdict(instance_request.metadata),
      )
      return dicom_store_util.dicom_store_proxy()
    return _rendered_wsi_instance(instance_request)
  except metadata_util.ReadDicomMetadataError as exp:
    cloud_logging_client.error('Reading DICOM metadata.', exp)
    return flask_util.exception_flask_response(exp)


@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_STUDY_URL_DEFAULT_VERSION}/instances/<string:sop_instance_uid>/rendered',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_redirect_render_instance_missing_series_query',
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION,
    },
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_STUDY_URL}/instances/<string:sop_instance_uid>/rendered',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_redirect_render_instance_missing_series_query',
    strict_slashes=False,
)
@logging_util.log_exceptions
@execution_timer.log_execution_time(
    '_redirect_render_instance_missing_series_query'
)
@auth.validate_iap
@metadata_util.ClearLocalMetadata()
def _redirect_render_instance_missing_series_query(
    store_api_version: str,
    projectid: str,
    location: str,
    datasetid: str,
    dicomstore: str,
    study_instance_uid: str,
    sop_instance_uid: str,
) -> flask.Response:
  """Flask entry point for DICOMweb rendered instance request.

  Args:
    store_api_version: Healthcare API DICOM store version.
    projectid: GCP projectid.
    location: Location of healthcare api dataset.
    datasetid: Healthcare API dataset ID.
    dicomstore: DICOM store to connect to.
    study_instance_uid: DICOM study instance UID.
    sop_instance_uid: DICOM sop instance UID.

  Returns:
    Requested DICOM instance frame images (flask.Response)
  """
  return _missing_series_uid_redirect(
      dicom_url_util.DicomWebBaseURL(
          store_api_version, projectid, location, datasetid, dicomstore
      ),
      study_instance_uid,
      sop_instance_uid,
      '/rendered',
  )


def _get_sop_instance_uid_param_value() -> str:
  return flask_util.get_key_value(
      flask_util.get_first_key_args(), 'SOPInstanceUID', ''
  )


@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_SERIES_URL_DEFAULT_VERSION}/instances',
    methods=flask_util.GET_AND_POST_METHODS,
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION,
    },
    endpoint='_instances_search',
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_STUDY_URL_DEFAULT_VERSION}/instances',
    methods=flask_util.GET_AND_POST_METHODS,
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION,
        'series_instance_uid': '',
    },
    endpoint='_instances_search',
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_BASE_URL_DEFAULT_VERSION}/instances',
    methods=flask_util.GET_AND_POST_METHODS,
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION,
        'study_instance_uid': '',
        'series_instance_uid': '',
    },
    endpoint='_instances_search',
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_SERIES_URL}/instances',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_instances_search',
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_STUDY_URL}/instances',
    methods=flask_util.GET_AND_POST_METHODS,
    defaults={
        'series_instance_uid': '',
    },
    endpoint='_instances_search',
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_BASE_URL}/instances',
    methods=flask_util.GET_AND_POST_METHODS,
    defaults={
        'study_instance_uid': '',
        'series_instance_uid': '',
    },
    endpoint='_instances_search',
    strict_slashes=False,
)
@logging_util.log_exceptions
@execution_timer.log_execution_time('_instances_search')
@auth.validate_iap
@compress.compressed()
@metadata_util.ClearLocalMetadata()
def _instances_search(
    store_api_version: str,
    projectid: str,
    location: str,
    datasetid: str,
    dicomstore: str,
    study_instance_uid: str,
    series_instance_uid: str,
) -> flask.Response:
  """Flask entry point for DICOMweb instance metadata search request.

  Args:
    store_api_version: Healthcare API DICOM store version.
    projectid: GCP projectid.
    location: Location of healthcare api dataset.
    datasetid: Healthcare API dataset ID.
    dicomstore: DICOM store to connect to.
    study_instance_uid: DICOM Study Instance UID to return metadata.
    series_instance_uid: DICOM Series Instance UID to return metadata.

  Returns:
    Flask response containing dicom metadata (optionally downsampled).
  """
  if study_instance_uid and series_instance_uid:
    sop_instance_uid = _get_sop_instance_uid_param_value()
  else:
    sop_instance_uid = ''
  cloud_logging_client.info(
      'External DICOM instance search entrypoint',
      {
          proxy_const.LogKeywords.HEALTHCARE_API_DICOM_VERSION: (
              store_api_version
          ),
          proxy_const.LogKeywords.PROJECT_ID: projectid,
          proxy_const.LogKeywords.LOCATION: location,
          proxy_const.LogKeywords.DATASET_ID: datasetid,
          proxy_const.LogKeywords.DICOM_STORE: dicomstore,
          proxy_const.LogKeywords.STUDY_INSTANCE_UID: study_instance_uid,
          proxy_const.LogKeywords.SERIES_INSTANCE_UID: series_instance_uid,
          proxy_const.LogKeywords.SOP_INSTANCE_UID: sop_instance_uid,
      },
      _get_masked_flask_headers(),
  )
  dicom_web_base_url = dicom_url_util.DicomWebBaseURL(
      store_api_version, projectid, location, datasetid, dicomstore
  )
  # Sparse WSI DICOM encodes the position of each frame within the per-frame
  # functional group sequence. If this sequence is to large the Google DICOM
  # store (V1 API & V1Beta1 API) will not return the contents of the sequence.
  # The only way to retrieve the  metadata is to retireve the whole instance.
  # Sparse DICOM is identified by encoding the value "TILED_SPARSE" in
  # the root DICOM tag keyword DimensionalOrganizationType. The proxy will
  # check and correct metadata responses for sparse WSI DICOM for missing per-
  # frame functional group sequence. To determine if a DICOM is sparse the
  # response must include the DimensionalOrganizationType in the response.
  # Metadata queries will always return this. Instance tag queres will not
  # unless the instance query is perfromed with the parameter includefield=all
  # or includefield=DimensionalOrganizationType or includefield=00209311. The
  # code here tests if an instance query is requesting the
  # PerFrameFunctionalGroupSequence and the DimensionalOrganizationType. Queries
  # requesting PerFrameFunctionalGroupSequence but not the
  # DimensionalOrganizationType are augmented to include a
  # DimensionalOrganizationType metadata request.
  if (
      sparse_dicom_util.do_includefields_request_perframe_functional_group_seq()
      and not sparse_dicom_util.do_includefields_request_dimensional_organization_type()
  ):
    additional_parameters = [
        f'includefield={sparse_dicom_util.DIMENSIONAL_ORGANIZATION_TYPE_DICOM_TAG}'
    ]
  else:
    additional_parameters = None
  return metadata_augmentation.augment_instance_metadata(
      dicom_web_base_url,
      dicom_url_util.StudyInstanceUID(study_instance_uid),
      dicom_url_util.SeriesInstanceUID(series_instance_uid),
      dicom_url_util.SOPInstanceUID(sop_instance_uid),
      dicom_store_util.dicom_store_proxy(params=additional_parameters),
  )


@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_STUDY_URL_DEFAULT_VERSION}/series',
    methods=flask_util.GET_AND_POST_METHODS,
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION,
    },
    endpoint='_series_search',
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_BASE_URL_DEFAULT_VERSION}/series',
    methods=flask_util.GET_AND_POST_METHODS,
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION,
        'study_instance_uid': '',
    },
    endpoint='_series_search',
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_STUDY_URL}/series',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_series_search',
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_BASE_URL}/series',
    methods=flask_util.GET_AND_POST_METHODS,
    defaults={
        'study_instance_uid': '',
    },
    endpoint='_series_search',
    strict_slashes=False,
)
@logging_util.log_exceptions
@execution_timer.log_execution_time('_studysearch')
@auth.validate_iap
@compress.compressed()
@metadata_util.ClearLocalMetadata()
def _series_search(
    store_api_version: str,
    projectid: str,
    location: str,
    datasetid: str,
    dicomstore: str,
    study_instance_uid: str,
) -> flask.Response:
  """Flask entry point for DICOMweb study metadata search request.

  Args:
    store_api_version: Healthcare API DICOM store version.
    projectid: GCP projectid.
    location: Location of healthcare api dataset.
    datasetid: Healthcare API dataset ID.
    dicomstore: DICOM store to connect to.
    study_instance_uid: DICOM Study Instance UID to return metadata.

  Returns:
    Flask response containing dicom metadata.
  """
  cloud_logging_client.info(
      'External DICOM study search entrypoint',
      {
          proxy_const.LogKeywords.HEALTHCARE_API_DICOM_VERSION: (
              store_api_version
          ),
          proxy_const.LogKeywords.PROJECT_ID: projectid,
          proxy_const.LogKeywords.LOCATION: location,
          proxy_const.LogKeywords.DATASET_ID: datasetid,
          proxy_const.LogKeywords.DICOM_STORE: dicomstore,
      },
      _get_masked_flask_headers(),
  )
  dicom_web_base_url = dicom_url_util.DicomWebBaseURL(
      store_api_version, projectid, location, datasetid, dicomstore
  )
  return metadata_augmentation.augment_series_response_metadata(
      dicom_web_base_url,
      dicom_url_util.StudyInstanceUID(study_instance_uid),
      dicom_store_util.dicom_store_proxy(),
  )


@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_BASE_URL_DEFAULT_VERSION}/studies',
    methods=flask_util.GET_AND_POST_METHODS,
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION,
    },
    endpoint='_studies_search',
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_BASE_URL}/studies',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_studies_search',
    strict_slashes=False,
)
@logging_util.log_exceptions
@execution_timer.log_execution_time('_studysearch')
@auth.validate_iap
@compress.compressed()
@metadata_util.ClearLocalMetadata()
def _studies_search(
    store_api_version: str,
    projectid: str,
    location: str,
    datasetid: str,
    dicomstore: str,
) -> flask.Response:
  """Flask entry point for DICOMweb study metadata search request.

  Args:
    store_api_version: Healthcare API DICOM store version.
    projectid: GCP projectid.
    location: Location of healthcare api dataset.
    datasetid: Healthcare API dataset ID.
    dicomstore: DICOM store to connect to.

  Returns:
    Flask response containing dicom metadata.
  """
  cloud_logging_client.info(
      'External DICOM study search entrypoint',
      {
          proxy_const.LogKeywords.HEALTHCARE_API_DICOM_VERSION: (
              store_api_version
          ),
          proxy_const.LogKeywords.PROJECT_ID: projectid,
          proxy_const.LogKeywords.LOCATION: location,
          proxy_const.LogKeywords.DATASET_ID: datasetid,
          proxy_const.LogKeywords.DICOM_STORE: dicomstore,
      },
      _get_masked_flask_headers(),
  )
  dicom_web_base_url = dicom_url_util.DicomWebBaseURL(
      store_api_version, projectid, location, datasetid, dicomstore
  )
  return metadata_augmentation.augment_study_response_metadata(
      dicom_web_base_url,
      dicom_store_util.dicom_store_proxy(),
  )


@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_INSTANCE_URL_DEFAULT_VERSION}/metadata',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_metadata_search',
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION
    },
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_SERIES_URL_DEFAULT_VERSION}/metadata',
    methods=flask_util.GET_AND_POST_METHODS,
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION,
        'sop_instance_uid': '',
    },
    endpoint='_metadata_search',
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_STUDY_URL_DEFAULT_VERSION}/metadata',
    methods=flask_util.GET_AND_POST_METHODS,
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION,
        'series_instance_uid': '',
        'sop_instance_uid': '',
    },
    endpoint='_metadata_search',
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_BASE_URL_DEFAULT_VERSION}/metadata',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_metadata_search',
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION,
        'study_instance_uid': '',
        'series_instance_uid': '',
        'sop_instance_uid': '',
    },
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_INSTANCE_URL}/metadata',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_metadata_search',
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_SERIES_URL}/metadata',
    methods=flask_util.GET_AND_POST_METHODS,
    defaults={'sop_instance_uid': ''},
    endpoint='_metadata_search',
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_STUDY_URL}/metadata',
    methods=flask_util.GET_AND_POST_METHODS,
    defaults={
        'series_instance_uid': '',
        'sop_instance_uid': '',
    },
    endpoint='_metadata_search',
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_BASE_URL}/metadata',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_metadata_search',
    defaults={
        'study_instance_uid': '',
        'series_instance_uid': '',
        'sop_instance_uid': '',
    },
    strict_slashes=False,
)
@logging_util.log_exceptions
@execution_timer.log_execution_time('_metadata_search')
@auth.validate_iap
@compress.compressed()
@metadata_util.ClearLocalMetadata()
def _metadata_search(
    store_api_version: str,
    projectid: str,
    location: str,
    datasetid: str,
    dicomstore: str,
    study_instance_uid: str,
    series_instance_uid: str,
    sop_instance_uid: str,
) -> flask.Response:
  """Flask entry point for DICOMweb instance metadata search request.

  Args:
    store_api_version: Healthcare API DICOM store version.
    projectid: GCP projectid.
    location: Location of healthcare api dataset.
    datasetid: Healthcare API dataset ID.
    dicomstore: DICOM store to connect to.
    study_instance_uid: DICOM Study Instance UID to return metadata.
    series_instance_uid: DICOM Series Instance UID to return metadata.
    sop_instance_uid: DICOM SOP Instance UID to return metadata.

  Returns:
    Flask response containing dicom metadata (optionally downsampled).
  """
  cloud_logging_client.info(
      'External DICOM metadata search entrypoint',
      {
          proxy_const.LogKeywords.HEALTHCARE_API_DICOM_VERSION: (
              store_api_version
          ),
          proxy_const.LogKeywords.PROJECT_ID: projectid,
          proxy_const.LogKeywords.LOCATION: location,
          proxy_const.LogKeywords.DATASET_ID: datasetid,
          proxy_const.LogKeywords.DICOM_STORE: dicomstore,
          proxy_const.LogKeywords.STUDY_INSTANCE_UID: study_instance_uid,
          proxy_const.LogKeywords.SERIES_INSTANCE_UID: series_instance_uid,
          proxy_const.LogKeywords.SOP_INSTANCE_UID: sop_instance_uid,
      },
      _get_masked_flask_headers(),
  )
  dicom_web_base_url = dicom_url_util.DicomWebBaseURL(
      store_api_version, projectid, location, datasetid, dicomstore
  )
  return metadata_augmentation.augment_instance_metadata(
      dicom_web_base_url,
      dicom_url_util.StudyInstanceUID(study_instance_uid),
      dicom_url_util.SeriesInstanceUID(series_instance_uid),
      dicom_url_util.SOPInstanceUID(sop_instance_uid),
      dicom_store_util.dicom_store_proxy(),
  )


@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_BASE_URL_DEFAULT_VERSION}/<path:path>',
    methods=flask_util.GET_POST_AND_DELETE_METHODS,
    endpoint='_general_path',
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION
    },
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_BASE_URL}/<path:path>',
    methods=flask_util.GET_POST_AND_DELETE_METHODS,
    endpoint='_general_path',
)
@logging_util.log_exceptions
@execution_timer.log_execution_time('_general_path')
@auth.validate_iap
@metadata_util.ClearLocalMetadata()
def _general_path(
    store_api_version: str,
    projectid: str,
    location: str,
    datasetid: str,
    dicomstore: str,
    path: str,
) -> flask.Response:
  """Flask entry point for DICOM web request to proxy.

  Args:
    store_api_version: Healthcare API DICOM store version.
    projectid: GCP projectid.
    location: Location of healthcare api dataset.
    datasetid: Healthcare API dataset ID.
    dicomstore: DICOM store to connect to.
    path: DICOM store web request.

  Returns:
    Requested DICOM onstance frame images (flask.Response)
  """
  cloud_logging_client.debug(
      'External DICOM proxy entrypoint',
      {
          proxy_const.LogKeywords.HEALTHCARE_API_DICOM_VERSION: (
              store_api_version
          ),
          proxy_const.LogKeywords.PROJECT_ID: projectid,
          proxy_const.LogKeywords.LOCATION: location,
          proxy_const.LogKeywords.DATASET_ID: datasetid,
          proxy_const.LogKeywords.DICOM_STORE: dicomstore,
          proxy_const.LogKeywords.DICOMWEB_URL: path,
      },
      _get_masked_flask_headers(),
  )
  return dicom_store_util.dicom_store_proxy()


def _dicom_instance_icc_profile_bulkdata(
    instance_request: _DicomInstanceRequest,
) -> flask.Response:
  """Return DICOM instance ICCProfile bulkdata.

  Args:
    instance_request: DICOM instance requesting ICCProfile bulkdata.

  Returns:
    ICCProfile bulkdata.
  """
  try:
    # validate the user can read metadata for instance before returning.
    _ = instance_request.metadata
    icc_profile_bytes = instance_request.icc_profile()
  except metadata_util.ReadDicomMetadataError as exp:
    cloud_logging_client.error('Reading DICOM metadata.', exp)
    return flask_util.exception_flask_response(exp)
  if icc_profile_bytes is None:
    cloud_logging_client.error('ICCProfile is not found.')
    return flask_util.exception_flask_response('ICCProfile is not found.')
  multipart_data = requests_toolbelt.MultipartEncoder(
      fields=[(
          'icc_profile.icc',
          (
              None,
              icc_profile_bytes,
              'application/octet-stream',
          ),
      )]
  )
  return flask.Response(
      response=multipart_data.read(),
      headers={
          'Content-Type': (
              f'multipart/related; boundary={multipart_data.boundary_value}'
          )
      },
      direct_passthrough=True,
  )


@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_INSTANCE_URL_DEFAULT_VERSION}/{bulkdata_util.PROXY_BULK_DATA_URI}/<path:path>',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_get_bulkdata',
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION
    },
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_INSTANCE_URL}/{bulkdata_util.PROXY_BULK_DATA_URI}/<path:path>',
    methods=flask_util.GET_AND_POST_METHODS,
    endpoint='_get_bulkdata',
)
@logging_util.log_exceptions
@execution_timer.log_execution_time('_get_bulkdata')
@auth.validate_iap
@metadata_util.ClearLocalMetadata()
def _get_bulkdata(
    store_api_version: str,
    projectid: str,
    location: str,
    datasetid: str,
    dicomstore: str,
    study_instance_uid: str,
    series_instance_uid: str,
    sop_instance_uid: str,
    path: str,
) -> flask.Response:
  """Flask entry point for ICCProfile bulkdata request.

  Args:
    store_api_version: Healthcare API DICOM store version.
    projectid: GCP projectid.
    location: Location of healthcare api dataset.
    datasetid: Healthcare API dataset ID.
    dicomstore: DICOM store to connect to.
    study_instance_uid: DICOM study instance UID.
    series_instance_uid: DICOM series instance UID.
    sop_instance_uid: DICOM sop instance UID.
    path: Path to instance bulk data.

  Returns:
    Proxy response or ICCProfile bulkdata.
  """
  cloud_logging_client.info(
      'External bulkdata retrieval entrypoint',
      {
          proxy_const.LogKeywords.HEALTHCARE_API_DICOM_VERSION: (
              store_api_version
          ),
          proxy_const.LogKeywords.PROJECT_ID: projectid,
          proxy_const.LogKeywords.LOCATION: location,
          proxy_const.LogKeywords.DATASET_ID: datasetid,
          proxy_const.LogKeywords.DICOM_STORE: dicomstore,
          proxy_const.LogKeywords.STUDY_INSTANCE_UID: study_instance_uid,
          proxy_const.LogKeywords.SERIES_INSTANCE_UID: series_instance_uid,
          proxy_const.LogKeywords.SOP_INSTANCE_UID: sop_instance_uid,
          proxy_const.LogKeywords.PATH_TO_BULK_DATA: path,
      },
      _get_masked_flask_headers(),
  )
  dicom_web_base_url = dicom_url_util.DicomWebBaseURL(
      store_api_version, projectid, location, datasetid, dicomstore
  )
  if bulkdata_util.does_dicom_store_support_bulkdata(dicom_web_base_url):
    cloud_logging_client.debug(
        'DICOM store supports bulkdata proxying request.'
    )
    return dicom_store_util.dicom_store_proxy()
  instance_request = _create_dicom_instance_web_request(
      dicom_web_base_url,
      study_instance_uid,
      series_instance_uid,
      sop_instance_uid,
  )
  if path.endswith(iccprofile_bulk_metadata_util.ICCPROFILE_DICOM_TAG_KEYWORD):
    cloud_logging_client.debug('Performing ICC profile bulkdata retreval.')
    return _dicom_instance_icc_profile_bulkdata(instance_request)
  cloud_logging_client.debug('Performing bulkdata retrieval using proxy.')
  return dicom_store_util.dicom_store_proxy()


@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_BASE_URL_DEFAULT_VERSION}/studies/<string:study_instance_uid>',
    methods=flask_util.POST_METHOD,
    endpoint='_store_annotation_instance',
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION
    },
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_BASE_URL_DEFAULT_VERSION}/studies',
    methods=flask_util.POST_METHOD,
    endpoint='_store_annotation_instance',
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION,
        'study_instance_uid': '',
    },
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_BASE_URL}/studies/<string:study_instance_uid>',
    methods=flask_util.POST_METHOD,
    endpoint='_store_annotation_instance',
    strict_slashes=False,
)
@dicom_proxy.route(
    f'{dicom_url_util.DICOM_WEB_BASE_URL}/studies',
    methods=flask_util.POST_METHOD,
    endpoint='_store_annotation_instance',
    defaults={'study_instance_uid': ''},
    strict_slashes=False,
)
@auth.validate_iap
@logging_util.log_exceptions
@metadata_util.ClearLocalMetadata()
def _store_annotation_instance(
    store_api_version: str,
    projectid: str,
    location: str,
    datasetid: str,
    dicomstore: str,
    study_instance_uid: str = '',
) -> flask.Response:
  """Flask entry point for DICOMweb request to store an annotation instance.

  Args:
    store_api_version: Healthcare API DICOM store version.
    projectid: GCP projectid.
    location: Location of healthcare api dataset.
    datasetid: Healthcare API dataset ID.
    dicomstore: DICOM store to connect to.
    study_instance_uid: Optional study instance uid

  Returns:
    flask.Response
  """
  cloud_logging_client.info(
      'Externalstore DICOM instance entrypoint',
      {
          proxy_const.LogKeywords.HEALTHCARE_API_DICOM_VERSION: (
              store_api_version
          ),
          proxy_const.LogKeywords.PROJECT_ID: projectid,
          proxy_const.LogKeywords.LOCATION: location,
          proxy_const.LogKeywords.DATASET_ID: datasetid,
          proxy_const.LogKeywords.DICOM_STORE: dicomstore,
          proxy_const.LogKeywords.STUDY_INSTANCE_UID: study_instance_uid,
      },
      _get_masked_flask_headers(),
  )
  if dicom_proxy_flags.ENABLE_ANNOTATIONS_ENDPOINT_FLG.value:
    return annotations_util.store_instance(
        dicom_url_util.DicomWebBaseURL(
            store_api_version, projectid, location, datasetid, dicomstore
        ),
        study_instance_uid,
    )
  cloud_logging_client.debug('Performing instance store using proxy.')
  return dicom_store_util.dicom_store_proxy()


@dicom_proxy.route(
    dicom_url_util.DICOM_WEB_INSTANCE_URL_DEFAULT_VERSION,
    methods=flask_util.DELETE_METHOD,
    endpoint='_delete_annotation_instance',
    defaults={
        'store_api_version': dicom_proxy_flags.DEFAULT_DICOM_STORE_API_VERSION
    },
    strict_slashes=False,
)
@dicom_proxy.route(
    dicom_url_util.DICOM_WEB_INSTANCE_URL,
    methods=flask_util.DELETE_METHOD,
    endpoint='_delete_annotation_instance',
    strict_slashes=False,
)
@auth.validate_iap
@logging_util.log_exceptions
@metadata_util.ClearLocalMetadata()
def _delete_annotation_instance(
    store_api_version: str,
    projectid: str,
    location: str,
    datasetid: str,
    dicomstore: str,
    study_instance_uid: str,
    series_instance_uid: str,
    sop_instance_uid: str,
) -> flask.Response:
  """Flask entry point for DICOMweb request to delete annotations.

  Deletes all annotations for a user on a slide.

  Args:
    store_api_version: Healthcare API DICOM store version.
    projectid: GCP projectid.
    location: Location of healthcare api dataset.
    datasetid: Healthcare API dataset ID.
    dicomstore: DICOM store to connect to.
    study_instance_uid: Study uid of instance to delete.
    series_instance_uid: Series uid of instance to delete.
    sop_instance_uid: Instance uid of instance to delete.

  Returns:
    flask.Response
  """
  cloud_logging_client.info(
      'External delete DICOM instance entrypoint.',
      {
          proxy_const.LogKeywords.HEALTHCARE_API_DICOM_VERSION: (
              store_api_version
          ),
          proxy_const.LogKeywords.PROJECT_ID: projectid,
          proxy_const.LogKeywords.LOCATION: location,
          proxy_const.LogKeywords.DATASET_ID: datasetid,
          proxy_const.LogKeywords.DICOM_STORE: dicomstore,
          proxy_const.LogKeywords.STUDY_INSTANCE_UID: study_instance_uid,
          proxy_const.LogKeywords.SERIES_INSTANCE_UID: series_instance_uid,
          proxy_const.LogKeywords.SOP_INSTANCE_UID: sop_instance_uid,
      },
      _get_masked_flask_headers(),
  )
  if dicom_proxy_flags.ENABLE_ANNOTATIONS_ENDPOINT_FLG.value:
    return annotations_util.delete_instance(
        dicom_url_util.DicomWebBaseURL(
            store_api_version, projectid, location, datasetid, dicomstore
        ),
        study_instance_uid,
        series_instance_uid,
        sop_instance_uid,
    )
  cloud_logging_client.debug('Performing instance delete using proxy.')
  return dicom_store_util.dicom_store_proxy()


build_version.init_cloud_logging_build_version()

# Required to initialize build version in logs of forked child processes.
os.register_at_fork(
    after_in_child=build_version.init_cloud_logging_build_version
)
