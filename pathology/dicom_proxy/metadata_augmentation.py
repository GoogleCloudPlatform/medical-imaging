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
"""Augment instance search and instance metadata responses."""
from collections.abc import Callable
from concurrent import futures
import functools
import http
import json
from typing import Any, Iterator, List, Mapping, MutableMapping, Sequence, Set, Union

import flask

from pathology.dicom_proxy import annotations_util
from pathology.dicom_proxy import bulkdata_util
from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import dicom_store_util
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import flask_util
from pathology.dicom_proxy import metadata_util
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import sparse_dicom_util
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.logging_lib import cloud_logging_client

# DICOM Tags
_RETRIEV_URL_TAG = '00081190'
_INSTANCE_AVAILABILITY_TAG = '00080056'
_NUMBER_OF_STUDY_RELATED_INSTANCES_TAG = '00201208'
_NUMBER_OF_STUDY_RELATED_SERIES_TAG = '00201206'
_MODALITIES_IN_STUDY_TAG = '00080061'
_STUDY_INSTANCE_UID_TAG = '0020000D'
_SERIES_INSTANCE_UID_TAG = '0020000E'
_MODALITY_TAG = '00080060'

# DICOM METRICs Keys
_INSTANCE_COUNT = 'instanceCount'
_SERIES_COUNT = 'seriesCount'

_DICOM_JSON_CONTENT_TYPE = 'dicom+json'


class _InstanceMetadataDecodeError(Exception):
  """Error occured decodeing instance metadata response."""


def _stream_metadata_response(
    instance_metadata_list: Union[Sequence[Union[str, futures.Future[str]]],],
) -> Iterator[str]:
  """Stream metadata response."""
  buffer = ['[']
  buffer_len = 1
  if instance_metadata_list:
    max_chunk_size = (
        dicom_proxy_flags.DICOM_INSTANCE_DOWNLOAD_STREAMING_CHUNKSIZE_FLG.value
    )
    metadata_written = False
    for instance_metadata in instance_metadata_list:
      if metadata_written:
        buffer.append(',')
        buffer_len += 1
      metadata_written = False
      if not isinstance(instance_metadata, str):
        instance_metadata = instance_metadata.result()
      metadata_size = len(instance_metadata)
      if not metadata_size:
        continue
      start = 0
      while start < metadata_size:
        read_size = min(
            metadata_size - start, max(0, max_chunk_size - buffer_len)
        )
        buffer.append(instance_metadata[start : start + read_size])
        buffer_len += read_size
        start += read_size
        if buffer_len >= max_chunk_size:
          yield ''.join(buffer)
          buffer_len = 0
          buffer.clear()
      metadata_written = True
  buffer.append(']')
  yield ''.join(buffer)


def _is_response_encoded_with_dicom_json_content_type(
    response: flask.Response,
) -> bool:
  return _DICOM_JSON_CONTENT_TYPE in response.content_type.lower()


def _encode_tag(vr: str, value: List[Any]) -> Mapping[str, Any]:
  return dict(vr=vr, Value=value)


def _add_retrieve_url_to_metadata(
    json_metadata: MutableMapping[str, Any], url: str
) -> None:
  json_metadata[_RETRIEV_URL_TAG] = _encode_tag('UR', [url])


def _add_instance_online_metadata(
    json_metadata: MutableMapping[str, Any],
) -> None:
  if _INSTANCE_AVAILABILITY_TAG not in json_metadata:
    json_metadata[_INSTANCE_AVAILABILITY_TAG] = _encode_tag('CS', ['ONLINE'])


def _add_instance_count_metadata(
    json_metadata: MutableMapping[str, Any], instance_count: int
) -> None:
  json_metadata[_NUMBER_OF_STUDY_RELATED_INSTANCES_TAG] = _encode_tag(
      'IS', [instance_count]
  )


def _has_instance_count_metadata(
    json_metadata: MutableMapping[str, Any],
) -> bool:
  return _NUMBER_OF_STUDY_RELATED_INSTANCES_TAG in json_metadata


def _add_series_count_metadata(
    json_metadata: MutableMapping[str, Any], series_count: int
) -> None:
  json_metadata[_NUMBER_OF_STUDY_RELATED_SERIES_TAG] = _encode_tag(
      'IS', [series_count]
  )


def _has_series_count_metadata(
    json_metadata: MutableMapping[str, Any],
) -> bool:
  return _NUMBER_OF_STUDY_RELATED_SERIES_TAG in json_metadata


def _add_series_modalites_metadata(
    json_metadata: MutableMapping[str, Any], series_modalites: Set[str]
) -> None:
  json_metadata[_MODALITIES_IN_STUDY_TAG] = _encode_tag(
      'CS', list(series_modalites)
  )


def _has_series_modalites_metadata(
    json_metadata: MutableMapping[str, Any],
) -> bool:
  return _MODALITIES_IN_STUDY_TAG in json_metadata


def _retrieve_url_base(
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL,
) -> str:
  val = dicom_proxy_flags.BULK_DATA_PROXY_URL_FLG.value
  if val:
    val = val.strip().rstrip('/')
  if val:
    return val
  return dicom_web_base_url.full_url


def _retrieve_instance_url(
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL,
    metadata_uid: metadata_util.MetadataUID,
) -> str:
  return f'{_retrieve_url_base(dicom_web_base_url)}/{dicom_url_util.StudyInstanceUID(metadata_uid.study_instance_uid)}/{dicom_url_util.SeriesInstanceUID(metadata_uid.series_instance_uid)}/{dicom_url_util.SOPInstanceUID(metadata_uid.sop_instance_uid)}'


def _retrieve_study_url(
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL,
    study_instance_uid: dicom_url_util.StudyInstanceUID,
) -> str:
  return f'{_retrieve_url_base(dicom_web_base_url)}/{study_instance_uid}'


def _retrieve_series_url(
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL,
    study_instance_uid: dicom_url_util.StudyInstanceUID,
    series_instance_uid: dicom_url_util.SeriesInstanceUID,
) -> str:
  return f'{_retrieve_url_base(dicom_web_base_url)}/{study_instance_uid}/{series_instance_uid}'


def _decode_instance_metadata_response(
    response: flask.Response,
    is_metadata_query: bool,
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL,
    study_instance_uid: dicom_url_util.StudyInstanceUID,
    series_instance_uid: dicom_url_util.SeriesInstanceUID,
    sop_instance_uid: dicom_url_util.SOPInstanceUID,
) -> List[MutableMapping[str, Any]]:
  """Decode instance metadata response."""
  try:
    instance_metadata_list = json.loads(response.data)
  except json.decoder.JSONDecodeError as exp:
    # If error occurred decoding JSON. Then return the response as is.
    cloud_logging_client.error(
        'Error occured decoding DICOM metadata.',
        {'metadata': response.data},
        exp,
    )
    raise _InstanceMetadataDecodeError(str(exp)) from exp

  if isinstance(instance_metadata_list, dict):
    instance_metadata_list = [instance_metadata_list]
  elif not isinstance(instance_metadata_list, list):
    msg = 'Unexpected DICOM Store response returning metadata as is.'
    cloud_logging_client.warning(msg, {'metadata': instance_metadata_list})
    raise _InstanceMetadataDecodeError(msg)
  for metadata in instance_metadata_list:
    if not isinstance(metadata, dict):
      msg = 'Unexpected DICOM Store response returning metadata as is.'
      cloud_logging_client.warning(msg, {'metadata': instance_metadata_list})
      raise _InstanceMetadataDecodeError(msg)
    if (
        not is_metadata_query
        and dicom_proxy_flags.ENABLE_AUGMENTED_INSTANCE_SEARCH_FLG.value
    ):
      instance_md = metadata_util.get_metadata_uid(
          study_instance_uid, series_instance_uid, sop_instance_uid, metadata
      )
      _add_instance_online_metadata(metadata)
      _add_retrieve_url_to_metadata(
          metadata, _retrieve_instance_url(dicom_web_base_url, instance_md)
      )
  return instance_metadata_list


def augment_instance_metadata(
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL,
    study_instance_uid: dicom_url_util.StudyInstanceUID,
    series_instance_uid: dicom_url_util.SeriesInstanceUID,
    sop_instance_uid: dicom_url_util.SOPInstanceUID,
    response: flask.Response,
) -> flask.Response:
  """Downsamples metadata returned in dicom store proxy response.

  Returns error if not JSON, not able to parse JSON metadata, JSON is poorly
  formatted, cannot downsample or parameters are poorly formatted.

  Otherwise returns downsampled metadata.

  Args:
    dicom_web_base_url: Base DICOMweb URL for store.
    study_instance_uid: DICOM Study Instance UID queried
    series_instance_uid: DICOM Series Instance UID queried
    sop_instance_uid: DICOM SOP Instance UID queried
    response: Flask.Response containing metadata to be downsampled.

  Returns:
    Metadata response
  """
  # If response not ok return as is
  if response.status_code >= 300:
    cloud_logging_client.error(
        'Error occured retrieving DICOM metadata (augment entry point)',
        {proxy_const.LogKeywords.HTTP_STATUS_CODE: response.status_code},
    )
    return response
  if response.status_code == http.HTTPStatus.NO_CONTENT:
    return response
  try:
    fl_request_args = flask_util.get_dicom_proxy_request_args()
    downsample = flask_util.parse_downsample(fl_request_args)
    enable_caching = flask_util.parse_cache_enabled(fl_request_args)
  except ValueError as ve:
    # Return error if parameters cannot be parsed
    cloud_logging_client.error('Error parsing url parameters', ve)
    return flask_util.exception_flask_response(ve)
  base_url = flask_util.get_base_url().lower().rstrip('/')
  is_metadata_query = base_url.endswith('metadata')
  is_instances_query = base_url.endswith('instances')
  if not is_metadata_query and not is_instances_query:
    # Unrecognized query not expected ever to occur.
    cloud_logging_client.warning('Unexpected query', {'query': base_url})
    return response
  instance_request_that_includes_binary_tags = (
      is_instances_query and flask_util.includefield_binary_tags()
  )
  # if configured alter bulk uri in metadata response
  # to hide underlying dicom store.
  if dicom_proxy_flags.PROXY_DICOM_STORE_BULK_DATA_FLG.value:
    bulkdata_util.proxy_dicom_store_bulkdata_response(
        dicom_web_base_url, response
    )
  # application/dicom+json was returned
  if not _is_response_encoded_with_dicom_json_content_type(response):
    return response

  # the response is empty just return
  if not response.data:
    return response

  patch_missing_metadata_tags = False
  if instance_request_that_includes_binary_tags:
    augment_bulk_simple_annotation_metadata = True
  elif (
      is_metadata_query
      and dicom_proxy_flags.ENABLE_ANNOTATION_BULKDATA_METADATA_PATCH.value
  ):
    # temporary fix for bug in dicom store where store does not short binary
    # tags
    patch_missing_metadata_tags = True
    augment_bulk_simple_annotation_metadata = True
  else:
    augment_bulk_simple_annotation_metadata = False

  if is_metadata_query:
    correct_sparse_dicom_metadata = True
  elif (
      is_instances_query
      and sparse_dicom_util.do_includefields_request_perframe_functional_group_seq()
  ):
    correct_sparse_dicom_metadata = True
  else:
    correct_sparse_dicom_metadata = False

  response_status_code = response.status_code
  response_mimetype = response.content_type
  try:
    if (
        downsample <= 1.0
        and not augment_bulk_simple_annotation_metadata
        and not correct_sparse_dicom_metadata
    ):
      if not dicom_proxy_flags.ENABLE_AUGMENTED_INSTANCE_SEARCH_FLG.value:
        return response
      # Decode JSON response and augment metadata.
      instance_metadata_list = _decode_instance_metadata_response(
          response,
          is_metadata_query,
          dicom_web_base_url,
          study_instance_uid,
          series_instance_uid,
          sop_instance_uid,
      )
      return flask.Response(
          flask.stream_with_context(
              _stream_metadata_response([
                  json.dumps(metadata, sort_keys=True)
                  for metadata in instance_metadata_list
              ])
          ),
          status=response_status_code,
          mimetype=response_mimetype,
      )

    # Decode JSON response
    instance_metadata_list = _decode_instance_metadata_response(
        response,
        is_metadata_query,
        dicom_web_base_url,
        study_instance_uid,
        series_instance_uid,
        sop_instance_uid,
    )
  except _InstanceMetadataDecodeError:
    # If error occurred decoding JSON. Then return the response as is.
    return response

  del response
  # Augment metadata.
  try:
    metadata_length = len(instance_metadata_list)
    with dicom_store_util.MetadataThreadPoolDownloadManager(
        metadata_length,
        dicom_proxy_flags.MAX_AUGMENTED_METADATA_DOWNLOAD_SIZE_FLG.value,
    ) as thread_pool_mgr:
      output_list = []
      # reverse to keep poped metadata in order,
      instance_metadata_list.reverse()
      for _ in range(metadata_length):
        metadata = instance_metadata_list.pop()
        if (
            augment_bulk_simple_annotation_metadata
            and metadata_util.is_bulk_microscopy_simple_annotation(metadata)
        ):
          output_list.append(
              annotations_util.download_return_annotation_metadata(
                  dicom_web_base_url,
                  study_instance_uid,
                  series_instance_uid,
                  sop_instance_uid,
                  metadata,
                  patch_missing_metadata_tags,
                  thread_pool_mgr,
              )
          )
          continue
        if metadata_util.is_vl_wholeside_microscopy_iod(metadata):
          if correct_sparse_dicom_metadata and metadata_util.is_sparse_dicom(
              metadata
          ):
            if downsample != 1.0:
              msg = 'Metadata downsampling is not supported on sparse DICOM.'
              cloud_logging_client.error(msg)
              return flask_util.exception_flask_response(msg)
            output_list.append(
                sparse_dicom_util.download_and_return_sparse_dicom_metadata(
                    dicom_web_base_url,
                    study_instance_uid,
                    series_instance_uid,
                    sop_instance_uid,
                    metadata,
                    enable_caching,
                    thread_pool_mgr,
                )
            )
            continue
          if downsample != 1.0:
            try:
              metadata_util.downsample_json_metadata(metadata, downsample)
              output_list.append(json.dumps(metadata))
              continue
            except metadata_util.JsonMetadataDownsampleError as err:
              # If a error occures downsampling return the error.
              cloud_logging_client.error(
                  'Error downsampling JSON metadata', err
              )
              return flask_util.exception_flask_response(err)
        output_list.append(json.dumps(metadata, sort_keys=True))
      return flask.Response(
          flask.stream_with_context(_stream_metadata_response(output_list)),
          status=response_status_code,
          mimetype=response_mimetype,
      )
  except dicom_store_util.MetadataDownloadExceedsMaxSizeLimitError as exp:
    size_limit = (
        dicom_proxy_flags.MAX_AUGMENTED_METADATA_DOWNLOAD_SIZE_FLG.value
    )
    msg = f'DICOM instance metadata retrieval exceeded {size_limit} byte limit.'
    cloud_logging_client.error(msg, exp)
    status = http.HTTPStatus.BAD_REQUEST
    return flask.Response(msg, status=status, mimetype='text/plain')
  except dicom_store_util.DicomInstanceMetadataRetrievalError as exp:
    msg = 'Error occured retrieving DICOM metadata.'
    cloud_logging_client.error(msg, exp)
    status = http.HTTPStatus.INTERNAL_SERVER_ERROR
    return flask.Response(msg, status=status, mimetype='text/plain')


def _augment_response_metadata(
    augment_function: Callable[[MutableMapping[str, Any]], flask.Response],
    response: flask.Response,
) -> flask.Response:
  """Augment study search response metadata."""
  if response.status_code >= 300:
    cloud_logging_client.error(
        'Error occured retrieving DICOM metadata (augment entry point)',
        {proxy_const.LogKeywords.HTTP_STATUS_CODE: response.status_code},
    )
    return response
  if response.status_code == http.HTTPStatus.NO_CONTENT:
    cloud_logging_client.info(
        'Not augmenting metadata; response has no content.'
    )
    return response
  if not _is_response_encoded_with_dicom_json_content_type(response):
    cloud_logging_client.info(
        'Not augmenting metadata; response is not encoded with dicom+json'
        ' content type.'
    )
    return response
  try:
    # Decode JSON response
    metadata_list = json.loads(response.data)
  except json.decoder.JSONDecodeError as exp:
    # If error occurred decoding JSON. Then return the response as is.
    cloud_logging_client.error(
        'Error occured decoding DICOM metadata.',
        {'metadata': response.data},
        exp,
    )
    return response
  if not metadata_list:
    cloud_logging_client.info('Not augmenting empty metadata.')
    return response
  try:
    with dicom_store_util.MetadataThreadPoolDownloadManager(
        len(metadata_list),
        dicom_proxy_flags.MAX_AUGMENTED_METADATA_DOWNLOAD_SIZE_FLG.value,
    ) as thread_pool_mgr:
      results = thread_pool_mgr.map(augment_function, metadata_list)
  except dicom_store_util.MetadataDownloadExceedsMaxSizeLimitError as exp:
    size_limit = (
        dicom_proxy_flags.MAX_AUGMENTED_METADATA_DOWNLOAD_SIZE_FLG.value
    )
    msg = f'DICOM instance metadata retrieval exceeded {size_limit} byte limit.'
    cloud_logging_client.error(msg, exp)
    status = http.HTTPStatus.BAD_REQUEST
    return flask.Response(msg, status=status, mimetype='text/plain')
  except dicom_store_util.DicomInstanceMetadataRetrievalError as exp:
    msg = 'Error occured retrieving DICOM metadata.'
    cloud_logging_client.error(msg, exp)
    status = http.HTTPStatus.INTERNAL_SERVER_ERROR
    return flask.Response(msg, status=status, mimetype='text/plain')
  return flask.Response(
      flask.stream_with_context(_stream_metadata_response(results)),
      status=response.status_code,
      mimetype=response.content_type,
  )


def _get_json_value(metadata: Mapping[str, Any], tag: str) -> str:
  try:
    json_dict = metadata.get(tag, {})
    return json_dict.get('Value', [])[0]
  except (KeyError, IndexError, ValueError) as _:
    return ''


def _augment_single_study_md(
    user_auth: user_auth_util.AuthSession,
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL,
    json_metadata: MutableMapping[str, Any],
) -> str:
  """Augment single study search response metadata."""
  study_instance_uid = _get_json_value(json_metadata, _STUDY_INSTANCE_UID_TAG)
  if not study_instance_uid:
    cloud_logging_client.warning(
        'Cannot augment series metadata missing study instance uid.'
    )
    return json.dumps(json_metadata)
  study_instance_uid = dicom_url_util.StudyInstanceUID(study_instance_uid)
  if not (
      _has_series_count_metadata(json_metadata)
      and _has_instance_count_metadata(json_metadata)
  ):
    metrics = dicom_store_util.get_dicom_study_metrics(
        user_auth, dicom_web_base_url, study_instance_uid
    )
    series_count = metrics.get(_SERIES_COUNT, '')
    instance_count = metrics.get(_INSTANCE_COUNT, '')
    # Number of Study Related Series
    if series_count:
      try:
        series_count = int(series_count)
        _add_series_count_metadata(json_metadata, series_count)
      except ValueError:
        pass
    # Number of Study Related Instances
    if instance_count:
      try:
        instance_count = int(instance_count)
        _add_instance_count_metadata(json_metadata, instance_count)
      except ValueError:
        pass
  # Instance Availability
  _add_instance_online_metadata(json_metadata)
  # retrieve url
  _add_retrieve_url_to_metadata(
      json_metadata, _retrieve_study_url(dicom_web_base_url, study_instance_uid)
  )
  # Modalities in Study
  if not _has_series_modalites_metadata(json_metadata):
    series_modalites = set()
    for md in dicom_store_util.get_dicom_study_series_metadata(
        user_auth, dicom_web_base_url, study_instance_uid
    ):
      modality = _get_json_value(md, _MODALITY_TAG)
      if modality:
        series_modalites.add(modality)
    _add_series_modalites_metadata(json_metadata, series_modalites)
  return json.dumps(json_metadata)


def augment_study_response_metadata(
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL, response: flask.Response
) -> flask.Response:
  """Augment study search response metadata."""
  if not dicom_proxy_flags.ENABLE_AUGMENTED_STUDY_SEARCH_FLG.value:
    return response
  cloud_logging_client.info('Augmenting study search metadata.')
  return _augment_response_metadata(
      functools.partial(
          _augment_single_study_md,
          user_auth_util.AuthSession(flask_util.get_headers()),
          dicom_web_base_url,
      ),
      response,
  )


def _augment_single_series_md(
    user_auth: user_auth_util.AuthSession,
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL,
    study_instance_uid: dicom_url_util.StudyInstanceUID,
    json_metadata: MutableMapping[str, Any],
) -> str:
  """Augment single series search response metadata."""
  if not study_instance_uid.study_instance_uid:
    study_instance_uid = dicom_url_util.StudyInstanceUID(
        _get_json_value(json_metadata, _STUDY_INSTANCE_UID_TAG)
    )
  series_instance_uid = dicom_url_util.SeriesInstanceUID(
      _get_json_value(json_metadata, _SERIES_INSTANCE_UID_TAG)
  )
  if (
      not study_instance_uid.study_instance_uid
      or not series_instance_uid.series_instance_uid
  ):
    cloud_logging_client.warning(
        'Cannot augment series metadata missing study or series instance uid.'
    )
    return json.dumps(json_metadata)
  if not _has_instance_count_metadata(json_metadata):
    metrics = dicom_store_util.get_dicom_series_metrics(
        user_auth, dicom_web_base_url, study_instance_uid, series_instance_uid
    )
    instance_count = metrics.get(_INSTANCE_COUNT, '')
    # Number of Series Related Instances
    if instance_count:
      try:
        _add_instance_count_metadata(json_metadata, int(instance_count))
      except ValueError:
        pass
  # Instance Availability
  _add_instance_online_metadata(json_metadata)
  _add_retrieve_url_to_metadata(
      json_metadata,
      _retrieve_series_url(
          dicom_web_base_url, study_instance_uid, series_instance_uid
      ),
  )
  return json.dumps(json_metadata)


def augment_series_response_metadata(
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL,
    study_instance_uid: dicom_url_util.StudyInstanceUID,
    response: flask.Response,
) -> flask.Response:
  """Augment series search response metadata."""
  if not dicom_proxy_flags.ENABLE_AUGMENTED_SERIES_SEARCH_FLG.value:
    return response
  cloud_logging_client.info('Augmenting series search metadata.')
  return _augment_response_metadata(
      functools.partial(
          _augment_single_series_md,
          user_auth_util.AuthSession(flask_util.get_headers()),
          dicom_web_base_url,
          study_instance_uid,
      ),
      response,
  )
