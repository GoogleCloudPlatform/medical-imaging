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
"""Utility functions to interact with DICOM Store."""
from __future__ import annotations

from concurrent import futures
import contextlib
import http
import io
import re
import tempfile
import threading
from typing import Any, Callable, IO, Iterator, List, Mapping, Optional, Union

import flask
import pydicom
import requests

from pathology.dicom_proxy import base_dicom_request_error
from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import flask_util
from pathology.dicom_proxy import logging_util
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.logging_lib import cloud_logging_client


AuthSession = user_auth_util.AuthSession
_UNSUPPORTED_PROXY_PARAMS = frozenset([
    'disable_caching',
    'downsample',
    'embed_iccprofile',
    'iccprofile',
    'interpolation',
    'quality',
])

_PARSE_URL = re.compile(r'(http.*)/?\?(.*)')


class DicomInstanceMetadataRetrievalError(Exception):
  pass


class MetadataDownloadExceedsMaxSizeLimitError(Exception):
  pass


class DicomInstanceRequestError(base_dicom_request_error.BaseDicomRequestError):
  """Exception which wraps error responses from DICOM store."""

  def __init__(self, response: requests.Response, msg: Optional[str] = None):
    super().__init__('DicomInstanceRequestError', response, msg)


class DicomMetadataRequestError(base_dicom_request_error.BaseDicomRequestError):
  """Exception which wraps error responses from DICOM store."""

  def __init__(self, response: requests.Response, msg: Optional[str] = None):
    super().__init__('DicomMetadataRequestError', response, msg)


class MetadataThreadPoolDownloadManagerError(Exception):
  pass


class MetadataThreadPoolDownloadManager(contextlib.ExitStack):
  """Manages async download dicom metadata."""

  def __init__(self, num_threads: int, max_data_size: int):
    super().__init__()
    self._max_threads = min(
        num_threads,
        dicom_proxy_flags.MAX_AUGMENTED_METADATA_DOWNLOAD_THREAD_COUNT_FLG.value,
    )
    self._pool = None
    self._total_size = 0
    self._lock = threading.Lock()
    self._max_data_size = max_data_size
    self._future_list = []
    self._context_entered = False
    self.callback(self._close_context)

  def _close_context(self) -> None:
    if self._future_list:
      futures.wait(self._future_list, return_when=futures.FIRST_EXCEPTION)
      self._future_list.clear()
    self._context_entered = False

  def __enter__(self):
    self._context_entered = True
    return super().__enter__()

  def _get_thread_pool(self) -> futures.ThreadPoolExecutor:
    if self._pool is not None:
      return self._pool
    self._pool = self.enter_context(
        futures.ThreadPoolExecutor(max_workers=self._max_threads)
    )
    return self._pool

  def inc_data_downloaded(self, size: int) -> int:
    if not self._context_entered:
      raise MetadataThreadPoolDownloadManagerError('Not in context block.')
    with self._lock:
      self._total_size += size
      if self._total_size > self._max_data_size:
        raise MetadataDownloadExceedsMaxSizeLimitError()
      return self._total_size

  def submit(
      self, func: Callable[..., Any], *args
  ) -> Union[futures.Future[str], str]:
    if not self._context_entered:
      raise MetadataThreadPoolDownloadManagerError('Not in context block.')
    if self._max_threads < 2:
      return func(*args)
    else:
      future_metadata = self._get_thread_pool().submit(func, *args)
      self._future_list.append(future_metadata)
      return future_metadata


def _download_bytes(
    query: dicom_url_util.DicomStoreTransaction,
    base_log: Mapping[str, Any],
    output: Union[str, IO[bytes]],
) -> int:
  """Downloads binary data from DICOM to file or file like object.

  Args:
    query: DICOM web query to perform.
    base_log: Structured logging to include.
    output: Path to write downloaded DICOM file or File Like Object.

  Returns:
    Data size in bytes downloaded.

  Raises:
    DicomInstanceRequestError: Error downloading DICOM instance.
  """
  bytes_downloaded = 0
  with requests.Session() as session:
    response = session.get(query.url, headers=query.headers, stream=True)
    try:
      response.raise_for_status()
      chunk_size = (
          dicom_proxy_flags.DICOM_INSTANCE_DOWNLOAD_STREAMING_CHUNKSIZE_FLG.value
      )
      if isinstance(output, str):
        with open(output, 'wb') as outfile:
          for chunk in response.iter_content(chunk_size=chunk_size):
            response.raise_for_status()
            outfile.write(chunk)
            bytes_downloaded += len(chunk)
      else:
        for chunk in response.iter_content(chunk_size=chunk_size):
          response.raise_for_status()
          output.write(chunk)
          bytes_downloaded += len(chunk)
        output.seek(0)
      cloud_logging_client.info('Downloaded DICOM instance.', base_log)
      return bytes_downloaded
    except requests.exceptions.HTTPError as exp:
      cloud_logging_client.error(
          'Error occured downloading DICOM instance.', base_log, exp
      )
      raise DicomInstanceRequestError(response) from exp


def download_bulkdata(
    user_auth: user_auth_util.AuthSession,
    bulk_data_uri: str,
    output: Union[str, IO[bytes]],
) -> int:
  """Downloads DICOM bulkdata from bulkdata uri.

  Args:
    user_auth: User authentication to use to download instance.
    bulk_data_uri: Bulk data uri to download data.
    output: Path to write downloaded data or file like object.

  Returns:
    Data size in bytes downloaded.

  Raises:
    DicomInstanceRequestError: Error downloading DICOM instance.
  """
  query = dicom_url_util.download_bulkdata(user_auth, bulk_data_uri)
  base_log = {
      proxy_const.LogKeywords.DICOMWEB_URL: query.url,
      proxy_const.LogKeywords.USER_EMAIL_OF_DICOMWEB_REQUEST: user_auth.email,
  }
  return _download_bytes(query, base_log, output)


def download_dicom_instance(
    user_auth: user_auth_util.AuthSession,
    dicom_series_url: dicom_url_util.DicomSeriesUrl,
    found_instance: dicom_url_util.SOPInstanceUID,
    output: Union[str, IO[bytes]],
) -> int:
  """Downloads DICOM instance from store to container.

  Args:
    user_auth: User authentication to use to download instance.
    dicom_series_url: DICOM web url to series containing instance.
    found_instance: SOP Instance UID to download.
    output: Path to write downloaded DICOM instance or file like object.

  Returns:
    data size in bytes downloaded

  Raises:
    DicomInstanceRequestError: Error downloading DICOM instance.
  """
  url = dicom_url_util.series_dicom_instance_url(
      dicom_series_url, found_instance
  )
  query = dicom_url_util.download_dicom_instance_not_transcoded(user_auth, url)
  base_log = {
      proxy_const.LogKeywords.DICOMWEB_URL: query.url,
      proxy_const.LogKeywords.USER_EMAIL_OF_DICOMWEB_REQUEST: user_auth.email,
  }
  return _download_bytes(query, base_log, output)


def download_instance_return_metadata(
    user_auth: user_auth_util.AuthSession,
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL,
    study_instance_uid: str,
    series_instance_uid: str,
    sop_instance_uid: str,
    exclude_tags: Optional[List[str]] = None,
) -> Mapping[str, Any]:
  """Flask entry point for request to retrieve an instance annotation.

  Args:
    user_auth: User authentication to use to download instance.
    dicom_web_base_url: Base DICOMweb URL for store.
    study_instance_uid: DICOM study instance UID.
    series_instance_uid: DICOM series instance UID.
    sop_instance_uid: DICOM sop instance UID.
    exclude_tags: Tags to excludes from response

  Returns:
    JSON Metadata

  Raises:
    DicomInstanceMetadataRetrievalError: Unable to download or unable to read
      DICOM.
  """
  series_url = dicom_url_util.base_dicom_series_url(
      dicom_web_base_url,
      dicom_url_util.StudyInstanceUID(study_instance_uid),
      dicom_url_util.SeriesInstanceUID(series_instance_uid),
  )
  exclude_tags = exclude_tags if exclude_tags is not None else []
  with tempfile.TemporaryFile() as dicom_instance:
    try:
      download_dicom_instance(
          user_auth,
          series_url,
          dicom_url_util.SOPInstanceUID(sop_instance_uid),
          dicom_instance,
      )
    except DicomInstanceRequestError as exp:
      raise DicomInstanceMetadataRetrievalError(  # pylint: disable=bad-exception-cause
          'Error downloading DICOM instance'
      ) from exp
    try:
      with pydicom.dcmread(dicom_instance) as dcm:
        for tag in exclude_tags:
          try:
            del dcm[tag]
          except (KeyError, ValueError, TypeError) as _:
            pass
        base_dict = dcm.to_json_dict()
        base_dict.update(dcm.file_meta.to_json_dict())
        return base_dict
    except pydicom.errors.InvalidDicomError as exp:
      raise DicomInstanceMetadataRetrievalError(
          'Error decoding DICOM instance'
      ) from exp


def _get_dicom_json(
    query: dicom_url_util.DicomStoreTransaction,
    base_log: Mapping[str, Any],
) -> List[Mapping[str, Any]]:
  """Returns DICOM json metadata.

  Args:
    query: DICOMweb query to perfrom.
    base_log: Structured logging to include with request logs.

  Returns:
    List of DICOM instance metadata (JSON) stored in series.

  Raises:
    DicomMetadataRequestError: HTTP error or error decoding dicomWeb response.
  """
  response = requests.get(query.url, headers=query.headers, stream=False)
  try:
    response.raise_for_status()
    metadata = response.json()
    cloud_logging_client.info(
        'Retrieved DICOM instance metadata.',
        base_log,
        {proxy_const.LogKeywords.DICOMWEB_URL: query.url},
    )
    return metadata
  except requests.exceptions.HTTPError as exp:
    cloud_logging_client.error(
        'Error occured downloading DICOM instance metadata.',
        base_log,
        exp,
        {
            proxy_const.LogKeywords.DICOMWEB_RESPONSE: response.text,
            proxy_const.LogKeywords.DICOMWEB_URL: query.url,
        },
    )
    raise DicomMetadataRequestError(response) from exp
  except requests.exceptions.JSONDecodeError as exp:
    cloud_logging_client.error(
        'Error occured decoding DICOM instance metadata JSON.',
        base_log,
        exp,
        {
            proxy_const.LogKeywords.DICOMWEB_RESPONSE: response.text,
            proxy_const.LogKeywords.DICOMWEB_URL: query.url,
        },
    )
    raise DicomMetadataRequestError(response, 'invalid JSON response.') from exp


def get_series_instance_tags(
    user_auth: user_auth_util.AuthSession,
    dicom_series_url: dicom_url_util.DicomSeriesUrl,
    additional_tags: Optional[List[str]] = None,
) -> List[Mapping[str, Any]]:
  """Returns instance tag metadata for all instances in a series.

  Args:
    user_auth: User authentication to use to download instance.
    dicom_series_url: DICOM web url to series containing instance.
    additional_tags: Additional DICOM tags(keywords) to request metadata for.

  Returns:
    List of DICOM instance metadata (JSON) stored in series.

  Raises:
    DicomMetadataRequestError: HTTP error or error decoding dicomWeb response.
  """
  return _get_dicom_json(
      dicom_url_util.dicom_instance_tag_query(
          user_auth, dicom_series_url, None, additional_tags
      ),
      {proxy_const.LogKeywords.USER_EMAIL_OF_DICOMWEB_REQUEST: user_auth.email},
  )


def get_instance_tags(
    user_auth: user_auth_util.AuthSession,
    dicom_series_url: dicom_url_util.DicomSeriesUrl,
    instance: dicom_url_util.SOPInstanceUID,
    additional_tags: Optional[List[str]] = None,
) -> List[Mapping[str, Any]]:
  """Returns instance tag metadata.

  Args:
    user_auth: User authentication to use to download instance.
    dicom_series_url: DICOM web url to series containing instance.
    instance: DICOM instance in series to return metadata for.
    additional_tags: Additional DICOM tags(keywords) to request metadata for.

  Returns:
    DICOM instance metadata (JSON) [does not include bulkdatauri].

  Raises:
    DicomMetadataRequestError: HTTP error or error decoding dicomWeb response.
  """
  return _get_dicom_json(
      dicom_url_util.dicom_instance_tag_query(
          user_auth, dicom_series_url, instance, additional_tags
      ),
      {proxy_const.LogKeywords.USER_EMAIL_OF_DICOMWEB_REQUEST: user_auth.email},
  )


def get_dicom_instance_metadata(
    user_auth: user_auth_util.AuthSession,
    dicom_series_url: dicom_url_util.DicomSeriesUrl,
    instance: dicom_url_util.SOPInstanceUID,
) -> List[Mapping[str, Any]]:
  """Returns instance level metadata.

  Args:
    user_auth: User authentication to use to download instance.
    dicom_series_url: DICOM web url to series containing instance.
    instance: DICOM instance in series to return metadata for.

  Returns:
    DICOM instance metadata (JSON) [includes bulkdatauri].

  Raises:
    DicomMetadataRequestError: HTTP error or error decoding dicomWeb response.
  """
  return _get_dicom_json(
      dicom_url_util.dicom_instance_metadata_query(
          user_auth, dicom_series_url, instance
      ),
      {proxy_const.LogKeywords.USER_EMAIL_OF_DICOMWEB_REQUEST: user_auth.email},
  )


def get_dicom_study_instance_metadata(
    user_auth: user_auth_util.AuthSession,
    dicom_study_url: dicom_url_util.DicomStudyUrl,
    instance: dicom_url_util.SOPInstanceUID,
) -> List[Mapping[str, Any]]:
  """Returns instance level metadata.

  Args:
    user_auth: User authentication to use to download instance.
    dicom_study_url: DICOM web url for study containing instance.
    instance: DICOM instance in series to return metadata for.

  Returns:
    DICOM instance metadata (JSON) [includes bulkdatauri].

  Raises:
    DicomMetadataRequestError: HTTP error or error decoding dicomWeb response.
  """
  return _get_dicom_json(
      dicom_url_util.dicom_instance_tag_query(
          user_auth, dicom_study_url, instance
      ),
      {proxy_const.LogKeywords.USER_EMAIL_OF_DICOMWEB_REQUEST: user_auth.email},
  )


def delete_instance_from_dicom_store(
    user_auth: user_auth_util.AuthSession, dicom_path: str
) -> flask.Response:
  """Deletes instance from dicom store within a session.

  Args:
    user_auth: User_auth used to delete instance to DICOM Store
    dicom_path: DICOMweb instance path of instance to delete.

  Returns:
    Delete Response
  """
  base_log = {
      proxy_const.LogKeywords.DICOMWEB_URL: dicom_path,
      proxy_const.LogKeywords.USER_EMAIL_OF_DICOMWEB_REQUEST: user_auth.email,
  }
  try:
    headers = user_auth.add_to_header(
        {'Content-Type': 'application/dicom+json; charset=utf-8'}
    )
    response = requests.delete(dicom_path, headers=headers)
    response.raise_for_status()
    cloud_logging_client.info(
        f'Deleted instance {dicom_path} from DICOM Store.', base_log
    )
  except requests.HTTPError as exc:
    cloud_logging_client.error(
        f'Failed to delete instance {dicom_path}.', base_log, exc
    )
    response = exc.response
  return flask.Response(
      response.text,
      status=response.status_code,
      content_type=flask_util.get_key_value(
          response.headers, 'content-type', ''
      ),
  )


def upload_instance_to_dicom_store(
    user_auth: user_auth_util.AuthSession,
    dataset: pydicom.dataset.Dataset,
    dicom_path: str,
    headers: Optional[Mapping[str, str]] = None,
) -> flask.Response:
  """Adds headers and uploads to dicom store within a session.

  Args:
    user_auth: User auth used to upload instance to DICOM Store
    dataset: DICOM dataset to upload.
    dicom_path: DICOMweb study path to upload to.
    headers: Optional headers to include with request.

  Returns:
    POST Response.
  """
  if headers is None:
    headers = {}
  flask_util.add_key_value_to_dict(headers, 'Content-Type', 'application/dicom')
  headers = user_auth.add_to_header(headers)
  with io.BytesIO() as f:
    dataset.save_as(f)
    # Get value of buffer as bytearray independent from the ByteIO buffer.
    data = f.getvalue()
  base_log = {
      proxy_const.LogKeywords.DICOMWEB_URL: dicom_path,
      proxy_const.LogKeywords.USER_EMAIL_OF_DICOMWEB_REQUEST: user_auth.email,
  }
  try:
    response = requests.post(dicom_path, data=data, headers=headers)
    response.raise_for_status()
    cloud_logging_client.info(
        'Uploaded DICOM instance to DICOM store.', base_log
    )
    return flask.Response(
        response.text,
        status=response.status_code,
        content_type=flask_util.get_key_value(
            response.headers, 'content-type', ''
        ),
    )
  except requests.HTTPError as exp:
    if exp.response.status_code == http.HTTPStatus.CONFLICT:
      cloud_logging_client.warning(
          (
              'Cannot create DICOM Annotation. UID triple (StudyInstanceUID, '
              'SeriesInstanceUID, SOPInstanceUID) already exists in store.'
          ),
          base_log,
          exp,
      )
    else:
      cloud_logging_client.warning(
          'Error occurred when trying to create the annotation.', base_log, exp
      )
    return flask.Response(
        exp.response.text,
        status=exp.response.status_code,
        content_type=flask_util.get_key_value(
            exp.response.headers, 'content-type', ''
        ),
    )


def upload_multipart_to_dicom_store(
    user_auth: user_auth_util.AuthSession,
    multipart_data: bytes,
    content_type: str,
    dicom_path: str,
    headers: Optional[Mapping[str, str]] = None,
) -> flask.Response:
  """Uploads multipart dicom to dicom store within a session.

  Args:
    user_auth: User auth used to upload instance to DICOM Store
    multipart_data: Multipart data to upload.
    content_type: Multipart content type.
    dicom_path: DICOMweb study path to upload to.
    headers: Optional headers to include with request.

  Returns:
    POST Response.
  """
  if headers is None:
    headers = {}
  flask_util.add_key_value_to_dict(headers, 'Content-Type', content_type)
  headers = user_auth.add_to_header(headers)
  base_log = {
      proxy_const.LogKeywords.DICOMWEB_URL: dicom_path,
      proxy_const.LogKeywords.USER_EMAIL_OF_DICOMWEB_REQUEST: user_auth.email,
  }
  try:
    response = requests.post(dicom_path, data=multipart_data, headers=headers)
    response.raise_for_status()
    cloud_logging_client.warning(
        'Uploaded multipart data to DICOM store.', base_log
    )
    return flask.Response(
        response.text,
        status=response.status_code,
        content_type=flask_util.get_key_value(
            response.headers, 'content-type', ''
        ),
    )
  except requests.HTTPError as exp:
    if exp.response.status_code == http.HTTPStatus.CONFLICT:
      cloud_logging_client.warning(
          (
              'Cannot create DICOM Annotation. UID triple (StudyInstanceUID, '
              'SeriesInstanceUID, SOPInstanceUID) already exists in store.'
          ),
          exp,
      )
    else:
      cloud_logging_client.warning(
          'Error occurred when trying to create the annotation.',
          exp,
      )
    return flask.Response(
        exp.response.text,
        status=exp.response.status_code,
        content_type=flask_util.get_key_value(
            exp.response.headers, 'content-type', ''
        ),
    )


def _remove_dicom_proxy_url_path_prefix(url_path: str) -> str:
  """Removes tileserver URL prefix from url path."""
  if url_path.startswith(dicom_proxy_flags.PROXY_SERVER_URL_PATH_PREFIX):
    return url_path[len(dicom_proxy_flags.PROXY_SERVER_URL_PATH_PREFIX) :]
  return url_path


def _get_flask_full_path_removing_unsupported_proxy_params() -> str:
  """Returns flask path removing unsupported proxy query parameters."""
  base_url = flask_util.get_path()
  flask_args = flask_util.get_key_args_list()
  if not flask_args:
    return base_url
  returned_params = []
  for key, value_list in flask_args.items():
    if key.lower() in _UNSUPPORTED_PROXY_PARAMS:
      continue
    returned_params.extend([f'{key}={value}' for value in value_list])
  if not returned_params:
    return base_url
  returned_params = '&'.join(returned_params)
  return f'{base_url}?{returned_params}'


def _stream_proxy_content(response: requests.Response) -> Iterator[bytes]:
  """Generator streams bytes from proxied DICOMweb request."""
  for content in response.iter_content(
      chunk_size=dicom_proxy_flags.DICOM_INSTANCE_DOWNLOAD_STREAMING_CHUNKSIZE_FLG.value
  ):
    yield content
  return None


def _add_parameters_to_url(url: str, params_to_add: List[str]) -> str:
  """Returns URL with parameters added to it."""
  if url[-1] == '?':
    url = url[:-1]
    params = ''
  else:
    match = _PARSE_URL.fullmatch(url)
    if not match:
      params = ''
    else:
      url, params = match.groups()
  url = url.rstrip('/')
  params_to_add = '&'.join(params_to_add)
  if params:
    params = f'{params}&{params_to_add}'
  else:
    params = params_to_add
  return f'{url}?{params}'


def dicom_store_proxy(params: Optional[List[str]] = None) -> flask.Response:
  """Processes generic DICOM Store proxy request and return results.

  Args:
    params: Optional parameters to add to proxy request.

  Returns:
    flask.Response
  """
  method = flask_util.get_method()
  # Convert proxy web request url into url against DICOM store.
  full_path = _get_flask_full_path_removing_unsupported_proxy_params()
  cloud_logging_client.info(f'Entry Proxy request: {full_path}')
  if params is not None and params:
    full_path = _add_parameters_to_url(full_path, params)
  url = _remove_dicom_proxy_url_path_prefix(full_path)
  url = dicom_url_util.get_dicom_store_url(url)
  # The accept(key):value pair in the HTTP request defines the image/data
  # format of what the client is requesting. Copy the header key:value from
  # the request and use it in the call to the DICOM store.
  header = flask_util.norm_dict_keys(
      flask_util.get_headers(),
      [
          flask_util.ACCEPT_HEADER_KEY,
          flask_util.CONTENT_TYPE_HEADER_KEY,
          flask_util.CONTENT_LENGTH_HEADER_KEY,
          flask_util.ACCEPT_ENCODING_KEY,
      ],
  )
  headers = AuthSession(flask_util.get_headers()).add_to_header(header)
  cloud_logging_client.info(
      f'Proxied URL: {url}',
      {
          'proxy_headers': logging_util.mask_privileged_header_values(headers),
          'proxy_method': method,
      },
  )
  # Proxy using same method called with
  # Stream data from proxy to caller to avoid fully loading response in server
  if method == flask_util.GET:
    response = requests.get(url, headers=headers, stream=True)
  elif method == flask_util.POST:
    response = requests.post(
        url, headers=headers, stream=True, data=flask_util.get_data()
    )
  elif method == flask_util.DELETE:
    response = requests.delete(url, headers=headers, stream=True)
  else:
    msg = 'Unsupported HTTP request method.'
    cloud_logging_client.error(msg, {'method': method, 'url': url})
    return flask.Response(
        response=str(msg),
        status=http.HTTPStatus.BAD_REQUEST.value,
        content_type='text/plain',
    )
  fl_response = flask.Response(
      _stream_proxy_content(response),
      status=response.status_code,
      direct_passthrough=False,
  )
  fl_response.headers.clear()
  for key, value in response.headers.items():
    fl_response.headers[key] = value
  return fl_response
