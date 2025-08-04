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
"""Flask blueprint for Annotations using the DICOM Proxy."""
import dataclasses
import http
import io
import json
import os
import re
import tempfile
from typing import IO, List, Mapping, Set, Union
from xml.etree import ElementTree as ET

import flask
import pydicom
import requests
import requests_toolbelt

from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import dicom_store_util
from pathology.dicom_proxy import dicom_tag_util
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import flask_util
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.logging_lib import cloud_logging_client
from pathology.shared_libs.pydicom_version_util import pydicom_version_util


# Constants
_OPERATOR_IDENTIFICATION_SEQUENCE_TAG = dicom_tag_util.DicomTag(
    '00081072', 'OperatorIdentificationSequence'
)
_PERSON_IDENTIFICATION_CODE_SEQUENCE_TAG = dicom_tag_util.DicomTag(
    '00401101', 'PersonIdentificationCodeSequence'
)
_LONG_CODE_VALUE_TAG = dicom_tag_util.DicomTag('00080119', 'LongCodeValue')
_CODE_MEANING_TAG = dicom_tag_util.DicomTag('00080104', 'CodeMeaning')
_MICROSCOPY_BULK_SIMPLE_ANNOTATIONS_IOD = '1.2.840.10008.5.1.4.1.1.91.1'
_EMAIL_VALIDATION_REGEX = re.compile(r'.+@.+\..+')
_ACCEPT = 'Accept'
_APPLICATION_DICOM_JSON = 'application/dicom+json'
_APPLICATION_DICOM = 'application/dicom'
_APPLICATION_DICOM_XML = 'application/dicom+xml'
_TEXT_PLAIN = 'text/plain'
_MULTIPART_RELATED = 'multipart/related'
_UNRECOGNIZED_CONTENT_TYPE = 'unrecognized_content_type'
_CONTENT_TYPE = 'content-type'
_UNAUTHORIZED_CLIENT_ACCESS = 'Error cannot read from DICOM store.'
_GOOGLE_ACCOUNT_EMAIL_PREFIX = 'accounts.google.com:'
_DICOM_STORE_ID = re.compile(
    r'(.*/)?projects/(.*?)/locations/(.*?)/datasets/(.*?)/dicomStores/(.*?)(/.*)?',
    re.IGNORECASE,
)
_SOP_CLASS_UID_DICOM_TAG_ADDRESS = '00080016'
_VALUE = 'Value'


def _norm_dicom_store_url(store_url: str) -> str:
  match = _DICOM_STORE_ID.fullmatch(store_url)
  if not match:
    cloud_logging_client.warning(
        'Unexpected dicom store url formatting.', {'url': store_url}
    )
    return store_url.lower()
  return (':'.join(match.groups()[1:5])).lower()


def _get_dicom_store_allow_set() -> Set[str]:
  return {
      _norm_dicom_store_url(url)
      for url in dicom_proxy_flags.DICOM_ANNOTATIONS_STORE_ALLOW_LIST.value
  }


@dataclasses.dataclass(frozen=True)
class _MpRequestPart:
  headers: Mapping[bytes, bytes]
  content: bytes


class _UnableToAuthenticateUserError(Exception):
  """Unable to authenticate user.

  User does not have access or cannot be authenticated to read and write
  to annotations DICOM Store.
  """


class _MultiPartContentSopClassUidDecodingError(Exception):
  pass


class _InvalidDicomJsonError(Exception):
  pass


class _ServiceAccountCredentials(user_auth_util.AuthSession):
  """Inits UserAuth credentials using the Proxy server service account."""

  def __init__(self, dicom_web_base_url: dicom_url_util.DicomWebBaseURL):
    """Inits UserAuth credentials using the Proxy server service account.

    Args:
      dicom_web_base_url: Base URL of DICOM Annotations store for request.

    Raises:
      _UnableToAuthenticateUserError: If user cannot be authenticated.
    """
    studies_query = (
        f'{dicom_url_util._HEALTHCARE_API_URL}/{dicom_web_base_url}'
        '/studies?limit=1'
    )
    try:
      super().__init__(flask_util.get_headers())
      base_log = {
          proxy_const.LogKeywords.USER_EMAIL: self.email,
          proxy_const.LogKeywords.EMAIL_REGEX: _EMAIL_VALIDATION_REGEX,
          proxy_const.LogKeywords.AUTHORIZATION: self.authorization,
          proxy_const.LogKeywords.AUTHORITY: self.authority,
      }
      response = requests.get(
          studies_query,
          headers=self.add_to_header({_ACCEPT: _APPLICATION_DICOM_JSON}),
          stream=False,
      )
      try:
        response.raise_for_status()
        if self.email and re.fullmatch(
            _EMAIL_VALIDATION_REGEX,
            _normalize_email(self.email),
        ):
          self._user_email = self.email
          self._user_authorization = self.authorization
          self._user_authority = self.authority
          cloud_logging_client.info(
              'Authenticated user has read access to annotation DICOM store.',
              base_log,
          )
          self._init_to_service_account_credentials()
          return
        else:
          cloud_logging_client.error(
              'Could not authenticate user. User email empty or formattted'
              ' unexpectedly.',
              base_log,
          )
      except requests.exceptions.HTTPError as exp:
        cloud_logging_client.error(
            'User does not have access or cannot be authenticated.',
            base_log,
            exp,
        )
        raise _UnableToAuthenticateUserError(
            'User does not have access or cannot be authenticated.'
        ) from exp
    except user_auth_util.UserEmailRetrievalError as exp:
      cloud_logging_client.error(
          'User does not have access or cannot be authenticated.',
          {
              proxy_const.LogKeywords.USER_EMAIL: self.email,
              proxy_const.LogKeywords.EMAIL_REGEX: _EMAIL_VALIDATION_REGEX,
              proxy_const.LogKeywords.AUTHORIZATION: self.authorization,
              proxy_const.LogKeywords.AUTHORITY: self.authority,
          },
          exp,
      )
      raise _UnableToAuthenticateUserError(
          'User does not have access or cannot be authenticated.'
      ) from exp
    cloud_logging_client.error(
        'User does not have access or cannot be authenticated.'
    )
    raise _UnableToAuthenticateUserError(
        'User does not have access or cannot be authenticated.'
    )

  # Convenience methods to expose tokens, email.
  #
  # All http auth against DICOM store should rely on base class accessors in:
  #  self.email
  #  self.authorization
  #  self.authority

  @property
  def service_account_email(self) -> str:
    return self.email

  @property
  def service_account_authorization(self) -> str:
    return self.authorization

  @property
  def service_account_authority(self) -> str:
    return self.authority

  @property
  def user_email(self) -> str:
    return self._user_email

  @property
  def user_authorization(self) -> str:
    return self._user_authorization

  @property
  def user_authority(self) -> str:
    return self._user_authority


def _normalize_email(email: str) -> str:
  """Returns normalized email.

  Converts email to lowercase, removes bounding white space, and removes
  google account prefix that is added by IAP.

  Args:
    email: User email.

  Returns:
    Normalized email.
  """
  google_prefix = _GOOGLE_ACCOUNT_EMAIL_PREFIX.lower()
  email = email.strip().lower()
  if email.startswith(google_prefix):
    return email[len(google_prefix) :].strip()
  return email


def _are_emails_different(email_1: str, email_2: str) -> bool:
  return _normalize_email(email_1) != _normalize_email(email_2)


def _get_code_value(dataset: pydicom.dataset.Dataset) -> str:
  if _LONG_CODE_VALUE_TAG.keyword in dataset:
    return dataset.LongCodeValue
  return dataset.CodeValue


def _set_code_value(dataset: pydicom.dataset.Dataset, value: str) -> None:
  if len(value) <= 16:
    dataset.CodeValue = value
  else:
    dataset.LongCodeValue = value


def _convert_creator_email_to_dicom_dataset(
    user_email: str,
    institution_name: str,
) -> pydicom.dataset.Dataset:
  """Converts user email to a DICOM OperatorIdentification dataset.

  Args:
    user_email: Email to store for user.
    institution_name: Annotator's institution name.

  Returns:
    DICOM dataset
  """
  operator_id = pydicom.dataset.Dataset()
  operator_id.InstitutionName = institution_name
  person_id = pydicom.dataset.Dataset()
  email = _normalize_email(user_email)
  _set_code_value(person_id, email)
  person_id.CodeMeaning = 'Annotator Id'
  person_id.CodingSchemeDesignator = '99Google'
  operator_id.PersonIdentificationCodeSequence = [person_id]
  return operator_id


def _verify_allowed_dicom_annotations_store_address(
    url: dicom_url_util.DicomWebBaseURL,
) -> bool:
  """Verifies if the request is for an allowed Annotations DICOM Store.

  Args:
    url: Base DICOMweb URL for store.

  Returns:
    True if address is in allow list
  """
  return _norm_dicom_store_url(str(url)) in _get_dicom_store_allow_set()


def _text_response(
    msg: Union[str, bytes], status: http.HTTPStatus
) -> flask.Response:
  return flask.Response(
      msg, status=status, content_type=_TEXT_PLAIN, mimetype=_TEXT_PLAIN
  )


def delete_instance(
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL,
    study_instance_uid: str,
    series_instance_uid: str,
    sop_instance_uid: str,
) -> flask.Response:
  """Flask entry point for DICOMweb request to delete annotations.

  Deletes all annotations for a user on a slide.

  Args:
    dicom_web_base_url: Base DICOMweb URL for store.
    study_instance_uid: Study uid of instance to delete.
    series_instance_uid: Series uid of instance to delete.
    sop_instance_uid: Instance uid of instance to delete.

  Returns:
    flask.Response
  """
  base_log = {
      proxy_const.LogKeywords.BASE_DICOMWEB_URL: str(dicom_web_base_url),
      proxy_const.LogKeywords.STUDY_INSTANCE_UID: study_instance_uid,
      proxy_const.LogKeywords.SERIES_INSTANCE_UID: series_instance_uid,
      proxy_const.LogKeywords.SOP_INSTANCE_UID: sop_instance_uid,
  }
  cloud_logging_client.info('Delete instance', base_log)
  if not _verify_allowed_dicom_annotations_store_address(dicom_web_base_url):
    # If DICOM store not in annotation list. Proxy delete request.
    cloud_logging_client.debug(
        f'DICOM Store {dicom_web_base_url} is not'
        ' allow-listed. Proxying delete request.',
        {proxy_const.LogKeywords.DICOMWEB_URL: dicom_web_base_url},
        base_log,
    )
    return dicom_store_util.dicom_store_proxy()

  dicom_series_path = dicom_url_util.base_dicom_series_url(
      dicom_web_base_url,
      dicom_url_util.StudyInstanceUID(study_instance_uid),
      dicom_url_util.SeriesInstanceUID(series_instance_uid),
  )
  dicom_instance_path = dicom_url_util.series_dicom_instance_url(
      dicom_series_path, dicom_url_util.SOPInstanceUID(sop_instance_uid)
  )
  base_log[proxy_const.LogKeywords.DICOMWEB_URL] = dicom_instance_path
  # Get DICOM instance metadata.
  try:
    instances_metadata = dicom_store_util.get_instance_tags(
        user_auth_util.AuthSession(flask_util.get_headers()),
        dicom_series_path,
        dicom_url_util.SOPInstanceUID(sop_instance_uid),
        additional_tags=[
            _OPERATOR_IDENTIFICATION_SEQUENCE_TAG.keyword,
            _PERSON_IDENTIFICATION_CODE_SEQUENCE_TAG.keyword,
            _LONG_CODE_VALUE_TAG.keyword,
            _CODE_MEANING_TAG.keyword,
        ],
    )
  except dicom_store_util.DicomMetadataRequestError:
    instances_metadata = {}
  if not instances_metadata:
    # test if user has read access to DICOM store.
    try:
      _ServiceAccountCredentials(dicom_web_base_url)
    except _UnableToAuthenticateUserError:
      msg = _UNAUTHORIZED_CLIENT_ACCESS
      cloud_logging_client.error(msg, base_log)
      return _text_response(msg, http.HTTPStatus.UNAUTHORIZED)
    # If user can read from store then metadata read failed because DICOM does
    # not exist.
    msg = 'DICOM instance does not exist.'
    cloud_logging_client.warning(msg, base_log)
    return _text_response(msg, http.HTTPStatus.NOT_FOUND)

  # Read metadata using PyDICOM.
  try:
    dcm_metadata = pydicom.dataset.Dataset.from_json(
        dict(instances_metadata[0])
    )
  except (pydicom.errors.InvalidDicomError, IndexError) as exp:
    msg = 'Error decoding DICOM instance.'
    cloud_logging_client.error(msg, base_log, exp)
    return _text_response(msg, http.HTTPStatus.BAD_REQUEST)

  try:
    # Test if DICOM is Annotation if not proxy request.
    if dcm_metadata.SOPClassUID != _MICROSCOPY_BULK_SIMPLE_ANNOTATIONS_IOD:
      cloud_logging_client.debug(
          'DICOM SOPClassUID != MICROSCOPY_BULK_SIMPLE_ANNOTATIONS_IOD proxying'
          ' delete request.',
          {proxy_const.LogKeywords.SOP_CLASS_UID: dcm_metadata.SOPClassUID},
      )
      return dicom_store_util.dicom_store_proxy()
  except AttributeError as exp:
    msg = 'Error cannot determine DICOM instance SOPClassUID.'
    cloud_logging_client.error(msg, base_log, exp)
    return _text_response(msg, http.HTTPStatus.BAD_REQUEST)

  # Get service account credientals to user to perfrom delete.
  try:
    service_account = _ServiceAccountCredentials(dicom_web_base_url)
  except _UnableToAuthenticateUserError:
    msg = _UNAUTHORIZED_CLIENT_ACCESS
    cloud_logging_client.error(msg, base_log)
    return _text_response(msg, http.HTTPStatus.UNAUTHORIZED)
  # Test that annotation creator == user
  try:
    annotation_creator_email = _get_code_value(
        dcm_metadata.OperatorIdentificationSequence[
            0
        ].PersonIdentificationCodeSequence[0]
    )
  except (IndexError, AttributeError) as exp:
    msg = (
        'Error deleting DICOM annotation instance. Missing creator email in'
        ' DICOM instance.'
    )
    cloud_logging_client.error(msg, base_log, exp)
    return _text_response(msg, http.HTTPStatus.BAD_REQUEST)

  if _are_emails_different(
      annotation_creator_email, service_account.user_email
  ):
    msg = (
        'Cannot delete annotation. User email does not match DICOM operator'
        ' identifier.'
    )
    cloud_logging_client.error(
        msg,
        base_log,
        {
            'operator_identifer': annotation_creator_email,
            proxy_const.LogKeywords.USER_EMAIL: service_account.user_email,
        },
    )
    return _text_response(msg, http.HTTPStatus.UNAUTHORIZED)

  cloud_logging_client.info(
      'Validated annotation creator email for annotation delete.',
      base_log,
      {proxy_const.LogKeywords.USER_EMAIL: annotation_creator_email},
  )
  # Delete annotation
  return dicom_store_util.delete_instance_from_dicom_store(
      service_account, dicom_instance_path
  )


def _get_operator_identifier(dcm_file: pydicom.dataset.Dataset) -> List[str]:
  """Returns WSI Annotation operator indentifier SQ."""
  try:
    result = []
    for op in dcm_file.OperatorIdentificationSequence:
      for per_seq in op.PersonIdentificationCodeSequence:
        result.append(_get_code_value(per_seq))
    return result
  except AttributeError:
    return []


def _fix_incorrectly_formatted_filemeta_header(
    dcm: pydicom.FileDataset,
) -> pydicom.FileDataset:
  """Fix incorrectly fromatted filemeta header."""
  re_write_dicom_if_missing = [
      'MediaStorageSOPClassUID',
      'MediaStorageSOPInstanceUID',
      'FileMetaInformationGroupLength',
      'FileMetaInformationVersion',
      'ImplementationClassUID',
  ]
  if all([keyword in dcm.file_meta for keyword in re_write_dicom_if_missing]):
    return dcm
  with tempfile.TemporaryDirectory() as temp_dir:
    path = os.path.join(temp_dir, 'temp.dcm')
    # Force pydicom to add the above filemetadata headers to the DICOM if they
    # are missing.
    pydicom_version_util.save_as_validated_dicom(dcm, path)
    return pydicom.dcmread(path)


def _upload_wsi_annotation(
    service_account: _ServiceAccountCredentials,
    dcm_file: pydicom.FileDataset,
    store_instance_path: str,
    headers: Mapping[str, str],
) -> flask.Response:
  """Upload part 10 DICOM to DICOM Store."""
  dcm_file = _fix_incorrectly_formatted_filemeta_header(dcm_file)
  user_email = service_account.user_email
  operators = _get_operator_identifier(dcm_file)
  if not operators:
    cloud_logging_client.warning(
        'DICOM operator identifier is empty adding current user.',
        {proxy_const.LogKeywords.USER_EMAIL: user_email},
    )
    dcm_file.OperatorIdentificationSequence = [
        _convert_creator_email_to_dicom_dataset(
            user_email,
            dicom_proxy_flags.DEFAULT_ANNOTATOR_INSTITUTION_FLG.value,
        )
    ]
  elif len(operators) > 1:
    msg = (
        'Failed to create DICOM Annotation. DICOM annotation describes multiple'
        ' operators.'
    )
    cloud_logging_client.error(msg, dcm_file.to_json_dict())
    return _text_response(msg, http.HTTPStatus.BAD_REQUEST)
  elif _are_emails_different(operators[0], user_email):
    msg = (
        'Failed to create DICOM Annotation. Operator identifier does not '
        'match current user email.'
    )
    cloud_logging_client.error(
        msg,
        {
            'operator_identifer': operators[0],
            proxy_const.LogKeywords.USER_EMAIL: user_email,
        },
    )
    return _text_response(msg, http.HTTPStatus.BAD_REQUEST)
  cloud_logging_client.debug(
      'Sending annotation create request to dicom store.',
      dcm_file.to_json_dict(),
  )
  return dicom_store_util.upload_instance_to_dicom_store(
      service_account, dcm_file, store_instance_path, headers
  )


def _build_pydicom_dicom_from_request_json(
    content: bytes,
) -> pydicom.FileDataset:
  """Converts DICOM store formatted json into PyDicom FileDataset.

  Args:
    content: bytes recieved in multipart DICOM json data.

  Returns:
    PyDicom FileDataset represented by JSON

  Raises:
    _InvalidDicomJsonError: Invalid DICOM JSON
  """
  try:
    all_tags = json.loads(content)
    if isinstance(all_tags, list):
      if len(all_tags) != 1:
        cloud_logging_client.error(
            'Invalid DICOM annotation JSON, unexpected number of parts.',
            {proxy_const.LogKeywords.JSON: all_tags},
        )
        raise ValueError(f'Error found {len(all_tags)} instances in part.')
      all_tags = all_tags[0]
    if not isinstance(all_tags, dict):
      cloud_logging_client.error(
          'Invalid DICOM annotation JSON, metadata is not a dict.',
          {proxy_const.LogKeywords.JSON: all_tags},
      )
      raise ValueError('Invalid formatted DICOM JSON.')
    file_meta_tags = {}
    dataset_tags = {}
    for address, value in all_tags.items():
      if address.startswith('0002'):
        file_meta_tags[address] = value
      else:
        dataset_tags[address] = value
    file_meta = pydicom.dataset.Dataset().from_json(json.dumps(file_meta_tags))
    base_dataset = pydicom.Dataset().from_json(json.dumps(dataset_tags))
    dcm = pydicom.dataset.FileDataset(
        '',
        base_dataset,
        preamble=b'\0' * 128,
        file_meta=pydicom.dataset.FileMetaDataset(file_meta),
    )
    cloud_logging_client.info('Generated DICOM annotation instance.')
    return dcm
  except Exception as exp:
    raise _InvalidDicomJsonError(
        f'Error decoding DICOM from JSON. JSON: {content}'
    ) from exp


def _get_status_code_from_multiple_responses(
    responses: List[flask.Response],
) -> http.HTTPStatus:
  """Returns single status code for multiple DICOM instance store requests.

  DICOM API (Table 6.6.1-1. HTTP/1.1 Standard Response Code):
  https://dicom.nema.org/dicom/2013/output/chtml/part18/sect_6.6.html

  Args:
    responses: List of responses from multple store requests.

  Returns:
    Status code for all requests.
  """
  status_codes = {response.status_code for response in responses}
  # if all responses have the same status codes return that code.
  if len(status_codes) == 1:
    return status_codes.pop()
  if any([code < 300 for code in status_codes]):
    return http.HTTPStatus.ACCEPTED
  return http.HTTPStatus.CONFLICT


def _generate_multipart_upload_response(
    responses: List[flask.Response],
) -> flask.Response:
  """Combines list of DICOM insert responses into single response."""
  if not responses:
    return _text_response('Bad Request.', http.HTTPStatus.BAD_REQUEST)

  if len(responses) == 1:
    # if single response
    return responses[0]
  status_code = _get_status_code_from_multiple_responses(responses)
  try:
    dicom_xml = None
    json_response = []
    other_response = []
    content_type = _TEXT_PLAIN
    # Iteratate over each response
    for response in responses:
      # determine content type of resposne
      content_type = (
          flask_util.get_key_value(response.headers, _CONTENT_TYPE, _TEXT_PLAIN)
          .lower()
          .strip()
      )
      response_bytes = response.get_data(as_text=False)
      cloud_logging_client.info(
          'Generating multipart upload.',
          {'response_bytes': response_bytes, 'content-type': {content_type}},
      )
      if content_type == _APPLICATION_DICOM_XML:
        # If content type is DICOM XML Read XML and build XML response
        if dicom_xml is None:
          dicom_xml = ET.fromstring(response_bytes)
        else:
          temp = ET.fromstring(response_bytes)
          for child in temp.findall('*'):
            dicom_xml.append(child)
      elif content_type == _APPLICATION_DICOM_JSON:
        # If content type is DICOM JSO Read JSON and build JSON response
        dicom = json.loads(response_bytes)
        if isinstance(dicom, list):
          json_response.extend(dicom)
        else:
          json_response.append(dicom)
      else:
        # If something else just create a list.
        other_response.append(response_bytes)
    response_bytes = b''
    if dicom_xml is not None:
      # If XML found generate XML string response
      response_bytes = ET.tostring(dicom_xml)
    if json_response:
      # If JSON found generate JSON response
      if len(json_response) == 1:
        json_response = json_response[0]
      json_response = json.dumps(json_response).encode('us-ascii')
      # If XML and JSON found, combine and return as plain text.
      if response_bytes:
        content_type = _TEXT_PLAIN
        response_bytes = b'\n\n'.join([response_bytes, json_response])
      else:
        response_bytes = json_response
    if other_response:
      # If Something unexpected found. Return as plain text and combine with
      # pre-existing XML and JSON results.
      content_type = _TEXT_PLAIN
      other_response = b'\n\n'.join(other_response)
      if response_bytes:
        response_bytes = b'\n\n'.join([response_bytes, other_response])
      else:
        response_bytes = other_response
    # Return combined response
    return flask.Response(
        response_bytes,
        status=status_code,
        content_type=content_type,
    )
  except (ET.ParseError, json.decoder.JSONDecodeError) as exp:
    response_bytes = b'\n\n'.join(
        [response.get_data(as_text=False) for response in responses]
    )
    cloud_logging_client.error(
        'Error decoding responses.', {'responses': response_bytes}, exp
    )
    return _text_response(response_bytes, status_code)


def _get_pydicom_sopclassuid(dcm_bytes: IO[bytes]) -> str:
  """Returns SOPClassUID of part10 binary DICOM.

  Args:
    dcm_bytes: Part10 binary DICOM bytes.

  Raises:
    pydicom.errors.InvalidDicomError: Bytes are invalid.
  """
  with pydicom.dcmread(
      dcm_bytes, specific_tags=[_SOP_CLASS_UID_DICOM_TAG_ADDRESS]
  ) as dcm:
    return dcm.SOPClassUID


def _get_sop_class_uid_of_part(
    part_content_type: str, mp_data_content: bytes
) -> str:
  """Returns SOPClassUID of value encoded in multipart part.

  Args:
    part_content_type: Content type of part.
    mp_data_content: Part data.

  Returns:
    SOPClassUID

  Raises:
    _MultiPartContentSopClassUidDecodingError: Cannot determine SOPClassUID.
  """
  try:
    if part_content_type == _APPLICATION_DICOM_JSON:
      all_tags = json.loads(mp_data_content)
      if isinstance(all_tags, list):
        if len(all_tags) != 1:
          raise ValueError(f'Error found {len(all_tags)} instances in part.')
        all_tags = all_tags[0]
      if not isinstance(all_tags, dict):
        raise ValueError('Invalid formatted DICOM JSON.')
      return all_tags[_SOP_CLASS_UID_DICOM_TAG_ADDRESS][_VALUE][0]
    elif part_content_type == _APPLICATION_DICOM:
      with io.BytesIO(mp_data_content) as dcm_bytes:
        return _get_pydicom_sopclassuid(dcm_bytes)
    # Unrecongized content type could be a variety of things
    return _UNRECOGNIZED_CONTENT_TYPE
  except Exception as exp:
    raise _MultiPartContentSopClassUidDecodingError(
        f'Error determining SOPClassUID. Content-Type: {part_content_type};'
        f' Content: {mp_data_content}'
    ) from exp


def _multipart_encoder(parts: List[_MpRequestPart], boundary: bytes) -> bytes:
  """Converts Multipart parts back into Multipart request."""
  with io.BytesIO() as mp_bytes:
    crlf = b'\r\n'
    marker = b'--'
    start_of_part = b''.join([marker, boundary, crlf])
    for part in parts:
      mp_bytes.write(start_of_part)
      for key, value in part.headers.items():
        mp_bytes.write(key)
        mp_bytes.write(b': ')
        mp_bytes.write(value)
        mp_bytes.write(crlf)
      mp_bytes.write(crlf)
      mp_bytes.write(part.content)
      mp_bytes.write(crlf)
    mp_bytes.write(marker)
    mp_bytes.write(boundary)
    mp_bytes.write(marker)
    return mp_bytes.getvalue()


def store_instance(
    dicom_web_base_url: dicom_url_util.DicomWebBaseURL,
    study_uid: str = '',
) -> flask.Response:
  """Flask entry point for DICOMweb request to store an annotation instance.

  Args:
    dicom_web_base_url: Base DICOMweb URL for store.
    study_uid: Optional Study instance uid that added instances required to
      belong to.

  Returns:
    flask.Response
  """
  store_instance_path = f'{dicom_web_base_url.full_url}/studies'
  if study_uid:
    store_instance_path = f'{store_instance_path}/{study_uid}'
  base_log = {
      proxy_const.LogKeywords.BASE_DICOMWEB_URL: str(dicom_web_base_url),
      proxy_const.LogKeywords.STUDY_INSTANCE_UID: study_uid,
      proxy_const.LogKeywords.DICOM_STORE_URL: dicom_web_base_url,
      proxy_const.LogKeywords.DICOMWEB_URL: store_instance_path,
  }
  cloud_logging_client.info('Store instance annotation instance.', base_log)
  if not _verify_allowed_dicom_annotations_store_address(dicom_web_base_url):
    cloud_logging_client.debug(
        f'The DICOM Store {dicom_web_base_url} in the request is not'
        ' allow-listed for annotations; proxying request.',
        base_log,
    )
    # If not uploading request to annotation DICOM store proxy request.
    return dicom_store_util.dicom_store_proxy()
  # Get accept and content type headers
  accept_value = flask_util.get_key_value(flask_util.get_headers(), _ACCEPT, '')
  headers = {} if not accept_value else {_ACCEPT: accept_value}
  original_ct = flask_util.get_key_value(
      flask_util.get_headers(), _CONTENT_TYPE, ''
  ).strip()
  # Break content type into parts ";" deliminate parts
  content_type = original_ct.lower().replace(' ', '')
  if not content_type:
    # If content type is empty proxy request; will return error.
    cloud_logging_client.debug(
        'Missing content-type header; proxying annotation store request.',
        base_log,
    )
    return dicom_store_util.dicom_store_proxy()
  cloud_logging_client.info(
      f'Store annotation content-type={content_type}', base_log
  )
  if content_type == _APPLICATION_DICOM:
    # Handle upload of Part10 Binary DICOM instance.
    # Simple binary upload.
    with tempfile.TemporaryFile() as dicom_instance:
      dicom_instance.write(flask_util.get_data())
      dicom_instance.seek(0)
      try:
        # Load uploaded instance and test if instance is a
        # MICROSCOPY_BULK_SIMPLE_ANNOTATIONS_IOD
        sopclass_uid = _get_pydicom_sopclassuid(dicom_instance)
        if sopclass_uid != _MICROSCOPY_BULK_SIMPLE_ANNOTATIONS_IOD:
          # If not MICROSCOPY_BULK_SIMPLE_ANNOTATIONS_IOD then Proxy the request
          cloud_logging_client.debug(
              'Annotation DICOM is not MICROSCOPY_BULK_SIMPLE_ANNOTATIONS_IOD;'
              ' proxying annotation store request.',
              base_log,
              {proxy_const.LogKeywords.SOP_CLASS_UID: sopclass_uid},
          )
          return dicom_store_util.dicom_store_proxy()
        # Binary DICOM is MICROSCOPY_BULK_SIMPLE_ANNOTATIONS_IOD;
        # Read DICOM Fully
        dicom_instance.seek(0)
        with pydicom.dcmread(dicom_instance) as dcm:
          # Get proxy service account credientals.
          try:
            service_account = _ServiceAccountCredentials(dicom_web_base_url)
          except _UnableToAuthenticateUserError as exp:
            cloud_logging_client.error(
                'Unable to authenticate user cannot save annotation.',
                base_log,
                exp,
            )
            return _text_response(
                _UNAUTHORIZED_CLIENT_ACCESS,
                http.HTTPStatus.UNAUTHORIZED,
            )
          # Upload instance to DICOM store using service account credientals.
          return _upload_wsi_annotation(
              service_account, dcm, store_instance_path, headers
          )
      except pydicom.errors.InvalidDicomError as exp:
        cloud_logging_client.error('Invalid DICOM instance.', base_log, exp)
        return _text_response(
            'Invalid DICOM instance.',
            http.HTTPStatus.BAD_REQUEST,
        )
  elif content_type.startswith(f'{_MULTIPART_RELATED};'):
    # Hand upload of multipart/related data to DICOM Store.
    # Decoded recieved multi-part response.
    try:
      mp_response = requests_toolbelt.MultipartDecoder(
          flask_util.get_data(), original_ct
      )
    except (
        requests_toolbelt.NonMultipartContentTypeException,
        requests_toolbelt.ImproperBodyPartContentException,
    ) as exp:
      cloud_logging_client.error('Invalid multipart request.', base_log, exp)
      return _text_response(
          'Invalid multipart request.', http.HTTPStatus.BAD_REQUEST
      )
    # Determine content type and boundary of multipart/related.
    service_account = None
    non_annotation_parts = []
    boundary = mp_response.boundary
    upload_results = []
    # Process each part of the multipart response individually.
    # Parts describing annotations uploaded using service account credientals.
    # All other uploaded using user credientals.
    for mp_data in mp_response.parts:
      # Get content type of part.
      part_content_type = flask_util.get_key_value(
          {
              key.decode('utf-8'): value.decode('utf-8')
              for key, value in mp_data.headers.items()
          },
          _CONTENT_TYPE,
          '',
      )
      if not part_content_type:
        msg = 'Invalid multipart part. Missing content-type.'
        cloud_logging_client.error(msg, base_log)
        return _text_response(msg, http.HTTPStatus.BAD_REQUEST)
      part_content_type = (
          part_content_type.split(';')[0].replace(' ', '').strip('"').lower()
      )
      # Determine SOPClassUID of part.
      try:
        sopclass_uid = _get_sop_class_uid_of_part(
            part_content_type, mp_data.content
        )
      except _MultiPartContentSopClassUidDecodingError as exp:
        cloud_logging_client.error(
            'Cannot decode sopclass uid.',
            {proxy_const.LogKeywords.MULTIPART_CONTENT: mp_data.text},
            base_log,
            exp,
        )
        return _text_response(
            'Invalid multipart request.', http.HTTPStatus.BAD_REQUEST
        )
      # If multipart content is not a annotation IOD then upload the
      # part as a part of multipart/related request that includes everything
      # but wsi annotations.
      if sopclass_uid != _MICROSCOPY_BULK_SIMPLE_ANNOTATIONS_IOD:
        non_annotation_parts.append(
            _MpRequestPart(mp_data.headers, mp_data.content)
        )
        continue
      # Part describes an WSI Annotation DICOM instance.
      # Read instance, could be inline JSON or inline Part10 Binary DICOM.
      try:
        if part_content_type == _APPLICATION_DICOM_JSON:
          dcm = _build_pydicom_dicom_from_request_json(mp_data.content)
        elif part_content_type == _APPLICATION_DICOM:
          with io.BytesIO(mp_data.content) as dcm_bytes:
            dcm = pydicom.dcmread(dcm_bytes)
        else:
          raise ValueError(
              f'Invalid multipart content-type {part_content_type}.'
          )
      except (
          ValueError,
          _InvalidDicomJsonError,
          pydicom.errors.InvalidDicomError,
      ) as exp:
        cloud_logging_client.error(
            'Invalid DICOM instance in multipart request.',
            {proxy_const.LogKeywords.MULTIPART_CONTENT: mp_data.content},
            exp,
            base_log,
        )
        return _text_response(
            'Invalid DICOM instance in multipart request.',
            http.HTTPStatus.BAD_REQUEST,
        )
      # Get service account credientials if not initalized.
      if service_account is None:
        try:
          service_account = _ServiceAccountCredentials(dicom_web_base_url)
        except _UnableToAuthenticateUserError as exp:
          cloud_logging_client.error(_UNAUTHORIZED_CLIENT_ACCESS, base_log, exp)
          return _text_response(
              _UNAUTHORIZED_CLIENT_ACCESS, status=http.HTTPStatus.UNAUTHORIZED
          )
      # Upload WSI annotations using service account credientals.
      upload_wsi_annotation = _upload_wsi_annotation(
          service_account, dcm, store_instance_path, headers
      )
      upload_results.append(upload_wsi_annotation)
    # If non-annotation parts were included in multipart/releated request
    # Re-assemble multipart related request without annotations and upload
    # to store using user credientals.
    if non_annotation_parts:
      multipart_data = _multipart_encoder(non_annotation_parts, boundary)
      user_auth = user_auth_util.AuthSession(flask_util.get_headers())
      multipart_result = dicom_store_util.upload_multipart_to_dicom_store(
          user_auth,
          multipart_data,
          original_ct,
          store_instance_path,
          headers,
      )
      upload_results.append(multipart_result)
    # Combine responses from WSI Annotation uploads and upload of multipart
    # response into a single response and return combined results.
    return _generate_multipart_upload_response(upload_results)
  else:
    # Content type describes something else proxy request across DICOM store
    # and return response.
    cloud_logging_client.debug(
        'Unrecognized content-type; proxying annotation store request.',
        base_log,
    )
    return dicom_store_util.dicom_store_proxy()
