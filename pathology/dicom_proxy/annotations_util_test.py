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
"""Tests for annotations util."""

from __future__ import annotations

import contextlib
import copy
import functools
import http
import json
from typing import List, Mapping, Optional
from unittest import mock
import uuid

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import flask
import google.auth
import pydicom
import requests
import requests_mock

from pathology.dicom_proxy import annotations_util
from pathology.dicom_proxy import dicom_store_util
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import shared_test_util
from pathology.dicom_proxy import user_auth_util
from pathology.shared_libs.test_utils.dicom_store_mock import dicom_store_mock
from pathology.shared_libs.test_utils.dicom_store_mock import dicom_store_mock_types

MOCK_TEST_API_VERSION = 'v1'
MOCK_EMAIL = 'test@hostname.com'
MOCK_USER_ID = '1234'
MOCK_TOKEN = 'token'
MOCK_USER_AUTHORITY = 'user_authority'
MOCK_SERVICE_ACCOUNT_TOKEN = 'mock_service_account_token'
MOCK_SERVICE_ACCOUNT_EMAIL = 'mock_service_account_email'
MOCK_INSTITUTION = 'mock_institution'


def _bearer_token(token: str) -> str:
  return f'Bearer {token}'


def _user_auth_header_value() -> str:
  return _bearer_token(f'{MOCK_TOKEN}_{uuid.uuid1()}')


def _mock_request_response_callback(user_email, request, unused_context):
  auth_key = request.headers.get(proxy_const.HeaderKeywords.AUTH_HEADER_KEY, '')
  if auth_key.startswith(_bearer_token(f'{MOCK_TOKEN}_')):
    return user_email
  elif auth_key == _bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN):
    return json.dumps(
        {'email': MOCK_SERVICE_ACCOUNT_EMAIL, 'verified_email': True}
    )
  return ''


def _file_bytes(path: str) -> bytes:
  with open(path, 'rb') as infile:
    return infile.read()


def _multipart_request(
    boundary: bytes,
    *content_paths,
    include_content_type: bool = True,
    content_type: bytes = b'application/dicom',
) -> bytes:
  boundry_token = b''.join([b'--', boundary])
  close_boundry_token = b''.join([b'--', boundary, b'--'])
  content_list = []
  content_type = b''.join([b'Content-Type: ', content_type])
  for content_path in content_paths:
    content_list.append(boundry_token)
    if include_content_type:
      content_list.append(content_type)
    content_list.append(b'')
    content_list.append(_file_bytes(content_path))
  content_list.append(close_boundry_token)
  return b'\r\n'.join(content_list)


class _MockDicomStoreTest(contextlib.ExitStack):

  def __init__(
      self,
      flask_request_args: Optional[Mapping[str, str]] = None,
      flask_request_data: bytes = b'',
      flask_request_headers: Optional[Mapping[str, str]] = None,
      flask_request_method: str = 'GET',
      flask_request_path: str = '',
      dicomstore_project: str = 'mock_project',
      dicomstore_location: str = 'earth',
      dicomstore_dataset: str = 'mock_dateset',
      dicomstore_name: str = 'mock_store',
      dicomstore_in_annotation_allow_list: bool = False,
      validate_iap: bool = False,
      read_auth_bearer_tokens: Optional[List[str]] = None,
      write_auth_bearer_tokens: Optional[List[str]] = None,
  ):
    super().__init__()
    self._flask_request_args = flask_request_args
    self._flask_request_data = flask_request_data
    if flask_request_headers is None:
      self._flask_request_headers = {}
    else:
      self._flask_request_headers = dict(flask_request_headers)
    self._flask_request_method = flask_request_method
    self._flask_request_path = flask_request_path
    self._project = dicomstore_project
    self._location = dicomstore_location
    self._dataset = dicomstore_dataset
    self._dicomstore = dicomstore_name
    self._flagsaver = {}
    self._read_auth_bearer_tokens = copy.copy(read_auth_bearer_tokens)
    self._write_auth_bearer_tokens = copy.copy(write_auth_bearer_tokens)
    if not dicomstore_in_annotation_allow_list:
      self._flagsaver['dicom_annotations_store_allow_list'] = []
    else:
      self._flagsaver['dicom_annotations_store_allow_list'] = [
          str(self.dicomweb_base_url)
      ]

    self._validate_iap = validate_iap
    self._flagsaver['validate_iap'] = validate_iap

  @property
  def project(self) -> str:
    return self._project

  @property
  def location(self) -> str:
    return self._location

  @property
  def dataset(self) -> str:
    return self._dataset

  @property
  def dicomstore(self) -> str:
    return self._dicomstore

  @property
  def dicomweb_base_url(self) -> dicom_url_util.DicomWebBaseURL:
    return dicom_url_util.DicomWebBaseURL(
        MOCK_TEST_API_VERSION,
        self.project,
        self.location,
        self.dataset,
        self.dicomstore,
    )

  @property
  def mocked_dicomstore(self) -> dicom_store_mock.MockDicomStoreClient:
    return self._mk_dicomstore  # pytype: disable=attribute-error

  def __enter__(self) -> _MockDicomStoreTest:
    result = super().__enter__()

    self.enter_context(
        mock.patch.object(
            google.auth,
            'default',
            autospec=True,
            return_value=(_MockServiceAccountCredentials(), 'mock_project'),
        )
    )
    self._mock_request = result.enter_context(requests_mock.Mocker())
    if self._validate_iap:
      self._flask_request_headers.update({
          proxy_const.HeaderKeywords.IAP_EMAIL_KEY: MOCK_EMAIL,
          proxy_const.HeaderKeywords.IAP_USER_ID_KEY: MOCK_USER_ID,
      })
      mock_request_response = ''
    else:
      mock_request_response = json.dumps(
          {'email': MOCK_EMAIL, 'verified_email': True}
      )
    self._mock_request.get(
        user_auth_util._USER_INFO_REQUEST_URL,
        status_code=http.client.OK,
        text=functools.partial(
            _mock_request_response_callback, mock_request_response
        ),
    )
    result.enter_context(flagsaver.flagsaver(**self._flagsaver))
    store_path = f'https://healthcare.googleapis.com/{self.dicomweb_base_url}'
    self._mk_dicomstore = result.enter_context(
        dicom_store_mock.MockDicomStores(
            store_path,
            mock_request=self._mock_request,
            mock_credential=False,
            read_auth_bearer_tokens=self._read_auth_bearer_tokens,
            write_auth_bearer_tokens=self._write_auth_bearer_tokens,
        )
    )[store_path]
    result.enter_context(
        shared_test_util.MockFlaskRequest(
            args=self._flask_request_args,
            data=self._flask_request_data,
            headers=self._flask_request_headers,
            method=self._flask_request_method,
            path=f'{self.dicomweb_base_url}{self._flask_request_path}',
        )
    )
    return self


class _MockDicomStoreDeleteTest(_MockDicomStoreTest):

  def __init__(
      self,
      user_token: str,
      dicomstore_in_allow_list: bool,
      validate_iap: bool,
      dicomstore_read_auth_token: str,
      dicomstore_write_auth_token: str,
      dcm: pydicom.FileDataset,
  ):
    super().__init__(
        flask_request_headers={
            'accept': 'application/dicom+json',
            proxy_const.HeaderKeywords.AUTH_HEADER_KEY: user_token,
        },
        flask_request_method='DELETE',
        flask_request_path=f'/studies/{dcm.StudyInstanceUID}/series/{dcm.SeriesInstanceUID}/instances/{dcm.SOPInstanceUID}',
        dicomstore_in_annotation_allow_list=dicomstore_in_allow_list,
        validate_iap=validate_iap,
        read_auth_bearer_tokens=[dicomstore_read_auth_token],
        write_auth_bearer_tokens=[dicomstore_write_auth_token],
    )


class _MockServiceAccountCredentials:

  def __init__(self):
    self._token = ''

  def refresh(self, _: requests.Request) -> None:
    self._token = MOCK_SERVICE_ACCOUNT_TOKEN

  @property
  def token(self) -> str:
    return self._token


class AnnotationsUtilTest(parameterized.TestCase):

  def test_get_operator_identifier_missing(self):
    dcm = shared_test_util.wsi_dicom_annotation_instance()
    self.assertEmpty(annotations_util._get_operator_identifier(dcm))

  @parameterized.named_parameters([
      dict(
          testcase_name='empty',
          test='',
          expected='',
      ),
      dict(
          testcase_name='unrecognized',
          test='AbCd',
          expected='abcd',
      ),
      dict(
          testcase_name='min',
          test=(
              'Projects/Foo/locations/us-central1/datasets/bar/dicomStores/baz'
          ),
          expected='foo:us-central1:bar:baz',
      ),
      dict(
          testcase_name='fukll',
          test='https//abc.com/Projects/Foo/locations/us-central1/datasets/bar/dicomStores/baz/dicomWeb/studies',
          expected='foo:us-central1:bar:baz',
      ),
  ])
  def test_norm_dicom_store_url(self, test, expected):
    self.assertEqual(annotations_util._norm_dicom_store_url(test), expected)

  @parameterized.named_parameters([
      dict(testcase_name='empty', creator_sq=[], exp=[]),
      dict(
          testcase_name='single_short',
          creator_sq=[
              annotations_util._convert_creator_email_to_dicom_dataset(
                  'foo', MOCK_INSTITUTION
              )
          ],
          exp=['foo'],
      ),
      dict(
          testcase_name='single_long',
          creator_sq=[
              annotations_util._convert_creator_email_to_dicom_dataset(
                  'foo@example_company.com', MOCK_INSTITUTION
              )
          ],
          exp=['foo@example_company.com'],
      ),
      dict(
          testcase_name='multiple',
          creator_sq=[
              annotations_util._convert_creator_email_to_dicom_dataset(
                  'foo', MOCK_INSTITUTION
              ),
              annotations_util._convert_creator_email_to_dicom_dataset(
                  'bar', MOCK_INSTITUTION
              ),
          ],
          exp=['foo', 'bar'],
      ),
  ])
  def test_get_operator_identifier(self, creator_sq, exp):
    dcm = shared_test_util.wsi_dicom_annotation_instance()
    dcm.OperatorIdentificationSequence = creator_sq
    self.assertEqual(annotations_util._get_operator_identifier(dcm), exp)

  @parameterized.named_parameters([
      dict(
          testcase_name='invalid_single',
          creator_sq=[
              annotations_util._convert_creator_email_to_dicom_dataset(
                  'bar@example.com', MOCK_INSTITUTION
              )
          ],
          expected_status_code=http.HTTPStatus.BAD_REQUEST,
      ),
      dict(
          testcase_name='invalid_multiple',
          creator_sq=[
              annotations_util._convert_creator_email_to_dicom_dataset(
                  'foo@example.com', MOCK_INSTITUTION
              ),
              annotations_util._convert_creator_email_to_dicom_dataset(
                  'bar@example.com', MOCK_INSTITUTION
              ),
          ],
          expected_status_code=http.HTTPStatus.BAD_REQUEST,
      ),
      dict(
          testcase_name='valid',
          creator_sq=[
              annotations_util._convert_creator_email_to_dicom_dataset(
                  'foo@example.com', MOCK_INSTITUTION
              ),
          ],
          expected_status_code=http.HTTPStatus.OK,
      ),
  ])
  @mock.patch.object(
      dicom_store_util,
      'upload_instance_to_dicom_store',
      autospec=True,
      return_value=flask.Response(
          response='mock_ok',
          status=http.HTTPStatus.OK,
          mimetype=annotations_util._TEXT_PLAIN,
          content_type=annotations_util._TEXT_PLAIN,
      ),
  )
  def test_upload_wsi_annotation_operator_test(
      self, unused_mk_upload, creator_sq, expected_status_code
  ):
    sa = mock.create_autospec(annotations_util._ServiceAccountCredentials)
    type(sa).user_email = mock.PropertyMock(return_value='foo@example.com')
    dcm = shared_test_util.wsi_dicom_annotation_instance()
    dcm.OperatorIdentificationSequence = creator_sq
    store_instance_path = 'unused'
    headers = {}
    result = annotations_util._upload_wsi_annotation(
        sa, dcm, store_instance_path, headers
    )
    self.assertEqual(result.status_code, expected_status_code)

  @mock.patch.object(
      dicom_store_util,
      'upload_instance_to_dicom_store',
      autospec=True,
      return_value=flask.Response(
          response='mock_ok',
          status=http.HTTPStatus.OK,
          mimetype=annotations_util._TEXT_PLAIN,
          content_type=annotations_util._TEXT_PLAIN,
      ),
  )
  def test_upload_wsi_annotation_fixs_incorrectly_formatted_filemeta_header(
      self, mk_upload
  ):
    sa = mock.create_autospec(annotations_util._ServiceAccountCredentials)
    type(sa).user_email = mock.PropertyMock(return_value='foo@example.com')
    dcm = shared_test_util.wsi_dicom_annotation_instance()
    dcm.OperatorIdentificationSequence = [
        annotations_util._convert_creator_email_to_dicom_dataset(
            'foo@example.com', MOCK_INSTITUTION
        ),
    ]
    fix_tags = [
        'MediaStorageSOPClassUID',
        'MediaStorageSOPInstanceUID',
        'FileMetaInformationGroupLength',
        'FileMetaInformationVersion',
        'ImplementationClassUID',
    ]
    for tag in fix_tags:
      del dcm.file_meta[tag]
    store_instance_path = 'unused'
    headers = {}
    annotations_util._upload_wsi_annotation(
        sa, dcm, store_instance_path, headers
    )
    dcm = mk_upload.call_args.args[1]
    for tag in fix_tags:
      self.assertIn(tag, dcm.file_meta)

  @mock.patch.object(
      dicom_store_util,
      'upload_instance_to_dicom_store',
      autospec=True,
      return_value=flask.Response(
          response='mock_ok',
          status=http.HTTPStatus.OK,
          mimetype=annotations_util._TEXT_PLAIN,
          content_type=annotations_util._TEXT_PLAIN,
      ),
  )
  @flagsaver.flagsaver(default_annotator_institution='magic_health')
  def test_upload_wsi_annotation_test_annotation_creator_instiution(
      self, mk_upload
  ):
    sa = mock.create_autospec(annotations_util._ServiceAccountCredentials)
    type(sa).user_email = mock.PropertyMock(return_value='foo@example.com')
    dcm = shared_test_util.wsi_dicom_annotation_instance()

    store_instance_path = 'unused'
    headers = {}
    annotations_util._upload_wsi_annotation(
        sa, dcm, store_instance_path, headers
    )
    dcm = mk_upload.call_args.args[1]
    self.assertEqual(
        dcm.OperatorIdentificationSequence[0].InstitutionName,
        'magic_health',
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='short',
          user_email='foo@m.com',
          expected_tag_keyword='CodeValue',
      ),
      dict(
          testcase_name='long_short',
          user_email='foobar@example_compay.com',
          expected_tag_keyword='LongCodeValue',
      ),
  ])
  @mock.patch.object(
      dicom_store_util,
      'upload_instance_to_dicom_store',
      autospec=True,
      return_value=flask.Response(
          response='mock_ok',
          status=http.HTTPStatus.OK,
          mimetype=annotations_util._TEXT_PLAIN,
          content_type=annotations_util._TEXT_PLAIN,
      ),
  )
  def test_upload_wsi_annotation_test_annotation_user_email(
      self, mk_upload, user_email, expected_tag_keyword
  ):
    sa = mock.create_autospec(annotations_util._ServiceAccountCredentials)
    type(sa).user_email = mock.PropertyMock(return_value=user_email)
    dcm = shared_test_util.wsi_dicom_annotation_instance()

    store_instance_path = 'unused'
    headers = {}
    annotations_util._upload_wsi_annotation(
        sa, dcm, store_instance_path, headers
    )
    dcm = mk_upload.call_args.args[1]
    self.assertEqual(
        dcm.OperatorIdentificationSequence[0]
        .PersonIdentificationCodeSequence[0][expected_tag_keyword]
        .value,
        user_email,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='invalid_json',
          content=b'abc',
      ),
      dict(
          testcase_name='invalid_json_list_not_contain_dict',
          content=b'["abc"]',
      ),
      dict(
          testcase_name='invalid_json_list_contain_multiple_dict',
          content=b'[{}, {}]',
      ),
      dict(
          testcase_name='invalid_pydicom',
          content=b'[{"abc": [123]}]',
      ),
  ])
  def test_build_pydicom_dicom_from_request_json_throws_invalid_content(
      self, content
  ):
    with self.assertRaises(annotations_util._InvalidDicomJsonError):
      annotations_util._build_pydicom_dicom_from_request_json(content)

  @parameterized.named_parameters([
      dict(
          testcase_name='invalid_json',
          content=b'abc',
      ),
      dict(
          testcase_name='invalid_json_list_not_contain_dict',
          content=b'["abc"]',
      ),
      dict(
          testcase_name='invalid_json_list_contain_multiple_dict',
          content=b'[{}, {}]',
      ),
      dict(
          testcase_name='invalid_pydicom',
          content=b'[{"abc": [123]}]',
      ),
  ])
  def test_get_sop_class_uid_of_part_throws_(self, content):
    with self.assertRaises(annotations_util._InvalidDicomJsonError):
      annotations_util._build_pydicom_dicom_from_request_json(content)

  @parameterized.named_parameters([
      dict(
          testcase_name='invalid_json',
          content_type=annotations_util._APPLICATION_DICOM_JSON,
          content=b'abc',
      ),
      dict(
          testcase_name='invalid_json_list_not_contain_dict',
          content_type=annotations_util._APPLICATION_DICOM_JSON,
          content=b'["abc"]',
      ),
      dict(
          testcase_name='invalid_json_list_contain_multiple_dict',
          content_type=annotations_util._APPLICATION_DICOM_JSON,
          content=b'[{}, {}]',
      ),
      dict(
          testcase_name='invalid_pydicom',
          content_type=annotations_util._APPLICATION_DICOM,
          content=b'123',
      ),
  ])
  def test_get_sop_class_uid_of_part_throws_if_uid_decoding_fails(
      self, content_type, content
  ):
    with self.assertRaises(
        annotations_util._MultiPartContentSopClassUidDecodingError
    ):
      annotations_util._get_sop_class_uid_of_part(content_type, content)

  def test_get_sop_class_uid_returns_unrecognized_if_content_type_unknown(self):
    self.assertEqual(
        annotations_util._get_sop_class_uid_of_part('abc', b'123'),
        annotations_util._UNRECOGNIZED_CONTENT_TYPE,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='all_ok',
          codes=[http.HTTPStatus.OK, http.HTTPStatus.OK],
          expected=http.HTTPStatus.OK.value,
      ),
      dict(
          testcase_name='all_BadRequest',
          codes=[http.HTTPStatus.BAD_REQUEST, http.HTTPStatus.BAD_REQUEST],
          expected=http.HTTPStatus.BAD_REQUEST.value,
      ),
      dict(
          testcase_name='mixed_ok',
          codes=[http.HTTPStatus.BAD_REQUEST, http.HTTPStatus.OK],
          expected=http.HTTPStatus.ACCEPTED.value,
      ),
      dict(
          testcase_name='mixed_failures',
          codes=[http.HTTPStatus.BAD_REQUEST, http.HTTPStatus.UNAUTHORIZED],
          expected=http.HTTPStatus.CONFLICT.value,
      ),
  ])
  def test_generate_multipart_upload_response(self, codes, expected):
    response_list = [flask.Response(b'empty', status=code) for code in codes]
    self.assertEqual(
        annotations_util._get_status_code_from_multiple_responses(
            response_list
        ),
        expected,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='empty',
          response_list=[],
          expected_status_code=http.HTTPStatus.BAD_REQUEST,
      ),
      dict(
          testcase_name='http_status_ok',
          response_list=[
              flask.Response(
                  'empty',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._TEXT_PLAIN,
              )
          ],
          expected_status_code=http.HTTPStatus.OK,
      ),
      dict(
          testcase_name='http_status_ok_multiple',
          response_list=[
              flask.Response(
                  'empty',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._TEXT_PLAIN,
              ),
              flask.Response(
                  'empty',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._TEXT_PLAIN,
              ),
          ],
          expected_status_code=http.HTTPStatus.OK,
      ),
      dict(
          testcase_name='http_status_bad_request',
          response_list=[
              flask.Response(
                  'empty',
                  status=http.HTTPStatus.BAD_REQUEST,
                  content_type=annotations_util._TEXT_PLAIN,
              )
          ],
          expected_status_code=http.HTTPStatus.BAD_REQUEST,
      ),
      dict(
          testcase_name='http_status_bad_request_multiple',
          response_list=[
              flask.Response(
                  'empty',
                  status=http.HTTPStatus.BAD_REQUEST,
                  content_type=annotations_util._TEXT_PLAIN,
              ),
              flask.Response(
                  'empty',
                  status=http.HTTPStatus.BAD_REQUEST,
                  content_type=annotations_util._TEXT_PLAIN,
              ),
          ],
          expected_status_code=http.HTTPStatus.BAD_REQUEST,
      ),
      dict(
          testcase_name='http_status_mixed_response_good_bad',
          response_list=[
              flask.Response(
                  'empty',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._TEXT_PLAIN,
              ),
              flask.Response(
                  'empty',
                  status=http.HTTPStatus.BAD_REQUEST,
                  content_type=annotations_util._TEXT_PLAIN,
              ),
          ],
          expected_status_code=http.HTTPStatus.ACCEPTED,
      ),
      dict(
          testcase_name='http_status_mixed_response_bad_good',
          response_list=[
              flask.Response(
                  'empty',
                  status=http.HTTPStatus.BAD_REQUEST,
                  content_type=annotations_util._TEXT_PLAIN,
              ),
              flask.Response(
                  'empty',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._TEXT_PLAIN,
              ),
          ],
          expected_status_code=http.HTTPStatus.ACCEPTED,
      ),
  ])
  def test_generate_multipart_upload_response_returns_expected_status_code(
      self, response_list, expected_status_code
  ):
    self.assertEqual(
        annotations_util._generate_multipart_upload_response(
            response_list
        ).status_code,
        expected_status_code,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='json_response',
          response_list=[
              flask.Response(
                  b'[{"test1": "abc"}]',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_JSON,
              ),
          ],
          expected_content_type=annotations_util._APPLICATION_DICOM_JSON,
      ),
      dict(
          testcase_name='multiple_json_response',
          response_list=[
              flask.Response(
                  b'[{"test1": "abc"}]',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_JSON,
              ),
              flask.Response(
                  b'[{"test1": "abc"}]',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_JSON,
              ),
          ],
          expected_content_type=annotations_util._APPLICATION_DICOM_JSON,
      ),
      dict(
          testcase_name='xml_response',
          response_list=[
              flask.Response(
                  b'<ROOT><Attribute></Attribute></ROOT>',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_XML,
              ),
          ],
          expected_content_type=annotations_util._APPLICATION_DICOM_XML,
      ),
      dict(
          testcase_name='multiple_xml_response',
          response_list=[
              flask.Response(
                  b'<ROOT><Attribute></Attribute></ROOT>',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_XML,
              ),
              flask.Response(
                  b'<ROOT><Attribute></Attribute></ROOT>',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_XML,
              ),
          ],
          expected_content_type=annotations_util._APPLICATION_DICOM_XML,
      ),
      dict(
          testcase_name='other_response',
          response_list=[
              flask.Response(
                  b'abc',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._TEXT_PLAIN,
              ),
          ],
          expected_content_type=annotations_util._TEXT_PLAIN,
      ),
      dict(
          testcase_name='multiple_other_response',
          response_list=[
              flask.Response(
                  b'abc',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._TEXT_PLAIN,
              ),
              flask.Response(
                  b'abc',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._TEXT_PLAIN,
              ),
          ],
          expected_content_type=annotations_util._TEXT_PLAIN,
      ),
      dict(
          testcase_name='mixed_response',
          response_list=[
              flask.Response(
                  '[{"test1": "abc"}]',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_JSON,
              ),
              flask.Response(
                  '<ROOT><Attribute></Attribute></ROOT>',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_XML,
              ),
          ],
          expected_content_type=annotations_util._TEXT_PLAIN,
      ),
  ])
  def test_generate_multipart_upload_response_content_type(
      self, response_list, expected_content_type
  ):
    self.assertEqual(
        annotations_util._generate_multipart_upload_response(
            response_list
        ).content_type,
        expected_content_type,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='json_response',
          response_list=[
              flask.Response(
                  b'[{"test1": "abc"}]',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_JSON,
              ),
          ],
          expected_content=b'[{"test1": "abc"}]',
      ),
      dict(
          testcase_name='multiple_json_response',
          response_list=[
              flask.Response(
                  b'[{"test1": "abc"}]',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_JSON,
              ),
              flask.Response(
                  b'[{"test2": "efg"}]',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_JSON,
              ),
          ],
          expected_content=b'[{"test1": "abc"}, {"test2": "efg"}]',
      ),
      dict(
          testcase_name='xml_response',
          response_list=[
              flask.Response(
                  b'<ROOT><Attribute></Attribute></ROOT>',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_XML,
              ),
          ],
          expected_content=b'<ROOT><Attribute></Attribute></ROOT>',
      ),
      dict(
          testcase_name='multiple_xml_response',
          response_list=[
              flask.Response(
                  '<ROOT><Attribute1>one</Attribute1></ROOT>',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_XML,
              ),
              flask.Response(
                  '<ROOT><Attribute2>two</Attribute2></ROOT>',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_XML,
              ),
          ],
          expected_content=(
              b'<ROOT><Attribute1>one</Attribute1><Attribute2>two</Attribute2></ROOT>'
          ),
      ),
      dict(
          testcase_name='other_response',
          response_list=[
              flask.Response(
                  'abc',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._TEXT_PLAIN,
              ),
          ],
          expected_content=b'abc',
      ),
      dict(
          testcase_name='multiple_other_response',
          response_list=[
              flask.Response(
                  'abc',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._TEXT_PLAIN,
              ),
              flask.Response(
                  'efg',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._TEXT_PLAIN,
              ),
          ],
          expected_content=b'abc\n\nefg',
      ),
      dict(
          testcase_name='mixed_json_xml_response',
          response_list=[
              flask.Response(
                  '[{"test1": "abc"}]',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_JSON,
              ),
              flask.Response(
                  '<ROOT><Attribute>one</Attribute></ROOT>',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_XML,
              ),
          ],
          expected_content=(
              b'<ROOT><Attribute>one</Attribute></ROOT>\n\n{"test1": "abc"}'
          ),
      ),
      dict(
          testcase_name='mixed_json_xml_txt_response',
          response_list=[
              flask.Response(
                  '[{"test1": "abc"}]',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_JSON,
              ),
              flask.Response(
                  '[{"test2": "efg"}]',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_JSON,
              ),
              flask.Response(
                  '<ROOT><Attribute>one</Attribute></ROOT>',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_XML,
              ),
              flask.Response(
                  'BAD',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._TEXT_PLAIN,
              ),
          ],
          expected_content=(
              b'<ROOT><Attribute>one</Attribute></ROOT>\n\n[{"test1": "abc"},'
              b' {"test2": "efg"}]\n\nBAD'
          ),
      ),
      dict(
          testcase_name='bad_json',
          response_list=[
              flask.Response(
                  '{"test1": "abc"}',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_JSON,
              ),
              flask.Response(
                  'test2',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_JSON,
              ),
          ],
          expected_content=b'{"test1": "abc"}\n\ntest2',
      ),
      dict(
          testcase_name='bad_xml',
          response_list=[
              flask.Response(
                  '<ROOT><Attribute>one</Attribute></ROOT>',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_XML,
              ),
              flask.Response(
                  '<ROOT></Attribute></ROOT>',
                  status=http.HTTPStatus.OK,
                  content_type=annotations_util._APPLICATION_DICOM_XML,
              ),
          ],
          expected_content=b'<ROOT><Attribute>one</Attribute></ROOT>\n\n<ROOT></Attribute></ROOT>',
      ),
  ])
  def test_generate_multipart_upload_response_content(
      self, response_list, expected_content
  ):
    self.assertEqual(
        annotations_util._generate_multipart_upload_response(
            response_list
        ).get_data(),
        expected_content,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='non_allow_listed_store',
          dicomstore_in_allow_list=False,
          validate_iap=False,
      ),
      dict(
          testcase_name='allow_listed_store',
          dicomstore_in_allow_list=True,
          validate_iap=False,
      ),
      dict(
          testcase_name='non_allow_listed_store_iap',
          dicomstore_in_allow_list=False,
          validate_iap=True,
      ),
      dict(
          testcase_name='allow_listed_store_iap',
          dicomstore_in_allow_list=True,
          validate_iap=True,
      ),
  ])
  def test_store_add_binary_generic_dicom_instance_succeeds(
      self, dicomstore_in_allow_list: bool, validate_iap: bool
  ):
    with _MockDicomStoreTest(
        flask_request_data=_file_bytes(
            shared_test_util.jpeg_encoded_dicom_instance_test_path()
        ),
        flask_request_headers={
            'Content-Type': 'application/dicom',
            'accept': 'application/dicom+json',
            proxy_const.HeaderKeywords.AUTH_HEADER_KEY: (
                _user_auth_header_value()
            ),
        },
        flask_request_method='POST',
        flask_request_path='/studies',
        dicomstore_in_annotation_allow_list=dicomstore_in_allow_list,
        validate_iap=validate_iap,
    ) as mk_test:
      annotations_util.store_instance(mk_test.dicomweb_base_url, '')
      dcm = shared_test_util.jpeg_encoded_dicom_instance()
      self.assertTrue(all(mk_test.mocked_dicomstore.in_store(dcm)))

  def test_store_add_binary_invalid_dicom_instance_fails(self):
    with _MockDicomStoreTest(
        flask_request_data=b'1234',
        flask_request_headers={
            'Content-Type': 'application/dicom',
            'accept': 'application/dicom+json',
            proxy_const.HeaderKeywords.AUTH_HEADER_KEY: (
                _user_auth_header_value()
            ),
        },
        flask_request_method='POST',
        flask_request_path='/studies',
        dicomstore_in_annotation_allow_list=True,
        validate_iap=True,
    ) as mk_test:
      result = annotations_util.store_instance(mk_test.dicomweb_base_url, '')
      self.assertEqual(result.status_code, http.HTTPStatus.BAD_REQUEST)

  @parameterized.named_parameters([
      dict(
          testcase_name='invalid_boundary',
          boundary=b'InvalidDICOMwebBoundary',
          include_content_type=True,
      ),
      dict(
          testcase_name='invalid_boundary_spacing',
          boundary=b'DICOMwebBoundary\r\nDICOMwebBoundary',
          include_content_type=True,
      ),
      dict(
          testcase_name='missing_content_type',
          boundary=b'DICOMwebBoundary',
          include_content_type=False,
      ),
  ])
  def test_store_add_multipart_binary_invalid_multipart_formating_fails(
      self, boundary, include_content_type
  ):
    with _MockDicomStoreTest(
        flask_request_data=_multipart_request(
            boundary,
            shared_test_util.wsi_dicom_annotation_path(),
            include_content_type=include_content_type,
        ),
        flask_request_headers={
            'Content-Type': (
                'multipart/related; type="application/dicom";'
                'boundary=DICOMwebBoundary'
            ),
            'accept': 'application/dicom+json',
            proxy_const.HeaderKeywords.AUTH_HEADER_KEY: (
                _user_auth_header_value()
            ),
        },
        flask_request_method='POST',
        flask_request_path='/studies',
        dicomstore_in_annotation_allow_list=True,
        validate_iap=True,
    ) as mk_test:
      result = annotations_util.store_instance(mk_test.dicomweb_base_url, '')
      self.assertEqual(result.status_code, http.HTTPStatus.BAD_REQUEST)

  def test_store_invalid_multipart_instance_fails(self):
    tmp = self.create_tempfile()
    with open(tmp.full_path, 'wb') as outfile:
      outfile.write(b'1234')
    with _MockDicomStoreTest(
        flask_request_data=_multipart_request(
            b'DICOMwebBoundary',
            tmp.full_path,
        ),
        flask_request_headers={
            'Content-Type': (
                'multipart/related; type="application/dicom";'
                'boundary=DICOMwebBoundary'
            ),
            'accept': 'application/dicom+json',
            proxy_const.HeaderKeywords.AUTH_HEADER_KEY: (
                _user_auth_header_value()
            ),
        },
        flask_request_method='POST',
        flask_request_path='/studies',
        dicomstore_in_annotation_allow_list=True,
        validate_iap=True,
    ) as mk_test:
      result = annotations_util.store_instance(mk_test.dicomweb_base_url, '')
      self.assertEqual(result.status_code, http.HTTPStatus.BAD_REQUEST)

  @parameterized.named_parameters([
      dict(
          testcase_name='non_allow_listed_store',
          dicomstore_in_allow_list=False,
          validate_iap=False,
      ),
      dict(
          testcase_name='allow_listed_store',
          dicomstore_in_allow_list=True,
          validate_iap=False,
      ),
      dict(
          testcase_name='non_allow_listed_store_iap',
          dicomstore_in_allow_list=False,
          validate_iap=True,
      ),
      dict(
          testcase_name='allow_listed_store_iap',
          dicomstore_in_allow_list=True,
          validate_iap=True,
      ),
  ])
  def test_store_add_binary_annotation_dicom_instance_succeeds(
      self, dicomstore_in_allow_list: bool, validate_iap: bool
  ):
    with _MockDicomStoreTest(
        flask_request_data=_file_bytes(
            shared_test_util.wsi_dicom_annotation_path()
        ),
        flask_request_headers={
            'Content-Type': 'application/dicom',
            'accept': 'application/dicom+json',
            proxy_const.HeaderKeywords.AUTH_HEADER_KEY: (
                _user_auth_header_value()
            ),
        },
        flask_request_method='POST',
        flask_request_path='/studies',
        dicomstore_in_annotation_allow_list=dicomstore_in_allow_list,
        validate_iap=validate_iap,
    ) as mk_test:
      annotations_util.store_instance(mk_test.dicomweb_base_url, '')
      dcm = shared_test_util.wsi_dicom_annotation_instance()
      self.assertTrue(all(mk_test.mocked_dicomstore.in_store(dcm)))

  @parameterized.named_parameters([
      dict(
          testcase_name='non_allow_listed_store',
          dicomstore_in_allow_list=False,
          validate_iap=False,
      ),
      dict(
          testcase_name='allow_listed_store',
          dicomstore_in_allow_list=True,
          validate_iap=False,
      ),
      dict(
          testcase_name='non_allow_listed_store_iap',
          dicomstore_in_allow_list=False,
          validate_iap=True,
      ),
      dict(
          testcase_name='allow_listed_store_iap',
          dicomstore_in_allow_list=True,
          validate_iap=True,
      ),
  ])
  def test_store_add_multipart_binary_instance_succeeds(
      self, dicomstore_in_allow_list: bool, validate_iap: bool
  ):
    data = _multipart_request(
        b'DICOMwebBoundary',
        shared_test_util.jpeg_encoded_dicom_instance_test_path(),
        shared_test_util.wsi_dicom_annotation_path(),
    )
    with _MockDicomStoreTest(
        flask_request_data=data,
        flask_request_headers={
            'Content-Type': (
                'multipart/related; type="application/dicom";'
                'boundary=DICOMwebBoundary'
            ),
            'accept': 'application/dicom+json',
            proxy_const.HeaderKeywords.AUTH_HEADER_KEY: (
                _user_auth_header_value()
            ),
        },
        flask_request_method='POST',
        flask_request_path='/studies',
        dicomstore_in_annotation_allow_list=dicomstore_in_allow_list,
        validate_iap=validate_iap,
    ) as mk_test:
      annotations_util.store_instance(mk_test.dicomweb_base_url, '')
      generic_dcm = shared_test_util.jpeg_encoded_dicom_instance()
      self.assertTrue(all(mk_test.mocked_dicomstore.in_store(generic_dcm)))
      annotation_dcm = shared_test_util.wsi_dicom_annotation_instance()
      self.assertTrue(all(mk_test.mocked_dicomstore.in_store(annotation_dcm)))

  @parameterized.named_parameters([
      dict(
          testcase_name='non_allow_listed_store',
          dicomstore_in_allow_list=False,
          validate_iap=False,
      ),
      dict(
          testcase_name='allow_listed_store',
          dicomstore_in_allow_list=True,
          validate_iap=False,
      ),
      dict(
          testcase_name='non_allow_listed_store_iap',
          dicomstore_in_allow_list=False,
          validate_iap=True,
      ),
      dict(
          testcase_name='allow_listed_store_iap',
          dicomstore_in_allow_list=True,
          validate_iap=True,
      ),
  ])
  def test_store_add_multipart_json_instance_succeeds(
      self, dicomstore_in_allow_list: bool, validate_iap: bool
  ):
    flask_request_data = _multipart_request(
        b'DICOMwebBoundary',
        shared_test_util.mock_annotation_dicom_json_path(),
        shared_test_util.mock_jpeg_dicom_json_path(),
        content_type=(
            b'application/dicom+json; transfer-syntax=1.2.840.10008.1.2.1'
        ),
    )

    with _MockDicomStoreTest(
        flask_request_data=flask_request_data,
        flask_request_headers={
            'Content-Type': (
                'multipart/related; type="application/dicom+json";'
                'transfer-syntax=1.2.840.10008.1.2.1;boundary=DICOMwebBoundary'
            ),
            'accept': 'application/dicom+json',
            proxy_const.HeaderKeywords.AUTH_HEADER_KEY: (
                _user_auth_header_value()
            ),
        },
        flask_request_method='POST',
        flask_request_path='/studies',
        dicomstore_in_annotation_allow_list=dicomstore_in_allow_list,
        validate_iap=validate_iap,
    ) as mk_test:
      annotations_util.store_instance(mk_test.dicomweb_base_url, '')
      self.assertTrue(
          all(
              mk_test.mocked_dicomstore.in_store(
                  dicom_store_mock_types.DicomUidTriple('1.1', '1.2', '1.13')
              )
          )
      )
      self.assertTrue(
          all(
              mk_test.mocked_dicomstore.in_store(
                  dicom_store_mock_types.DicomUidTriple(
                      '1.2.3.4.5.6', '1.2.3.4.5.6.7', '1.2.3.4.5'
                  )
              )
          )
      )

  def _inner_test_dicomstore_add_annotation_requires_credientals(
      self,
      content_type,
      request_data,
      validate_iap,
      user_token,
      dicomstore_read_auth_token,
      dicomstore_write_auth_token,
      flask_request_path='',
  ) -> bool:
    dcm = shared_test_util.wsi_dicom_annotation_instance()
    if not flask_request_path:
      flask_request_path = f'/studies/{dcm.StudyInstanceUID}'
    if flask_request_path.startswith('/studies/'):
      study_instance_uid_added = flask_request_path[len('/studies/') :]
    else:
      study_instance_uid_added = ''
    flask_request_headers = {
        'accept': 'application/dicom+json',
        proxy_const.HeaderKeywords.AUTH_HEADER_KEY: user_token,
    }
    if content_type:
      flask_request_headers['Content-Type'] = content_type
    with _MockDicomStoreTest(
        flask_request_data=request_data,
        flask_request_headers=flask_request_headers,
        flask_request_method='POST',
        flask_request_path=flask_request_path,
        dicomstore_in_annotation_allow_list=True,
        validate_iap=validate_iap,
        read_auth_bearer_tokens=[dicomstore_read_auth_token],
        write_auth_bearer_tokens=[dicomstore_write_auth_token],
    ) as mk_test:
      annotations_util.store_instance(
          mk_test.dicomweb_base_url,
          study_instance_uid_added,
      )
      return all(mk_test.mocked_dicomstore.in_store(dcm))

  @parameterized.parameters([True, False])
  def test_store_add_annotation_binary_requires_user_and_service_credientals(
      self, validate_iap
  ):
    content_type = 'application/dicom'
    user_token = _user_auth_header_value()
    flask_request_data = _file_bytes(
        shared_test_util.wsi_dicom_annotation_path()
    )
    self.assertTrue(
        self._inner_test_dicomstore_add_annotation_requires_credientals(
            content_type,
            flask_request_data,
            validate_iap,
            user_token,
            user_token,
            _bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN),
        )
    )

  @parameterized.parameters([True, False])
  def test_store_add_annotation_multipart_requires_user_and_service_credientals(
      self, validate_iap
  ):
    content_type = (
        'multipart/related; type="application/dicom; boundary=DICOMwebBoundary'
    )
    user_token = _user_auth_header_value()
    flask_request_data = _multipart_request(
        b'DICOMwebBoundary',
        shared_test_util.wsi_dicom_annotation_path(),
    )
    self.assertTrue(
        self._inner_test_dicomstore_add_annotation_requires_credientals(
            content_type,
            flask_request_data,
            validate_iap,
            user_token,
            user_token,
            _bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN),
        )
    )

  @parameterized.parameters([True, False])
  def test_store_add_annotation_binary_fails_study_uid_does_not_match_instance(
      self, validate_iap
  ):
    content_type = 'application/dicom'
    user_token = _user_auth_header_value()
    flask_request_data = _file_bytes(
        shared_test_util.wsi_dicom_annotation_path()
    )
    self.assertFalse(
        self._inner_test_dicomstore_add_annotation_requires_credientals(
            content_type,
            flask_request_data,
            validate_iap,
            user_token,
            user_token,
            _bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN),
            flask_request_path='/studies/1.2.3.4',
        )
    )

  @parameterized.parameters([True, False])
  def test_store_add_annotation_multipart_fails_study_uid_not_match_instance(
      self, validate_iap
  ):
    content_type = (
        'multipart/related; type="application/dicom; boundary=DICOMwebBoundary'
    )
    user_token = _user_auth_header_value()
    flask_request_data = _multipart_request(
        b'DICOMwebBoundary',
        shared_test_util.wsi_dicom_annotation_path(),
    )
    self.assertFalse(
        self._inner_test_dicomstore_add_annotation_requires_credientals(
            content_type,
            flask_request_data,
            validate_iap,
            user_token,
            user_token,
            _bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN),
            flask_request_path='/studies/1.2.3.4',
        )
    )

  @parameterized.parameters([True, False])
  def test_store_add_generic_dicom_binary_to_annotation_store_fails(
      self, validate_iap
  ):
    content_type = 'application/dicom'
    user_token = _user_auth_header_value()
    flask_request_data = _file_bytes(
        shared_test_util.jpeg_encoded_dicom_instance_test_path()
    )
    self.assertFalse(
        self._inner_test_dicomstore_add_annotation_requires_credientals(
            content_type,
            flask_request_data,
            validate_iap,
            user_token,
            user_token,
            _bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN),
        )
    )

  @parameterized.parameters([True, False])
  def test_store_add_generic_dicom_multipart_to_annotation_store_fails(
      self, validate_iap
  ):
    content_type = (
        'multipart/related; type="application/dicom; boundary=DICOMwebBoundary'
    )
    user_token = _user_auth_header_value()
    flask_request_data = _multipart_request(
        b'DICOMwebBoundary',
        shared_test_util.jpeg_encoded_dicom_instance_test_path(),
    )
    self.assertFalse(
        self._inner_test_dicomstore_add_annotation_requires_credientals(
            content_type,
            flask_request_data,
            validate_iap,
            user_token,
            user_token,
            _bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN),
        )
    )

  @parameterized.parameters([True, False])
  def test_store_add_binary_annotation_fails_if_user_crediental_cannot_read(
      self, validate_iap
  ):
    content_type = 'application/dicom'
    flask_request_data = _file_bytes(
        shared_test_util.wsi_dicom_annotation_path()
    )
    self.assertFalse(
        self._inner_test_dicomstore_add_annotation_requires_credientals(
            content_type,
            flask_request_data,
            validate_iap,
            _user_auth_header_value(),
            'UserCannotRead',
            _bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN),
        )
    )

  @parameterized.parameters([True, False])
  def test_store_add_multipart_binary_annotation_fails_if_user_crediental_cannot_read(
      self, validate_iap
  ):
    content_type = (
        'multipart/related; type="application/dicom; boundary=DICOMwebBoundary'
    )
    flask_request_data = _multipart_request(
        b'DICOMwebBoundary',
        shared_test_util.wsi_dicom_annotation_path(),
    )
    self.assertFalse(
        self._inner_test_dicomstore_add_annotation_requires_credientals(
            content_type,
            flask_request_data,
            validate_iap,
            _user_auth_header_value(),
            'UserCannotRead',
            _bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN),
        )
    )

  @parameterized.parameters([True, False])
  def test_store_add_binary_annotation_fails_if_service_crediental_cannot_write(
      self, validate_iap
  ):
    content_type = 'application/dicom'
    user_token = _user_auth_header_value()
    flask_request_data = _file_bytes(
        shared_test_util.wsi_dicom_annotation_path()
    )
    self.assertFalse(
        self._inner_test_dicomstore_add_annotation_requires_credientals(
            content_type,
            flask_request_data,
            validate_iap,
            user_token,
            user_token,
            _bearer_token('ServiceAccountCannotWrite'),
        )
    )

  @parameterized.parameters([True, False])
  def test_store_add_multipart_binary_annotation_fails_if_service_crediental_cannot_write(
      self, validate_iap
  ):
    content_type = (
        'multipart/related; type="application/dicom; boundary=DICOMwebBoundary'
    )
    user_token = _user_auth_header_value()
    flask_request_data = _multipart_request(
        b'DICOMwebBoundary',
        shared_test_util.wsi_dicom_annotation_path(),
    )
    self.assertFalse(
        self._inner_test_dicomstore_add_annotation_requires_credientals(
            content_type,
            flask_request_data,
            validate_iap,
            user_token,
            user_token,
            _bearer_token('ServiceAccountCannotWrite'),
        )
    )

  @parameterized.named_parameters([
      dict(testcase_name='missing_request_content_type', content_type=''),
      dict(
          testcase_name='unrecognized_content_type',
          content_type='unrecongized',
      ),
  ])
  @mock.patch.object(dicom_store_util, 'dicom_store_proxy', autospec=True)
  def test_store_dicom_missing_or_unrecongized_content_type_calls_proxy(
      self, dicom_store_proxy, content_type
  ):
    validate_iap = True
    user_token = _user_auth_header_value()
    flask_request_data = _multipart_request(
        b'DICOMwebBoundary',
        shared_test_util.wsi_dicom_annotation_path(),
    )
    self._inner_test_dicomstore_add_annotation_requires_credientals(
        content_type,
        flask_request_data,
        validate_iap,
        user_token,
        user_token,
        _bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN),
    )
    dicom_store_proxy.assert_called_once()

  def _assert_instances_in_store(
      self, instances_in_store: List[bool], expectation: bool
  ) -> None:
    if expectation:
      self.assertTrue(all(instances_in_store))
    else:
      self.assertTrue(all([not in_store for in_store in instances_in_store]))

  @parameterized.named_parameters([
      dict(
          testcase_name='iap',
          validate_iap=True,
      ),
      dict(
          testcase_name='no_iap',
          validate_iap=False,
      ),
  ])
  @mock.patch.object(dicom_store_util, 'dicom_store_proxy', autospec=True)
  def test_delete_annotation_instance_succeeds(
      self,
      mk_dicom_store_proxy,
      validate_iap,
  ):
    dcm = shared_test_util.wsi_dicom_annotation_instance()
    dcm.OperatorIdentificationSequence = [
        annotations_util._convert_creator_email_to_dicom_dataset(
            MOCK_EMAIL, MOCK_INSTITUTION
        )
    ]
    user_token = _user_auth_header_value()
    with _MockDicomStoreDeleteTest(
        user_token=user_token,
        dicomstore_in_allow_list=True,
        validate_iap=validate_iap,
        dicomstore_read_auth_token=user_token,
        dicomstore_write_auth_token=_bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN),
        dcm=dcm,
    ) as mk_test:
      mk_test.mocked_dicomstore.add_instance(dcm)

      result = annotations_util.delete_instance(
          mk_test.dicomweb_base_url,
          dcm.StudyInstanceUID,
          dcm.SeriesInstanceUID,
          dcm.SOPInstanceUID,
      )
      self._assert_instances_in_store(
          mk_test.mocked_dicomstore.in_store(dcm), False
      )
      self.assertEqual(result.status_code, http.HTTPStatus.OK)
      mk_dicom_store_proxy.assert_not_called()

  @parameterized.named_parameters([
      dict(
          testcase_name='iap',
          validate_iap=True,
      ),
      dict(
          testcase_name='no_iap',
          validate_iap=False,
      ),
  ])
  @mock.patch.object(dicom_store_util, 'dicom_store_proxy', autospec=True)
  def test_delete_annotation_instance_fails_user_cannot_read_annotation_store(
      self,
      mk_dicom_store_proxy,
      validate_iap,
  ):
    dcm = shared_test_util.wsi_dicom_annotation_instance()
    dcm.OperatorIdentificationSequence = [
        annotations_util._convert_creator_email_to_dicom_dataset(
            MOCK_EMAIL, MOCK_INSTITUTION
        )
    ]
    user_token = _user_auth_header_value()
    with _MockDicomStoreDeleteTest(
        user_token=user_token,
        dicomstore_in_allow_list=True,
        validate_iap=validate_iap,
        dicomstore_read_auth_token='invalid_user_token',
        dicomstore_write_auth_token=_bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN),
        dcm=dcm,
    ) as mk_test:
      mk_test.mocked_dicomstore.add_instance(dcm)

      result = annotations_util.delete_instance(
          mk_test.dicomweb_base_url,
          dcm.StudyInstanceUID,
          dcm.SeriesInstanceUID,
          dcm.SOPInstanceUID,
      )
      self._assert_instances_in_store(
          mk_test.mocked_dicomstore.in_store(dcm), True
      )
      self.assertEqual(result.status_code, http.HTTPStatus.UNAUTHORIZED)
      mk_dicom_store_proxy.assert_not_called()

  @parameterized.named_parameters([
      dict(
          testcase_name='iap',
          validate_iap=True,
      ),
      dict(
          testcase_name='no_iap',
          validate_iap=False,
      ),
  ])
  @mock.patch.object(dicom_store_util, 'dicom_store_proxy', autospec=True)
  def test_delete_annotation_instance_fails_sa_cannot_write_annotation_store(
      self,
      mk_dicom_store_proxy,
      validate_iap,
  ):
    dcm = shared_test_util.wsi_dicom_annotation_instance()
    dcm.OperatorIdentificationSequence = [
        annotations_util._convert_creator_email_to_dicom_dataset(
            MOCK_EMAIL, MOCK_INSTITUTION
        )
    ]
    user_token = _user_auth_header_value()
    with _MockDicomStoreDeleteTest(
        user_token=user_token,
        dicomstore_in_allow_list=True,
        validate_iap=validate_iap,
        dicomstore_read_auth_token=user_token,
        dicomstore_write_auth_token='invalid_sa_token',
        dcm=dcm,
    ) as mk_test:
      mk_test.mocked_dicomstore.add_instance(dcm)

      result = annotations_util.delete_instance(
          mk_test.dicomweb_base_url,
          dcm.StudyInstanceUID,
          dcm.SeriesInstanceUID,
          dcm.SOPInstanceUID,
      )
      self._assert_instances_in_store(
          mk_test.mocked_dicomstore.in_store(dcm), True
      )
      self.assertEqual(result.status_code, http.HTTPStatus.UNAUTHORIZED)
      mk_dicom_store_proxy.assert_not_called()

  @parameterized.named_parameters([
      dict(
          testcase_name='iap',
          validate_iap=True,
      ),
      dict(
          testcase_name='no_iap',
          validate_iap=False,
      ),
  ])
  @mock.patch.object(dicom_store_util, 'dicom_store_proxy', autospec=True)
  def test_delete_annotation_instance_dicom_not_in_store(
      self,
      mk_dicom_store_proxy,
      validate_iap,
  ):
    dcm = shared_test_util.wsi_dicom_annotation_instance()
    dcm.OperatorIdentificationSequence = [
        annotations_util._convert_creator_email_to_dicom_dataset(
            MOCK_EMAIL, MOCK_INSTITUTION
        )
    ]
    user_token = _user_auth_header_value()
    with _MockDicomStoreDeleteTest(
        user_token=user_token,
        dicomstore_in_allow_list=True,
        validate_iap=validate_iap,
        dicomstore_read_auth_token=user_token,
        dicomstore_write_auth_token=_bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN),
        dcm=dcm,
    ) as mk_test:
      result = annotations_util.delete_instance(
          mk_test.dicomweb_base_url,
          dcm.StudyInstanceUID,
          dcm.SeriesInstanceUID,
          dcm.SOPInstanceUID,
      )
      self._assert_instances_in_store(
          mk_test.mocked_dicomstore.in_store(dcm), False
      )
      self.assertEqual(result.status_code, http.HTTPStatus.NOT_FOUND)
      mk_dicom_store_proxy.assert_not_called()

  @parameterized.named_parameters([
      dict(
          testcase_name='operator_email_not_match',
          operator_sq=[
              annotations_util._convert_creator_email_to_dicom_dataset(
                  'Bad@email.com', MOCK_INSTITUTION
              )
          ],
          expected_status_code=http.HTTPStatus.UNAUTHORIZED,
      ),
      dict(
          testcase_name='no_operator_sq',
          operator_sq=[],
          expected_status_code=http.HTTPStatus.BAD_REQUEST,
      ),
  ])
  @mock.patch.object(dicom_store_util, 'dicom_store_proxy', autospec=True)
  def test_delete_annotation_fails_incorrect_annotator_email(
      self,
      mk_dicom_store_proxy,
      operator_sq,
      expected_status_code,
  ):
    dcm = shared_test_util.wsi_dicom_annotation_instance()
    dcm.OperatorIdentificationSequence = operator_sq
    user_token = _user_auth_header_value()
    with _MockDicomStoreDeleteTest(
        user_token=user_token,
        dicomstore_in_allow_list=True,
        validate_iap=True,
        dicomstore_read_auth_token=user_token,
        dicomstore_write_auth_token=_bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN),
        dcm=dcm,
    ) as mk_test:
      mk_test.mocked_dicomstore.add_instance(dcm)

      result = annotations_util.delete_instance(
          mk_test.dicomweb_base_url,
          dcm.StudyInstanceUID,
          dcm.SeriesInstanceUID,
          dcm.SOPInstanceUID,
      )
      self._assert_instances_in_store(
          mk_test.mocked_dicomstore.in_store(dcm), True
      )
      self.assertEqual(result.status_code, expected_status_code)
      mk_dicom_store_proxy.assert_not_called()

  @mock.patch.object(dicom_store_util, 'dicom_store_proxy', autospec=True)
  def test_dicom_store_not_in_allow_list_calls_proxy(
      self,
      mk_dicom_store_proxy,
  ):
    dcm = shared_test_util.wsi_dicom_annotation_instance()
    dcm.OperatorIdentificationSequence = [
        annotations_util._convert_creator_email_to_dicom_dataset(
            MOCK_EMAIL, MOCK_INSTITUTION
        )
    ]
    user_token = _user_auth_header_value()
    with _MockDicomStoreDeleteTest(
        user_token=user_token,
        dicomstore_in_allow_list=False,
        validate_iap=False,
        dicomstore_read_auth_token=user_token,
        dicomstore_write_auth_token=_bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN),
        dcm=dcm,
    ) as mk_test:
      mk_test.mocked_dicomstore.add_instance(dcm)

      annotations_util.delete_instance(
          mk_test.dicomweb_base_url,
          dcm.StudyInstanceUID,
          dcm.SeriesInstanceUID,
          dcm.SOPInstanceUID,
      )
      mk_dicom_store_proxy.assert_called_once()

  @parameterized.named_parameters([
      dict(
          testcase_name='iap',
          validate_iap=True,
      ),
      dict(
          testcase_name='no_iap',
          validate_iap=False,
      ),
  ])
  @mock.patch.object(dicom_store_util, 'dicom_store_proxy', autospec=True)
  def test_delete_dicom_not_annotation_calls_proxy(
      self, mk_dicom_store_proxy, validate_iap
  ):
    dcm = shared_test_util.jpeg_encoded_dicom_instance()
    dcm.OperatorIdentificationSequence = [
        annotations_util._convert_creator_email_to_dicom_dataset(
            MOCK_EMAIL, MOCK_INSTITUTION
        )
    ]
    user_token = _user_auth_header_value()
    with _MockDicomStoreDeleteTest(
        user_token=user_token,
        dicomstore_in_allow_list=True,
        validate_iap=validate_iap,
        dicomstore_read_auth_token=user_token,
        dicomstore_write_auth_token=_bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN),
        dcm=dcm,
    ) as mk_test:
      mk_test.mocked_dicomstore.add_instance(dcm)

      annotations_util.delete_instance(
          mk_test.dicomweb_base_url,
          dcm.StudyInstanceUID,
          dcm.SeriesInstanceUID,
          dcm.SOPInstanceUID,
      )
      mk_dicom_store_proxy.assert_called_once()

  @parameterized.named_parameters([
      dict(
          testcase_name='iap',
          validate_iap=True,
      ),
      dict(
          testcase_name='no_iap',
          validate_iap=False,
      ),
  ])
  @mock.patch.object(
      dicom_store_util, 'delete_instance_from_dicom_store', autospec=True
  )
  def test_core_proxy_delete_dicom_instance_succeeds_without_dicom_store_read(
      self, mock_delete_instance, validate_iap
  ):
    dcm = shared_test_util.jpeg_encoded_dicom_instance()
    dcm.OperatorIdentificationSequence = [
        annotations_util._convert_creator_email_to_dicom_dataset(
            MOCK_EMAIL, MOCK_INSTITUTION
        )
    ]
    user_token = _user_auth_header_value()
    with _MockDicomStoreDeleteTest(
        user_token=user_token,
        dicomstore_in_allow_list=False,
        validate_iap=validate_iap,
        dicomstore_read_auth_token='bad_token',
        dicomstore_write_auth_token=user_token,
        dcm=dcm,
    ) as mk_test:
      mk_test.mocked_dicomstore.add_instance(dcm)

      result = annotations_util.delete_instance(
          mk_test.dicomweb_base_url,
          dcm.StudyInstanceUID,
          dcm.SeriesInstanceUID,
          dcm.SOPInstanceUID,
      )
      self._assert_instances_in_store(
          mk_test.mocked_dicomstore.in_store(dcm), False
      )
      self.assertEqual(result.status_code, http.HTTPStatus.OK)
      mock_delete_instance.assert_not_called()

  @parameterized.named_parameters([
      dict(
          testcase_name='iap',
          validate_iap=True,
      ),
      dict(
          testcase_name='no_iap',
          validate_iap=False,
      ),
  ])
  @mock.patch.object(
      dicom_store_util, 'delete_instance_from_dicom_store', autospec=True
  )
  def test_proxy_delete_dicom_instance_fails_user_cannot_write(
      self, mock_delete_instance, validate_iap
  ):
    dcm = shared_test_util.jpeg_encoded_dicom_instance()
    dcm.OperatorIdentificationSequence = [
        annotations_util._convert_creator_email_to_dicom_dataset(
            MOCK_EMAIL, MOCK_INSTITUTION
        )
    ]
    user_token = _user_auth_header_value()
    with _MockDicomStoreDeleteTest(
        user_token=user_token,
        dicomstore_in_allow_list=False,
        validate_iap=validate_iap,
        dicomstore_read_auth_token='bad_token',
        dicomstore_write_auth_token='bad_token',
        dcm=dcm,
    ) as mk_test:
      mk_test.mocked_dicomstore.add_instance(dcm)

      result = annotations_util.delete_instance(
          mk_test.dicomweb_base_url,
          dcm.StudyInstanceUID,
          dcm.SeriesInstanceUID,
          dcm.SOPInstanceUID,
      )
      self._assert_instances_in_store(
          mk_test.mocked_dicomstore.in_store(dcm), True
      )
      self.assertEqual(result.status_code, http.HTTPStatus.UNAUTHORIZED)
      mock_delete_instance.assert_not_called()

  @parameterized.named_parameters([
      dict(
          testcase_name='iap_auth',
          validate_iap=True,
          header_params={
              proxy_const.HeaderKeywords.IAP_EMAIL_KEY: MOCK_EMAIL,
              proxy_const.HeaderKeywords.IAP_USER_ID_KEY: MOCK_USER_ID,
              proxy_const.HeaderKeywords.AUTH_HEADER_KEY: (
                  _user_auth_header_value()
              ),
              proxy_const.HeaderKeywords.AUTHORITY_HEADER_KEY: 'user_authority',
          },
          expected_iap_user_id=MOCK_USER_ID,
      ),
      dict(
          testcase_name='bearer_token_auth',
          validate_iap=False,
          header_params={
              proxy_const.HeaderKeywords.IAP_EMAIL_KEY: 'fake_email',
              proxy_const.HeaderKeywords.IAP_USER_ID_KEY: 'fake_user_id',
              proxy_const.HeaderKeywords.AUTH_HEADER_KEY: (
                  _user_auth_header_value()
              ),
              proxy_const.HeaderKeywords.AUTHORITY_HEADER_KEY: 'user_authority',
          },
          expected_iap_user_id='',
      ),
  ])
  def test_serviceaccountcredentials_constructor_iap(
      self,
      validate_iap,
      header_params,
      expected_iap_user_id,
  ):
    with _MockDicomStoreTest(
        flask_request_headers=header_params, validate_iap=validate_iap
    ) as mk_test:
      credentials = annotations_util._ServiceAccountCredentials(
          mk_test.dicomweb_base_url
      )

    self.assertEqual(credentials.email, MOCK_SERVICE_ACCOUNT_EMAIL)
    self.assertEqual(
        credentials.service_account_email, MOCK_SERVICE_ACCOUNT_EMAIL
    )
    self.assertEqual(
        credentials.service_account_authorization,
        _bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN),
    )
    self.assertEqual(
        credentials.authorization, _bearer_token(MOCK_SERVICE_ACCOUNT_TOKEN)
    )
    self.assertEmpty(credentials.service_account_authority)
    self.assertEmpty(credentials.authority)
    self.assertEqual(credentials.user_email, MOCK_EMAIL)
    self.assertStartsWith(
        credentials.user_authorization, _bearer_token(f'{MOCK_TOKEN}_')
    )
    self.assertEqual(credentials.user_authority, MOCK_USER_AUTHORITY)
    self.assertEqual(credentials.iap_user_id, expected_iap_user_id)

  @parameterized.parameters([
      ('', ''),
      ('abc@test.com', 'abc@test.com'),
      (' ABC@test.com ', 'abc@test.com'),
      ('accounts.google.com:abc@test.com', 'abc@test.com'),
      (' Accounts.google.com: ABC@test.com ', 'abc@test.com'),
  ])
  def test_normalize_email(self, email, expected):
    self.assertEqual(annotations_util._normalize_email(email), expected)

  @parameterized.parameters([
      'abc@test.com',
      ' ABC@test.com ',
      'accounts.google.com:abc@test.com',
      ' Accounts.google.com: ABC@test.com ',
  ])
  def test_emails_same(self, email: str):
    self.assertFalse(
        annotations_util._are_emails_different(email, 'abc@test.com')
    )

  @parameterized.parameters([
      'abc@test.com',
      ' ABC@test.com ',
      'accounts.google.com:abc@test.com',
      ' Accounts.google.com: ABC@test.com ',
  ])
  def test_emails_different(self, email: str):
    self.assertTrue(
        annotations_util._are_emails_different(email, 'abc@foo.com')
    )


if __name__ == '__main__':
  absltest.main()
