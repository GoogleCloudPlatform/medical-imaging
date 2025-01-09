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
"""Constants used in DICOM proxy."""

from pathology.dicom_proxy import enum_types
from pathology.shared_libs.iap_auth_lib import auth


class HeaderKeywords:
  AUTH_HEADER_KEY = 'authorization'
  AUTHORITY_HEADER_KEY = 'authority'
  COOKIE = 'cookie'
  IAP_EMAIL_KEY = 'X-Goog-Authenticated-User-Email'
  IAP_JWT_HEADER = auth.IAP_JWT_HEADER
  IAP_USER_ID_KEY = 'X-Goog-Authenticated-User-Id'


class LogKeywords:
  """Keywords used in structured logs."""

  AUTHORITY = 'authority'
  AUTHORIZATION = 'authorization'
  BASE_DICOMWEB_URL = 'baseDicomWebURL'
  DATASET_ID = 'dataset_id'
  DICOM_STORE = 'dicom_store'
  DICOM_STORE_URL = 'dicom_store_url'
  DICOM_PROXY_SESSION_KEY = 'dicom_proxy_session_key'
  DICOMWEB = 'dicomWeb'
  DICOMWEB_RESPONSE = 'dicomWeb_response'
  DICOMWEB_URL = 'dicomWebUrl'
  EMAIL_REGEX = 'email_regex'
  FRAME_LIST = 'frame_list'
  HEALTHCARE_API_DICOM_VERSION = 'healthcare_api_dicom_store_version'
  HTTP_REQUEST = 'http_request'
  HTTP_STATUS_CODE = 'http_status_code'
  JSON = 'json'
  LOCATION = 'location'
  METADATA = 'metadata'
  MULTIPART_CONTENT = 'multipart_content'
  PATH_TO_BULK_DATA = 'path_to_bulk_data'
  PROJECT_ID = 'project_id'
  SERIES_INSTANCE_UID = 'series_instance_uid'
  SOP_CLASS_UID = 'sop_class_uid'
  SOP_INSTANCE_UID = 'sop_instance_uid'
  STUDY_INSTANCE_UID = 'study_instance_uid'
  USER_BEARER_TOKEN_EMAIL = 'user_bearer_token_email'
  USER_EMAIL = 'user_email'
  USER_EMAIL_OF_DICOMWEB_REQUEST = 'user_email_of_dicom_web_request'


class ICCProfile:
  """ICC profile conversion to perform on images."""

  NO = enum_types.ICCProfile('NO')
  YES = enum_types.ICCProfile('YES')
  SRGB = enum_types.ICCProfile('SRGB')
  ADOBERGB = enum_types.ICCProfile('ADOBERGB')
  ROMMRGB = enum_types.ICCProfile('ROMMRGB')
