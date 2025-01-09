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
"""Utility for adding ICCProfile bulk data to DICOM metadata."""
import json
import re
from typing import Any, List, Mapping, MutableMapping, Tuple
from xml.etree import ElementTree as ET

import flask
import requests_toolbelt

from pathology.dicom_proxy import bulkdata_util
from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import color_conversion_util
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import flask_util
from pathology.dicom_proxy import user_auth_util

_ICCPROFILE_PATH_HAS_INCORRECT_NUMBER_OF_PARTS = (
    'ICCProfile path has incorrect number of parts.'
)
_ICCPROFILE_PATH_DOES_NOT_REF_ICCPROFILE = (
    'ICCProfile path does not ref ICCProfile.'
)
_OPTICAL_PATHSQ_DICOM_TAG_ADDRESS = '00480105'
_ICCPROFILE_DICOM_TAG_ADDRESS = '00282000'
_SOPINSTANCEUID_DICOM_TAG_ADDRESS = '00080018'
_SOPCLASSUID_DICOM_TAG_ADDRESS = '00080016'
_ICCPROFILE_DICOM_TAG_VR_CODE = 'OB'
_JSON_METADATA_CONTENT_TYPE = 'application/dicom+json'
_OPTICAL_PATH_SQ_DICOM_TAG_KEYWORD = 'OpticalPathSequence'
_PROXY_BULK_DATA_URI = bulkdata_util.PROXY_BULK_DATA_URI
ICCPROFILE_DICOM_TAG_KEYWORD = 'ICCProfile'
_XML_METADATA_CONTENT_TYPE = 'application/dicom+xml'
_VALUE = 'Value'
_IMAGE_TYPE_TAG = '00080008'
# Image Type CS consts.
_LABEL = 'LABEL'
_OVERVIEW = 'OVERVIEW'
_VL_WHOLE_SLIDE_MICROSCOPY_IMAGE_IOD_UID = '1.2.840.10008.5.1.4.1.1.77.1.6'
_HEALTHCARE_API_BULKDATA_RESPONSE = re.compile(
    r'"https://healthcare\.googleapis\.com/.+?/projects/.+?/locations/.+?/'
    r'datasets/.+?/dicomStores/.+?/dicomWeb/studies/(.+?)/series/(.+?)/'
    r'instances/(.+?)/bulkdata/(.+?)"'.encode('utf-8'),
    re.IGNORECASE,
)


class InvalidICCProfilePathError(Exception):
  pass


class _UnexpectedNumberOfXmlElementsError(Exception):
  pass


def _get_optical_path_sequence_index(path: str) -> int:
  """DICOM ICCProfile tag path optical path sequence index if defined.

  Args:
    path: String path defining path to ICCProfile in DICOM.  Expected format for
      ICCProfile placed in OpticalPathSequence:
      OpticalPathSequence/{index}/ICCProfile

  Returns:
    OpticalPathSequence index containing ICCProfile or -1.

  Raises:
    InvalidICCProfilePathError: Path defining position of ICC profile in JSON
      is not formatted correctly.
  """
  if not path.startswith(
      f'{_OPTICAL_PATH_SQ_DICOM_TAG_KEYWORD}/'
  ) or not path.endswith(f'/{ICCPROFILE_DICOM_TAG_KEYWORD}'):
    return -1
  parts = path.split('/')
  if len(parts) != 3:
    raise InvalidICCProfilePathError(
        _ICCPROFILE_PATH_HAS_INCORRECT_NUMBER_OF_PARTS
    )
  try:
    index = int(parts[1])
  except ValueError as exp:
    raise InvalidICCProfilePathError(
        _ICCPROFILE_PATH_DOES_NOT_REF_ICCPROFILE
    ) from exp
  return index


def _find_single_value(dicom_xml: ET.Element, query: str) -> ET.Element:
  dicom_xml = dicom_xml.findall(query)
  if len(dicom_xml) != 1:
    raise _UnexpectedNumberOfXmlElementsError()
  return dicom_xml[0]


def _add_xml_iccprofile_bulkdata_url(
    dicom_xml: ET.Element,
    path: str,
    bulk_data_uri: str,
) -> bool:
  """Add ICCProfile bulkdata uri to XML instance metadata.

  Args:
    dicom_xml: XML metadata to add URI to.
    path: Path to DICOM tag.
    bulk_data_uri: Bulkdata URI.

  Returns:
     True if metadata changed.

  Raises:
    InvalidICCProfilePathError: Path defining position of ICC profile in JSON
      is not formatted correctly.
  """
  index = _get_optical_path_sequence_index(path)
  if index >= 0:
    try:
      dicom_xml = _find_single_value(
          dicom_xml,
          f"./DicomAttribute/[@tag='{_OPTICAL_PATHSQ_DICOM_TAG_ADDRESS}']",
      )
      dicom_xml = _find_single_value(dicom_xml, f"./Item/[@number='{index+1}']")
    except _UnexpectedNumberOfXmlElementsError:
      return False
  elif path != ICCPROFILE_DICOM_TAG_KEYWORD:
    raise InvalidICCProfilePathError(_ICCPROFILE_PATH_DOES_NOT_REF_ICCPROFILE)
  if dicom_xml.findall(
      f"./DicomAttribute/[@tag='{_ICCPROFILE_DICOM_TAG_ADDRESS}']"
  ):
    return False
  se = ET.SubElement(
      dicom_xml,
      'DicomAttribute',
      attrib=dict(
          tag=_ICCPROFILE_DICOM_TAG_ADDRESS,
          vr=_ICCPROFILE_DICOM_TAG_VR_CODE,
          keyword=ICCPROFILE_DICOM_TAG_KEYWORD,
      ),
  )
  ET.SubElement(se, 'BulkData', attrib=dict(URI=bulk_data_uri))
  return True


def _add_json_iccprofile_bulkdata_url(
    metadata: MutableMapping[str, Any],
    path: str,
    bulk_data_uri: str,
) -> bool:
  """Add ICCProfile bulkdata uri to JSON instance metadata.

  Args:
    metadata: DICOM instance JSON metadata.
    path: Path to DICOM tag.
    bulk_data_uri: Bulkdata URI.

  Returns:
     True if metadata changed.

  Raises:
    InvalidICCProfilePathError: Path defining position of ICC profile in JSON
      is not formatted correctly.
  """
  index = _get_optical_path_sequence_index(path)
  if index >= 0:
    try:
      metadata = metadata[_OPTICAL_PATHSQ_DICOM_TAG_ADDRESS][_VALUE][index]
    except (KeyError, IndexError) as _:
      return False
  elif path != ICCPROFILE_DICOM_TAG_KEYWORD:
    raise InvalidICCProfilePathError(_ICCPROFILE_PATH_DOES_NOT_REF_ICCPROFILE)
  if _ICCPROFILE_DICOM_TAG_ADDRESS in metadata:
    return False
  metadata[_ICCPROFILE_DICOM_TAG_ADDRESS] = dict(
      vr=_ICCPROFILE_DICOM_TAG_VR_CODE,
      BulkDataURI=bulk_data_uri,
  )
  return True


def _xml_field_content(xml_txt: str) -> Tuple[None, Tuple[None, str, str]]:
  return (None, (None, xml_txt, _XML_METADATA_CONTENT_TYPE))


def _get_image_type_values_from_json(instance: Mapping[str, Any]) -> List[str]:
  try:
    return [str(tg) for tg in instance[_IMAGE_TYPE_TAG][_VALUE]]
  except (IndexError, KeyError) as _:
    return []


def _get_image_type_values_from_xml(dicom_xml: ET.Element) -> List[str]:
  try:
    dicom_xml = _find_single_value(
        dicom_xml, f"./DicomAttribute/[@tag='{_IMAGE_TYPE_TAG}']"
    )
  except _UnexpectedNumberOfXmlElementsError:
    return []
  return [
      val.text for val in dicom_xml.findall('./Value') if val.text is not None
  ]


def _is_image_type_label_or_overview(image_type_vals: List[str]) -> bool:
  if not image_type_vals:
    return True
  for val in image_type_vals:
    if val.strip().upper() in (_LABEL, _OVERVIEW):
      return True
  return False


def _get_uid_from_xml(dicom_xml: ET.Element, tag_address: str) -> str:
  """Return UID tag value from DICOM XML.

  Args:
    dicom_xml: XML to retrieve tag value from.
    tag_address: DICOM tag address.

  Returns:
    DICOM tag value as string.

  Raises:
    _UnexpectedNumberOfXmlElementsError: XML contained unexpected number of
      xml elements.
  """
  xml_tag = _find_single_value(
      dicom_xml,
      f"./DicomAttribute/[@tag='{tag_address}']",
  )
  val = _find_single_value(xml_tag, f'./{_VALUE}').text
  if val is None:
    raise _UnexpectedNumberOfXmlElementsError()
  return val


def _get_uid_from_json(instance: Mapping[str, Any], tag_address: str) -> str:
  """Return UID tag value from DICOM JSON.

  Args:
    instance: DICOM instance JSON.
    tag_address: DICOM tag address.

  Returns:
    DICOM tag value as string.

  Raises:
    IndexError, KeyError: JSON does not contain address or incorrectly
      formatted.
  """
  return instance[tag_address][_VALUE][0]


def augment_dicom_iccprofile_bulkdata_metadata(
    dicom_store_base_url: dicom_url_util.DicomWebBaseURL,
    study_instance_uid: dicom_url_util.StudyInstanceUID,
    series_instance_uid: dicom_url_util.SeriesInstanceUID,
    sop_instance_uid: dicom_url_util.SOPInstanceUID,
    response: flask.Response,
    enable_caching: cache_enabled_type.CachingEnabled,
) -> None:
  """Determine if instance has ICCProfile and add bulkdata uri to JSON metadata.

  Args:
    dicom_store_base_url: Base url for the DICOM Proxy.
    study_instance_uid: DICOM Study Instance UID queried
    series_instance_uid: DICOM Series Instance UID queried
    sop_instance_uid: DICOM SOP Instance UID queried
    response: Response from DICOM store that contains JSON metadata to augment.
    enable_caching: Enable ICCProfile cache.

  Returns:
     None
  """
  if not str(study_instance_uid.study_instance_uid):
    return
  if not str(series_instance_uid.series_instance_uid):
    return
  base_url = bulkdata_util.get_bulk_data_base_url(dicom_store_base_url)
  if not base_url:
    return
  user_auth = user_auth_util.AuthSession(flask_util.get_headers())
  series_url = dicom_url_util.base_dicom_series_url(
      dicom_store_base_url, study_instance_uid, series_instance_uid
  )
  path = color_conversion_util.get_series_icc_profile_path(
      user_auth, series_url, sop_instance_uid, enable_caching
  )
  if not path:
    return
  bulk_data_uri = f'{base_url}/{study_instance_uid}/{series_instance_uid}'
  metadata_updated = False
  response_content_type = response.content_type.strip().lower()
  if response_content_type == _JSON_METADATA_CONTENT_TYPE:
    try:
      metadata = json.loads(response.data)
    except json.JSONDecodeError:
      return
    if not isinstance(metadata, list):
      return
    for instance in metadata:
      # ICC Color profiles typically do not apply to label and overview images.
      if _is_image_type_label_or_overview(
          _get_image_type_values_from_json(instance)
      ):
        # Do not add bulk transport if label, overview, image type is not
        # defined.
        continue
      try:
        if (
            _get_uid_from_json(instance, _SOPCLASSUID_DICOM_TAG_ADDRESS)
            != _VL_WHOLE_SLIDE_MICROSCOPY_IMAGE_IOD_UID
        ):
          # DICOM must be VL_WHOLE_SLIDE_MICROSCOPY_IMAGE_IOD_UID
          continue
        if not str(sop_instance_uid.sop_instance_uid):
          sop_instance_uid = dicom_url_util.SOPInstanceUID(
              _get_uid_from_json(instance, _SOPINSTANCEUID_DICOM_TAG_ADDRESS)
          )
      except (IndexError, KeyError) as _:
        continue
      if _add_json_iccprofile_bulkdata_url(
          instance,
          path,
          f'{bulk_data_uri}/{sop_instance_uid}/{_PROXY_BULK_DATA_URI}/{path}',
      ):
        metadata_updated = True
    if metadata_updated:
      response.data = json.dumps(metadata)
  elif _XML_METADATA_CONTENT_TYPE in response_content_type:
    fields = []
    try:
      mp_response = requests_toolbelt.MultipartDecoder(
          response.data, response.content_type
      )
    except (
        requests_toolbelt.NonMultipartContentTypeException,
        requests_toolbelt.ImproperBodyPartContentException,
    ) as _:
      return
    boundary = mp_response.boundary.decode('utf-8')
    for mp_data in mp_response.parts:
      xml_doc = mp_data.content
      fields.append(xml_doc)
      try:
        dicom_xml = ET.fromstring(xml_doc)
      except ET.ParseError:
        continue
      # ICC Color profiles typically do not apply to label and overview images.
      if _is_image_type_label_or_overview(
          _get_image_type_values_from_xml(dicom_xml)
      ):
        # Do not add bulk transport if label, overview, image type is not
        # defined.
        continue
      try:
        if (
            _get_uid_from_xml(dicom_xml, _SOPCLASSUID_DICOM_TAG_ADDRESS)
            != _VL_WHOLE_SLIDE_MICROSCOPY_IMAGE_IOD_UID
        ):
          # DICOM must be VL_WHOLE_SLIDE_MICROSCOPY_IMAGE_IOD_UID
          continue
        if not str(sop_instance_uid.sop_instance_uid):
          sop_instance_uid = dicom_url_util.SOPInstanceUID(
              _get_uid_from_xml(dicom_xml, _SOPINSTANCEUID_DICOM_TAG_ADDRESS)
          )
      except _UnexpectedNumberOfXmlElementsError:
        continue
      if _add_xml_iccprofile_bulkdata_url(
          dicom_xml,
          path,
          f'{bulk_data_uri}/{sop_instance_uid}/{_PROXY_BULK_DATA_URI}/{path}',
      ):
        fields[-1] = ET.tostring(dicom_xml).decode('us-ascii')
        metadata_updated = True
    if metadata_updated:
      response.data = requests_toolbelt.MultipartEncoder(
          fields=[_xml_field_content(field) for field in fields],
          boundary=boundary,
      ).read()
