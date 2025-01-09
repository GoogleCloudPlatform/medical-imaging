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
"""Test iccprofile bulk metadata util."""

import copy
import dataclasses
import json
from typing import Any, List, Mapping, MutableMapping
from unittest import mock
from xml.etree import ElementTree as ET

from absl.testing import absltest
from absl.testing import parameterized
import requests_toolbelt

from pathology.dicom_proxy import bulkdata_util
from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import color_conversion_util
from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import flask_util
from pathology.dicom_proxy import iccprofile_bulk_metadata_util


_DICOMSTORE_BASEURL = dicom_url_util.DicomWebBaseURL(
    'V1', 'GCP_PROJECTID', 'GCP_LOCATION', 'FOO_DATASET', 'BAR_DICOMSTORE'
)
_STUDY_INSTANCE_UID = dicom_url_util.StudyInstanceUID('123.456')
_SERIES_INSTANCE_UID = dicom_url_util.SeriesInstanceUID('123.456.789')
_SOP_INSTANCE_UID = dicom_url_util.SOPInstanceUID('123.456.789.10')
_BULKDATA_URI = 'https://bulkdatauri'
_BOUNDARY = 'da654e080d86fef6aa8541fb427401772d9ebe36bc77e3015c4feef56095'

_IMAGE_TYPE_XML = (
    '<DicomAttribute tag="00080008" vr="CS"'
    ' keyword="ImageType"><Value'
    ' number="1">ORIGINAL</Value><Value'
    ' number="2">PRIMARY</Value><Value'
    ' number="3">VOLUME</Value><Value'
    ' number="4">NONE</Value></DicomAttribute>'
)

_SOPINSTANCEUID_XML = (
    '<DicomAttribute tag="00080018" vr="UI" keyword="SOPInstanceUID">'
    f'<Value number="1">{_SOP_INSTANCE_UID.sop_instance_uid}</Value>'
    '</DicomAttribute>'
)

_SOPCLASSUID_XML = (
    '<DicomAttribute tag="00080016" vr="UI"'
    ' keyword="SOPClassUID"><Value number="1">'
    f'{iccprofile_bulk_metadata_util._VL_WHOLE_SLIDE_MICROSCOPY_IMAGE_IOD_UID}'
    '</Value></DicomAttribute>'
)

_TEST_DICOM_XML_DOC_FRAGMENT = (
    f'<NativeDicomModel>{_SOPINSTANCEUID_XML}<DicomAttribute'
    ' tag="00480105" vr="SQ" keyword="OpticalPathSequence"><Item number="1"'
    ' /><Item number="2"><DicomAttribute tag="00282000" vr="OB"'
    ' keyword="ICCProfile"><Value'
    ' number="1">111741</Value></DicomAttribute></Item></DicomAttribute>'
    f'{_IMAGE_TYPE_XML}{_SOPCLASSUID_XML}</NativeDicomModel>'
)


def _expected_result_add_bulkdata_to_root_in_dicom_xml_fragment(
    uri: str,
) -> str:
  return (
      f'<NativeDicomModel>{_SOPINSTANCEUID_XML}<DicomAttribute tag="00480105"'
      ' vr="SQ" keyword="OpticalPathSequence"><Item number="1" /><Item'
      ' number="2"><DicomAttribute tag="00282000" vr="OB"'
      ' keyword="ICCProfile"><Value'
      ' number="1">111741</Value></DicomAttribute></Item></DicomAttribute><DicomAttribute'
      f' tag="00282000" vr="OB" keyword="ICCProfile"><BulkData URI="{uri}"'
      ' /></DicomAttribute></NativeDicomModel>'
  )


def _expected_result_add_bulkdata_to_optical_path_sq_in_dicom_xml_fragment(
    uri: str,
) -> str:
  return (
      f'<NativeDicomModel>{_SOPINSTANCEUID_XML}<DicomAttribute'
      ' tag="00480105" vr="SQ"'
      ' keyword="OpticalPathSequence"><Item number="1"><DicomAttribute'
      ' tag="00282000" vr="OB" keyword="ICCProfile"><BulkData'
      f' URI="{uri}" /></DicomAttribute></Item><Item'
      ' number="2"><DicomAttribute tag="00282000" vr="OB"'
      ' keyword="ICCProfile"><Value'
      ' number="1">111741</Value></DicomAttribute></Item></DicomAttribute></NativeDicomModel>'
  )


def _remove_required_xml_tags_and_convert_to_str(root_xml: ET.Element) -> str:
  for tag in (
      iccprofile_bulk_metadata_util._IMAGE_TYPE_TAG,
      iccprofile_bulk_metadata_util._SOPCLASSUID_DICOM_TAG_ADDRESS,
  ):
    for node in root_xml.findall(f"./DicomAttribute/[@tag='{tag}']"):
      root_xml.remove(node)
  return ET.tostring(root_xml).decode('us-ascii')


def _icc_bulk_data(uri: str) -> Mapping[str, Any]:
  return {
      'vr': iccprofile_bulk_metadata_util._ICCPROFILE_DICOM_TAG_VR_CODE,
      'BulkDataURI': uri,
  }


_ICC_BULKDATA_URI = _icc_bulk_data(_BULKDATA_URI)
_PREEXISTING_ICC_BULKDATA = _icc_bulk_data('pre_existing')


def _expected_bulkdata_uri(icc_path: str) -> str:
  return (
      f'https://dpas.cloudflyer.info/tile/{_DICOMSTORE_BASEURL}/'
      f'{_STUDY_INSTANCE_UID}/{_SERIES_INSTANCE_UID}/{_SOP_INSTANCE_UID}'
      f'/{bulkdata_util.PROXY_BULK_DATA_URI}/{icc_path}'
  )


def _test_dicom_no_icc() -> MutableMapping[str, Any]:
  return {
      '00080008': {
          'Value': ['ORIGINAL', 'PRIMARY', 'VOLUME', 'NONE'],
          'vr': 'CS',
      },
      '00080016': {
          'Value': [
              iccprofile_bulk_metadata_util._VL_WHOLE_SLIDE_MICROSCOPY_IMAGE_IOD_UID
          ],
          'vr': 'UI',
      },
      '00080018': {'Value': [_SOP_INSTANCE_UID.sop_instance_uid], 'vr': 'UI'},
      iccprofile_bulk_metadata_util._OPTICAL_PATHSQ_DICOM_TAG_ADDRESS: {
          'Value': [{}, {}, {}],
          'vr': 'SQ',
      },
  }


def _icc_sq_json(
    index: int, icc_json: Mapping[str, Any]
) -> MutableMapping[str, Any]:
  dcm_json = _test_dicom_no_icc()
  dcm_json[iccprofile_bulk_metadata_util._OPTICAL_PATHSQ_DICOM_TAG_ADDRESS][
      'Value'
  ][index][
      iccprofile_bulk_metadata_util._ICCPROFILE_DICOM_TAG_ADDRESS
  ] = copy.deepcopy(
      icc_json
  )
  return dcm_json


def _icc_root_json(icc_json: Mapping[str, Any]) -> MutableMapping[str, Any]:
  dcm_json = _test_dicom_no_icc()
  dcm_json[iccprofile_bulk_metadata_util._ICCPROFILE_DICOM_TAG_ADDRESS] = (
      copy.deepcopy(icc_json)
  )
  return dcm_json


def _create_multipart_xml_http_response(metadata: str) -> str:
  return requests_toolbelt.MultipartEncoder(
      fields=[iccprofile_bulk_metadata_util._xml_field_content(metadata)],
      boundary=_BOUNDARY,
  ).read()


@dataclasses.dataclass
class _MockFlaskResponse:
  content_type: str
  data: str


def _decode_multipart_response(response: _MockFlaskResponse) -> str:
  mp_response = requests_toolbelt.MultipartDecoder(
      response.data, response.content_type
  )
  return mp_response.parts[0].content


@dataclasses.dataclass
class _MockFlaskRequest:
  base_url: str
  headers: Mapping[str, str]


class IccprofileBulkMetadataUtilTest(parameterized.TestCase):

  def test_get_optical_path_sequence_index_success(self):
    self.assertEqual(
        iccprofile_bulk_metadata_util._get_optical_path_sequence_index(
            'OpticalPathSequence/5/ICCProfile'
        ),
        5,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='does_not_start_with_optical_path_sq',
          path='foo/3/ICCProfile',
      ),
      dict(
          testcase_name='does_not_end_with_iccprofile',
          path='OpticalPathSequence/3/bar',
      ),
  ])
  def test_get_optical_path_sequence_index_fails(self, path: str):
    self.assertEqual(
        iccprofile_bulk_metadata_util._get_optical_path_sequence_index(path), -1
    )

  def test_find_single_value_succeeds(self):
    found_xml = iccprofile_bulk_metadata_util._find_single_value(
        ET.fromstring(_TEST_DICOM_XML_DOC_FRAGMENT),
        "./DicomAttribute/[@tag='00480105']",
    )
    self.assertEqual(
        ET.tostring(found_xml).decode('us-ascii'),
        (
            '<DicomAttribute tag="00480105" vr="SQ"'
            ' keyword="OpticalPathSequence"><Item number="1" /><Item'
            ' number="2"><DicomAttribute tag="00282000" vr="OB"'
            ' keyword="ICCProfile"><Value'
            ' number="1">111741</Value></DicomAttribute></Item></DicomAttribute>'
        ),
    )

  def test_find_single_value_fails(self):
    with self.assertRaises(
        iccprofile_bulk_metadata_util._UnexpectedNumberOfXmlElementsError
    ):
      iccprofile_bulk_metadata_util._find_single_value(
          ET.fromstring(_TEST_DICOM_XML_DOC_FRAGMENT), './DicomAttribute/Item'
      )

  @parameterized.named_parameters([
      dict(
          testcase_name='to_many_parts',
          path='OpticalPathSequence/3/4/ICCProfile',
          exception_str=iccprofile_bulk_metadata_util._ICCPROFILE_PATH_HAS_INCORRECT_NUMBER_OF_PARTS,
      ),
      dict(
          testcase_name='to_few_parts',
          path='OpticalPathSequence/ICCProfile',
          exception_str=iccprofile_bulk_metadata_util._ICCPROFILE_PATH_HAS_INCORRECT_NUMBER_OF_PARTS,
      ),
      dict(
          testcase_name='can_cannot_parse_index',
          path='OpticalPathSequence/A/ICCProfile',
          exception_str=iccprofile_bulk_metadata_util._ICCPROFILE_PATH_DOES_NOT_REF_ICCPROFILE,
      ),
  ])
  def test_get_optical_path_sequence_index_raises(
      self, path: str, exception_str: str
  ):
    with self.assertRaisesRegex(
        iccprofile_bulk_metadata_util.InvalidICCProfilePathError, exception_str
    ):
      iccprofile_bulk_metadata_util._get_optical_path_sequence_index(path)

  @parameterized.named_parameters([
      dict(
          testcase_name='add_root_icc_profile',
          path='ICCProfile',
          expected=_expected_result_add_bulkdata_to_root_in_dicom_xml_fragment(
              _BULKDATA_URI
          ),
      ),
      dict(
          testcase_name='add_optical_path_seq_icc_profile',
          path='OpticalPathSequence/0/ICCProfile',
          expected=_expected_result_add_bulkdata_to_optical_path_sq_in_dicom_xml_fragment(
              _BULKDATA_URI
          ),
      ),
  ])
  def test_add_xml_iccprofile_bulkdata_url_succeeds(
      self, path: str, expected: str
  ):
    xml = ET.fromstring(_TEST_DICOM_XML_DOC_FRAGMENT)
    self.assertTrue(
        iccprofile_bulk_metadata_util._add_xml_iccprofile_bulkdata_url(
            xml, path, _BULKDATA_URI
        )
    )
    self.assertEqual(
        _remove_required_xml_tags_and_convert_to_str(xml), expected
    )

  def test_add_xml_iccprofile_bulkdata_url_unchanged(self):
    path = 'OpticalPathSequence/1/ICCProfile'
    xml = ET.fromstring(_TEST_DICOM_XML_DOC_FRAGMENT)
    self.assertFalse(
        iccprofile_bulk_metadata_util._add_xml_iccprofile_bulkdata_url(
            xml, path, _BULKDATA_URI
        )
    )
    self.assertEqual(
        _remove_required_xml_tags_and_convert_to_str(xml),
        _remove_required_xml_tags_and_convert_to_str(
            ET.fromstring(_TEST_DICOM_XML_DOC_FRAGMENT)
        ),
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='add_iccprofile_bulkdata_to_root',
          metadata=_test_dicom_no_icc(),
          path='ICCProfile',
          expected_metadata=_icc_root_json(_ICC_BULKDATA_URI),
      ),
      dict(
          testcase_name='add_iccprofile_bulkdata_to_first_optical_path_seq',
          metadata=_test_dicom_no_icc(),
          path='OpticalPathSequence/0/ICCProfile',
          expected_metadata=_icc_sq_json(0, _ICC_BULKDATA_URI),
      ),
      dict(
          testcase_name='add_iccprofile_bulkdata_to_last_optical_path_seq',
          metadata=_test_dicom_no_icc(),
          path='OpticalPathSequence/2/ICCProfile',
          expected_metadata=_icc_sq_json(2, _ICC_BULKDATA_URI),
      ),
      dict(
          testcase_name='root_has_iccprofile_bulkdata',
          metadata=_icc_root_json(_PREEXISTING_ICC_BULKDATA),
          path='ICCProfile',
          expected_metadata=_icc_root_json(_PREEXISTING_ICC_BULKDATA),
      ),
      dict(
          testcase_name='first_optical_path_seq_has_iccprofile_bulkdata',
          metadata=_icc_sq_json(0, _PREEXISTING_ICC_BULKDATA),
          path='OpticalPathSequence/0/ICCProfile',
          expected_metadata=_icc_sq_json(0, _PREEXISTING_ICC_BULKDATA),
      ),
      dict(
          testcase_name='last_optical_path_seq_has_iccprofile_bulkdata',
          metadata=_icc_sq_json(2, _PREEXISTING_ICC_BULKDATA),
          path='OpticalPathSequence/2/ICCProfile',
          expected_metadata=_icc_sq_json(2, _PREEXISTING_ICC_BULKDATA),
      ),
  ])
  def test_add_json_iccprofile_bulkdata_url(
      self,
      metadata: MutableMapping[str, Any],
      path: str,
      expected_metadata: MutableMapping[str, Any],
  ):
    expected_result = bool(expected_metadata != metadata)

    result = iccprofile_bulk_metadata_util._add_json_iccprofile_bulkdata_url(
        metadata, path, _BULKDATA_URI
    )
    self.assertEqual(metadata, expected_metadata)
    self.assertEqual(result, expected_result)

  def test_add_json_iccprofile_bulkdata_url_returns_false_if_seq_invalid(self):
    input_data = _test_dicom_no_icc()
    self.assertFalse(
        iccprofile_bulk_metadata_util._add_json_iccprofile_bulkdata_url(
            input_data, 'OpticalPathSequence/10/ICCProfile', _BULKDATA_URI
        )
    )
    self.assertEqual(input_data, _test_dicom_no_icc())

  @parameterized.named_parameters([
      dict(
          testcase_name='to_many_parts',
          path='OpticalPathSequence/10/12/ICCProfile',
          exception_regex=iccprofile_bulk_metadata_util._ICCPROFILE_PATH_HAS_INCORRECT_NUMBER_OF_PARTS,
      ),
      dict(
          testcase_name='invalid_icc_profile_path',
          path='Invalid',
          exception_regex=iccprofile_bulk_metadata_util._ICCPROFILE_PATH_DOES_NOT_REF_ICCPROFILE,
      ),
      dict(
          testcase_name='invalid_icc_profile_path_index',
          path='OpticalPathSequence/A/ICCProfile',
          exception_regex=iccprofile_bulk_metadata_util._ICCPROFILE_PATH_DOES_NOT_REF_ICCPROFILE,
      ),
      dict(
          testcase_name='invalid_icc_profile_path_invalid_prefix',
          path='foo/10/ICCProfile',
          exception_regex=iccprofile_bulk_metadata_util._ICCPROFILE_PATH_DOES_NOT_REF_ICCPROFILE,
      ),
      dict(
          testcase_name='invalid_icc_profile_path_invalid_suffix',
          path='OpticalPathSequence/10/bar',
          exception_regex=iccprofile_bulk_metadata_util._ICCPROFILE_PATH_DOES_NOT_REF_ICCPROFILE,
      ),
      dict(
          testcase_name='invalid_icc_profile_path_missing_index',
          path='OpticalPathSequence/ICCProfile',
          exception_regex=iccprofile_bulk_metadata_util._ICCPROFILE_PATH_HAS_INCORRECT_NUMBER_OF_PARTS,
      ),
  ])
  def test_invalid_icc_profile_path_raises(
      self, path: str, exception_regex: str
  ):
    with self.assertRaisesRegex(
        iccprofile_bulk_metadata_util.InvalidICCProfilePathError,
        exception_regex,
    ):
      iccprofile_bulk_metadata_util._add_json_iccprofile_bulkdata_url(
          _test_dicom_no_icc(), path, _BULKDATA_URI
      )

  @parameterized.named_parameters([
      dict(
          testcase_name='add_root_level_iccprofile_bulkdata',
          icc_profile_path='ICCProfile',
          sop_instance_uid=_SOP_INSTANCE_UID,
          expected_data=_icc_root_json(
              _icc_bulk_data(_expected_bulkdata_uri('ICCProfile'))
          ),
      ),
      dict(
          testcase_name='add_iccprofile_bulkdata_to_middle_optical_path_sq',
          icc_profile_path='OpticalPathSequence/1/ICCProfile',
          sop_instance_uid=_SOP_INSTANCE_UID,
          expected_data=_icc_sq_json(
              1,
              _icc_bulk_data(
                  _expected_bulkdata_uri('OpticalPathSequence/1/ICCProfile')
              ),
          ),
      ),
      dict(
          testcase_name='add_root_level_iccprofile_bulkdata2',
          icc_profile_path='ICCProfile',
          sop_instance_uid=dicom_url_util.SOPInstanceUID(''),
          expected_data=_icc_root_json(
              _icc_bulk_data(_expected_bulkdata_uri('ICCProfile'))
          ),
      ),
  ])
  @mock.patch.object(
      flask_util,
      'get_request',
      autospec=True,
      return_value=_MockFlaskRequest(
          (
              f'https://dpas.cloudflyer.info/tile/{_DICOMSTORE_BASEURL}/'
              f'{_STUDY_INSTANCE_UID}/{_SERIES_INSTANCE_UID}/instances'
          ),
          {},
      ),
  )
  @mock.patch.object(
      flask_util,
      'get_url_root',
      autospec=True,
      return_value='https://dpas.cloudflyer.info',
  )
  def test_augment_json_dicom_iccprofile_bulkdata_metadata(
      self,
      unused_get_url_root_mock,
      unused_get_request_mock,
      icc_profile_path,
      sop_instance_uid,
      expected_data,
  ):
    metadata = _test_dicom_no_icc()
    with mock.patch.object(
        color_conversion_util,
        'get_series_icc_profile_path',
        autospec=True,
        return_value=icc_profile_path,
    ):
      response = _MockFlaskResponse(
          iccprofile_bulk_metadata_util._JSON_METADATA_CONTENT_TYPE,
          json.dumps([metadata]),
      )

      iccprofile_bulk_metadata_util.augment_dicom_iccprofile_bulkdata_metadata(
          _DICOMSTORE_BASEURL,
          _STUDY_INSTANCE_UID,
          _SERIES_INSTANCE_UID,
          sop_instance_uid,
          response,
          cache_enabled_type.CachingEnabled(False),
      )

    self.assertEqual(json.loads(response.data), [expected_data])

  @parameterized.named_parameters([
      dict(
          testcase_name='add_root_level_iccprofile_bulkdata',
          icc_profile_path='ICCProfile',
          sop_instance_uid=dicom_url_util.SOPInstanceUID(''),
          expected_metadata=_expected_result_add_bulkdata_to_root_in_dicom_xml_fragment(
              _expected_bulkdata_uri('ICCProfile')
          ),
      ),
      dict(
          testcase_name='add_iccprofile_bulkdata_to_middle_optical_path_sq',
          icc_profile_path='OpticalPathSequence/0/ICCProfile',
          sop_instance_uid=dicom_url_util.SOPInstanceUID(''),
          expected_metadata=_expected_result_add_bulkdata_to_optical_path_sq_in_dicom_xml_fragment(
              _expected_bulkdata_uri('OpticalPathSequence/0/ICCProfile')
          ),
      ),
      dict(
          testcase_name='add_iccprofile_bulkdata_to_middle_optical_path_sq_2',
          sop_instance_uid=dicom_url_util.SOPInstanceUID(''),
          icc_profile_path='OpticalPathSequence/1/ICCProfile',
          expected_metadata=_TEST_DICOM_XML_DOC_FRAGMENT,
      ),
      dict(
          testcase_name='add_root_level_iccprofile_bulkdata2',
          icc_profile_path='ICCProfile',
          sop_instance_uid=_SOP_INSTANCE_UID,
          expected_metadata=_expected_result_add_bulkdata_to_root_in_dicom_xml_fragment(
              _expected_bulkdata_uri('ICCProfile')
          ),
      ),
  ])
  @mock.patch.object(
      flask_util,
      'get_request',
      autospec=True,
      return_value=_MockFlaskRequest(
          (
              f'https://dpas.cloudflyer.info/tile/{_DICOMSTORE_BASEURL}/'
              f'{_STUDY_INSTANCE_UID}/{_SERIES_INSTANCE_UID}/instances'
          ),
          {},
      ),
  )
  @mock.patch.object(
      flask_util,
      'get_url_root',
      autospec=True,
      return_value='https://dpas.cloudflyer.info',
  )
  def test_augment_xml_dicom_iccprofile_bulkdata_metadata(
      self,
      unused_get_url_root_mock,
      unused_get_request_mock,
      icc_profile_path,
      sop_instance_uid,
      expected_metadata,
  ):
    with mock.patch.object(
        color_conversion_util,
        'get_series_icc_profile_path',
        autospec=True,
        return_value=icc_profile_path,
    ):
      response = _MockFlaskResponse(
          f'multipart/related;type={iccprofile_bulk_metadata_util._XML_METADATA_CONTENT_TYPE};boundary="{_BOUNDARY}"',
          _create_multipart_xml_http_response(_TEST_DICOM_XML_DOC_FRAGMENT),
      )
      iccprofile_bulk_metadata_util.augment_dicom_iccprofile_bulkdata_metadata(
          _DICOMSTORE_BASEURL,
          _STUDY_INSTANCE_UID,
          _SERIES_INSTANCE_UID,
          sop_instance_uid,
          response,
          cache_enabled_type.CachingEnabled(False),
      )
    self.assertEqual(
        _remove_required_xml_tags_and_convert_to_str(
            ET.fromstring(_decode_multipart_response(response))
        ),
        _remove_required_xml_tags_and_convert_to_str(
            ET.fromstring(expected_metadata)
        ),
    )

  def test_add_xml_iccprofile_bulkdata_url_raises_invalid_path(self):
    xml = ET.fromstring(_TEST_DICOM_XML_DOC_FRAGMENT)
    with self.assertRaisesRegex(
        iccprofile_bulk_metadata_util.InvalidICCProfilePathError,
        iccprofile_bulk_metadata_util._ICCPROFILE_PATH_DOES_NOT_REF_ICCPROFILE,
    ):
      iccprofile_bulk_metadata_util._add_xml_iccprofile_bulkdata_url(
          xml, 'foo', _BULKDATA_URI
      )

  def test_add_xml_iccprofile_bulkdata_url_returns_false_if_xml_invalid(self):
    xml = ET.fromstring(
        '<NativeDicomModel><DicomAttribute  tag="00480105" vr="SQ"'
        ' keyword="OpticalPathSequence" /><DicomAttribute  tag="00480105"'
        ' vr="SQ" keyword="OpticalPathSequence" /></NativeDicomModel>'
    )
    self.assertFalse(
        iccprofile_bulk_metadata_util._add_xml_iccprofile_bulkdata_url(
            xml, 'OpticalPathSequence/0/ICCProfile', _BULKDATA_URI
        )
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='bad_json',
          response=_MockFlaskResponse(
              iccprofile_bulk_metadata_util._JSON_METADATA_CONTENT_TYPE,
              'bad_json = ',
          ),
      ),
      dict(
          testcase_name='json_not_list',
          response=_MockFlaskResponse(
              iccprofile_bulk_metadata_util._JSON_METADATA_CONTENT_TYPE,
              '"bad_json"',
          ),
      ),
      dict(
          testcase_name='json_no_sop_instance_value_defined',
          response=_MockFlaskResponse(
              iccprofile_bulk_metadata_util._JSON_METADATA_CONTENT_TYPE,
              (
                  '[{"00080008" : {"Value": ["ORIGINAL", "PRIMARY",'
                  ' "VOLUME", "NONE"]}, "00080016" : {"Value":'
                  f' ["{iccprofile_bulk_metadata_util._VL_WHOLE_SLIDE_MICROSCOPY_IMAGE_IOD_UID}"],'
                  ' "vr":"UI"}}]'
              ),
          ),
      ),
      dict(
          testcase_name='json_no_image_type_defined',
          response=_MockFlaskResponse(
              iccprofile_bulk_metadata_util._JSON_METADATA_CONTENT_TYPE,
              (
                  '[{"00080018" : {"Value": ["1.2.3.4.5"]}, "00080016" :'
                  ' {"Value":'
                  f' ["{iccprofile_bulk_metadata_util._VL_WHOLE_SLIDE_MICROSCOPY_IMAGE_IOD_UID}"],'
                  ' "vr":"UI"}}]'
              ),
          ),
      ),
      dict(
          testcase_name='json_no_sop_class_value_defined',
          response=_MockFlaskResponse(
              iccprofile_bulk_metadata_util._JSON_METADATA_CONTENT_TYPE,
              (
                  '[{"00080018" : {"Value": ["]'
                  f'{_SOP_INSTANCE_UID.sop_instance_uid}'
                  '"], "vr":"UI"}, "00080008": '
                  '{"Value": ["ORIGINAL", "PRIMARY", "VOLUME", "NONE"]}}]'
              ),
          ),
      ),
      dict(
          testcase_name='json_invalid_sop_class_value_defined',
          response=_MockFlaskResponse(
              iccprofile_bulk_metadata_util._JSON_METADATA_CONTENT_TYPE,
              (
                  '[{"00080018" : {"Value": ["]'
                  f'{_SOP_INSTANCE_UID.sop_instance_uid}'
                  '"], "vr":"UI"}, "00080008": '
                  '{"Value": ["ORIGINAL", "PRIMARY", "VOLUME", "NONE"]}, '
                  ' "00080016": {"Value": ["1.2.3.4"],  "vr":"UI"}}]'
              ),
          ),
      ),
      dict(
          testcase_name='xml_invalid_multipart_response',
          response=_MockFlaskResponse(
              iccprofile_bulk_metadata_util._XML_METADATA_CONTENT_TYPE,
              'bad_response',
          ),
      ),
      dict(
          testcase_name='bad_xml',
          response=_MockFlaskResponse(
              f'multipart/related;type={iccprofile_bulk_metadata_util._XML_METADATA_CONTENT_TYPE};boundary="{_BOUNDARY}"',
              _create_multipart_xml_http_response('<bad>'),
          ),
      ),
      dict(
          testcase_name='bad_xml_multiple_sopinstance_uid',
          response=_MockFlaskResponse(
              f'multipart/related;type={iccprofile_bulk_metadata_util._XML_METADATA_CONTENT_TYPE};boundary="{_BOUNDARY}"',
              _create_multipart_xml_http_response(
                  f'<NativeDicomModel>{_SOPINSTANCEUID_XML}{_SOPINSTANCEUID_XML}'
                  f'{_IMAGE_TYPE_XML}{_SOPCLASSUID_XML}</NativeDicomModel>'
              ),
          ),
      ),
      dict(
          testcase_name='no_xml_sopinstance_uid',
          response=_MockFlaskResponse(
              f'multipart/related;type={iccprofile_bulk_metadata_util._XML_METADATA_CONTENT_TYPE};boundary="{_BOUNDARY}"',
              _create_multipart_xml_http_response(
                  f'<NativeDicomModel>{_IMAGE_TYPE_XML}{_SOPCLASSUID_XML}'
                  '</NativeDicomModel>'
              ),
          ),
      ),
      dict(
          testcase_name='no_xml_image_type',
          response=_MockFlaskResponse(
              f'multipart/related;type={iccprofile_bulk_metadata_util._XML_METADATA_CONTENT_TYPE};boundary="{_BOUNDARY}"',
              _create_multipart_xml_http_response(
                  f'<NativeDicomModel>{_SOPINSTANCEUID_XML}{_SOPCLASSUID_XML}'
                  '</NativeDicomModel>'
              ),
          ),
      ),
      dict(
          testcase_name='bad_xml_missing_sopclass_uid',
          response=_MockFlaskResponse(
              f'multipart/related;type={iccprofile_bulk_metadata_util._XML_METADATA_CONTENT_TYPE};boundary="{_BOUNDARY}"',
              _create_multipart_xml_http_response(
                  f'<NativeDicomModel>{_SOPINSTANCEUID_XML}{_IMAGE_TYPE_XML}'
                  '</NativeDicomModel>'
              ),
          ),
      ),
      dict(
          testcase_name='bad_xml_invalid_sopclass_uid',
          response=_MockFlaskResponse(
              f'multipart/related;type={iccprofile_bulk_metadata_util._XML_METADATA_CONTENT_TYPE};boundary="{_BOUNDARY}"',
              _create_multipart_xml_http_response(
                  f'<NativeDicomModel>{_SOPINSTANCEUID_XML}{_IMAGE_TYPE_XML}'
                  '<DicomAttribute tag="00080016" vr="UI"'
                  ' keyword="SOPClassUID"><Value number="1">1.2.3'
                  '</Value></DicomAttribute></NativeDicomModel>'
              ),
          ),
      ),
  ])
  @mock.patch.object(
      flask_util,
      'get_url_root',
      autospec=True,
      return_value='https://dpas.cloudflyer.info',
  )
  def test_augment_dicom_iccprofile_bulkdata_metadata_with_invalid_response_nop(
      self,
      unused_get_url_root_mock,
      response,
  ):
    sop_instance_uid = dicom_url_util.SOPInstanceUID('')
    with mock.patch.object(
        flask_util,
        'get_request',
        autospec=True,
        return_value=_MockFlaskRequest(
            (
                f'https://dpas.cloudflyer.info/tile/{_DICOMSTORE_BASEURL}/'
                f'{_STUDY_INSTANCE_UID}/{_SERIES_INSTANCE_UID}/instances'
            ),
            {},
        ),
    ):
      with mock.patch.object(
          color_conversion_util,
          'get_series_icc_profile_path',
          autospec=True,
          return_value='ICCProfile',
      ):
        pre_call_data = copy.deepcopy(response.data)
        iccprofile_bulk_metadata_util.augment_dicom_iccprofile_bulkdata_metadata(
            _DICOMSTORE_BASEURL,
            _STUDY_INSTANCE_UID,
            _SERIES_INSTANCE_UID,
            sop_instance_uid,
            response,
            cache_enabled_type.CachingEnabled(False),
        )
        self.assertEqual(pre_call_data, response.data)

  @parameterized.named_parameters([
      dict(
          testcase_name='missing_study_instance_uid',
          study_instance_uid=dicom_url_util.StudyInstanceUID(''),
          series_instance_uid=_SERIES_INSTANCE_UID,
          icc_profile_path='ICCProfile',
          flask_request_url=(
              f'https://dpas.cloudflyer.info/tile/{_DICOMSTORE_BASEURL}/'
              f'{_STUDY_INSTANCE_UID}/{_SERIES_INSTANCE_UID}/instances'
          ),
      ),
      dict(
          testcase_name='missing_series_instance_uid',
          study_instance_uid=_STUDY_INSTANCE_UID,
          series_instance_uid=dicom_url_util.SeriesInstanceUID(''),
          icc_profile_path='ICCProfile',
          flask_request_url=(
              f'https://dpas.cloudflyer.info/tile/{_DICOMSTORE_BASEURL}/'
              f'{_STUDY_INSTANCE_UID}/{_SERIES_INSTANCE_UID}/instances'
          ),
      ),
      dict(
          testcase_name='invalid_icc_profile_path',
          study_instance_uid=_STUDY_INSTANCE_UID,
          series_instance_uid=_SERIES_INSTANCE_UID,
          icc_profile_path='',
          flask_request_url=(
              f'https://dpas.cloudflyer.info/tile/{_DICOMSTORE_BASEURL}/'
              f'{_STUDY_INSTANCE_UID}/{_SERIES_INSTANCE_UID}/instances'
          ),
      ),
      dict(
          testcase_name='invalid_flask_request_url',
          study_instance_uid=_STUDY_INSTANCE_UID,
          series_instance_uid=_SERIES_INSTANCE_UID,
          icc_profile_path='ICCProfile',
          flask_request_url='https://dpas.cloudflyer.info/tile/',
      ),
  ])
  @mock.patch.object(
      flask_util,
      'get_url_root',
      autospec=True,
      return_value='https://dpas.cloudflyer.info',
  )
  def test_augment_xml_dicom_iccprofile_bulkdata_metadata_invalid_params_nop(
      self,
      unused_get_url_root,
      study_instance_uid,
      series_instance_uid,
      icc_profile_path,
      flask_request_url,
  ):
    metadata = _TEST_DICOM_XML_DOC_FRAGMENT
    sop_instance_uid = dicom_url_util.SOPInstanceUID('')
    with mock.patch.object(
        flask_util,
        'get_request',
        autospec=True,
        return_value=_MockFlaskRequest(flask_request_url, {}),
    ):
      with mock.patch.object(
          color_conversion_util,
          'get_series_icc_profile_path',
          autospec=True,
          return_value=icc_profile_path,
      ):
        response = _MockFlaskResponse(
            f'multipart/related;type={iccprofile_bulk_metadata_util._XML_METADATA_CONTENT_TYPE};boundary="{_BOUNDARY}"',
            _create_multipart_xml_http_response(metadata),
        )
        pre_call_data = copy.deepcopy(response.data)
        iccprofile_bulk_metadata_util.augment_dicom_iccprofile_bulkdata_metadata(
            _DICOMSTORE_BASEURL,
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid,
            response,
            cache_enabled_type.CachingEnabled(False),
        )
        self.assertEqual(pre_call_data, response.data)

  @parameterized.named_parameters([
      dict(
          testcase_name='json_dicom_metadata_missing_image_type',
          test_mapping={},
          expected=[],
      ),
      dict(
          testcase_name='json_dicom_metadata_missing_image_type_value',
          test_mapping={iccprofile_bulk_metadata_util._IMAGE_TYPE_TAG: {}},
          expected=[],
      ),
      dict(
          testcase_name='json_dicom_metadata_with_image_type',
          test_mapping={
              iccprofile_bulk_metadata_util._IMAGE_TYPE_TAG: {
                  iccprofile_bulk_metadata_util._VALUE: [
                      'foo',
                      'bar',
                      'Google',
                  ]
              }
          },
          expected=['foo', 'bar', 'Google'],
      ),
  ])
  def test_get_image_type_values_from_json(
      self, test_mapping: Mapping[str, Any], expected: List[str]
  ):
    self.assertEqual(
        iccprofile_bulk_metadata_util._get_image_type_values_from_json(
            test_mapping
        ),
        expected,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='xml_dicom_metadata_missing_image_type',
          test_xml='<NativeDicomModel></NativeDicomModel>',
          expected=[],
      ),
      dict(
          testcase_name='xml_dicom_metadata_missing_image_type_value',
          test_xml=(
              '<NativeDicomModel><DicomAttribute tag="00080008" vr="CS"'
              ' keyword="ImageType"/></NativeDicomModel>'
          ),
          expected=[],
      ),
      dict(
          testcase_name='xml_dicom_metadata_multiple_image_type',
          test_xml=(
              '<NativeDicomModel><DicomAttribute tag="00080008" vr="CS"'
              ' keyword="ImageType"><Value'
              ' number="1">foo</Value></DicomAttribute><DicomAttribute'
              ' tag="00080008" vr="CS" keyword="ImageType"/></NativeDicomModel>'
          ),
          expected=[],
      ),
      dict(
          testcase_name='xml_dicom_metadata_with_image_type',
          test_xml=f'<NativeDicomModel>{_IMAGE_TYPE_XML}</NativeDicomModel>',
          expected=['ORIGINAL', 'PRIMARY', 'VOLUME', 'NONE'],
      ),
  ])
  def test_get_image_type_values_from_xml(
      self, test_xml: str, expected: List[str]
  ):
    self.assertEqual(
        iccprofile_bulk_metadata_util._get_image_type_values_from_xml(
            ET.fromstring(test_xml)
        ),
        expected,
    )

  @parameterized.named_parameters([
      dict(
          testcase_name='missing_image_type',
          image_type_vals=[],
          expected=True,
      ),
      dict(
          testcase_name='has_label',
          image_type_vals=['foo', ' label ', 'bar'],
          expected=True,
      ),
      dict(
          testcase_name='has_overview',
          image_type_vals=['foo', ' overview ', 'bar'],
          expected=True,
      ),
      dict(
          testcase_name='is_not_label_overview',
          image_type_vals=['foo', 'bar'],
          expected=False,
      ),
  ])
  def test_is_image_type_label_or_overview(
      self, image_type_vals: List[str], expected: bool
  ):
    self.assertEqual(
        iccprofile_bulk_metadata_util._is_image_type_label_or_overview(
            image_type_vals
        ),
        expected,
    )

  def test_get_uid_from_xml_succeeds(self):
    tag_address = iccprofile_bulk_metadata_util._SOPCLASSUID_DICOM_TAG_ADDRESS
    tag_value = '1.2.3.4'
    self.assertEqual(
        iccprofile_bulk_metadata_util._get_uid_from_xml(
            ET.fromstring(
                f'<NativeDicomModel><DicomAttribute tag="{tag_address}" vr="UI"'
                ' keyword="SOPClassUID"><Value'
                f' number="1">{tag_value}</Value></DicomAttribute>'
                '</NativeDicomModel>'
            ),
            tag_address,
        ),
        tag_value,
    )

  @parameterized.named_parameters([
      dict(testcase_name='tag_not_found', xml='<NativeDicomModel/>'),
      dict(
          testcase_name='tag_missing_value',
          xml=(
              '<NativeDicomModel><DicomAttribute tag="00080016"'
              ' vr="UI" keyword="SOPClassUID"/></NativeDicomModel>'
          ),
      ),
      dict(
          testcase_name='tag_empty_value',
          xml=(
              '<NativeDicomModel><DicomAttribute tag="00080016" vr="UI"'
              ' keyword="SOPClassUID"><Value number="1" /></DicomAttribute>'
              '</NativeDicomModel>'
          ),
      ),
  ])
  def test_get_uid_from_xml_raises(self, xml: str):
    with self.assertRaises(
        iccprofile_bulk_metadata_util._UnexpectedNumberOfXmlElementsError
    ):
      iccprofile_bulk_metadata_util._get_uid_from_xml(
          ET.fromstring(xml),
          iccprofile_bulk_metadata_util._SOPCLASSUID_DICOM_TAG_ADDRESS,
      )

  def test_get_uid_from_json_succeeds(self):
    tag_address = iccprofile_bulk_metadata_util._SOPCLASSUID_DICOM_TAG_ADDRESS
    tag_value = '1.2.3.4'
    self.assertEqual(
        iccprofile_bulk_metadata_util._get_uid_from_json(
            {tag_address: {'Value': [tag_value]}},
            tag_address,
        ),
        tag_value,
    )

  @parameterized.named_parameters([
      dict(testcase_name='tag_not_found', metadata={}, exception=KeyError),
      dict(
          testcase_name='tag_missing_value',
          metadata={'00080016': {}},
          exception=KeyError,
      ),
      dict(
          testcase_name='empty_value',
          metadata={'00080016': {'Value': []}},
          exception=IndexError,
      ),
  ])
  def test_get_uid_from_json_raises(
      self, metadata: Mapping[str, Any], exception: Exception
  ):
    with self.assertRaises(exception):
      iccprofile_bulk_metadata_util._get_uid_from_json(
          metadata,
          iccprofile_bulk_metadata_util._SOPCLASSUID_DICOM_TAG_ADDRESS,
      )


if __name__ == '__main__':
  absltest.main()
