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
"""Generates a filter file for DeId operations."""
import dataclasses
from json import decoder
from typing import Any, List, Mapping, Optional
import urllib.error

from absl import flags
from ez_wsi_dicomweb import pixel_spacing
import google.auth
from google.auth.transport import requests

from pathology.orchestrator import healthcare_api_const
from pathology.orchestrator.v1alpha import cohorts_pb2
from pathology.shared_libs.flags import flag_utils
from pathology.shared_libs.flags import secret_flag_utils
from pathology.shared_libs.logging_lib import cloud_logging_client
from pathology.transformation_pipeline.ingestion_lib.dicom_util import dicomtag

_SCOPES = ['https://www.googleapis.com/auth/cloud-platform']
_HEADERS = {'Content-Type': 'application/dicom+json; charset=utf-8'}

# DICOM tags. Include all tags listed below in _includefield_tag_list
# defined below. Tag list used to generate DICOM instance metadata query.
DicomTag = dicomtag.DicomTag
_DICOM_SOP_CLASS_UID_TAG = DicomTag.tag_address('00080016', 'SOPClassUID')
_DICOM_SHARED_FUNCTIONAL_GROUPS_SEQUENCE_TAG = DicomTag.tag_address(
    '52009229', 'SharedFunctionalGroupsSequence'
)
_DICOM_PIXEL_MEASURES_SEQUENCE_TAG = DicomTag.tag_address(
    '00289110', 'PixelMeasuresSequence'
)
_DICOM_PIXEL_SPACING_TAG = DicomTag.tag_address('00280030', 'PixelSpacing')
_DICOM_TOTAL_PIXEL_MATRIX_COLUMNS_TAG = DicomTag.tag_address(
    '00480006', 'TotalPixelMatrixColumns'
)
_DICOM_TOTAL_PIXEL_MATRIX_ROWS_TAG = DicomTag.tag_address(
    '00480007', 'TotalPixelMatrixRows'
)
_DICOM_IMAGE_VOLUME_HEIGHT_TAG = DicomTag.tag_address(
    '00480002', 'ImagedVolumeHeight'
)
_DICOM_IMAGE_VOLUME_WIDTH_TAG = DicomTag.tag_address(
    '00480001', 'ImagedVolumeWidth'
)
_DICOM_IMAGE_TYPE_TAG = DicomTag.tag_address('00080008', 'ImageType')
_DICOM_RECOGNIZABLE_VISUAL_FEATURES_TAG = DicomTag.tag_address(
    '00280302', 'RecognizableVisualFeatures'
)
_DICOM_SPECIMEN_LABEL_TAG = DicomTag.tag_address(
    '00480010', 'SpecimenLabelInImage'
)
_DICOM_INSTANCE_UID_TAG = DicomTag.tag_address('00080018', 'SOPInstanceUID')

_INCLUDE_FIELD_TAG_LIST = [
    _DICOM_SOP_CLASS_UID_TAG,
    _DICOM_SHARED_FUNCTIONAL_GROUPS_SEQUENCE_TAG,
    _DICOM_PIXEL_MEASURES_SEQUENCE_TAG,
    _DICOM_PIXEL_SPACING_TAG,
    _DICOM_TOTAL_PIXEL_MATRIX_COLUMNS_TAG,
    _DICOM_TOTAL_PIXEL_MATRIX_ROWS_TAG,
    _DICOM_IMAGE_VOLUME_HEIGHT_TAG,
    _DICOM_IMAGE_VOLUME_WIDTH_TAG,
    _DICOM_IMAGE_TYPE_TAG,
    _DICOM_RECOGNIZABLE_VISUAL_FEATURES_TAG,
    _DICOM_SPECIMEN_LABEL_TAG,
    _DICOM_INSTANCE_UID_TAG,
]
# generate _LABEL_METADATA_QUERY (dicom instance metadata query)
_INCLUDE_FIELD_TAGS = '&'.join(
    [f'includefield={tag_address}' for tag_address in _INCLUDE_FIELD_TAG_LIST]
)
_LABEL_METADATA_QUERY = f'instances?{_INCLUDE_FIELD_TAGS}'
del _INCLUDE_FIELD_TAG_LIST
del _INCLUDE_FIELD_TAGS

_IMAGE_TYPE_KEYWORDS = ['LABEL', 'OVERVIEW', 'THUMBNAIL']
_YES = 'YES'
_VALUE = 'Value'

# DICOM IOD
# https://dicom.nema.org/dicom/2013/output/chtml/part04/sect_i.4.html
_VL_MICROSCOPE_IMAGE_IOD_UID = '1.2.840.10008.5.1.4.1.1.77.1.2'
_VL_SLIDE_COORD_MICROSCOPE_IMAGE_IOD_UID = '1.2.840.10008.5.1.4.1.1.77.1.3'
_VL_WHOLE_SLIDE_MICROSCOPE_IMAGE_IOD_UID = '1.2.840.10008.5.1.4.1.1.77.1.6'

_SUPPORTED_MAGNIFICATIONS = (
    '100X',
    '80X',
    '40X',
    '20X',
    '10X',
    '5X',
    '2.5X',
    '1.25X',
    '0.625X',
    '0.3125X',
    '0.15625X',
    '0.078125X',
    '0.0390625X',
)

# By default this is disabled.
_DEID_MAX_MAGNIFICATION_FLG = flags.DEFINE_string(
    'deid_max_magnification',
    secret_flag_utils.get_secret_or_env('DEID_MAX_MAGNIFICATION', 'False'),
    'Maximum magnification of images to DeID; To disable, set False.'
    'Default: False',
)


def _get_float_env(env: str, default: str) -> float:
  """Return floating point value encoded in string."""
  val = secret_flag_utils.get_secret_or_env(env, default)
  try:
    return float(val)
  except ValueError:
    cloud_logging_client.critical(
        f'Environmental variable {env} is float; value = {val}.'
    )
    raise


_DEID_PIXEL_SPACING_THRESHOLD_OFFSET_FLG = flags.DEFINE_float(
    'deid_pixel_spacing_threshold_offset',
    _get_float_env('DEID_PIXEL_SPACING_THRESHOLD_OFFSET', '0.0'),
    'Pixel spacing threshold offset; Default=0.0,',
)
# https://dicom.nema.org/dicom/2013/output/chtml/part04/sect_i.4.html
DEID_IOD_LIST_FLG = flags.DEFINE_string(
    'deid_iod_list',
    secret_flag_utils.get_secret_or_env(
        'DEID_IOD_LIST', _VL_WHOLE_SLIDE_MICROSCOPE_IMAGE_IOD_UID
    ),
    (
        'Comma separated value list of SOPClassIOD UIDs to include in DeID'
        ' filter list.'
    ),
)


@dataclasses.dataclass
class _PixelSpacing:
  """DICOM Pixel spacing."""

  row: float  # mm per pixel vertically
  column: float  # mm per pixel horizontally

  @property
  def is_defined(self) -> bool:
    return self.row > 0.0 and self.column > 0.0

  @property
  def min_spacing(self) -> float:
    return min(self.row, self.column)


def get_full_path_from_dicom_uri(dicom_uri: str) -> str:
  return dicom_uri.split('dicomWeb/')[1]


def get_slides_for_deid_filter_list(
    cohort: cohorts_pb2.PathologyCohort,
) -> List[str]:
  """Returns list of slides (series URI paths) to include in DeID filter list.

  Specifying a Series URI in the filter file includes all instances within that
  series in a given operation.

  Args:
    cohort: Cohort to get slides from.
  """
  cloud_logging_client.info(
      f'Generating filter file list for cohort {cohort.name}.'
  )
  return [
      get_full_path_from_dicom_uri(slide.dicom_uri) for slide in cohort.slides
  ]


def _get_iod(dicom_json: Mapping[str, Any]) -> str:
  """Returns SOPClassUID value from DICOM Json."""
  try:
    return dicom_json[_DICOM_SOP_CLASS_UID_TAG]['Value'][0]
  except (KeyError, IndexError) as _:
    return ''


def _is_vl_whole_slide_microscope_image_iod(
    dicom_json: Mapping[str, Any],
) -> bool:
  return _get_iod(dicom_json) == _VL_WHOLE_SLIDE_MICROSCOPE_IMAGE_IOD_UID


def _is_flat_microscope_image_iod(dicom_json: Mapping[str, Any]) -> bool:
  iod_list = (
      _VL_MICROSCOPE_IMAGE_IOD_UID,
      _VL_SLIDE_COORD_MICROSCOPE_IMAGE_IOD_UID,
  )
  return _get_iod(dicom_json) in iod_list


def _get_shared_functional_group_pixel_spacing(
    dicom_json: Mapping[str, Any],
) -> _PixelSpacing:
  """Returns pixel spacing stored in SharedFunctionalGroupsSequence."""
  # https://dicom.innolitics.com/ciods/vl-whole-slide-microscopy-image/vl-whole-slide-microscopy-image-multi-frame-functional-groups/52009229/00289110/00280030
  seq = dicom_json[_DICOM_SHARED_FUNCTIONAL_GROUPS_SEQUENCE_TAG]['Value'][0]
  pixel_measures = seq[_DICOM_PIXEL_MEASURES_SEQUENCE_TAG]['Value'][0]
  pixel_spacings = pixel_measures[_DICOM_PIXEL_SPACING_TAG]['Value']
  return _PixelSpacing(pixel_spacings[0], pixel_spacings[1])


def _get_derived_pixel_spacing(dicom_json: Mapping[str, Any]) -> _PixelSpacing:
  """Returns pixel spacing derived from image world dim and pixel dim."""
  column = dicom_json[_DICOM_TOTAL_PIXEL_MATRIX_COLUMNS_TAG]['Value'][0]
  row = dicom_json[_DICOM_TOTAL_PIXEL_MATRIX_ROWS_TAG]['Value'][0]
  height = dicom_json[_DICOM_IMAGE_VOLUME_HEIGHT_TAG]['Value'][0]
  width = dicom_json[_DICOM_IMAGE_VOLUME_WIDTH_TAG]['Value'][0]
  return _PixelSpacing(height / row, column / width)


def _get_flat_image_pixel_spacing(
    dicom_json: Mapping[str, Any],
) -> _PixelSpacing:
  """Returns flat image pixel spacing."""
  # Return Pixel Spacing stored in:
  #   https://dicom.innolitics.com/ciods/vl-microscopic-image/vl-image/00280030
  #   https://dicom.innolitics.com/ciods/vl-slide-coordinates-microscopic-image/vl-image/00280030
  pixel_spacings = dicom_json[_DICOM_PIXEL_SPACING_TAG]['Value']
  return _PixelSpacing(pixel_spacings[0], pixel_spacings[1])


def _get_instance_pixel_spacing(
    dicom_json: Mapping[str, Any],
) -> Optional[_PixelSpacing]:
  """Returns pixel spacing of instance; None if unknown."""
  if _is_vl_whole_slide_microscope_image_iod(dicom_json):
    try:
      ps = _get_shared_functional_group_pixel_spacing(dicom_json)
      if ps.is_defined:
        return ps
    except (KeyError, IndexError) as _:
      pass
    try:
      return _get_derived_pixel_spacing(dicom_json)
    except (KeyError, IndexError, ZeroDivisionError) as _:
      pass
  elif _is_flat_microscope_image_iod(dicom_json):
    try:
      return _get_flat_image_pixel_spacing(dicom_json)
    except (KeyError, IndexError) as _:
      pass
  return None


def _get_iod_deid_list() -> List[str]:
  """Returns list of SOPClassUID defined in _DEID_IOD_LIST_FLG."""
  return [uid.strip() for uid in DEID_IOD_LIST_FLG.value.split(',')]


def _max_magnification_flg_val() -> str:
  return _DEID_MAX_MAGNIFICATION_FLG.value.strip().upper()


def _is_deid_max_magnification_flag_enabled() -> bool:
  try:
    return flag_utils.str_to_bool(_max_magnification_flg_val())
  except ValueError:
    return True


def _get_deid_min_pixel_spacing() -> Optional[float]:
  """Returns pixel spacing criterion threshold for DeID Max Magnfication Flag."""
  if _is_deid_max_magnification_flag_enabled():
    ps = pixel_spacing.PixelSpacing.FromMagnificationString(
        _max_magnification_flg_val()
    )
    if ps.pixel_spacing_mm == 0:
      return None
    pixel_spacing_offset = _DEID_PIXEL_SPACING_THRESHOLD_OFFSET_FLG.value
    if pixel_spacing_offset <= 0.0:
      # Default set pixel spacing criterion to be halfway between current size
      # and pixel spacing double.
      pixel_spacing_offset = ps.pixel_spacing_mm / 4.0
    return max(ps.pixel_spacing_mm - pixel_spacing_offset, 0.0)
  return None


def _cs_tag_has_value(
    metadata: Mapping[str, Any], tag: str, expected_value: str
) -> bool:
  """Returns true if tag value is set to expected_value."""
  try:
    return metadata[tag][_VALUE][0].strip().upper() == expected_value
  except (KeyError, IndexError) as _:
    return False


def _should_deid_instance(
    metadata: Mapping[str, Any],
    instance_path: str,
    min_pixel_spacing_threshold: Optional[float],
) -> bool:
  """Returns true if DICOM instance should be included in DeID filter list.

  Args:
    metadata: DICOM instance metadata.
    instance_path: DICOMweb instance URL.
    min_pixel_spacing_threshold: Minimum pixel spacing.

  Returns:
    True if DICOM instance should be included in DeID filter list.
  """
  # Skip any instances with label keywords.
  if any(
      label.strip().upper() in _IMAGE_TYPE_KEYWORDS
      for label in metadata[_DICOM_IMAGE_TYPE_TAG][_VALUE]
  ):
    cloud_logging_client.warning(
        f'Skipping instance {instance_path}, as it may contain patient '
        'identifying information; (ImageType contains '
        f'{str(_IMAGE_TYPE_KEYWORDS)}).',
        {'instance_image_type': metadata[_DICOM_IMAGE_TYPE_TAG][_VALUE]},
    )
    return False

  if _cs_tag_has_value(metadata, _DICOM_SPECIMEN_LABEL_TAG, _YES):
    cloud_logging_client.warning(
        f'Skipping instance {instance_path}, as it may contain patient '
        'identifying information (SpecimenLabelInImage=YES).'
    )
    return False

  # If tag 00280302 is set, only append if value YES is not present.
  if _cs_tag_has_value(metadata, _DICOM_RECOGNIZABLE_VISUAL_FEATURES_TAG, _YES):
    cloud_logging_client.warning(
        f'Skipping instance {instance_path}, as it may contain patient '
        'identifying information.'
    )
    return False

  instance_iod = _get_iod(metadata)
  supported_iod = _get_iod_deid_list()
  if instance_iod not in supported_iod:
    cloud_logging_client.warning(
        f'Skipping instance {instance_path}, describes DICOM in IOD '
        'not included in DEID_IOD_LIST.',
        {'instance_IOD': instance_iod, 'DeID_IOD_list': str(supported_iod)},
    )
    return False

  instance_pixel_spacing = _get_instance_pixel_spacing(metadata)
  if instance_pixel_spacing is None and min_pixel_spacing_threshold is not None:
    cloud_logging_client.warning(
        f'Skipping instance {instance_path}, could not determine pixel spacing.'
    )
    return False

  if (
      min_pixel_spacing_threshold is not None
      and instance_pixel_spacing.is_defined
      and instance_pixel_spacing.min_spacing < min_pixel_spacing_threshold
  ):
    cloud_logging_client.warning(
        'Skipping instance estimated image pixel spacing is smaller than '
        'DeID max magnification pixel spacing.',
        {
            'DeID_Max_Magnification': '{_DEID_MAX_MAGNIFICATION_FLG.value}X',
            'dicom_instance': instance_path,
            'dicom_instance_min_pixel_spacing': (
                instance_pixel_spacing.min_spacing
            ),
            'pixel_spacing_threshold': min_pixel_spacing_threshold,
        },
    )
    return False
  return True


def get_instances_for_deid(cohort: cohorts_pb2.PathologyCohort) -> List[str]:
  """Returns list of valid Instance URIs in a cohort for a Deid operation.

  Removes any instances with metadata tags that would indicate it contains a
  label or PHI.

  Args:
    cohort: Cohort to get instances from.

  Raises:
    HttpError if metadata retrieval fails.
    JSONDecodeError if case is missing instances.
    ValueError: Invalid env setting DEID_MAX_MAGNIFICATION.
  """
  cloud_logging_client.info(
      f'Generating Deid filter file for cohort {cohort.name}.'
  )

  # Gets credentials from the environment.
  scoped_credentials = google.auth.default(scopes=_SCOPES)[0]

  # Creates a requests Session object with the credentials.
  session = requests.AuthorizedSession(scoped_credentials)

  # URL to the Cloud Healthcare API endpoint and version
  base_url = healthcare_api_const.HEALTHCARE_API_BASE_URL_FLG.value

  if (
      _max_magnification_flg_val() not in _SUPPORTED_MAGNIFICATIONS
      and _is_deid_max_magnification_flag_enabled()
  ):
    cloud_logging_client.critical(
        'DEID_MAX_MAGNIFICATION does not define supported magnification'
        f'; DEID_MAX_MAGNIFICATION={_DEID_MAX_MAGNIFICATION_FLG.value}; '
        f'supported values={str(_SUPPORTED_MAGNIFICATIONS)}'
    )
    raise ValueError(
        'DEID_MAX_MAGNIFICATION does not define supported magnification'
    )
  min_pixel_spacing_threshold = _get_deid_min_pixel_spacing()

  instances = []
  for slide in cohort.slides:
    slide_path = get_full_path_from_dicom_uri(slide.dicom_uri)
    query_url = f'{base_url}/{slide.dicom_uri}/{_LABEL_METADATA_QUERY}'
    response = session.get(query_url, headers=_HEADERS)
    try:
      response.raise_for_status()
    except urllib.error.HTTPError as exc:
      cloud_logging_client.critical(
          'Exception occurred in querying the DICOM Store.', exc
      )
    try:
      for metadata in response.json():
        instance_path = (
            f'{slide_path}/instances/'
            f'{metadata[_DICOM_INSTANCE_UID_TAG][_VALUE][0]}'
        )
        if _should_deid_instance(
            metadata, instance_path, min_pixel_spacing_threshold
        ):
          instances.append(instance_path)
    except decoder.JSONDecodeError as exc:
      cloud_logging_client.critical(
          'Exception occurred in retrieving instances from cases in cohort.',
          exc,
          {'response_text': response.text, 'status_code': response.status_code},
      )

  if instances:
    cloud_logging_client.info(
        'Instances listed in DeID filter file.',
        {'cohort name': cohort.name, 'instance_list': str(instances)},
    )
  else:
    cloud_logging_client.warning(
        'No instances listed in DeID filter file.', {'cohort name': cohort.name}
    )
  return instances
