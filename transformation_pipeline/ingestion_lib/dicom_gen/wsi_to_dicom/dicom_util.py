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
"""Utility functions supporting DICOM construction from SVS and DICOM files."""
from __future__ import annotations

import copy
import datetime
import io
import itertools
import os
from typing import Any, List, Mapping, MutableMapping, Optional, Sequence, Union

import openslide
from PIL import ImageCms
import PIL.Image
import pydicom

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_general_equipment
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_json_util
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_private_tag_generator
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen import wsi_dicom_file_ref
from transformation_pipeline.ingestion_lib.dicom_util import pydicom_util

_UM_IN_MM = 1000.0  # Number of micrometers in 1 milimeter.
_MAX_PIXEL_DATA_SIZE_FOR_BASIC_OFFSET_TABLE = 0xFFFFFFFF
_SRGB = 'sRGB'


class InvalidICCProfileError(Exception):
  pass


def _read_icc_profile(filename: str) -> bytes:
  with open(
      os.path.join(
          os.path.dirname(__file__),
          f'../../../../../pathology_cloud_icc_profile/icc_profile/{filename}',
      ),
      'rb',
  ) as infile:
    return infile.read()


def _get_srgb_iccprofile() -> bytes:
  try:
    return _read_icc_profile('sRGB_v4_ICC_preference.icc')
  except FileNotFoundError:
    return ImageCms.ImageCmsProfile(ImageCms.createProfile(_SRGB)).tobytes()


def _get_adobergb_iccprofile() -> bytes:
  return _read_icc_profile('AdobeRGB1998.icc')


def _get_rommrgb_iccprofile() -> bytes:
  return _read_icc_profile('ISO22028-2_ROMM-RGB.icc')


def get_default_icc_profile_color() -> Optional[bytes]:
  """Returns default ICC profile to that should be used if none is provided."""
  default_icc_profile = ingest_flags.DEFAULT_ICCPROFILE_FLG.value
  try:
    if default_icc_profile == ingest_flags.DefaultIccProfile.SRGB:
      profile = _get_srgb_iccprofile()
    elif default_icc_profile == ingest_flags.DefaultIccProfile.ADOBERGB:
      profile = _get_adobergb_iccprofile()
    elif default_icc_profile == ingest_flags.DefaultIccProfile.ROMMRGB:
      profile = _get_rommrgb_iccprofile()
    elif default_icc_profile == ingest_flags.DefaultIccProfile.NONE:
      # Not recommended, ICC Profile are required for all WSI imaging that
      # do not encode monochrome imaging.
      profile = None
    else:
      raise ValueError(
          f'Unrecognized default ICC profile: {default_icc_profile}'
      )
    if default_icc_profile != ingest_flags.DefaultIccProfile.NONE:
      cloud_logging_client.info(
          'ICC profile missing from source imaging; embedding default profile:'
          f' {default_icc_profile.name} in DICOM.'
      )
    return profile
  except FileNotFoundError as exp:
    cloud_logging_client.error('Could not load default ICC Profile.', exp)
    raise exp


def _should_fix_icc_colorspace(dcm: pydicom.Dataset) -> bool:
  return ingest_const.DICOMTagKeywords.ICC_PROFILE in dcm and (
      ingest_const.DICOMTagKeywords.COLOR_SPACE not in dcm or not dcm.ColorSpace
  )


def _dicom_formatted_date(scan_datetime: datetime.datetime) -> str:
  """Returns DICOM VR DA formatted date string."""
  return datetime.datetime.strftime(scan_datetime, '%Y%m%d')


def _dicom_formatted_time(scan_datetime: datetime.datetime) -> str:
  """Returns DICOM VR TM formatted time string."""
  return datetime.datetime.strftime(scan_datetime, '%H%M%S.%f')


def _aperio_scan_time(
    properties: Mapping[str, str],
) -> Optional[datetime.datetime]:
  """Parses Aperio formatted slide scan date/time from openslide properties.

  Args:
    properties: OpenSlide.properties mapping containing aperio metadata.

  Returns:
    Python Datetime in UCT representation of Aperio DateTime metadata.
  """
  parse_date = properties.get('aperio.Date')
  if parse_date is None or not parse_date:
    return None
  parse_time = properties.get('aperio.Time')
  if parse_time is None or not parse_time:
    return None
  text_list = [parse_date, parse_time]
  parse_list = ['%m/%d/%Y', '%H:%M:%S']
  tz = properties.get('aperio.Time Zone')
  if tz is not None and tz:
    text_list.append(tz)
    parse_list.append('%Z%z')
  try:
    return datetime.datetime.strptime(' '.join(text_list), ' '.join(parse_list))
  except ValueError:
    return None


def _add_objective_power(
    metadata: pydicom.Dataset, o_slide: openslide.OpenSlide
) -> None:
  """Adds imaging objective power to DICOM metadata.

  Args:
    metadata: Pydicom dataset to add tag data to.
    o_slide: Openslide object.

  Raises:
    ValueError: Imaging misssing optical path sequence.
  """
  try:
    objective_power = o_slide.properties.get(
        openslide.PROPERTY_NAME_OBJECTIVE_POWER
    )
  except AttributeError:
    return
  if 'OpticalPathSequence' not in metadata:
    raise ValueError('DICOM metadata missing optical path sequence.')
  if objective_power is not None and len(metadata.OpticalPathSequence) == 1:
    metadata.OpticalPathSequence[0].ObjectiveLensPower = objective_power


def _add_background_color(
    metadata: pydicom.Dataset, o_slide: openslide.OpenSlide
) -> None:
  """Adds background color to DICOM metadata.

  Args:
    metadata: Pydicom dataset to add tag data to.
    o_slide: Openslide object.

  Returns:
    None
  """
  if not ingest_flags.ADD_OPENSLIDE_BACKGROUND_COLOR_METADATA_FLG.value:
    return
  try:
    background_color = o_slide.properties.get(
        openslide.PROPERTY_NAME_BACKGROUND_COLOR
    )
    color_profile = o_slide.color_profile  # pytype: disable=attribute-error
  except AttributeError:
    return
  if background_color is None:
    return
  if color_profile is None:
    cloud_logging_client.error(
        'WSI image defines a background color but does not contain a ICC Color'
        ' profile. Cannot generate LAB colorvalue for'
        ' RecommendedAbsentPixelCIELabValue tag.'
    )
    return
  if len(background_color) != 6:
    cloud_logging_client.error(
        'Openslide returned background color that does not match expected'
        ' RRGGBB hex encoding. Cannot generate'
        ' RecommendedAbsentPixelCIELabValue tag'
    )
    return
  lab_p = ImageCms.createProfile('LAB')
  rgb2lab = ImageCms.buildTransformFromOpenProfiles(
      color_profile, lab_p, 'RGB', 'LAB'
  )
  im = PIL.Image.new('RGB', (1, 1), f'#{background_color}')
  lab_image = ImageCms.applyTransform(im, rgb2lab)
  l, a, b = lab_image.split()
  l = int(0xFFFF * l[0, 0] / 100.0)
  a = int(0xFFFF * (a[0, 0] + 128.0) / 0xFF)
  b = int(0xFFFF * (b[0, 0] + 128.0) / 0xFF)
  metadata.RecommendedAbsentPixelCIELabValue = [l, a, b]


def _add_total_pixel_matrix_origin_sequence(
    metadata: pydicom.Dataset, o_slide: openslide.OpenSlide
) -> None:
  """Adds total pixel matrix origin sequence to DICOM metadata.

  Args:
    metadata: Pydicom dataset to add tag data to.
    o_slide: Openslide Object or None. If none adds a default zero offset
      origin.

  Returns:
    None
  Raises:
    ValueError: DICOM metadata missing total pixel matrix origin sequence or it
      is already initalized.
  """
  if not ingest_flags.ADD_OPENSLIDE_TOTAL_PIXEL_MATRIX_ORIGIN_SEQ_FLG.value:
    return
  if (
      'TotalPixelMatrixOriginSequence' not in metadata
      or len(metadata.TotalPixelMatrixOriginSequence) != 1
      or metadata.TotalPixelMatrixOriginSequence[
          0
      ].XOffsetInSlideCoordinateSystem
      != 0
      or metadata.TotalPixelMatrixOriginSequence[
          0
      ].YOffsetInSlideCoordinateSystem
      != 0
  ):
    raise ValueError(
        'DICOM metadata missing total pixel matrix origin sq or already'
        ' initalized.'
    )
  # Openslide defines PROPERTY_NAME_BOUNDS_X and PROPERTY_NAME_BOUNDS_Y
  # However imaging codecs do not appear to provide metadata.
  # Offset should be relative to slide as pictured.
  #
  # https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.8.12.2.html#figure_C.8-16
  bounds_x = o_slide.properties.get(openslide.PROPERTY_NAME_BOUNDS_X)
  if bounds_x is None:
    return
  bounds_y = o_slide.properties.get(openslide.PROPERTY_NAME_BOUNDS_Y)
  if bounds_y is None:
    return
  mpp_x = o_slide.properties.get(openslide.PROPERTY_NAME_MPP_X)
  if mpp_x is None:
    return
  mpp_y = o_slide.properties.get(openslide.PROPERTY_NAME_MPP_Y)
  if mpp_y is None:
    return
  sq = metadata.TotalPixelMatrixOriginSequence[0]
  # Converts pixel offset to offset in mm.
  # Openslide Coordinates in pixels (bounds_x  & bounds_y)
  # https://openslide.org/api/python/
  # mpp_x and mpp_y encode number of microns per pixel, 1 micron is 1000 mm.
  # DICOM coordinates XOffsetInSlideCoordinateSystem and
  # YOffsetInSlideCoordinateSystem are defined in mm from the slide origin.
  # https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.8.12.2.html#figure_C.8-16
  sq.XOffsetInSlideCoordinateSystem = bounds_x * mpp_x / 1000.0
  sq.YOffsetInSlideCoordinateSystem = bounds_y * mpp_y / 1000.0


def add_openslide_dicom_properties(
    metadata: pydicom.Dataset,
    source_imaging: str,
) -> None:
  """Adds openslide derived image acquisition params to generated DICOM.

    Total Pixel Matrix Origin SQ, Background Color, and Objective power.

  Args:
    metadata: Pydicom dataset to add tag data to.
    source_imaging: Path to openslide imaging.

  Returns:
    None
  """
  try:
    o_slide = openslide.OpenSlide(source_imaging)
  except openslide.lowlevel.OpenSlideUnsupportedFormatError:
    cloud_logging_client.warning(
        'OpenSlide cannot read file.',
        {ingest_const.LogKeywords.FILENAME: source_imaging},
    )
    return
  _add_total_pixel_matrix_origin_sequence(metadata, o_slide)
  _add_background_color(metadata, o_slide)
  _add_objective_power(metadata, o_slide)


def get_svs_metadata(dicom_path: str) -> Mapping[str, pydicom.DataElement]:
  """Returns DICOM formatted metadata parsed from aperio SVS file.

  Args:
    dicom_path: File path to SVS file.

  Returns:
    Mapping[DICOM keyword, pydicom.DataElement] of DICOM formatted metadata.
  """
  try:
    o_slide = openslide.OpenSlide(dicom_path)
  except openslide.lowlevel.OpenSlideUnsupportedFormatError:
    return {}
  scan_datetime = _aperio_scan_time(o_slide.properties)
  if not scan_datetime:
    return {}
  metadata = pydicom.Dataset()
  set_acquisition_date_time(metadata, scan_datetime)
  return {e.keyword: e for e in metadata}


def add_missing_type2_dicom_metadata(ds: pydicom.Dataset) -> None:
  """Sets missing type 2 tags to None.

  Args:
    ds: Pydicom.Dataset, must be initalized with SOPClassUID.

  Returns:
    None
  """
  init_type2c_tags = (
      ingest_flags.CREATE_NULL_TYPE2C_DICOM_TAG_IF_METADATA_IS_UNDEFINED_FLG.value
  )
  tags_added = []
  require_modules = (
      ['Slide Label']
      if ds.SOPClassUID == ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
      else []
  )
  try:
    undefined_tags = pydicom_util.get_undefined_dicom_tags_by_type(
        ds,
        {'2', '2C'} if init_type2c_tags else {'2'},
        require_modules=require_modules,
    )
  except pydicom_util.UndefinedIODError:
    return
  for tag_path in undefined_tags:
    try:
      data_element = pydicom.DataElement(
          tag_path.tag.address, list(tag_path.tag.vr)[0], None
      )
    except IndexError as _:
      cloud_logging_client.warning(
          'DICOM standard metadata error tag is missing vr code.',
          {'tag': tag_path.tag},
      )
      continue
    tag_path.get_dicom_dataset(ds).add(data_element)
    tags_added.append(str(tag_path))
  if tags_added:
    if init_type2c_tags:
      msg = 'Added undefined type 2 and 2c tags.'
      log_key = ingest_const.LogKeywords.TYPE2_AND_2C_TAGS_ADDED
    else:
      msg = 'Added undefined type 2 tags.'
      log_key = ingest_const.LogKeywords.TYPE2_TAGS_ADDED
    cloud_logging_client.info(
        msg,
        {log_key: tags_added},
        wsi_dicom_file_ref.init_from_pydicom_dataset('', ds).dict(),
    )


def _add_dicom_pyramid_module_to_dataset_metadata(
    dcm: pydicom.Dataset,
    dicom_store_refs: List[wsi_dicom_file_ref.WSIDicomFileRef],
    ingest_dicom_files: List[wsi_dicom_file_ref.WSIDicomFileRef],
) -> None:
  """Adds DICOM pyramid module metadata to pydicom dataset."""
  pyramid_uid = ''
  pyramid_label = ''
  pyramid_description = ''
  for dicom_file_ref in itertools.chain(ingest_dicom_files, dicom_store_refs):
    if not dicom_file_ref.pyramid_uid:
      continue
    pyramid_uid = dicom_file_ref.pyramid_uid
    pyramid_label = dicom_file_ref.pyramid_label
    pyramid_description = dicom_file_ref.pyramid_description
    break
  if not pyramid_uid:
    pyramid_uid = uid_generator.generate_uid()
  # dcm.PyramidUID = pyramid_uid
  dcm.add(
      pydicom.DataElement(
          ingest_const.DICOMTagAddress.PYRAMID_UID, 'UI', pyramid_uid
      )
  )
  if pyramid_label:
    # dcm.PyramidLabel = pyramid_label
    dcm.add(
        pydicom.DataElement(
            ingest_const.DICOMTagAddress.PYRAMID_LABEL, 'LO', pyramid_label
        )
    )
  if pyramid_description:
    # dcm.PyramidDescription = pyramid_description
    dcm.add(
        pydicom.DataElement(
            ingest_const.DICOMTagAddress.PYRAMID_DESCRIPTION,
            'LO',
            pyramid_description,
        )
    )


def _add_dicom_frame_of_ref_module_to_dataset_metadata(
    dcm: pydicom.Dataset,
    dicom_store_refs: List[wsi_dicom_file_ref.WSIDicomFileRef],
    ingest_dicom_files: List[wsi_dicom_file_ref.WSIDicomFileRef],
) -> None:
  """Adds DICOM frame of reference metadata to pydicom dataset."""
  frame_of_reference_uid = ''
  position_reference_indicator = ''
  for dicom_file_ref in itertools.chain(ingest_dicom_files, dicom_store_refs):
    if not dicom_file_ref.frame_of_reference_uid:
      continue
    frame_of_reference_uid = dicom_file_ref.frame_of_reference_uid
    position_reference_indicator = dicom_file_ref.position_reference_indicator
    cloud_logging_client.info(
        'DICOM FrameOfReferenceUID set from previous instance.',
        dicom_file_ref.dict(),
    )
    cloud_logging_client.info(
        'DICOM FrameOfReference',
        {
            'FrameOfReferenceUID': frame_of_reference_uid,
            'PositionReferenceIndicator': position_reference_indicator,
        },
    )
    break
  if not frame_of_reference_uid:
    frame_of_reference_uid = uid_generator.generate_uid()
    cloud_logging_client.info(
        'DICOM FrameOfReferenceUID initialized to new UID.',
        {
            'FrameOfReferenceUID': frame_of_reference_uid,
            'PositionReferenceIndicator': position_reference_indicator,
        },
    )
  dcm.FrameOfReferenceUID = frame_of_reference_uid
  if position_reference_indicator:
    dcm.PositionReferenceIndicator = position_reference_indicator


def get_additional_wsi_specific_dicom_metadata(
    dcm_store_client: dicom_store_client.DicomStoreClient,
    dicom_json: Mapping[str, Any],
    ingest_dicom_files: Optional[
        List[wsi_dicom_file_ref.WSIDicomFileRef]
    ] = None,
) -> pydicom.Dataset:
  """Returns additional DICOM tags to addd to WSI DICOM.

  Args:
    dcm_store_client: DICOM store client.
    dicom_json: Dicom JSON that encodes imaging study uid & series instance uid.
    ingest_dicom_files: List of references to instances of DICOMs ingested.

  Returns:
    Pydicom dataset containing the additional tag metadata.
  """
  if ingest_dicom_files is None:
    ingest_dicom_files = []
  refs_to_dicom_instances_in_store = dcm_store_client.get_study_dicom_file_ref(
      dicom_json_util.get_study_instance_uid(dicom_json),
      dicom_json_util.get_series_instance_uid(dicom_json),
  )
  additional_wsi_specific_metadata = pydicom.Dataset()
  _add_dicom_frame_of_ref_module_to_dataset_metadata(
      additional_wsi_specific_metadata,
      refs_to_dicom_instances_in_store,
      ingest_dicom_files,
  )
  _add_dicom_pyramid_module_to_dataset_metadata(
      additional_wsi_specific_metadata,
      refs_to_dicom_instances_in_store,
      ingest_dicom_files,
  )
  return additional_wsi_specific_metadata


def pad_bytes_to_even_length(data: bytes) -> bytes:
  """NULL Pad byte string to even length.

    Dicom standard requires OB VR types to be padded to even length see:
    https://dicom.nema.org/medical/dicom/current/output/chtml/part05/sect_6.2.html

  Args:
    data: Byte string.

  Returns:
    even length byte string
  """
  if len(data) % 2:
    return data + b'\x00'
  return data


def _get_colorspace_description_from_iccprofile_bytes(
    ds: pydicom.dataset.Dataset,
) -> str:
  """Returns embedded description of ICC Profile colorspace.

  Args:
    ds: pydicom dataset.

  Raises:
    InvalidICCProfileError: Raised if ICC profile cannot be decoded.
  """
  try:
    profile = ImageCms.ImageCmsProfile(io.BytesIO(ds.ICCProfile))
    return ImageCms.getProfileDescription(profile)
  except (OSError, ImageCms.PyCMSError) as exp:
    raise InvalidICCProfileError(
        'Could not decode embedded ICC color profile',
        ingest_const.ErrorMsgs.INVALID_ICC_PROFILE,
    ) from exp


def add_icc_colorspace_to_dicom(ds: pydicom.dataset.Dataset) -> None:
  """Adds ICC profile color space to DICOM.

  Args:
    ds: Pydicom dataset to add icc profile to.

  Raises:
    InvalidICCProfileError: Raised if ICC profile cannot be decoded.
  """
  if ingest_const.DICOMTagKeywords.COLOR_SPACE in ds:
    del ds.ColorSpace
  if ingest_const.DICOMTagKeywords.ICC_PROFILE not in ds or not ds.ICCProfile:
    return
  description = _get_colorspace_description_from_iccprofile_bytes(ds)
  # Color space is stored in DICOM VR type CS.
  # Value <= 16 characters. A bit tricky in unicode space. Approximating
  # as 16 characters assuming value isn't using multi-byte characters
  # removed spaces and new line per CS text definitions.
  # https://dicom.nema.org/medical/dicom/current/output/chtml/part05/sect_6.2.html
  if description:
    ds.ColorSpace = description.replace('\n', '').strip().upper()[:16]
    cloud_logging_client.info(
        'Adding ICC profile color space description DICOM',
        {
            'ICC_Profile_ColorSpace': description,
        },
    )


def _add_icc_profile_to_dicom(
    ds: pydicom.dataset.Dataset, icc_profile: Optional[bytes]
) -> None:
  """Add ICC profile and icc profile colorspace to DICOM images.

  Args:
    ds: Pydicom dataset to add icc profile to.
    icc_profile: Image ICC profile Bytes.

  Raises:
    InvalidICCProfileError: Raised if ICC profile cannot be decoded.
  """
  if icc_profile is None or not icc_profile:
    if ingest_const.DICOMTagKeywords.ICC_PROFILE in ds:
      del ds.ICCProfile
    if ingest_const.DICOMTagKeywords.COLOR_SPACE in ds:
      del ds.ColorSpace
    return

  # ICC Profile is saved in tag with OB VR Type.
  # Dicom standard requires OB VR types to be padded to even length see:
  # https://dicom.nema.org/medical/dicom/current/output/chtml/part05/sect_6.2.html
  ds.ICCProfile = pad_bytes_to_even_length(icc_profile)
  add_icc_colorspace_to_dicom(ds)
  cloud_logging_client.info('Adding ICC profile to DICOM')


def set_frametype_to_imagetype(dcm_file: pydicom.Dataset) -> None:
  """Set DICOM FrameType tag to same value as ImageType tag.

  Args:
    dcm_file: Pydicom dataset.
  """
  if 'ImageType' not in dcm_file:
    return
  if 'SharedFunctionalGroupsSequence' not in dcm_file:
    dcm_file.SharedFunctionalGroupsSequence = [pydicom.Dataset()]
  if (
      'WholeSlideMicroscopyImageFrameTypeSequence'
      not in dcm_file.SharedFunctionalGroupsSequence[0]
  ):
    dcm_file.SharedFunctionalGroupsSequence[
        0
    ].WholeSlideMicroscopyImageFrameTypeSequence = [pydicom.Dataset()]
  frame_type_sq = dcm_file.SharedFunctionalGroupsSequence[
      0
  ].WholeSlideMicroscopyImageFrameTypeSequence[0]
  frame_type_sq.FrameType = copy.copy(dcm_file.ImageType)


def set_acquisition_date_time(
    dcm_file: pydicom.Dataset, acquisition_datetime: datetime.datetime
) -> None:
  """Sets acquisition date & time tags in DICOM.

  Args:
    dcm_file: Pydicom file to set acquisition date time tags in.
    acquisition_datetime: Acquisition datetime to use.
  """
  ac_date = _dicom_formatted_date(acquisition_datetime)
  ac_time = _dicom_formatted_time(acquisition_datetime)
  dcm_file.AcquisitionDate = ac_date
  dcm_file.AcquisitionTime = ac_time
  dcm_file.AcquisitionDateTime = f'{ac_date}{ac_time}'


def set_content_date_time_to_now(dcm_file: pydicom.Dataset) -> None:
  """Sets content date & time tags in DICOM.

  Args:
    dcm_file: Pydicom file to set content date time tags in.
  """
  # Initialize content date time
  content_datetime = datetime.datetime.now(datetime.timezone.utc)
  dcm_file.ContentTime = _dicom_formatted_time(content_datetime)
  dcm_file.ContentDate = _dicom_formatted_date(content_datetime)


def set_sop_instance_uid(ds: pydicom.Dataset, sop_instance_uid: str) -> None:
  """Sets SOP Instance UID.

  Args:
    ds: Py_dicom_dataset.
    sop_instance_uid: SOP instance UID to use.
  """
  ds.file_meta.MediaStorageSOPInstanceUID = sop_instance_uid
  ds.SOPInstanceUID = sop_instance_uid


def set_wsi_dimensional_org(dcm_file: pydicom.Dataset) -> None:
  """Sets WSI DICOM dimensional organization metadata.

  Args:
    dcm_file: Py_dicom_dataset.
  """
  dim_org_uid = uid_generator.generate_uid()
  # Dimension Organization Sequence
  ds = pydicom.Dataset()
  ds.DimensionOrganizationUID = dim_org_uid
  dcm_file.DimensionOrganizationSequence = pydicom.Sequence([ds])

  # DimensionIndexSequence
  ds_list = []
  for _ in range(2):
    ds = pydicom.Dataset()
    ds.DimensionOrganizationUID = dim_org_uid
    ds.DimensionIndexPointer = 0x0048021E
    ds.FunctionalGroupPointer = 0x0048021A
    ds_list.append(ds)
  dcm_file.DimensionIndexSequence = pydicom.Sequence(ds_list)


def _init_slice_thickness_metadata_if_undefined(
    dcm_file: pydicom.Dataset,
) -> None:
  """Inits slice thickness metadata if undefined (Required for WSI DICOM).

  Sets ImagedVolumeDepth (thickness in um) and
    SharedFunctionalGroupsSequence[0].PixelMeasuresSequence[0].SliceThickness
    (unit = mm)
  If they are undefined. If one is defined will init missing value from defined
  tag value.  If neither are defined inits from value defined in
  WSI_DICOM_SLICE_THICKNESS_DEFAULT_VALUE_FLG (default 12 um)

  Args:
    dcm_file: Pydicom dataset.

  Returns:
    None:
  """
  if ingest_const.DICOMTagKeywords.IMAGED_VOLUME_DEPTH in dcm_file:
    # Imaged volume depth units = micrometers (1/1000) mm
    thickness = dcm_file.ImagedVolumeDepth
  else:
    # Slice thickeness units = mm or 1000 * um
    try:
      thickness = (
          float(
              dcm_file.SharedFunctionalGroupsSequence[0]
              .PixelMeasuresSequence[0]
              .SliceThickness
          )
          * _UM_IN_MM
      )
    except (AttributeError, TypeError, IndexError, KeyError, ValueError) as _:
      thickness = ingest_flags.WSI_DICOM_SLICE_THICKNESS_DEFAULT_VALUE_FLG.value
      cloud_logging_client.info(
          'Slice thickness metadata is undefined setting to default:'
          f' {thickness} um.'
      )
    dcm_file.ImagedVolumeDepth = thickness
  try:
    first_fuc_group_ds = dcm_file.SharedFunctionalGroupsSequence[0]
  except (AttributeError, IndexError) as _:
    first_fuc_group_ds = pydicom.Dataset()
    dcm_file.SharedFunctionalGroupsSequence = [first_fuc_group_ds]
  try:
    measure_ds = first_fuc_group_ds.PixelMeasuresSequence[0]
  except (AttributeError, IndexError) as _:
    measure_ds = pydicom.Dataset()
    first_fuc_group_ds.PixelMeasuresSequence = [measure_ds]
  if ingest_const.DICOMTagKeywords.SLICE_THICKNESS not in measure_ds:
    # Convert thickness (um) to Slice Thickness mm
    measure_ds.SliceThickness = thickness / _UM_IN_MM


def _pad_date(date_str: str) -> str:
  date_str = date_str[:8]
  pad = '0' * (8 - len(date_str))
  return f'{date_str}{pad}'


def _pad_time(time_str: str) -> str:
  pad = '0' * (6 - len(time_str)) if len(time_str) < 6 else ''
  return f'{time_str}{pad}'


def _init_acquision_date_time_if_undefined(dcm_file: pydicom.Dataset) -> None:
  """Inits acquision date time if undefined (Required for WSI DICOM).

  If tag is undefined attempts to build tag value from date/time components.
    date = AcquisitionDate, ContentDate, and then current date.
    time = AcquisitionTime, ContentTime, and then current time.

    if date metadata does not fully describe metadata in 8 chars (YYYYMMDD)
    then uses current date.

  Args:
    dcm_file: Pydicom dataset.

  Returns:
    None:
  """
  if ingest_const.DICOMTagKeywords.ACQUISITION_DATE_TIME in dcm_file:
    return
  datetime_now = datetime.datetime.now(datetime.timezone.utc)

  acquision_date = ''
  acquision_time = '000000'
  try:
    if len(dcm_file.AcquisitionDate) >= 4:
      acquision_date = dcm_file.AcquisitionDate
      acquision_time = dcm_file.AcquisitionTime
  except AttributeError:
    pass
  if not acquision_date:
    try:
      if len(dcm_file.ContentDate) >= 4:
        acquision_date = dcm_file.ContentDate
        acquision_time = dcm_file.ContentTime
    except AttributeError:
      pass
  if not acquision_date:
    acquision_date = _dicom_formatted_date(datetime_now)
    acquision_time = _dicom_formatted_time(datetime_now)
    cloud_logging_client.info(
        'Metadata missing required acquision datetime metadata setting datetime'
        ' current date/time.'
    )
  if ingest_const.DICOMTagKeywords.ACQUISITION_DATE not in dcm_file:
    dcm_file.AcquisitionDate = acquision_date
  if ingest_const.DICOMTagKeywords.ACQUISITION_TIME not in dcm_file:
    dcm_file.AcquisitionTime = acquision_time
  acquision_date = _pad_date(acquision_date)
  acquision_time = _pad_time(acquision_time)
  dcm_file.AcquisitionDateTime = f'{acquision_date}{acquision_time}'


def init_undefined_wsi_imaging_type1_tags(dcm_file: pydicom.Dataset) -> None:
  """Tests WSI imaging for missing required metadata and adds it if necessary.

  Args:
    dcm_file: Pydicom dataset.

  Returns:
    None:
  """
  if ingest_const.DICOMTagKeywords.EXTENDED_DEPTH_OF_FIELD not in dcm_file:
    dcm_file.ExtendedDepthOfField = (
        ingest_flags.WSI_DICOM_EXTENDED_DEPTH_OF_FIELD_DEFAULT_VALUE_FLG.value.name
    )
  if ingest_const.DICOMTagKeywords.FOCUS_METHOD not in dcm_file:
    dcm_file.FocusMethod = (
        ingest_flags.WSI_DICOM_FOCUS_METHOD_DEFAULT_VALUE_FLG.value.name
    )
  _init_slice_thickness_metadata_if_undefined(dcm_file)
  try:
    is_tiled_full = (
        dcm_file.DimensionOrganizationType.upper().strip()
        == ingest_const.TILED_FULL
    )
  except AttributeError:
    is_tiled_full = False
  if (
      is_tiled_full
      and ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_FOCAL_PLANES
      not in dcm_file
  ):
    dcm_file.TotalPixelMatrixFocalPlanes = 1
  if ingest_const.DICOMTagKeywords.VOLUMETRIC_PROPERTIES not in dcm_file:
    dcm_file.VolumetricProperties = ingest_const.VOLUME
  _init_acquision_date_time_if_undefined(dcm_file)


def add_general_metadata_to_dicom(dcm_file: pydicom.Dataset) -> None:
  """Adds general metadata (e.g date, equipment) to DICOM instance.

  Args:
    dcm_file: DICOM instance to add metadata to.
  """
  set_content_date_time_to_now(dcm_file)
  dicom_general_equipment.add_ingest_general_equipment(dcm_file)


def add_metadata_to_generated_wsi_dicom(
    additional_wsi_metadata: pydicom.Dataset,
    dcm_json: Optional[Mapping[str, Any]],
    private_tags: List[dicom_private_tag_generator.DicomPrivateTag],
    dcm_file: pydicom.Dataset,
) -> None:
  """Adds metadata to resampled WSI DICOM instance.

  Args:
    additional_wsi_metadata: Additional metadata to merge with gen DICOM.
    dcm_json: DICOM formated json metadata to add to DICOM instances.
    private_tags: List of private tags to add to DICOM.
    dcm_file: DICOM instance to add metadata to.
  """
  dicom_private_tag_generator.DicomPrivateTagGenerator.add_dicom_private_tags(
      private_tags, dcm_file
  )
  dicom_json_util.merge_json_metadata_with_pydicom_ds(dcm_file, dcm_json)
  set_frametype_to_imagetype(dcm_file)
  dcm_file.file_meta.ImplementationClassUID = (
      ingest_const.WSI_IMPLEMENTATION_CLASS_UID
  )
  dcm_file.file_meta.ImplementationVersionName = (
      ingest_const.IMPLEMENTATION_VERSION_NAME
  )
  set_sop_instance_uid(dcm_file, uid_generator.generate_uid())
  set_wsi_dimensional_org(dcm_file)
  copy_pydicom_dataset(additional_wsi_metadata, dcm_file)
  add_general_metadata_to_dicom(dcm_file)
  init_undefined_wsi_imaging_type1_tags(dcm_file)


# pytype: disable=bad-return-type
def get_pydicom_tags(
    pydicom_dataset: Union[str, pydicom.FileDataset],
    *tag_keywords: Union[str, Sequence[str]],
) -> MutableMapping[str, Optional[pydicom.DataElement]]:
  """Copies Optical Path module tags from highest mag to to resampled mags.

  Args:
    pydicom_dataset: File path to DICOM instance or pydicom file dataset.
    *tag_keywords: DICOM tag key words to return value for. Typing is confused
      by python dict comprehension incorrectly thinking return type should be
      Dict[str, Optional[Any]]

  Returns:
    Dict of tag keyword to tag value
  """
  tags = itertools.chain(
      *[[tag] if isinstance(tag, str) else tag for tag in tag_keywords]
  )
  if isinstance(pydicom_dataset, str):
    with pydicom.dcmread(pydicom_dataset, defer_size='512 KB') as dcm:
      return {key: dcm[key] if key in dcm else None for key in tags}
  # pydicom FileDataset
  dcm = pydicom_dataset
  return {key: dcm[key] if key in dcm else None for key in tags}


# pytype: enable=bad-return-type


def has_optical_path_sequence(dcm: pydicom.Dataset) -> bool:
  return (
      ingest_const.DICOMTagKeywords.OPTICAL_PATH_SEQUENCE in dcm
      and dcm.OpticalPathSequence
  )


def add_icc_colorspace_if_not_defined(dcm: pydicom.FileDataset):
  """Corrects ICC Color Space if not defined and ICC Profile is defined.

  Args:
    dcm: Pydicom.dataset to correct.

  Raises:
    InvalidICCProfileError: Raised if ICC profile cannot be decoded.
  """
  if _should_fix_icc_colorspace(dcm):
    add_icc_colorspace_to_dicom(dcm)
  if ingest_const.DICOMTagKeywords.OPTICAL_PATH_SEQUENCE in dcm:
    # if optical path sequence is defined then make sure color space is also
    # defined.
    for optical_path in dcm.OpticalPathSequence:
      if not _should_fix_icc_colorspace(optical_path):
        continue
      add_icc_colorspace_to_dicom(optical_path)


def _single_code_val_sq(
    code_value: str, designator: str, meaning: str
) -> pydicom.Sequence:
  """Return DICOM sequence containing single coded value."""
  ds = pydicom.Dataset()
  ds.CodeValue = code_value
  ds.CodingSchemeDesignator = designator
  ds.CodeMeaning = meaning
  return pydicom.Sequence([ds])


def add_default_optical_path_sequence(
    dcm: pydicom.Dataset, icc_profile: Optional[bytes]
) -> bool:
  """Adds default optical sequence.

  Args:
    dcm: PyDICOM Dataset to add optical path to.
    icc_profile: Image ICC profile Bytes.

  Returns:
    True if default optical path sequence added.

  Raises:
    InvalidICCProfileError: Raised if ICC profile cannot be decoded.
  """
  if has_optical_path_sequence(dcm):
    return False
  try:
    photometric_interpretation = dcm.PhotometricInterpretation
  except AttributeError:
    photometric_interpretation = ''
  if (
      icc_profile is None
      and photometric_interpretation != ingest_const.MONOCHROME2
  ):
    icc_profile = get_default_icc_profile_color()
  ds = pydicom.Dataset()
  ds.IlluminationTypeCodeSequence = _single_code_val_sq(
      '111741', 'DCM', 'Transmission illumination'
  )
  ds.OpticalPathIdentifier = '1'
  ds.OpticalPathDescription = 'Transmitted Light'
  _add_icc_profile_to_dicom(ds, icc_profile)
  ds.IlluminationColorCodeSequence = _single_code_val_sq(
      '11744', 'DCM', 'Brightfield illumination'
  )
  dcm.OpticalPathSequence = pydicom.Sequence([ds])
  dcm.NumberOfOpticalPaths = 1
  return True


def add_default_total_pixel_matrix_origin_sequence_if_not_defined(
    dcm: pydicom.Dataset,
) -> bool:
  if 'TotalPixelMatrixOriginSequence' in dcm:
    return False
  offset = pydicom.Dataset()
  offset.XOffsetInSlideCoordinateSystem = 0
  offset.YOffsetInSlideCoordinateSystem = 0
  dcm.TotalPixelMatrixOriginSequence = [offset]
  return True


def set_defined_pydicom_tags(
    dcm: pydicom.Dataset,
    tag_value_map: Mapping[str, Optional[pydicom.DataElement]],
    tag_keywords: Sequence[str],
    overwrite_existing_values: bool,
) -> None:
  """Sets select tags on pydicom dataset if value != None.

  Args:
    dcm: Pydicom dataset.
    tag_value_map: Pydicom tag keyword value map.
    tag_keywords: Tag keywords to copy to dataset.
    overwrite_existing_values: If False do not overwrite tags with pre-existing
      values.

  Returns:
    None
  """
  for tag_keyword in tag_keywords:
    tag_value = tag_value_map.get(tag_keyword)
    if tag_value is not None and (
        overwrite_existing_values or dcm.get(tag_keyword) is None
    ):
      dcm[tag_keyword] = tag_value


def copy_pydicom_dataset(
    source: pydicom.Dataset, dest: pydicom.Dataset
) -> None:
  """Copies all tags in source pydicom dataset to dest dataset."""
  for key, value in source.items():
    dest[key] = copy.deepcopy(value)


def set_all_defined_pydicom_tags(
    dcm: pydicom.Dataset,
    tag_value_map: Mapping[str, Optional[pydicom.DataElement]],
) -> None:
  """Sets all tags in map on pydicom dataset for value != None.

  Args:
    dcm: Pydicom dataset.
    tag_value_map: Pydicom tag keyword value map.

  Returns:
    None
  """
  set_defined_pydicom_tags(
      dcm, tag_value_map, tag_value_map, overwrite_existing_values=True
  )  # pytype: disable=wrong-arg-types  # mapping-is-not-sequence


def is_unencapsulated_dicom_transfer_syntax(
    unencapsulated_transfer_syntax_uid: str,
) -> bool:
  """Returns True if DICOM transfer syntax is un-encapsulated."""
  return unencapsulated_transfer_syntax_uid in (
      ingest_const.DicomImageTransferSyntax.IMPLICIT_VR_LITTLE_ENDIAN,
      ingest_const.DicomImageTransferSyntax.EXPLICIT_VR_LITTLE_ENDIAN,
      ingest_const.DicomImageTransferSyntax.DEFLATED_EXPLICIT_VR_LITTLE_ENDIAN,
      ingest_const.DicomImageTransferSyntax.EXPLICIT_VR_BIG_ENDIAN,
  )


def _has_offset_table(dcm: pydicom.Dataset) -> bool:
  """Returns True if DICOM instance has basic offset table."""
  if 'ExtendedOffsetTable' in dcm:
    return True
  file_like = pydicom.filebase.DicomFileLike(io.BytesIO(dcm.PixelData))
  file_like.is_little_endian = True
  has_offset_table, _ = pydicom.encaps.get_frame_offsets(file_like)
  return has_offset_table


def if_missing_create_encapsulated_frame_offset_table(
    dcm_file: pydicom.FileDataset,
) -> None:
  """Adds basic offset or extended offset table to DICOM if missing."""
  if 'PixelData' not in dcm_file:
    return
  if is_unencapsulated_dicom_transfer_syntax(
      dcm_file.file_meta.TransferSyntaxUID
  ):
    return
  if _has_offset_table(dcm_file):
    return
  # Offset table is needd, generate the offset table.
  frames = [
      fd[0]
      for fd in pydicom.encaps.generate_pixel_data(
          dcm_file.PixelData,
          dcm_file.NumberOfFrames,
      )
  ]
  # If the total size of the frames is less than or equal to 4 gigabytes,
  # then use a basic offset table. Otherwise, use an extended offset table.
  if len(dcm_file.PixelData) <= _MAX_PIXEL_DATA_SIZE_FOR_BASIC_OFFSET_TABLE:
    dcm_file.PixelData = pydicom.encaps.encapsulate(frames)
    table_type = 'basic'
  else:
    _, extended_offset_table, extend_offset_table_lengths = (
        pydicom.encaps.encapsulate_extended(frames)
    )
    dcm_file.ExtendedOffsetTable = extended_offset_table
    dcm_file.ExtendedOffsetTableLengths = extend_offset_table_lengths
    table_type = 'extended'
  del frames
  cloud_logging_client.info(
      f'Generated {table_type} offset table.',
      {
          'bytes': len(dcm_file.PixelData),
          'number_of_frames': dcm_file.NumberOfFrames,
      },
  )
