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
import copy
import dataclasses
import datetime
import io
import itertools
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Union

from PIL import ImageCms
import pydicom

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_general_equipment
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_json_util
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_private_tag_generator
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen import wsi_dicom_file_ref

# The following try/except block allows for two separate executions of the code:
# A docker container deployed in GKE and within unit testing framework.
# pylint: disable=g-import-not-at-top
try:
  import openslide  # pytype: disable=import-error  # Deployed GKE container
except ImportError:
  import openslide_python as openslide  # Google3


class InvalidICCProfileError(Exception):
  pass


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
    properties: Mapping[str, str]
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


@dataclasses.dataclass(frozen=True)
class DicomFrameOfReferenceModuleMetadata:
  uid: str
  position_reference_indicator: str


def create_dicom_frame_of_ref_module_metadata(
    study_instance_uid: str,
    series_instance_uid: str,
    dicom_store: dicom_store_client.DicomStoreClient,
    dicom_file_ref: Optional[wsi_dicom_file_ref.WSIDicomFileRef] = None,
    preexisting_dicom_refs: Optional[
        List[wsi_dicom_file_ref.WSIDicomFileRef]
    ] = None,
) -> DicomFrameOfReferenceModuleMetadata:
  """Creates/gets DICOM frame of reference uid for series instance.

  All instances in series share frame of reference. Possible DICOM are in store
  defining frame of reference (result of incomplete ingestion). If necessary
  this method will test for uid in these DICOM's.

  Args:
    study_instance_uid: DICOM StudyInstanceUID for instance.
    series_instance_uid: DICOM SeriesInstanceUID for instance.
    dicom_store: Instance to DicomStoreClient.
    dicom_file_ref: DICOM file reference to existing series metadata. may not be
      in DICOM store.
    preexisting_dicom_refs: List of all dicom refs in DICOM store study, series.
      Pass to avoid DICOM store metadata query.

  Returns:
    DicomFrameOfReferenceModuleMetadata
  """
  if dicom_file_ref is None:
    position_reference_indicator = ''
  else:
    position_reference_indicator = dicom_file_ref.position_reference_indicator

  # if metadata is known from prior ref use it.
  if dicom_file_ref is not None and dicom_file_ref.frame_of_reference_uid:
    frame_of_reference_uid = dicom_file_ref.frame_of_reference_uid
    cloud_logging_client.logger().info(
        'DICOM FrameOfReferenceUID set loaded instance.', dicom_file_ref.dict()
    )
    cloud_logging_client.logger().info(
        'DICOM FrameOfReference',
        {
            'FrameOfReferenceUID': frame_of_reference_uid,
            'PositionReferenceIndicator': position_reference_indicator,
        },
    )
    return DicomFrameOfReferenceModuleMetadata(
        frame_of_reference_uid, position_reference_indicator
    )
  # query store for pre-existing metadata to see if store already has
  # metadata for series
  if preexisting_dicom_refs is None or not preexisting_dicom_refs:
    preexisting_dicom_refs = dicom_store.get_study_dicom_file_ref(
        study_instance_uid, series_instance_uid
    )
  else:
    cloud_logging_client.logger().info(
        'Searching for FrameOfReferenceUID using prefetched DicomRefs.',
        {'dicom_ref_list': str(preexisting_dicom_refs)},
    )
  for dicom_file_ref in preexisting_dicom_refs:
    if not dicom_file_ref.frame_of_reference_uid:
      continue
    frame_of_reference_uid = dicom_file_ref.frame_of_reference_uid
    position_reference_indicator = dicom_file_ref.position_reference_indicator
    cloud_logging_client.logger().info(
        'DICOM FrameOfReferenceUID set from previous instance.',
        dicom_file_ref.dict(),
    )
    cloud_logging_client.logger().info(
        'DICOM FrameOfReference',
        {
            'FrameOfReferenceUID': frame_of_reference_uid,
            'PositionReferenceIndicator': position_reference_indicator,
        },
    )
    return DicomFrameOfReferenceModuleMetadata(
        frame_of_reference_uid, position_reference_indicator
    )
  # no metadata found allocate new uid
  frame_of_reference_uid = uid_generator.generate_uid()
  cloud_logging_client.logger().info(
      'DICOM FrameOfReferenceUID initialized to new UID.',
      {
          'FrameOfReferenceUID': frame_of_reference_uid,
          'PositionReferenceIndicator': position_reference_indicator,
      },
  )
  return DicomFrameOfReferenceModuleMetadata(
      frame_of_reference_uid, position_reference_indicator
  )


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
  try:
    profile = ImageCms.ImageCmsProfile(io.BytesIO(ds.ICCProfile))
    description = ImageCms.getProfileDescription(profile)
  except (OSError, ImageCms.PyCMSError) as exp:
    raise InvalidICCProfileError(
        'Could not decode embedded ICC color profile',
        ingest_const.ErrorMsgs.INVALID_ICC_PROFILE,
    ) from exp
  # Color space is stored in DICOM VR type CS.
  # Value <= 16 characters. A bit tricky in unicode space. Approximating
  # as 16 characters assuming value isn't using multi-byte characters
  # removed spaces and new line per CS text definitions.
  # https://dicom.nema.org/medical/dicom/current/output/chtml/part05/sect_6.2.html
  if description:
    ds.ColorSpace = description.replace('\n', '').strip().upper()[:16]
    cloud_logging_client.logger().info(
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
  cloud_logging_client.logger().info('Adding ICC profile to DICOM')


def set_frametype_to_imagetype(dcm_file: pydicom.Dataset):
  """Set DICOM FrameType tag to same value as ImageType tag.

  Args:
    dcm_file: Pydicom dataset.
  """
  frame_type_sq = pydicom.Dataset()
  frame_type_sq.FrameType = copy.copy(dcm_file.ImageType)
  dcm_file.WholeSlideMicroscopyImageFrameTypeSequence = pydicom.Sequence(
      [frame_type_sq]
  )


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


def set_sop_instance_uid(
    ds: pydicom.Dataset, sop_instance_uid: Optional[str] = None
):
  """Sets SOP Instance UID.

  Args:
    ds: Py_dicom_dataset.
    sop_instance_uid: SOP instance UID to use. Will be generated if unset.
  """
  if not sop_instance_uid:
    sop_instance_uid = uid_generator.generate_uid()
  ds.file_meta.MediaStorageSOPInstanceUID = sop_instance_uid
  ds.SOPInstanceUID = sop_instance_uid


def set_wsi_frame_of_ref_metadata(
    dcm_file: pydicom.Dataset, frame_of_ref: DicomFrameOfReferenceModuleMetadata
):
  """Sets frame of reference module metadata.

  Args:
    dcm_file: Py_dicom_dataset.
    frame_of_ref: DICOM frame of reference module metadata.
  """
  dcm_file.FrameOfReferenceUID = frame_of_ref.uid
  dcm_file.PositionReferenceIndicator = (
      frame_of_ref.position_reference_indicator
  )


def set_wsi_dimensional_org(dcm_file: pydicom.Dataset):
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


def add_general_metadata_to_dicom(dcm_file: pydicom.Dataset) -> None:
  """Adds general metadata (e.g date, equipment) to DICOM instance.

  Args:
    dcm_file: DICOM instance to add metadata to.
  """
  set_content_date_time_to_now(dcm_file)
  dicom_general_equipment.add_ingest_general_equipment(dcm_file)


def add_metadata_to_dicom(
    frame_of_ref_md: DicomFrameOfReferenceModuleMetadata,
    dcm_json: Optional[Dict[str, Any]],
    private_tags: List[dicom_private_tag_generator.DicomPrivateTag],
    dcm_file: pydicom.Dataset,
) -> None:
  """Adds metadata to resampled WSI DICOM instance.

  Args:
    frame_of_ref_md: DICOM frame of ref module metadata.
    dcm_json: JSON metadata to add to DICOM.
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
  set_sop_instance_uid(dcm_file)
  set_wsi_dimensional_org(dcm_file)
  set_wsi_frame_of_ref_metadata(dcm_file, frame_of_ref_md)
  add_general_metadata_to_dicom(dcm_file)


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
    dcm: pydicom.FileDataset, icc_profile: Optional[bytes]
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
  return True


def set_defined_pydicom_tags(
    dcm: pydicom.Dataset,
    tag_value_map: Mapping[str, Optional[pydicom.DataElement]],
    tag_keywords: Sequence[str],
) -> None:
  """Sets select tags on pydicom dataset if value != None.

  Args:
    dcm: Pydicom dataset.
    tag_value_map: Pydicom tag keyword value map.
    tag_keywords: Tag keywords to copy to dataset.

  Returns:
    None
  """
  for tag_keyword in tag_keywords:
    tag_value = tag_value_map.get(tag_keyword)
    if tag_value is not None:
      dcm[tag_keyword] = tag_value


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
  set_defined_pydicom_tags(dcm, tag_value_map, tag_value_map)  # pytype: disable=wrong-arg-types  # mapping-is-not-sequence
