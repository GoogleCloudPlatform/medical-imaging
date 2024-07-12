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
"""HCLS DICOM Store Client to make DICOM web requests."""

import copy
import dataclasses
import enum
import http
import json
import math
import os
import tempfile
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Set, Union

import google.auth
import pydicom
import requests

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import cloud_storage_client
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_json_util
from transformation_pipeline.ingestion_lib.dicom_gen import wsi_dicom_file_ref


def _get_quota_str(exp: requests.HTTPError) -> str:
  return (
      '; insufficient DICOM Store quota'
      if exp.response.status_code == http.HTTPStatus.TOO_MANY_REQUESTS
      else ''
  )


def _http_streaming_get(url: str, headers: Dict[str, Any], write_path: str):
  """Save streaming http get to file.

  Args:
    url: url to issue http get against.
    headers: to pass URL
    write_path: path to write file to.

  Raises:
    requests.HTTPError: something bad happened downloading.
  """
  with requests.Session() as session:
    try:
      with session.get(url, headers=headers, stream=True) as get_response:
        with open(write_path, 'wb') as dicom_file:
          for chunk in get_response.iter_content(chunk_size=50 * 1024):
            dicom_file.write(chunk)
            get_response.raise_for_status()
    except requests.HTTPError as exp:
      opt_quota_str = _get_quota_str(exp)
      cloud_logging_client.error(
          f'Error downloading DICOM instance from store{opt_quota_str}',
          {'dicom_store_web': url},
          exp,
      )
      raise


class DicomUploadToGcsError(Exception):

  def __init__(self):
    super().__init__(ingest_const.ErrorMsgs.ERROR_UPLOADING_DICOM_TO_GCS)


class StudyInstanceUIDSearchError(Exception):

  def __init__(self):
    super().__init__(
        ingest_const.ErrorMsgs.ERROR_DECODING_DICOM_STORE_STUDY_INSTANCE_UID_SEARCH_RESPONSE
    )


@dataclasses.dataclass(frozen=True)
class DicomStoreFileSaveResult:
  dicom_fileref: wsi_dicom_file_ref.WSIDicomFileRef
  is_duplicate: bool


class DiscoverExistingSeriesOptions(enum.Enum):
  """Options to discover existing instances when uploading to DICOM store."""

  # Do not look for existing series.
  IGNORE = 1
  # SVS ingestion allocates series UID hash embedded in generated imaging,
  # which can be used for re-discovery of ingested series UID.
  USE_HASH = 2
  # Lookup instances in DICOM store using existing study and series UIDs.
  USE_STUDY_AND_SERIES = 3


@dataclasses.dataclass(frozen=True)
class UploadSlideToDicomStoreResults:
  """Results of imaging upload to the DICOM store."""

  ingested: List[wsi_dicom_file_ref.WSIDicomFileRef]
  previously_ingested: List[wsi_dicom_file_ref.WSIDicomFileRef]

  def slide_has_instances_in_dicom_store(self) -> bool:
    """Returns True if series associated with upload has imaging in the store."""
    return bool(self.ingested) or bool(self.previously_ingested)

  @property
  def slide_instances_in_dicom_store(
      self,
  ) -> List[wsi_dicom_file_ref.WSIDicomFileRef]:
    """Returns list of slide instances in store following upload."""
    instance_list = copy.copy(self.ingested)
    instance_list.extend(self.previously_ingested)
    return instance_list

  @property
  def slide_series_instance_uid_in_dicom_store(self) -> Optional[str]:
    """Returns slides series instance uid as defined in store.

    The SeriesInstanceUID of a slide may be changed during upload if upload
    to DICOM store detects that a slides imaging was previously ingested into
    a different series. The actual series instance uid of the slide in the
    DICOM store is returned here.
    """
    if self.ingested:
      return self.ingested[0].series_instance_uid
    if self.previously_ingested:
      return self.previously_ingested[0].series_instance_uid
    return None


def _is_resampled_image_type(image_type: str) -> bool:
  return image_type.endswith(ingest_const.RESAMPLED)


def _is_original_or_derived(image_type: str) -> bool:
  """Tests if image defines either original or derived image type."""
  return image_type.startswith(
      ingest_const.ORIGINAL_PRIMARY_VOLUME
  ) or image_type.startswith(ingest_const.DERIVED_PRIMARY_VOLUME)


def _is_wsi_ancillary_image_type(
    img1: wsi_dicom_file_ref.WSIDicomFileRef,
    img2: wsi_dicom_file_ref.WSIDicomFileRef,
) -> bool:
  """Returns True if both refs describe ancillary images for WSI DICOM."""
  image_1_type = img1.image_type.upper()
  image_2_type = img2.image_type.upper()
  if img1.sop_class_uid != ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid:
    return False
  if img2.sop_class_uid != ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid:
    return False
  for ancillary_type in (
      ingest_const.THUMBNAIL,
      ingest_const.OVERVIEW,
      ingest_const.LABEL,
  ):
    if ancillary_type in image_1_type and ancillary_type in image_2_type:
      return True
  return False


def _do_image_types_match(
    img1: wsi_dicom_file_ref.WSIDicomFileRef,
    img2: wsi_dicom_file_ref.WSIDicomFileRef,
) -> bool:
  """Tests if two DICOM image types describe the same image type.

  Both images expected to have same sop_class_uid.

  Args:
    img1: Image type tag value for first DICOM instance.
    img2: Image type tag value for second DICOM instance.

  Returns:
    True if image types match.
  """
  image_1_type = img1.image_type.upper()
  image_2_type = img2.image_type.upper()
  if image_1_type == image_2_type:
    return True  # perfect match, simple.
  if img1.sop_class_uid == ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid:
    # Tests specifically for DICOM VL_WHOLE_SLIDE_MICROSCOPY IOD images.
    # These image could be made by transform pipleine or others.

    # Test if both images describes the same ancillary image.
    # if they do then the images are considered to have same image type.
    if _is_wsi_ancillary_image_type(img1, img2):
      return True

    # Tests that pyramid image types have the same resampled indicator.
    if _is_resampled_image_type(image_1_type) != _is_resampled_image_type(
        image_2_type
    ):
      return False

    # If the images are not ancillary then they are expected to be:
    # ORIGINAL\\PRIMARY\\VOLUME or DERIVED\\PRIMARY\\VOLUME
    #
    # ORIGINAL and DERIVED are used in the DICOM community inconsistently.
    # Here we are treating ORIGINAL and DERIVED as being the same. Doing this
    # enables the pipeline to de-duplicate between customer provided and
    # generated pyramid levels. However this also means that the pipeline
    # may have issues if multiple representation of single pyramid are provided
    # within a DICOM ingestion payload or if the pipeline  was used to ingest
    # imaging into a series that had imaging in it prior to pipeline execution.
    # The validation step that is performed at the start of DICOM WSI
    # transformation protects against multiple pyramids being provided by the
    # customer by limiting the number of non-resampled volumes to one.
    return _is_original_or_derived(image_1_type) and _is_original_or_derived(
        image_2_type
    )

  # Generic image type equality test
  image_1_type = image_1_type.split('\\')
  image_2_type = image_2_type.split('\\')
  img1_len = len(image_1_type)
  img2_len = len(image_2_type)
  for index in range(max(img1_len, img2_len)):
    im1_type = image_1_type[index] if index < img1_len else 'NONE'
    im2_type = image_2_type[index] if index < img2_len else 'NONE'
    if im1_type != im2_type:
      return False
  return True


def _similar_dimension(
    dcm_file_physical_dim: str,
    existing_file_physical_dim: str,
) -> bool:
  """Tests if physical dimension metadata is the "same" for two DICOM instances.

    See: _do_images_have_similar_dimensions for full comment.

  Args:
    dcm_file_physical_dim: Physical dimension in mm along one axis.
    existing_file_physical_dim: Physical dimension in mm of the corresponding
      axis in another DICOM.

  Returns:
    True if the metadata for the physical dimensions of the two images are
    identical or nearly identical.
  """
  if dcm_file_physical_dim == existing_file_physical_dim:
    return True
  try:
    dcm_file_physical_dim = float(dcm_file_physical_dim)
    existing_file_physical_dim = float(existing_file_physical_dim)
    return math.floor(dcm_file_physical_dim * 10.0) == math.floor(
        existing_file_physical_dim * 10.0
    )
  except ValueError as _:
    return False


def _do_images_have_similar_dimensions(
    dcm_file: wsi_dicom_file_ref.WSIDicomFileRef,
    existing_dicom: wsi_dicom_file_ref.WSIDicomFileRef,
) -> bool:
  """Test if the physical dimensions in two DICOM instances are similar.

  There is variability in how the physical dimensions are reported in
  downsampled DICOM instances. Some implementations report actual physical
  dimensions reported by the scanner (Most Corrrect), using this approach all
  downsampled images from the same pyramid have the same dimensions. This is the
  approach used internally by the DPAS transformation pipeline.

  Other (less correct) approaches infer the physical dimensions of downsampled
  imaging by deriving the downsampled image dimensions from the image
  downsampling factor and the pixel spacing. This approach results in the
  physical dimensions of the imaged area being reported differently across
  the pyramid imaging despite all levels of the pyramid being derived from a
  common optical acquisition.

  The comparision conducted here tests first if the two dimensions are exactly
  the same. This allows both numerical and missing value string equality.
  If this is not the case then the pipeline tests if the image dimensions are
  within 1/10th of a mm of each.

  Args:
    dcm_file: DICOM instance being tested.
    existing_dicom: DICOM from a broader list dcm_file is being tested against.

  Returns:
    True if DICOMs have similar similar physical dimensions (Width and Height).
  """
  # The use of physical image dimensions ancillary imaging, thumbnail, overview
  # and label imaging is not well defined. Ignoring physical dimension test fo
  # these image types.
  if _is_wsi_ancillary_image_type(dcm_file, existing_dicom):
    return True
  return _similar_dimension(
      dcm_file.imaged_volume_height,
      existing_dicom.imaged_volume_height,
  ) and _similar_dimension(
      dcm_file.imaged_volume_width,
      existing_dicom.imaged_volume_width,
  )


def _is_dpas_generated_dicom(
    dcm_file: wsi_dicom_file_ref.WSIDicomFileRef,
) -> bool:
  """Returns true if dcm_file was generated by DPAS.

    DPAS generates DICOM instances using a Google Specific UID prefix. DPAS
    also adds a private hash to generated DICOM which identifies the source
    byte payload used to generate the imaging. Very early versions of DPAS 1.0
    did not use the DPAS UID prefix. As a result of this it is necessary to
    check for both UID prefix or HASH to determine if the DICOM was generated By
    DPAS.

  Args:
    dcm_file: DICOM file ref to test if generated by DPAS.

  Returns:
    True if DICOM generated by DPAS.
  """
  return bool(dcm_file.hash) or dcm_file.sop_instance_uid.startswith(
      ingest_const.DPAS_UID_PREFIX
  )


def is_dpas_dicom_wsidcmref_in_list(
    dcm_file: wsi_dicom_file_ref.WSIDicomFileRef,
    ref_list: List[wsi_dicom_file_ref.WSIDicomFileRef],
    ignore_source_image_hash_tag: bool = False,
    ignore_tags: Optional[List[str]] = None,
) -> bool:
  """Test if WSIDicomFileRef is in list using equality test for DPAS gen DICOM.

    SOPInstanceUID cannot be directly compared due to 1) the need to
    de-duplicate internally generated and externally generated DICOM and 2) the
    inability to deterministically generate these UID for a given input.

    DICOM are considered the same if the DICOM:
     * Image study instance uid are equal and
     * Image series instance uid are equal and
     * Image SOP class uid are equal and
     * Image modality are equal and
     * Image dimensions are equal and
     * Image type are are equal and
     * Image concatenation are equal and
     * Image dimensions are equal and
     * Image type descriptions match.
     * Optional image hash match, generated from same source image.

  Args:
    dcm_file: WSIDicomFileRef to test if included in reference list.
    ref_list: List of existing WSIDicomFileRefs.
    ignore_source_image_hash_tag: Enable internally generated images be
      determined being the same without testing hash.
    ignore_tags: Optional list of tags keywords to remove from test.

  Returns:
    True if dcm_file in ref_list
  """
  if not ref_list:
    return False
  test_tags = [
      ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID,
      ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID,
      ingest_const.DICOMTagKeywords.SOP_CLASS_UID,
      ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_COLUMNS,
      ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_ROWS,
      ingest_const.DICOMTagKeywords.MODALITY,
      ingest_const.DICOMTagKeywords.COLUMNS,
      ingest_const.DICOMTagKeywords.ROWS,
      ingest_const.DICOMTagKeywords.CONCATENATION_UID,
      ingest_const.DICOMTagKeywords.CONCATENATION_FRAME_OFFSET_NUMBER,
      ingest_const.DICOMTagKeywords.IN_CONCATENATION_NUMBER,
  ]
  if ignore_tags is not None:
    for keyword in ignore_tags:
      test_tags.remove(keyword)
  for existing_dicom in ref_list:
    if not dcm_file.equals(
        existing_dicom, ignore_tags=None, test_tags=test_tags
    ):
      continue
    if not _do_images_have_similar_dimensions(dcm_file, existing_dicom):
      continue
    if not _do_image_types_match(dcm_file, existing_dicom):
      continue
    log_msg = 'Removed duplicate DICOM instance.'
    struct_log = {
        ingest_const.LogKeywords.TESTED_DICOM_INSTANCE: str(dcm_file.dict()),
        ingest_const.LogKeywords.EXISTING_DICOM_INSTANCE: str(
            existing_dicom.dict()
        ),
    }
    if (
        ignore_source_image_hash_tag
        or not dcm_file.hash
        or not existing_dicom.hash
        or not _is_dpas_generated_dicom(existing_dicom)
    ):
      cloud_logging_client.info(log_msg, struct_log)
      return True
    elif dcm_file.hash == existing_dicom.hash:
      # If testing against internally generated DICOM and hash test not disabled
      # require that hash values match indicating the images were generated from
      # the same source.
      cloud_logging_client.info(log_msg, struct_log)
      return True
  return False


def is_wsidcmref_in_list(
    dcm_file: wsi_dicom_file_ref.WSIDicomFileRef,
    ref_list: List[wsi_dicom_file_ref.WSIDicomFileRef],
    ignore_source_image_hash_tag: bool = False,
) -> bool:
  """Test if DICOM WSIDicomFileRef is in list of WSIDicomFileRef.

  The purpose of this function is to test if a DICOM instance ref is already
  represented in a list of existing DICOMs. This code is used to
  de-duplicate DICOM instance uploads to a DICOM store StudyUID SeriesUID.

  The tests checks if the dcm_file referenced DICOM is present in the
  the list of DICOMS using two different approaches: 1) a test for DICOM
  instances generated by others, and 2) a test for DPAS generated DICOM. DICOM
  generated by others are considered identical if the DICOM study instance uid,
  series instance uid, and sop instance uid match. The DPAS test is performed
  in is_dpas_dicom_wsidcmref_in_list and is used to test DPAS generated DICOM
  with DPAS generated and Non-DPAS generated DICOM
  (see function is_dpas_dicom_wsidcmref_in_list).

  Args:
    dcm_file: WSIDicomFileRef to test if included in reference list.
    ref_list: List of existing WSIDicomFileRefs.
    ignore_source_image_hash_tag: Enable internally generated images be
      determined being the same without testing hash.

  Returns:
    True if dcm_file in ref_list
  """
  if _is_dpas_generated_dicom(dcm_file):
    return is_dpas_dicom_wsidcmref_in_list(
        dcm_file,
        ref_list,
        ignore_source_image_hash_tag=ignore_source_image_hash_tag,
    )

  test_tags = [
      ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID,
      ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID,
      ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID,
  ]
  dpas_generated_dicom_ref_list = []
  for existing_dicom in ref_list:
    if _is_dpas_generated_dicom(existing_dicom):
      # if DICOM is a DPAS DICOM test using DPAS DICOM methods.
      dpas_generated_dicom_ref_list.append(existing_dicom)
      continue
    if dcm_file.equals(existing_dicom, ignore_tags=None, test_tags=test_tags):
      cloud_logging_client.info(
          'Removed duplicate DICOM instance.',
          {
              ingest_const.LogKeywords.TESTED_DICOM_INSTANCE: str(
                  dcm_file.dict()
              ),
              ingest_const.LogKeywords.EXISTING_DICOM_INSTANCE: str(
                  existing_dicom.dict()
              ),
          },
      )
      return True
  return is_dpas_dicom_wsidcmref_in_list(
      dcm_file,
      dpas_generated_dicom_ref_list,
      ignore_source_image_hash_tag=ignore_source_image_hash_tag,
      ignore_tags=None,
  )


def _get_existing_dicom_seriesuid(
    existing_dicoms: List[wsi_dicom_file_ref.WSIDicomFileRef],
) -> Optional[str]:
  """Returns series descrbied in list of WSIDicomFileRef.

  Args:
    existing_dicoms:  List[WSIDicomFileRef] to images in DICOM store. Note it is
      expected all file refs will describe the same series.

  Returns:
    series uid as string.
  """
  if not existing_dicoms:
    return None
  return existing_dicoms[0].series_instance_uid


def set_dicom_series_uid(
    dicom_paths: Union[List[str], Set[str]], series_uid: Optional[str]
) -> bool:
  """Changes series uid in DICOM file to specified uid, if defined.

  Args:
    dicom_paths: List of paths to DICOM files.
    series_uid: DICOM series instance uid to set DICOM file to.

  Returns:
    True if a DICOM instance is changed.
  """
  if series_uid is None or not series_uid:
    return False
  changed = False
  for dcm_file in dicom_paths:
    # first read DICOM instance quickly common case is no change needed.
    with pydicom.dcmread(dcm_file, defer_size='512 KB') as dcm:
      oldseries_uid = dcm.SeriesInstanceUID
    if oldseries_uid == series_uid:
      continue
    with pydicom.dcmread(dcm_file) as dcm:
      cloud_logging_client.warning(
          (
              'Merging with existing series; Changing SeriesInstanceUID old:'
              f'{oldseries_uid} new:{series_uid}'
          ),
          {
              ingest_const.LogKeywords.SOP_INSTANCE_UID: dcm.SOPInstanceUID,
              ingest_const.LogKeywords.STUDY_INSTANCE_UID: dcm.StudyInstanceUID,
              ingest_const.LogKeywords.OLD_SERIES_INSTANCE_UID: oldseries_uid,
              ingest_const.LogKeywords.NEW_SERIES_INSTANCE_UID: series_uid,
          },
      )
      dcm.SeriesInstanceUID = series_uid
      dcm.save_as(dcm_file, write_like_original=False)
      changed = True
  return changed


@dataclasses.dataclass(frozen=True)
class _SeriesUidAndDicomExistingInStore:
  series_instance_uid: Optional[str]
  preexisting_dicoms_in_store: List[wsi_dicom_file_ref.WSIDicomFileRef]


class DicomStoreClient:
  """Wrapper for HCLS Dicom Store Client to make dicom web requests."""

  def __init__(self, dicomweb_path: str):
    self._auth_credentials = None
    self._dicomweb_path = dicomweb_path

  @property
  def dicomweb_path(self) -> str:
    return self._dicomweb_path

  def _add_auth_to_header(self, msg_headers: Dict[str, str]) -> Dict[str, str]:
    """Updates credentials and adds to DICOM header.

    Args:
      msg_headers: Header to pass to DICOM Store

    Returns:
      Header with authentication added.
    """
    if not self._auth_credentials:
      self._auth_credentials = google.auth.default(
          scopes=['https://www.googleapis.com/auth/cloud-platform']
      )[0]
    if not self._auth_credentials.valid:
      auth_req = google.auth.transport.requests.Request()
      self._auth_credentials.refresh(auth_req)
    self._auth_credentials.apply(msg_headers)
    return msg_headers

  def study_instance_uid_search(
      self,
      accession_number: str = '',
      patient_id: str = '',
      find_studies_with_undefined_patient_id: bool = False,
      limit: Optional[int] = None,
  ) -> List[str]:
    """Find StudyInstanceUID associated with accession # and/or patient id.

    Args:
      accession_number: DICOM accession number; 16 characters max (DICOM
        standard).
      patient_id: DICOM patient id; 64 characters max (DICOM standard); Search
        parameter ignored if ''.
      find_studies_with_undefined_patient_id: If True returns studies uids that
        do not have patient uid values associated with them.
      limit: Limit number of returned results if undefined returns all results
        found.

    Returns:
      List of unique study instance uids found.

    Raises:
      StudyInstanceUIDSearchError: If error occures decoding response.
    """
    search_params = []
    if accession_number:
      search_params.append(f'AccessionNumber={accession_number}')
    if patient_id:
      search_params.append(f'PatientID={patient_id}')
    if not search_params:
      return []
    if limit is not None:
      if limit <= 0:
        raise ValueError('Defined limit must be greater than 0.')
      if patient_id or not find_studies_with_undefined_patient_id:
        search_params.append(f'limit={limit}')
    search_params = '&'.join(search_params)
    try:
      search_path = f'{self.dicomweb_path}/studies?{search_params}'
      headers = {'Content-Type': 'application/dicom+json; charset=utf-8'}
      headers = self._add_auth_to_header(headers)
      response = requests.get(search_path, headers=headers)
      response.raise_for_status()
      # if the search finds no matching cases text is none.
      if not response.text:
        return []
      results = set()
      for file_json in response.json():
        found_study_instance_uid = dicom_json_util.get_study_instance_uid(
            file_json
        )
        if patient_id or not find_studies_with_undefined_patient_id:
          results.add(found_study_instance_uid)
        elif dicom_json_util.missing_patient_id(file_json):
          results.add(found_study_instance_uid)
          if len(results) == limit:
            break
    except (
        dicom_json_util.MissingStudyUIDInMetadataError,
        json.JSONDecodeError,
    ) as exp:
      cloud_logging_client.error(
          'Exception decoding DICOM Store study search search response.',
          exp,
      )
      raise StudyInstanceUIDSearchError() from exp
    except requests.HTTPError as exp:
      opt_quota_str = _get_quota_str(exp)
      cloud_logging_client.error(
          'Exception decoding DICOM Store study search search response'
          f'{opt_quota_str}.',
          exp,
      )
      raise
    return list(results)

  def get_instance_tags_json(
      self,
      study_instance_uid: str,
      series_instance_uid: Optional[str] = None,
      sop_instance_uid: Optional[str] = None,
      tag_keyword_list: Optional[Union[List[str], FrozenSet[str]]] = None,
  ) -> List[Dict[str, Any]]:
    """Retrieve instance level DICOM JSON metadata.

    Args:
      study_instance_uid: DICOM Study Instance UID.
      series_instance_uid: DICOM Series Instance UID.
      sop_instance_uid: DICOM SOP Instance UID.
      tag_keyword_list: List of tag addresses or keywords to include.

    Returns:
      List of metadata retrieved.

    Raises:
      JSONDecodeError: Error occurred decoding JSON response.
      HTTPError: Error occurred receiving HTTP response.
    """
    log = {ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: study_instance_uid}
    search_path = [f'{self.dicomweb_path}/studies/{study_instance_uid}']
    if series_instance_uid is not None and series_instance_uid:
      search_path.append(f'series/{series_instance_uid}')
      log[ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID] = (
          series_instance_uid
      )
    search_path.append('instances?')
    search_path = '/'.join(search_path)
    search_params = []
    if sop_instance_uid is not None and sop_instance_uid:
      search_params.append(f'SOPInstanceUID={sop_instance_uid}')
      log[ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID] = sop_instance_uid
    if tag_keyword_list is not None and tag_keyword_list:
      includefields = ','.join(tag_keyword_list)
      search_params.append(f'includefield={includefields}')
    search_params = '&'.join(search_params)
    search_path = f'{search_path}{search_params}'
    headers = {'Content-Type': 'application/dicom+json; charset=utf-8'}
    headers = self._add_auth_to_header(headers)
    try:
      response = requests.get(search_path, headers=headers)
      response.raise_for_status()
      # if the search finds no matching cases text is none.
      if not response.text:
        return []
      # Expected to return a List[Dict[str, Any]]
      return response.json()
    except json.JSONDecodeError as exp:
      cloud_logging_client.error(
          'Exception decoding DICOM instance metadata.',
          log,
          {ingest_const.LogKeywords.DICOMWEB_PATH: search_path},
          exp,
      )
      raise
    except requests.HTTPError as exp:
      opt_quota_str = _get_quota_str(exp)
      cloud_logging_client.error(
          f'Exception searching for DICOM instance metadata{opt_quota_str}.',
          log,
          {ingest_const.LogKeywords.DICOMWEB_PATH: search_path},
          exp,
      )
      raise

  def get_study_dicom_file_ref(
      self, study_uid: str, series_uid: Optional[str] = None
  ) -> List[wsi_dicom_file_ref.WSIDicomFileRef]:
    """Returns list of file_refs that describe the a study_uid's DICOMs.

    Args:
      study_uid: DICOM study instance uid to search.
      series_uid: Optional Series instance uid to search within study.

    Returns:
      List of DICOM_file_ref that describe select set of tags for DICOMs.

    Raises:
      JSONDecodeError: Error occurred decoding JSON response.
      HTTPError: Error occurred receiving HTTP response.
    """
    response_json = self.get_instance_tags_json(
        study_uid,
        series_uid if series_uid is not None else '',
        '',
        wsi_dicom_file_ref.get_wsi_dicom_file_ref_tag_address_list(),
    )
    return [
        wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_json(file_json)
        for file_json in response_json
    ]

  def _copy_dicom_to_bucket(
      self,
      ingested_dicom_files: List[wsi_dicom_file_ref.WSIDicomFileRef],
      prev_ingest_dicom_files: List[wsi_dicom_file_ref.WSIDicomFileRef],
      bucketuri: str,
      dst_metadata: Optional[Mapping[str, str]],
  ):
    """Uploads DICOM files to GCS Bucket.

       Uploads files in organization:
       Bucket:/BucketPath/StudyInstanceUID/SeriesInstanceUID/SOPInstanceUID.dcm

    Args:
      ingested_dicom_files: List of WSIDicomFileRef DICOM files added now.
      prev_ingest_dicom_files: List of WSIDicomFileRef DICOM files prev added.
      bucketuri: URI of bucket to upload files to.
      dst_metadata: Metadata to tags to add to DICOMs written to gcs.

    Raises:
      DicomUploadToGcsError: if upload to GCS fails
    """

    if not bucketuri:
      return
    if not dst_metadata:
      dst_metadata = {}

    with tempfile.TemporaryDirectory() as previous_dicom_dir:
      if not prev_ingest_dicom_files:
        # Only local files were upload to DICOM store.
        upload_path_list = [dcmref.source for dcmref in ingested_dicom_files]
      else:
        # Not all local files were upload to DICOM store. Local versions !=
        # what is in store. Download store versions when != to local version
        # then backup everything to GCS. If store version is already in GCS
        # this will be detected in the file copy to GCS.
        first_dicom_ref = prev_ingest_dicom_files[0]
        all_dicom_files_for_study_series = self.get_study_dicom_file_ref(
            first_dicom_ref.study_instance_uid,
            first_dicom_ref.series_instance_uid,
        )
        upload_path_list = []
        for count, dcm_ref in enumerate(all_dicom_files_for_study_series):
          dicom_path = None
          # check if server instance is already stored locally
          for local_file_ref in ingested_dicom_files:
            if (
                local_file_ref.sop_instance_uid == dcm_ref.sop_instance_uid
                and local_file_ref.study_instance_uid
                == dcm_ref.study_instance_uid
                and local_file_ref.series_instance_uid
                == dcm_ref.series_instance_uid
            ):
              dicom_path = local_file_ref.source
              cloud_logging_client.info(
                  (
                      'Skipping DICOM download from store. Generated DICOM in '
                      'container is same in Store.'
                  ),
                  {
                      'generated_dicom': str(dcm_ref.dict()),
                      'dicom_in_dicom_store': str(local_file_ref),
                  },
              )
              break
          # Instance not stored locally. Download
          if dicom_path is None:
            dicom_path = os.path.join(previous_dicom_dir, f'dicom_{count}.dcm')
            try:
              self.download_instance(
                  dcm_ref.study_instance_uid,
                  dcm_ref.series_instance_uid,
                  dcm_ref.sop_instance_uid,
                  dicom_path,
              )
            except requests.HTTPError as exp:
              cloud_logging_client.error(
                  (
                      'Error occurred downloading DICOM instance to container. '
                      'Instance will not be copied to GCS.'
                  ),
                  dcm_ref.dict(),
                  exp,
              )
              continue
          upload_path_list.append(dicom_path)

      for dicom_file in upload_path_list:
        dcm_ref = wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_file(
            dicom_file
        )
        filename = f'{dcm_ref.sop_instance_uid}.dcm'
        dest_uri = os.path.join(
            bucketuri, dcm_ref.study_instance_uid, dcm_ref.series_instance_uid
        )

        additional_log = {'dicom_destination_bucket_uri': dest_uri}
        additional_log.update(dcm_ref.dict())
        if not cloud_storage_client.upload_blob_to_uri(
            dicom_file,
            dest_uri,
            filename,
            additional_log=additional_log,
            dst_metadata=dst_metadata,
        ):
          cloud_logging_client.error(
              'Error occurred uploading DICOM to GCS bucket.',
              {'dicom_destination_bucket_uri': dest_uri},
              dcm_ref.dict(),
          )
          raise DicomUploadToGcsError()

  def get_existing_dicoms_matching_study_hash(
      self, study_uid: str, hash_val: str
  ) -> List[wsi_dicom_file_ref.WSIDicomFileRef]:
    """Returns list of dicomfilerefs to images in a study which match the hash.

    SHA512 is a hash of the ingeststed source bytes (e.g., svs) used to
    generate a DICOM. This search identifies images in a study which were
    generated from a source image.

    Args:
      study_uid: study uid to query
      hash_val: SHA512 hash to search for.

    Returns:
      List[WSIDicomFileRef] to images with same SHA512 hash
    """
    if not study_uid:
      return list()
    existing_dicom_file_refs = self.get_study_dicom_file_ref(study_uid)
    list_of_prexisting_dicoms = list()

    if existing_dicom_file_refs:
      file_ref_str_lst = []
      series_uid_logged = set()
      for file_ref in existing_dicom_file_refs:
        if file_ref.series_instance_uid in series_uid_logged:
          continue
        series_uid_logged.add(file_ref.series_instance_uid)

        msg_str_lst = [
            f'SeriesInstanceUID: {file_ref.series_instance_uid}',
            f'  AccessionNumber: {file_ref.accession_number}',
            f'  Hash: {file_ref.hash}',
        ]
        file_ref_str_lst.append('\n'.join(msg_str_lst))

      cloud_logging_client.info(
          'Study has existing files',
          {
              ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: study_uid,
              'existing_dicom_file_refs': '\n'.join(file_ref_str_lst),
          },
      )

    for fref in existing_dicom_file_refs:
      if hash_val == fref.hash:
        list_of_prexisting_dicoms.append(fref)
    if list_of_prexisting_dicoms:
      series_uid = list_of_prexisting_dicoms[0].series_instance_uid
      cloud_logging_client.info(
          'DICOM study has a prexisting series with images matching the hash',
          {
              ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: study_uid,
              ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: series_uid,
              ingest_const.LogKeywords.HASH: hash_val,
          },
      )
    else:
      cloud_logging_client.info(
          'DICOM study has no prexisting images matching hash',
          {
              ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: study_uid,
              ingest_const.LogKeywords.HASH: hash_val,
          },
      )
    return list_of_prexisting_dicoms

  def get_series_uid_and_existing_dicom_for_study_and_hash(
      self, studyuid: str, hash_val: str
  ) -> _SeriesUidAndDicomExistingInStore:
    existing_dicoms = self.get_existing_dicoms_matching_study_hash(
        studyuid, hash_val
    )
    return _SeriesUidAndDicomExistingInStore(
        _get_existing_dicom_seriesuid(existing_dicoms), existing_dicoms
    )

  def download_instance_from_uri(self, uri: str, dicom_file_path: str):
    """Downloads DICOM instance from store to container.

    Args:
      uri: DICOM instance resource URI.
      dicom_file_path: File to write to.

    Raises:
      requests.HTTPError: Something bad happened downloading.
    """
    headers = self._add_auth_to_header(
        {'Accept': 'application/dicom; transfer-syntax=*'}
    )
    _http_streaming_get(uri, headers, dicom_file_path)
    dcm_file = wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_file(
        dicom_file_path
    )
    cloud_logging_client.info(
        'Downloaded DICOM instance from store',
        {'dicom_file_path': dicom_file_path, 'dicom_store_web': uri},
        dcm_file.dict(),
    )

  def download_instance(
      self,
      study_uid: str,
      series_uid: str,
      sop_instance_uid: str,
      dicom_file_path: str,
  ):
    """Downloads DICOM instance from store to container.

    Args:
      study_uid: DICOM StudyInstanceUID.
      series_uid: DICOM SeriesInstanceUID.
      sop_instance_uid: DICOM SOPInstanceUID.
      dicom_file_path: File to write to.

    Raises:
      requests.HTTPError: Something bad happened downloading.
    """
    instance_uri = (
        f'{self.dicomweb_path}/studies/{study_uid}/'
        f'series/{series_uid}/instances/{sop_instance_uid}'
    )
    self.download_instance_from_uri(instance_uri, dicom_file_path)

  def _discover_existing_seriesuid_and_update_dcm(
      self, dicom_paths: List[str]
  ) -> List[wsi_dicom_file_ref.WSIDicomFileRef]:
    """Discover if series previously uploaded.

       If yes update SeriesUID in generated DICOM to match prior files.
       and return list of prior files.

    Args:
      dicom_paths: List of file paths to dicom files to upload assumes all files
        were generated from same source.

    Returns:
      List of WSIDicomFileRef for any series instances already in store.
    """
    # get list of existing scans in Study
    dcm_file = wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_file(
        dicom_paths[0]
    )
    if not dcm_file.hash:
      cloud_logging_client.error('DICOM HASH not initialized.', dcm_file.dict())

    # if a slide was already partially added we should add the same
    # to the existing series. Otherwise create a new series.
    # hash identifies imaging source
    existing_dicoms = self.get_existing_dicoms_matching_study_hash(
        dcm_file.study_instance_uid, dcm_file.hash
    )
    series_uid = _get_existing_dicom_seriesuid(existing_dicoms)
    set_dicom_series_uid(dicom_paths, series_uid)
    return existing_dicoms

  def _get_existing_instances_for_study_series(
      self, dicom_paths: List[str]
  ) -> List[wsi_dicom_file_ref.WSIDicomFileRef]:
    """Returns list of instances already ingested for study, series.

    Args:
      dicom_paths: List of file paths to dicom files to upload assumes all files
        were generated from same source.

    Returns:
      List of WSIDicomFileRef for any series instances already in store.
    """
    dcm_file = wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_file(
        dicom_paths[0]
    )
    return self.get_study_dicom_file_ref(
        dcm_file.study_instance_uid, dcm_file.series_instance_uid
    )

  def _upload_single_instance_to_dicom_store(
      self,
      fname: str,
      existing_dicoms: Optional[
          List[wsi_dicom_file_ref.WSIDicomFileRef]
      ] = None,
  ) -> DicomStoreFileSaveResult:
    """Upload single file to DICOM store.

    Args:
      fname: Filename of DICOM to upload.
      existing_dicoms: Optional list of existing DICOMs which match study UID.

    Returns:
      DicomStoreFileSaveResult containing DICOMFileRef of uploaded DICOM and
      flag indicating if duplicate.

    Raises:
       requests.HTTPError: DICOM upload failed.
    """
    push_dicom_image = os.path.join(self.dicomweb_path, 'studies')
    dcm_file = wsi_dicom_file_ref.init_wsi_dicom_file_ref_from_file(fname)
    if existing_dicoms and is_wsidcmref_in_list(dcm_file, existing_dicoms):
      cloud_logging_client.warning(
          'Images exists in DICOM Store, skipping ingest.',
          {
              ingest_const.LogKeywords.FILENAME: fname,
              'dicom_store_web': self.dicomweb_path,
          },
          dcm_file.dict(),
      )
      return DicomStoreFileSaveResult(dcm_file, True)
    headers = self._add_auth_to_header({'Content-Type': 'application/dicom'})
    try:
      with open(fname, 'rb') as dcm:
        response = requests.post(push_dicom_image, data=dcm, headers=headers)
        response.raise_for_status()
    except requests.HTTPError as exp:
      if exp.response.status_code == http.HTTPStatus.CONFLICT:
        cloud_logging_client.warning(
            (
                'Cannot upload DICOM. UID triple (StudyInstanceUID, '
                'SeriesInstanceUID, SOPInstanceUID) already exists in store.'
            ),
            dcm_file.dict(),
            exp,
        )
        return DicomStoreFileSaveResult(dcm_file, True)
      opt_quota_str = _get_quota_str(exp)
      cloud_logging_client.error(
          f'Error uploading DICOM to DICOM store{opt_quota_str}',
          {
              ingest_const.LogKeywords.FILENAME: fname,
              'dicom_store_web': self.dicomweb_path,
          },
          exp,
          dcm_file.dict(),
      )
      raise
    cloud_logging_client.info(
        'Uploaded DICOM to DICOM store',
        {'dicom_file': fname, 'dicom_store_web': self.dicomweb_path},
        dcm_file.dict(),
    )
    return DicomStoreFileSaveResult(dcm_file, False)

  def upload_to_dicom_store(
      self,
      dicom_paths: List[str],
      discover_existing_series_option: DiscoverExistingSeriesOptions,
      copy_to_bucket_metadata: Optional[Mapping[str, str]] = None,
      copy_to_bucket_enabled: bool = True,
  ) -> UploadSlideToDicomStoreResults:
    """Uploads a list of DICOM files to the DICOM store.

    Args:
      dicom_paths: List of file paths to DICOMS.
      discover_existing_series_option: Whether and how to discover existing
        series and their respective instances.
      copy_to_bucket_metadata: Optional metadata to attach to for files copied
        to destination.
      copy_to_bucket_enabled: Whether to copy to GCS.

    Returns:
      UploadSlideToDicomStoreResults( <List if files (WSIDicomFileRef) stored
                                 in DICOM Store for ingestion>,
                                 <List if files (WSIDicomFileRef) previously
                                 ingested in DICOM Store> )

    Raises:
       requests.HTTPError: DICOM upload failed.
       DicomUploadToGcsError: GCS upload failed.
    """
    if not dicom_paths:
      return UploadSlideToDicomStoreResults([], [])
    if (
        discover_existing_series_option
        == DiscoverExistingSeriesOptions.USE_HASH
    ):
      existing_dicoms = self._discover_existing_seriesuid_and_update_dcm(
          dicom_paths
      )
    elif (
        discover_existing_series_option
        == DiscoverExistingSeriesOptions.USE_STUDY_AND_SERIES
    ):
      existing_dicoms = self._get_existing_instances_for_study_series(
          dicom_paths
      )
    else:  # discover_existing_series_option == IGNORE
      existing_dicoms = []

    # Iterate over files to upload.
    processed_dicom_files = [
        self._upload_single_instance_to_dicom_store(
            dicom_file_path, existing_dicoms
        )
        for dicom_file_path in dicom_paths
    ]

    # List of files ingested as a result of operation.
    # Some of the images may already be in store if cmd is being re-run
    # as a result of failure / recovery.
    ingested_dicom_files = []
    prev_ingest_dicom_files = []
    for dicom_file in processed_dicom_files:
      if dicom_file.is_duplicate:
        prev_ingest_dicom_files.append(dicom_file.dicom_fileref)
      else:
        ingested_dicom_files.append(dicom_file.dicom_fileref)

    if copy_to_bucket_enabled:
      self._copy_dicom_to_bucket(
          ingested_dicom_files,
          prev_ingest_dicom_files,
          ingest_flags.COPY_DICOM_TO_BUCKET_URI_FLG.value.strip(),
          copy_to_bucket_metadata,
      )

    return UploadSlideToDicomStoreResults(
        ingested_dicom_files, prev_ingest_dicom_files
    )

  def delete_resource_from_dicom_store(
      self,
      uri: str,
      success_msg: Optional[str] = None,
      failed_msg: Optional[str] = None,
      log_struct: Optional[Mapping[str, str]] = None,
  ) -> bool:
    """Deletes DICOM resource from store.

    Args:
      uri: DICOM store resource URI.
      success_msg: Optional message to log after successful delete.
      failed_msg: Optional message to log if delete fails.
      log_struct: Optional structure to log with message.

    Returns:
      True if deleted.
    """
    try:
      headers = self._add_auth_to_header(
          {'Content-Type': 'application/dicom+json; charset=utf-8'}
      )
      response = requests.delete(uri, headers=headers)
      response.raise_for_status()
      if success_msg is None:
        success_msg = f'Deleted resource {uri} from DICOM store.'
      cloud_logging_client.info(success_msg, log_struct)
      return True
    except requests.HTTPError as exc:
      if failed_msg is None:
        failed_msg = f'Failed to delete resource {uri} from DICOM store.'
      cloud_logging_client.error(failed_msg, log_struct, exc)
      return False

  def delete_series_from_dicom_store(
      self, study_uid: str, series_uid: str
  ) -> bool:
    """Issues a DICOM Store series delete on the series.

    Args:
      study_uid: Study UID of series to delete.
      series_uid: Series UID of series to delete.

    Returns:
      True if deleted.
    """
    series_path = (
        f'{self.dicomweb_path}/studies/{study_uid}/series/{series_uid}'
    )
    success_msg = f'Deleted series {series_path} from DICOM store.'
    failed_msg = f'Deleting series {series_path} from DICOM store.'
    log_struct = {
        ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: study_uid,
        ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: series_uid,
        ingest_const.LogKeywords.DICOMWEB_PATH: self.dicomweb_path,
    }
    return self.delete_resource_from_dicom_store(
        series_path,
        success_msg=success_msg,
        failed_msg=failed_msg,
        log_struct=log_struct,
    )
