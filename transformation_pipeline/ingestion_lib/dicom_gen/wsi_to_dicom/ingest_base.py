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
"""Base class for WSI to DICOM image pyramid conversion."""
import abc
import dataclasses
import os
import re
import subprocess
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Set, Union

import pydicom
import requests

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_json_util
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_schema_util
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import decode_slideid
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import wsi_pyramid_gen_config

_DICOM_DOWNSAMPLE_FILE_NAME_REGEX = re.compile(r'downsample-(\d+)-')


class GenDicomFailedError(Exception):
  pass


class Wsi2DcmFileNameFormatError(Exception):
  pass


@dataclasses.dataclass(frozen=True)
class DicomMetadata:
  dicom_json: Dict[str, Any]
  metadata_table_row: dicom_schema_util.MetadataTableWrapper


@dataclasses.dataclass(frozen=True)
class WsiDownsampleLayers:
  """Downsampling factors to generate for storage in main and OOF DICOM stores.

  If main_store == null then full pyramid will be generated.
  """

  main_store: Optional[set[int]]
  oof: set[int]

  @property
  def generate_full_pyramid(self) -> bool:
    return self.main_store is None


def _get_downsampled_dicom_files(
    dicom_filepaths: Union[List[str], Set[str]],
    downsamples: Set[int],
    include_largest_downsampled_dicom: bool = False,
) -> Set[str]:
  """Returns file paths that have file names specified in downsamples set.

  Args:
    dicom_filepaths: List of file paths to DICOM instances.
    downsamples: Set containing list of downsampling factors.
    include_largest_downsampled_dicom: Force largest downsampled DICOM to be
      included in returned list. Used to include single frame instances.

  Returns:
    List of file paths that contain imaging downsampled by a factor in the
    downsamples set.
  """
  file_set = set()
  largest_downsample_found = -1
  path_to_largest_downsampled_dicom = None
  for filepath in dicom_filepaths:
    filename = os.path.basename(filepath)
    match = _DICOM_DOWNSAMPLE_FILE_NAME_REGEX.search(filename)
    if match is None:
      continue
    downsample = int(match.groups()[0])
    if downsample in downsamples:
      file_set.add(filepath)
    if (
        include_largest_downsampled_dicom
        and downsample > largest_downsample_found
    ):
      largest_downsample_found = downsample
      path_to_largest_downsampled_dicom = filepath

  if len(downsamples) != len(file_set):
    cloud_logging_client.warning(
        'Wsi2Dcm failed to produce expected downsample.',
        {
            'files_found': str(sorted(file_set)),
            'downsamples_requested': str(sorted(downsamples)),
        },
    )
  if path_to_largest_downsampled_dicom is not None:
    file_set.add(path_to_largest_downsampled_dicom)
  return file_set


class DicomInstanceIngestionSets:
  """Contains set of DICOM instances (file paths) to upload to DICOM stores.

  Attributes:
    main_store_instance_list: List of DICOM instances to upload to main store.
    oof_store_instance_list: List of DICOM instances to upload to OOF store.
  """

  def __init__(
      self,
      dicom_files: Union[Set[str], List[str]],
      downsamples: Optional[WsiDownsampleLayers] = None,
      force_upload_to_main_store_list: Optional[
          Union[Set[str], List[str]]
      ] = None,
  ):
    """Constructor.

    Args:
      dicom_files: List of DICOM instance file paths.
      downsamples: Configuration for downsample generation.
      force_upload_to_main_store_list: List of files to force to be uploaded to
        the main store.
    """
    if downsamples is None or downsamples.generate_full_pyramid:
      self._main = set(dicom_files)
    else:
      self._main = _get_downsampled_dicom_files(
          dicom_files,
          downsamples.main_store,
          include_largest_downsampled_dicom=True,
      )
    if force_upload_to_main_store_list is not None:
      for item in force_upload_to_main_store_list:
        self._main.add(item)

    if downsamples is None or not downsamples.oof:
      self._oof = set()
    else:
      self._oof = _get_downsampled_dicom_files(dicom_files, downsamples.oof)
    cloud_logging_client.info(
        'Dicom Store ingestion lists',
        {
            ingest_const.LogKeywords.MAIN_DICOM_STORE: '\n'.join(
                sorted(self._main)
            ),
            ingest_const.LogKeywords.OOF_DICOM_STORE: '\n'.join(
                sorted(self._oof)
            ),
        },
    )

  @property
  def main_store_instances(self) -> Set[str]:
    """Returns set of DICOM instances to upload to main DICOM store."""
    return self._main

  @property
  def oof_store_instances(self) -> Set[str]:
    """Returns set of DICOM instances to upload to OOF DICOM store."""
    return self._oof

  def combine_main_and_oof_ingestion_and_main_store_sets(self):
    self._main = self._main.union(self._oof)
    self._oof = set()


@dataclasses.dataclass
class GenDicomResult:
  dicom_gen: abstract_dicom_generation.GeneratedDicomFiles
  dest_uri: str
  files_to_upload: DicomInstanceIngestionSets
  generated_series_instance_uid: bool


def _get_wsi2dcm_cmdline(
    file_to_convert: str,
    downsample_config: WsiDownsampleLayers,
    output_path: str,
    study_uid: Optional[str],
    series_uid: Optional[str],
    default_conversion_params: str,
) -> str:
  """Returns wsi2dcm commandline.

  Args:
    file_to_convert: Input file to convert to DICOM.
    downsample_config: WsiDownsampleLayers to generate.
    output_path: Path to write generated DICOM files.
    study_uid: Study instance uid to set DICOM files to.
    series_uid: Series instance uid to set DICOM files to.
    default_conversion_params: Default conversion params
  """
  cmd_line = [
      (
          '/wsi-to-dicom-converter/build/wsi2dcm '
          f'--input="{file_to_convert}" --outFolder="{output_path}" '
      ),
      default_conversion_params,
  ]
  if downsample_config.generate_full_pyramid:
    # levels is set to an absurdly large number to indicate multiple levels.
    # will be generated. Actual downsampled levels will be the number of levels
    # required to generate full pyramid which downsamples the image at 2x
    # increments until it fits in only one frame.
    cmd_line.append('--levels=999')
    cmd_line.append('--stopDownsamplingAtSingleFrame')
  else:
    downsamples = downsample_config.main_store.union(downsample_config.oof)
    downsamples = sorted(list(downsamples))
    downsamples = ' '.join([str(ds) for ds in downsamples])
    cmd_line.append(f'--downsamples {downsamples}')
  # Always store pseudo thumbnail image that fits in one frame.
  cmd_line.append('--singleFrameDownsample')
  if study_uid is not None and study_uid:
    cmd_line.append(f'--studyId={study_uid}')
  if series_uid is not None and series_uid:
    cmd_line.append(f'--seriesId={series_uid}')
  cmd_line = ' '.join(cmd_line)
  cloud_logging_client.debug(
      'Wsi2Dcm Command line',
      {'wsi2dcm_command_line': cmd_line},
  )
  return cmd_line


def _get_generated_dicom_files_sorted_by_size(output_path: str) -> List[str]:
  """Returns list of DICOM instances in directory, sorted by size.

  Args:
    output_path: Directory path.

  Returns:
    List of paths to DICOM instances (*.dcm) in directory

  Raises:
    Wsi2DcmFileNameFormatError: Filename does not contain parsable downsample
  """
  generated_dicom_files = []
  with os.scandir(output_path) as it:
    for entry in it:
      if entry.name.endswith('.dcm'):
        filepath = entry.path

        match = _DICOM_DOWNSAMPLE_FILE_NAME_REGEX.search(entry.name)
        if match is None:
          cloud_logging_client.critical(
              'Wsi2Dcm produced invalid filename.',
              {ingest_const.LogKeywords.FILENAME: entry.name},
          )
          # It is expected that C++ tooling writes the images downsampling
          # factor into the names of the files it produces. This keeps us from
          # having to open the files to determine what downsampling they
          # represent. Determining this after the C++ tooling is run is tricky.
          # At a minimum, as it would require reading all instances produced by
          # the tooling to determine the dimensions of the instance and the
          # dimensions of the highest magnification. In the future this approach
          # could fail if the tooling was configured to not produce DICOM which
          # represents the highest mag. The error thrown indicates Wsi2Dcm no
          # longer is not producing files that we can parse the images
          # downsampling factor from.
          raise Wsi2DcmFileNameFormatError(
              f'Wsi2Dcm produced invalid filename: {entry.name}.'
          )

        filesize = entry.stat().st_size
        generated_dicom_files.append((filepath, filesize))
        cloud_logging_client.info(
            'WSI-to-DICOM conversion created',
            {
                ingest_const.LogKeywords.FILENAME: filepath,
                ingest_const.LogKeywords.FILE_SIZE: filesize,
            },
        )
  # Sort DICOMs by filesize, to process largest first.
  generated_dicom_files = sorted(
      generated_dicom_files, reverse=True, key=lambda x: x[1]
  )
  generated_dicom_files = [
      dicom_file[0] for dicom_file in generated_dicom_files
  ]
  return generated_dicom_files


@dataclasses.dataclass(frozen=True)
class GcsIngestionBuckets:
  success_uri: str
  failure_uri: str


def _get_wsi2dcm_param_value(
    value: Any,
    enum_type: Any,
) -> Optional[str]:
  return enum_type[value.name].value


def _add_wsi2dcm_param(
    param_list: List[str],
    param: str,
    value: Optional[Union[str, int]],
) -> None:
  if value is None:
    return
  param_list.append(f'--{param}={value}')


def _add_wsi2dcm_flag(
    param_list: List[str],
    value: Optional[str],
) -> None:
  if value is None:
    return
  param_list.append(f'--{value}')


def _build_wsi2dcm_param_string() -> str:
  """Returns base commandline param string for WSI2DCM."""
  params = [
      '--opencvDownsampling=AREA',
      '--progressiveDownsample',
      '--floorCorrectOpenslideLevelDownsamples',
  ]
  _add_wsi2dcm_param(
      params, 'tileHeight', ingest_flags.WSI2DCM_DICOM_FRAME_HEIGHT_FLG.value
  )
  _add_wsi2dcm_param(
      params, 'tileWidth', ingest_flags.WSI2DCM_DICOM_FRAME_WIDTH_FLG.value
  )
  _add_wsi2dcm_param(
      params,
      'compression',
      _get_wsi2dcm_param_value(
          ingest_flags.WSI2DCM_COMPRESSION_FLG.value,
          ingest_flags.Wsi2DcmCompression,
      ),
  )
  _add_wsi2dcm_param(
      params,
      'jpegCompressionQuality',
      ingest_flags.WSI2DCM_JPEG_COMPRESSION_QUALITY_FLG.value,
  )
  _add_wsi2dcm_param(
      params,
      'jpegSubsampling',
      _get_wsi2dcm_param_value(
          ingest_flags.WSI2DCM_JPEG_COMPRESSION_SUBSAMPLING_FLG.value,
          ingest_flags.Wsi2DcmJpegCompressionSubsample,
      ),
  )
  _add_wsi2dcm_param(
      params,
      'firstLevelCompression',
      _get_wsi2dcm_param_value(
          ingest_flags.WSI2DCM_FIRST_LEVEL_COMPRESSION_FLG.value,
          ingest_flags.Wsi2DcmFirstLevelCompression,
      ),
  )
  _add_wsi2dcm_flag(
      params,
      _get_wsi2dcm_param_value(
          ingest_flags.WSI2DCM_PIXEL_EQUIVALENT_TRANSFORM_FLG.value,
          ingest_flags.Wsi2DcmPixelEquivalentTransform,
      ),
  )
  return ' '.join(params)


def _correct_missing_study_instance_uid_in_metadata(
    wsi_dcm_json: MutableMapping[str, Any],
    abstract_dicom_handler: abstract_dicom_generation.AbstractDicomGeneration,
) -> str:
  """Corrects missing study instance uid in DICOM metadata.

  Args:
    wsi_dcm_json: DICOM JSON formatted metadata to merge with generated imaging.
    abstract_dicom_handler: Abstract_dicom pub/sub message handler calling
      generate_dicom.

  Returns:
    Metadata Study Instance UID as string.

  Raises:
    GenDicomFailedError: Could not determine study instance uid.
    redis_client.CouldNotAcquireNonBlockingLockError: Could not acquire lock.
  """
  create_missing_study_instance_uid = (
      ingest_flags.ENABLE_CREATE_MISSING_STUDY_INSTANCE_UID_FLG.value
  )
  try:
    return dicom_json_util.get_study_instance_uid(
        wsi_dcm_json,
        log_error=not create_missing_study_instance_uid,
    )
  except dicom_json_util.MissingStudyUIDInMetadataError as exp:
    if not create_missing_study_instance_uid:
      raise GenDicomFailedError(
          ingest_const.ErrorMsgs.MISSING_STUDY_UID
      ) from exp
  try:
    accession_number = dicom_json_util.get_accession_number(wsi_dcm_json)
  except dicom_json_util.MissingAccessionNumberMetadataError as exp:
    msg = (
        'Unable to create StudyInstanceUID metadata is missing accession'
        ' number.'
    )
    cloud_logging_client.error(msg, exp, wsi_dcm_json)
    raise GenDicomFailedError(
        msg,
        ingest_const.ErrorMsgs.MISSING_ACCESSION_NUMBER_UNABLE_TO_CREATE_STUDY_INSTANCE_UID,
    ) from exp
  logs = {ingest_const.LogKeywords.ACCESSION_NUMBER: accession_number}
  try:
    patient_id = dicom_json_util.get_patient_id(wsi_dcm_json)
    logs[ingest_const.LogKeywords.PATIENT_ID] = patient_id
    search_patient_id = ' and patient id'
    undefined_patient_id = False
  except dicom_json_util.MissingPatientIDMetadataError:
    patient_id = ''  # Seach for studies that do not define patient id.
    search_patient_id = ''
    undefined_patient_id = True
  cloud_logging_client.info(
      'Searching for DICOM Study Instance UID associated with accession'
      f' number{search_patient_id}.',
      logs,
  )
  dicom_client = abstract_dicom_handler.dcm_store_client
  try:
    study_uid_found = dicom_client.study_instance_uid_search(
        accession_number=accession_number,
        patient_id=patient_id,
        limit=2,
        find_studies_with_undefined_patient_id=undefined_patient_id,
    )
  except (
      dicom_store_client.StudyInstanceUIDSearchError,
      requests.HTTPError,
  ) as exp:
    msg = (
        'Error occurred querying DICOM store. Unable to create'
        ' StudyInstanceUID.'
    )
    cloud_logging_client.error(msg, logs, exp)
    raise GenDicomFailedError(
        msg,
        ingest_const.ErrorMsgs.ERROR_OCCURRED_QUERYING_DICOM_STORE_UNABLE_TO_CREATE_STUDY_INSTANCE_UID,
    ) from exp
  if study_uid_found:
    logs[ingest_const.LogKeywords.STUDY_INSTANCE_UIDS_FOUND] = study_uid_found
    if len(study_uid_found) > 1:
      msg = (
          'Unable to create StudyInstanceUID. Accession number defined in'
          ' metadata is associated with multiple Study Instance UID in the'
          ' DICOM store.'
      )
      cloud_logging_client.error(msg, logs)
      raise GenDicomFailedError(
          msg,
          ingest_const.ErrorMsgs.UNABLE_TO_CREATE_STUDY_INSTANCE_UID_ACCESSION_NUMBER_IS_ASSOCIATED_WITH_MULTIPLE_STUDY_INSTANCE_UID,
      )
    study_instance_uid = study_uid_found[0]
  else:
    cloud_logging_client.info(
        'Accession number is not associated with existing StudyInstanceUID'
        ' UID; Generating new UID.',
        logs,
    )
    # This lock is only used if the pipeline has the
    # ENABLE_CREATE_MISSING_STUDY_INSTANCE_UID_FLG flag enabled
    # AND a study instance uid does not currently exist in the DICOM store
    # for the accession number that is being generated. Under these combined
    # conditions we want to block other instances of the pipeline from ingesting
    # a slide with the same accession number as this would likely result in the
    # imaging being ingested into different study instances uids
    # (the pipeline is creating study instance uids). If the accession number
    # exist then we will use the found study instance uid and we don't need to
    # lock.
    abstract_dicom_handler.acquire_non_blocking_lock(
        ingest_const.RedisLockKeywords.DICOM_ACCESSION_NUMBER % accession_number
    )
    study_instance_uid = uid_generator.generate_uid()
  logs[ingest_const.LogKeywords.STUDY_INSTANCE_UID] = study_instance_uid
  dicom_json_util.set_study_instance_uid_in_metadata(
      wsi_dcm_json, study_instance_uid
  )
  return study_instance_uid


def initialize_metadata_study_and_series_uids_for_dicom_triggered_ingestion(
    study_uid: str,
    series_uid: str,
    metadata: MutableMapping[str, Any],
    abstract_dicom_handler: abstract_dicom_generation.AbstractDicomGeneration,
    set_study_instance_uid_from_metadata: bool,
    set_series_instance_uid_from_metadata: bool,
) -> None:
  """Init metadata with study & series uids for DICOM triggered ingestion.

  Function defines StudyInstanceUID and SeriesInstanceUID in metadata for
  DICOM triggered transformations. DICOM images can be ingested using either
  the StudyInstanceUID and SeriesInstanceUID defined in the DICOM imaging, i.e.
  the UID in the DICOM are unchanged, or the Study Instance UID and Series
  instance UID can be modified to values defined within supplemental metadata or
  even generated if not defined. Transformation pipeline behavior is controlled
  by flags (enviromental variables, see:
    GCS_INGEST_STUDY_INSTANCE_UID_SOURCE_FLG (set source of StudyInstanceUid)
    INIT_SERIES_INSTANCE_UID_FROM_METADATA_FLG (set source of SeriesInstanceUID)
    ENABLE_CREATE_MISSING_STUDY_INSTANCE_UID_FLG, (create missing UID value)
    ENABLE_METADATA_FREE_INGESTION_FLG  (enable ingestion without metadata)
    ).

  The Study

  Args:
    study_uid: Study Instance UID of DICOM imaging triggering ingestion.
    series_uid: Series Instance UID of DICOM imaging triggering ingestion.
    metadata: DICOM formatted JSON metadata that will be merged with generated
      and ingested DICOM imaging.
    abstract_dicom_handler: Abstract_dicom pub/sub message handler calling
      generate_dicom.
    set_study_instance_uid_from_metadata: If true, set study instance uid to
      value used in metadata; otherwise use value passed in.
    set_series_instance_uid_from_metadata: If true, set series instance uid to
      value used in metadata; otherwise use value passed in.

  Raises:
    GenDicomFailedError: Unable to determine Study Instance UID or Series
      instance UID from metadata.
    redis_client.CouldNotAcquireNonBlockingLockError: Could not acquire lock.
  """
  # Determine Study Instance UID for DICOM ingestion.
  if set_study_instance_uid_from_metadata:
    study_uid = _correct_missing_study_instance_uid_in_metadata(
        metadata, abstract_dicom_handler
    )
  # Set metadata to match
  dicom_json_util.set_study_instance_uid_in_metadata(metadata, study_uid)

  # Determine Series Instance UID for DICOM ingestion.
  if set_series_instance_uid_from_metadata:
    try:
      series_uid = dicom_json_util.get_series_instance_uid(metadata)
    except dicom_json_util.MissingSeriesUIDInMetadataError as exp:
      raise GenDicomFailedError(
          'Missing SeriesInstanceUID in metadata.',
          ingest_const.ErrorMsgs.MISSING_SERIES_UID,
      ) from exp
  # Set metadata to match
  dicom_json_util.set_series_instance_uid_in_metadata(metadata, series_uid)
  # remove SOP Class UID in metadata to ensure metadata does not overwrite
  # value in DICOM.
  dicom_json_util.remove_sop_class_uid_from_metadata(metadata)
  cloud_logging_client.info(
      'Ingesting images into StudyInstanceUID and SeriesInstanceUID',
      {
          ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: study_uid,
          ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: series_uid,
      },
  )


def initialize_metadata_study_and_series_uids_for_non_dicom_image_triggered_ingestion(
    abstract_dicom_handler: abstract_dicom_generation.AbstractDicomGeneration,
    dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
    wsi_dcm_json: MutableMapping[str, Any],
    initialize_series_uid_from_metadata: bool = False,
) -> bool:
  """Init metadata with study & series uids for general image ingestion.

  Function defines StudyInstanceUID and SeriesInstanceUID in metadata for
  non DICOM image triggered transformations. Study UID is defined either in
  metadata directly, or inferred through acession number
  (ENABLE_CREATE_MISSING_STUDY_INSTANCE_UID_FLG), or generated via
  ENABLE_METADATA_FREE_INGESTION_FLG. SeriesInstanceUID is either generated
  default or defined in via metadata
  (INIT_SERIES_INSTANCE_UID_FROM_METADATA_FLG) or generated
  ENABLE_METADATA_FREE_INGESTION_FLG. The study and series instance uids used
  for generated DICOM are written to metadata (wsi_dcm_json).

  Transformation pipeline behavior is controlled
  by flags (enviromental variables, see:
    INIT_SERIES_INSTANCE_UID_FROM_METADATA_FLG (set source of SeriesInstanceUID)
    ENABLE_CREATE_MISSING_STUDY_INSTANCE_UID_FLG, (create missing UID value)
    ENABLE_METADATA_FREE_INGESTION_FLG  (enable ingestion without metadata)
    ).

  Args:
    abstract_dicom_handler: Abstract_dicom pub/sub message handler calling
      generate_dicom.
    dicom_gen: Payload for DICOM being generated.
    wsi_dcm_json: WSI DICOM Json formatted metadata.
    initialize_series_uid_from_metadata: Whether to initialize series UID from
      metadata.

  Returns:
    True if series uid was generated by the pipleine and not intialized
      from user metadata.

  Raises:
    GenDicomFailedError: Unable to determine Study Instance UID or Series
      instance UID from metadata.
    redis_client.CouldNotAcquireNonBlockingLockError: Could not acquire lock.
  """
  study_uid = _correct_missing_study_instance_uid_in_metadata(
      wsi_dcm_json, abstract_dicom_handler
  )
  # Search to determine if a slide was already partially added. If it was
  # add any remaining imaging to the existing series.
  dcm_store_client = abstract_dicom_handler.dcm_store_client
  series_uid = (
      dcm_store_client.get_series_uid_and_existing_dicom_for_study_and_hash(
          study_uid, dicom_gen.hash
      ).series_instance_uid
  )
  generated_series_instance_uid = False
  if series_uid is None and initialize_series_uid_from_metadata:
    # enable series uid to be defined in metadata.
    # if enabled automatic ingestion of rescans into new series is disabled.
    # and would need to be handled externally via the series def in metadata.
    try:
      series_uid = dicom_json_util.get_series_instance_uid(wsi_dcm_json)
      cloud_logging_client.info(
          'Series UID defined from metadata.',
          {'SeriesInstanceUID': series_uid},
      )
    except dicom_json_util.MissingSeriesUIDInMetadataError as exp:
      raise GenDicomFailedError(
          'Missing Series UID in metadata.',
          ingest_const.ErrorMsgs.MISSING_SERIES_UID,
      ) from exp

  if series_uid is None:
    series_uid = uid_generator.generate_uid()
    generated_series_instance_uid = True
    cloud_logging_client.info(
        'Generating new series instance UID',
        {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: series_uid},
    )
  else:
    cloud_logging_client.info(
        'Ingesting images into existing series',
        {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: series_uid},
    )
  dicom_json_util.set_series_instance_uid_in_metadata(wsi_dcm_json, series_uid)
  return generated_series_instance_uid


def generate_empty_slide_metadata() -> DicomMetadata:
  return DicomMetadata(
      {},
      dicom_schema_util.DictMetadataTableWrapper({}),
  )


def generate_metadata_free_slide_metadata(
    slide_id: str,
    dicom_client: dicom_store_client.DicomStoreClient,
) -> DicomMetadata:
  """Generates minimal metadata for metadata free slide ingestion.

  Args:
    slide_id: Slide ID.
    dicom_client: DICOM store client.

  Returns:
    DicomMetadata[DICOM JSON metadata, metadata table row representation]

  Raises:
    GenDicomFailedError: Cannot query to the DICOM store.
  """
  try:
    study_uid_found = dicom_client.study_instance_uid_search(
        accession_number='',
        patient_id=slide_id,
        limit=1,
        find_studies_with_undefined_patient_id=False,
    )
  except (
      dicom_store_client.StudyInstanceUIDSearchError,
      requests.HTTPError,
  ) as exp:
    msg = 'Error occurred querying the DICOM store.'
    cloud_logging_client.error(msg, exp)
    raise GenDicomFailedError(
        msg,
        'error_querying_dicom_store',
    ) from exp

  if study_uid_found:
    study_instance_uid = study_uid_found[0]
  else:
    study_instance_uid = uid_generator.generate_uid()
  ds = pydicom.Dataset()
  ds.StudyInstanceUID = study_instance_uid
  ds.PatientID = slide_id
  ds.ContainerIdentifier = slide_id
  metadata = {
      'StudyInstanceUID': study_instance_uid,
      'PatientID': slide_id,
      'ContainerIdentifier': slide_id,
  }
  return DicomMetadata(
      ds.to_json_dict(),
      dicom_schema_util.DictMetadataTableWrapper(metadata),
  )


class DetermineSlideIDError(Exception):
  """Error that occurred while determining slide id."""

  def __init__(
      self,
      dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
      dest_uri: str,
      user_msg: str = 'Error determining slide id.',
      failure_bucket_error_msg: str = ingest_const.ErrorMsgs.SLIDE_ID_MISSING,
  ):
    super().__init__(user_msg, failure_bucket_error_msg)
    self._dest_uri = dest_uri
    self._dicom_gen = dicom_gen

  @property
  def dest_uri(self) -> str:
    return self._dest_uri

  @property
  def dicom_gen(self) -> abstract_dicom_generation.GeneratedDicomFiles:
    return self._dicom_gen


class IngestBase(metaclass=abc.ABCMeta):
  """Converts image to DICOM."""

  def __init__(
      self,
      ingest_buckets: Optional[GcsIngestionBuckets],
      metadata_client: metadata_storage_client.MetadataStorageClient,
  ):
    self._wsi_pyramid_gen_config = (
        wsi_pyramid_gen_config.WsiPyramidGenerationConfig()
    )
    self._metadata_storage_client = metadata_client
    self.default_wsi_conv_params = _build_wsi2dcm_param_string()
    self._ingest_buckets = ingest_buckets
    self._slide_id = None
    self._is_metadata_free_slide_id = False

  def init_handler_for_ingestion(self) -> None:
    """Initializes the handler for pub/sub msg ingestion."""
    self._slide_id = None
    self._is_metadata_free_slide_id = False

  def set_slide_id(self, slide_id: str, is_metadata_free_slide_id: bool) -> str:
    self._slide_id = slide_id
    self._is_metadata_free_slide_id = is_metadata_free_slide_id
    return slide_id

  @property
  def slide_id(self) -> str:
    return self._slide_id

  @property
  def is_metadata_free_slide_id(self) -> bool:
    return self._is_metadata_free_slide_id

  @abc.abstractmethod
  def get_slide_id(
      self,
      dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
      abstract_dicom_handler: abstract_dicom_generation.AbstractDicomGeneration,
  ) -> str:
    """Returns slide id of ingested SVS.

    Args:
      dicom_gen: File payload to convert into full DICOM WSI image pyramid.
      abstract_dicom_handler: Abstract_dicom pub/sub message handler calling
        generate_dicom.

    Raises:
      DetermineSlideIDError: Cannot determine slide id of ingested DICOM.
    """

  @property
  def name(self) -> str:
    """Name of conversion class."""
    return self.__class__.__name__

  @property
  def metadata_storage_client(
      self,
  ) -> metadata_storage_client.MetadataStorageClient:
    return self._metadata_storage_client

  @property
  def ingest_succeeded_uri(self) -> str:
    if not self._ingest_buckets:
      return ''
    return self._ingest_buckets.success_uri

  @property
  def ingest_failed_uri(self) -> str:
    if not self._ingest_buckets:
      return ''
    return self._ingest_buckets.failure_uri

  def log_and_get_failure_bucket_path(self, exp: Exception) -> str:
    cloud_logging_client.error('Failed to ingest DICOM.', exp)
    if not self.ingest_failed_uri:
      return ''
    if len(exp.args) == 2:
      _, bucket_dir = exp.args
    else:
      bucket_dir = str(exp)
    return os.path.join(self.ingest_failed_uri, bucket_dir)

  def convert_wsi_to_dicom(
      self,
      file_to_convert: str,
      downsample_config: WsiDownsampleLayers,
      output_path: str,
      metadata: Mapping[str, Any],
  ) -> List[str]:
    """Runs wsi2dcm tool to convert svs to DICOM.

    Args:
      file_to_convert: Path to file (e.g., svs) to convert.
      downsample_config: WsiDownsampleLayers to generate.
      output_path: Output directory to write generated images to.
      metadata: DICOM JSON formatted metadata for imaging.

    Returns:
      List of paths to generated DICOM.

    Raises:
      GenDicomFailedError: if conversion fails
      Wsi2DcmFileNameFormatError: If C++ wsi2dcm tooling is producing files
        that do not identify image downsampling in the filename.
    """
    study_uid = dicom_json_util.get_study_instance_uid(metadata)
    series_uid = dicom_json_util.get_series_instance_uid(metadata)
    cmd_line = _get_wsi2dcm_cmdline(
        file_to_convert,
        downsample_config,
        output_path,
        study_uid,
        series_uid,
        self.default_wsi_conv_params,
    )
    try:
      cloud_logging_client.info(
          'Starting WSI-to-DICOM conversion', {'cmd_line': cmd_line}
      )
      subprocess.run(cmd_line, capture_output=True, check=True, shell=True)
    except subprocess.CalledProcessError as exp:
      cloud_logging_client.error(
          'WSI-to-DICOM conversion error',
          {
              'cmd': cmd_line,
              ingest_const.LogKeywords.RETURN_CODE: exp.returncode,
              ingest_const.LogKeywords.STDOUT: exp.stdout.decode('utf-8'),
              ingest_const.LogKeywords.STDERR: exp.stderr.decode('utf-8'),
          },
          exp,
      )
      raise GenDicomFailedError(
          'An error occurred converting WSI to DICOM.',
          ingest_const.ErrorMsgs.WSI_TO_DICOM_CONVERSION_FAILED,
      ) from exp
    return _get_generated_dicom_files_sorted_by_size(output_path)

  def update_metadata(self):
    """Updates container metadata from GCS.

    Raises:
      metadata_storage_client.MetadataDownloadExceptionError: Cannot download
        metadata.
    """
    self.metadata_storage_client.update_metadata()

  def get_dicom_metadata_schema(self, sop_class_name: str) -> Dict[str, Any]:
    """Updates metadata and returns DICOM metadata mapping schema.

    Args:
      sop_class_name: DICOM SOP class name.

    Returns:
      DICOM metadata mapping schema (JSON)

    Raises:
      GenDicomFailedError: Invalid metadata schema
    """
    try:
      return self.metadata_storage_client.get_dicom_schema(
          {'SOPClassUID_Name': sop_class_name}
      )
    except metadata_storage_client.MetadataSchemaExceptionError as exp:
      raise GenDicomFailedError(
          f'Invalid {sop_class_name} DICOM metadata schema.',
          ingest_const.ErrorMsgs.INVALID_METADATA_SCHEMA,
      ) from exp

  @abc.abstractmethod
  def _generate_metadata_free_slide_metadata(
      self, slide_id: str, dicom_client: dicom_store_client.DicomStoreClient
  ) -> DicomMetadata:
    """Returns minimal metadata for metadata free slide ingestion.

    Args:
      slide_id: Slide ID.
      dicom_client: DICOM store client.

    Returns:
      DicomMetadata[DICOM JSON metadata, metadata table row representation]
    """

  def get_slide_dicom_json_formatted_metadata(
      self,
      sop_class_name: str,
      slide_id: str,
      dicom_client: dicom_store_client.DicomStoreClient,
      metadata_row: Optional[dicom_schema_util.MetadataTableWrapper] = None,
  ) -> DicomMetadata:
    """Returns Metadata (DICOM JSON & CSV) for slide.

    Args:
      sop_class_name: DICOM SOP class name.
      slide_id: ID of slide.
      dicom_client: DICOM store client used to correct missing study instance
        uid.
      metadata_row: Optional slide metadata row; if not provided searchs for
        metadata.

    Returns:
      DicomMetadata[Json format DICOM metadata, CSV Metadata, bool if metadata
        was generated using metadata free]

    Raises:
      GenDicomFailedError: Error loading metadata mapping schema or unable to
        generate metadata.
    """
    if self.is_metadata_free_slide_id:
      if not ingest_flags.ENABLE_METADATA_FREE_INGESTION_FLG.value:
        raise ValueError('Metadata free ingestion triggered but flag is false.')
      return self._generate_metadata_free_slide_metadata(slide_id, dicom_client)
    # get_dicom_metadata_schema raises GenDicomFailedError if it unable to
    # return a metadata mapping schema for the specified sop_class_name
    # passing exception through to caller.
    dicom_schema = self.get_dicom_metadata_schema(sop_class_name)
    try:
      bq_table_id = ingest_flags.BIG_QUERY_METADATA_TABLE_FLG.value.strip()
      if metadata_row is not None:
        pass
      elif bq_table_id:
        bq_table_log = {'bq_table_id': bq_table_id}
        try:
          project_id, dataset_id, table_name = (
              decode_slideid.decode_bq_metadata_table_env()
          )
          metadata_row = (
              self.metadata_storage_client.get_slide_metadata_from_bigquery(
                  project_id, dataset_id, table_name, slide_id
              )
          )
          if metadata_row is None:
            msg = 'No BigQuery metadata found.'
            cloud_logging_client.error(msg, bq_table_log)
            raise GenDicomFailedError(
                msg,
                ingest_const.ErrorMsgs.BQ_METADATA_NOT_FOUND,
            )
        except (
            IndexError,
            decode_slideid.BigQueryMetadataTableEnvFormatError,
        ) as exc:
          msg = f'Error parsing BigQuery table id: {bq_table_id}'
          cloud_logging_client.critical(msg, bq_table_log, exc)
          raise GenDicomFailedError(
              msg,
              ingest_const.ErrorMsgs.BQ_METADATA_NOT_FOUND,
          ) from exc
      else:
        metadata = self.metadata_storage_client.get_slide_metadata_from_csv(
            slide_id
        )
        metadata_row = dicom_schema_util.PandasMetadataTableWrapper(metadata)
      dcm_json = dicom_schema_util.get_json_dicom(
          sop_class_name, metadata_row, dicom_schema
      )
      dicom_json_util.remove_sop_instance_uid_from_metadata(dcm_json)
      return DicomMetadata(dcm_json, metadata_row)
    except (
        dicom_schema_util.DICOMSchemaTagError,
        dicom_schema_util.DICOMSchemaError,
    ) as exp:
      raise GenDicomFailedError(
          f'Invalid {sop_class_name} DICOM metadata schema.',
          ingest_const.ErrorMsgs.INVALID_METADATA_SCHEMA,
      ) from exp
    except dicom_schema_util.DICOMSpecMetadataError as exp:
      raise GenDicomFailedError(
          'Error in internal representation of DICOM standard.',
          ingest_const.ErrorMsgs.INVALID_DICOM_STANDARD_REPRESENTATION,
      ) from exp
    except dicom_schema_util.MissingRequiredMetadataValueError as exp:
      raise GenDicomFailedError(
          'Missing metadata for required DICOM tag.',
          ingest_const.ErrorMsgs.MISSING_METADATA_FOR_REQUIRED_DICOM_TAG,
      ) from exp

  @abc.abstractmethod
  def generate_dicom(
      self,
      dicom_gen_dir: str,
      dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
      message_id: str,
      abstract_dicom_handler: abstract_dicom_generation.AbstractDicomGeneration,
  ) -> GenDicomResult:
    """Converts downloaded image to full DICOM WSI pyramid.

    Args:
      dicom_gen_dir: directory to generate DICOM files in.
      dicom_gen: File payload to convert into full DICOM WSI image pyramid.
      message_id: pub/sub msg id.
      abstract_dicom_handler: Abstract_dicom pub/sub message handler calling
        generate_dicom.

    Returns:
      GenDicomResult describing generated dicom
    """

  def get_downsamples_to_generate(
      self, pixel_spacing: float, oof_ingestion_enabled: bool
  ) -> WsiDownsampleLayers:
    """Returns WSI downsampling to generate for input image pixel spacing."""
    main_store_downsamples = self._wsi_pyramid_gen_config.get_downsample(
        pixel_spacing
    )

    if main_store_downsamples is not None:
      # If downsamples defined by user always store base level magnification.
      main_store_downsamples.add(1)

    oof_downsamples = (
        wsi_pyramid_gen_config.get_oof_downsample_levels(pixel_spacing)
        if oof_ingestion_enabled
        else set()
    )

    return WsiDownsampleLayers(main_store_downsamples, oof_downsamples)
