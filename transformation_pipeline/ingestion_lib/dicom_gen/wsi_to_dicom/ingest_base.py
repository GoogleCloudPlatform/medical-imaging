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
import contextlib
import dataclasses
import os
import re
import subprocess
from typing import Any, ContextManager, Dict, List, Mapping, MutableMapping, Optional, Set, Tuple, Union
from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib import redis_client
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_json_util
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_schema_util
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen import wsi_dicom_file_ref
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_image_extractor
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import decode_slideid
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import wsi_pyramid_gen_config

_DICOM_DOWNSAMPLE_FILE_NAME_REGEX = re.compile(r'downsample-(\d+)-')


class GenDicomFailedError(Exception):
  pass


class Wsi2DcmFileNameFormatError(Exception):
  pass


class _AccessionNumberAssociatedWithMultipleStudyInstanceUIDError(Exception):
  pass


@dataclasses.dataclass(frozen=True)
class StudyUIDSeriesUIDExistingDcm:
  study_instance_uid: str
  series_instance_uid: str
  preexisting_dicoms_in_store: List[wsi_dicom_file_ref.WSIDicomFileRef]


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
    cloud_logging_client.logger().warning(
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
    cloud_logging_client.logger().info(
        'Dicom Store ingestion lists',
        {
            ingest_const.LogKeywords.main_dicom_store: '\n'.join(
                sorted(self._main)
            ),
            ingest_const.LogKeywords.oof_dicom_store: '\n'.join(
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
  cloud_logging_client.logger().debug(
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
          cloud_logging_client.logger().critical(
              'Wsi2Dcm produced invalid filename.',
              {ingest_const.LogKeywords.filename: entry.name},
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
        cloud_logging_client.logger().info(
            'WSI-to-DICOM conversion created',
            {
                ingest_const.LogKeywords.filename: filepath,
                ingest_const.LogKeywords.file_size: filesize,
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
    dicom_client: dicom_store_client.DicomStoreClient,
) -> str:
  """Corrects missing study instance uid in DICOM metadata.

  Args:
    wsi_dcm_json: DICOM JSON formatted metadata to merge with generated imaging.
    dicom_client: DICOM store client.

  Returns:
    Metadata Study Instance UID as string.

  Raises:
    dicom_json_util.MissingStudyUIDInMetadataError: Json metadata is missing
      Study Instance UID. Metadata either does not define Study Instance UID
      or Metadata does not define accession number which is required for
      pipeline based allocation of the Study Instance UID.

    dicom_store_client.StudyInstanceUIDSearchError: Error occured decoding
      DICOM store search response.

    requests.HTTPError: HTTP error occured querying DICOM store for metadata.

    _AccessionNumberAssociatedWithMultipleStudyInstanceUIDError: Accession
      number associated with multiple Study Instance UID.
  """
  create_missing_study_instance_uid = (
      ingest_flags.ENABLE_CREATE_MISSING_STUDY_INSTANCE_UID_FLG.value
  )
  try:
    return dicom_json_util.get_study_instance_uid(
        wsi_dcm_json,
        log_error=not create_missing_study_instance_uid,
    )
  except dicom_json_util.MissingStudyUIDInMetadataError:
    if not create_missing_study_instance_uid:
      raise
  try:
    accession_number = dicom_json_util.get_accession_number(wsi_dcm_json)
  except dicom_json_util.MissingAccessionNumberMetadataError as exp:
    raise dicom_json_util.MissingStudyUIDInMetadataError() from exp
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
  cloud_logging_client.logger().info(
      'Searching for DICOM Study Instance UID associated with accession'
      f' number{search_patient_id}.',
      logs,
  )
  study_uid_found = dicom_client.study_instance_uid_search(
      accession_number=accession_number,
      patient_id=patient_id,
      limit=2,
      find_only_studies_with_undefined_patient_id=undefined_patient_id,
  )
  if study_uid_found:
    logs[ingest_const.LogKeywords.STUDY_INSTANCE_UIDS_FOUND] = study_uid_found
    if len(study_uid_found) > 1:
      cloud_logging_client.logger().error(
          'Accession number is associated with multiple Study Instance UID.',
          logs,
      )
      raise _AccessionNumberAssociatedWithMultipleStudyInstanceUIDError()
    study_instance_uid = study_uid_found[0]
  else:
    cloud_logging_client.logger().info(
        'Accession number is not assocated with exsting DICOM Study Instance'
        ' UID; Generating new UID.',
        logs,
    )
    study_instance_uid = uid_generator.generate_uid()
  logs[ingest_const.LogKeywords.study_instance_uid] = study_instance_uid
  dicom_json_util.set_study_instance_uid_in_metadata(
      wsi_dcm_json, study_instance_uid
  )
  return study_instance_uid


class IngestBase(metaclass=abc.ABCMeta):
  """Converts image to DICOM."""

  def __init__(self, ingest_buckets: Optional[GcsIngestionBuckets] = None):
    self._wsi_pyramid_gen_config = (
        wsi_pyramid_gen_config.WsiPyramidGenerationConfig()
    )
    self._metadata_storage_client = None
    self.default_wsi_conv_params = _build_wsi2dcm_param_string()
    self._ingest_buckets = ingest_buckets

  @property
  def name(self) -> str:
    """Name of conversion class."""
    return self.__class__.__name__

  @property
  def metadata_storage_client(
      self,
  ) -> metadata_storage_client.MetadataStorageClient:
    if not self._metadata_storage_client:
      self._metadata_storage_client = (
          metadata_storage_client.MetadataStorageClient()
      )
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
    cloud_logging_client.logger().error('Failed to ingest DICOM.', exp)
    if not self.ingest_failed_uri:
      return ''
    if len(exp.args) == 2:
      _, bucket_dir = exp.args
    else:
      bucket_dir = str(exp)
    return f'{self.ingest_failed_uri}/{bucket_dir}'

  def _determine_slideid(
      self,
      filename: str,
      ancillary_images: List[ancillary_image_extractor.AncillaryImage],
  ) -> str:
    """Determines slide id for ingested DICOM.

    Args:
      filename: Filename to test for slide id.
      ancillary_images: List of ancillary images (label, thumbnail, macro).

    Returns:
      SlideID as string

    Raises:
      ingest_base.GenDicomFailedError: Error resulting from inability to
        determine slide id
    """
    # attempt to get slide id from ingested file filename
    try:
      slide_id = decode_slideid.get_slide_id_from_filename(
          filename, self.metadata_storage_client
      )
      cloud_logging_client.logger().info(
          'Slide ID identified in file name.',
          {
              ingest_const.LogKeywords.filename: filename,
              ingest_const.LogKeywords.SLIDE_ID: slide_id,
          },
      )
      return slide_id
    except decode_slideid.SlideIdIdentificationError as exp:
      current_exception = exp

    # attempt to get slide id from barcodes in ancillary images
    try:
      slide_id = decode_slideid.get_slide_id_from_ancillary_images(
          ancillary_images,
          self.metadata_storage_client,
          current_exception,
      )
      cloud_logging_client.logger().info(
          'Slide ID identified in image barcode.',
          {ingest_const.LogKeywords.SLIDE_ID: slide_id},
      )
      return slide_id
    except decode_slideid.SlideIdIdentificationError as exp:
      raise GenDicomFailedError(
          'Failed to determine slide id.', str(exp)
      ) from exp

  def _get_study_series_uid_and_dcm_refs(
      self,
      dcm_store_client: dicom_store_client.DicomStoreClient,
      dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
      slide_id: str,
      wsi_dcm_json: Dict[str, Any],
      initialize_series_uid_from_metadata: bool = False,
  ) -> StudyUIDSeriesUIDExistingDcm:
    """Returns StudyInstanceUID, SeriesInstanceUID.

    Args:
      dcm_store_client: DICOM Store client.
      dicom_gen: Payload for DICOM being generated.
      slide_id: Slide_id of DICOM being generated.
      wsi_dcm_json: WSI DICOM Json formatted metadata.
      initialize_series_uid_from_metadata: Whether to initialize series UID from
        metadata.

    Returns:
      Tuple[StudyInstanceUID, SeriesInstanceUID, List of DICOM Refs for study
        and series in store]
    """
    try:
      study_uid = dicom_json_util.get_study_instance_uid(wsi_dcm_json)
    except dicom_json_util.MissingStudyUIDInMetadataError as exp:
      raise GenDicomFailedError(
          f'Missing Study UID in metadata: {slide_id}',
          ingest_const.ErrorMsgs.MISSING_STUDY_UID,
      ) from exp

    # if a slide was already partially added we should add the same
    # to the existing series. Otherwise create a new series.
    # hash identifies imaging source
    result = (
        dcm_store_client.get_series_uid_and_existing_dicom_for_study_and_hash(
            study_uid, dicom_gen.hash
        )
    )
    series_uid = result.series_instance_uid

    if series_uid is None and initialize_series_uid_from_metadata:
      # enable series uid to be defined in metadata.
      # if enabled automatic ingestion of rescans into new series is disabled.
      # and would need to be handled externally via the series def in metadata.
      try:
        series_uid = dicom_json_util.get_series_instance_uid(wsi_dcm_json)
        cloud_logging_client.logger().info(
            'Series UID defined from metadata.',
            {'SeriesInstanceUID': series_uid},
        )
      except dicom_json_util.MissingSeriesUIDInMetadataError as exp:
        raise GenDicomFailedError(
            f'Missing Series UID in metadata: {slide_id}',
            ingest_const.ErrorMsgs.MISSING_SERIES_UID,
        ) from exp

    if series_uid is None:
      series_uid = uid_generator.generate_uid()
      cloud_logging_client.logger().info(
          'Generating new series instance UID',
          {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: series_uid},
      )
    else:
      cloud_logging_client.logger().info(
          'Ingesting images into existing series',
          {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: series_uid},
      )
    return StudyUIDSeriesUIDExistingDcm(
        study_uid, series_uid, result.preexisting_dicoms_in_store
    )

  def _convert_wsi_to_dicom(
      self,
      file_to_convert: str,
      downsample_config: WsiDownsampleLayers,
      output_path: str,
      study_uid: Optional[str],
      series_uid: Optional[str],
  ) -> List[str]:
    """Runs wsi2dcm tool to convert svs to DICOM.

    Args:
      file_to_convert: Path to file (e.g., svs) to convert.
      downsample_config: WsiDownsampleLayers to generate.
      output_path: Output directory to write generated images to.
      study_uid: StudyUID to set generated DICOM to.
      series_uid: SeriesUID id value to set generated DICOM to.

    Returns:
      List of paths to generated DICOM.

    Raises:
      GenDicomFailedError: if conversion fails
      Wsi2DcmFileNameFormatError: If C++ wsi2dcm tooling is producing files
        that do not identify image downsampling in the filename.
    """
    cmd_line = _get_wsi2dcm_cmdline(
        file_to_convert,
        downsample_config,
        output_path,
        study_uid,
        series_uid,
        self.default_wsi_conv_params,
    )
    try:
      cloud_logging_client.logger().info(
          'Starting WSI-to-DICOM conversion', {'cmd_line': cmd_line}
      )
      subprocess.run(cmd_line, capture_output=True, check=True, shell=True)
    except subprocess.CalledProcessError as exp:
      cloud_logging_client.logger().error(
          'WSI-to-DICOM conversion error',
          {
              'cmd': cmd_line,
              ingest_const.LogKeywords.return_code: exp.returncode,
              ingest_const.LogKeywords.stdout: exp.stdout.decode('utf-8'),
              ingest_const.LogKeywords.stderr: exp.stderr.decode('utf-8'),
          },
          exp,
      )
      raise GenDicomFailedError(
          'An error occurred converting WSI to DICOM.',
          ingest_const.ErrorMsgs.WSI_TO_DICOM_CONVERSION_FAILED,
      ) from exp
    return _get_generated_dicom_files_sorted_by_size(output_path)

  def _get_context_manager(
      self, slide_id: str
  ) -> Union[ContextManager[bool], ContextManager[None]]:
    """Returns Redis based context manager Lock if Redis enabled.

      Lock used to protects against other processes interacting with
      dicom and or metadata stores prior to the data getting written into
      dicomstore. It is safe to move the ingested bits to success folder
      and acknowledge pub/sub msg in unlocked context.

    Args:
      slide_id: slide_id being ingested.

    Returns:
      Redis context manager if redis is enabled.
    """
    if redis_client.redis_client().has_redis_client():
      return redis_client.redis_client().non_blocking_lock(
          name=slide_id,
          value=cloud_logging_client.logger().pod_uid,
          expiry_seconds=ingest_const.MESSAGE_TTL_S,
      )
    else:
      return contextlib.nullcontext()

  def update_metadata(self):
    """Updates container metadata from GCS.

    Raises:
      metadata_storage_client.MetadataDownloadExceptionError: Cannot download
        metadata
    """
    self.metadata_storage_client.update_metadata()

  def _get_dicom_metadata_schema(self, sop_class_name: str) -> Dict[str, Any]:
    """Updates metadata and returns DICOM metadata mapping schema.

    Args:
      sop_class_name: DICOM SOP class name.

    Returns:
      DICOM metadata mapping schema (JSON)

    Raises:
      metadata_storage_client.MetadataDownloadExceptionError: unable to
        download metadata - retryable failure.
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

  def _get_slide_dicom_json_formatted_metadata(
      self,
      sop_class_name: str,
      slide_id: str,
      dicom_schema: Mapping[str, Any],
      dicom_client: dicom_store_client.DicomStoreClient,
      test_metadata_for_missing_study_instance_uid: bool,
  ) -> Tuple[Dict[str, Any], dicom_schema_util.PandasMetadataTableWrapper]:
    """Returns Metadata (DICOM JSON & CSV) for slide.

    Args:
      sop_class_name: DICOM SOP class name.
      slide_id: ID of slide.
      dicom_schema: JSON metadata mapping schema.
      dicom_client: DICOM store client used to correct missing study instance
        uid.
      test_metadata_for_missing_study_instance_uid: If true tests metadata for
        missing study instance uid enables study instance uid creation path.

    Returns:
      Tuple[Json format DICOM metadata, CSV Metadata]

    Raises:
      GenDicomFailedError: unable to generate metadata.
    """
    try:
      bq_table_id = ingest_flags.BIG_QUERY_METADATA_TABLE_FLG.value.strip()
      if bq_table_id:
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
            cloud_logging_client.logger().error(msg, bq_table_log)
            raise GenDicomFailedError(
                msg,
                ingest_const.ErrorMsgs.BQ_METADATA_NOT_FOUND,
            )
        except (
            IndexError,
            decode_slideid.BigQueryMetadataTableEnvFormatError,
        ) as exc:
          msg = f'Error parsing BigQuery table id: {bq_table_id}'
          cloud_logging_client.logger().critical(msg, bq_table_log, exc)
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
      if test_metadata_for_missing_study_instance_uid:
        _correct_missing_study_instance_uid_in_metadata(dcm_json, dicom_client)
      return (dcm_json, metadata_row)
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
    except (
        dicom_json_util.MissingStudyUIDInMetadataError,
        dicom_store_client.StudyInstanceUIDSearchError,
        _AccessionNumberAssociatedWithMultipleStudyInstanceUIDError,
    ) as exp:
      raise GenDicomFailedError(
          'Unable to determine DICOM Study Instance UID.',
          ingest_const.ErrorMsgs.MISSING_STUDY_UID,
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
