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
"""Ingest DICOM instance."""

import json
import os
from typing import Any, Mapping, Optional, Set

import pydicom
import requests

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_json_util
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_private_tag_generator
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import decode_slideid
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client


_OUTPUT_DICOM_FILENAME = 'output.dcm'
DICOM_ALREADY_INGESTED_TAG_ADDRESSES = frozenset([
    ingest_const.DICOMTagKeywords.DICOM_GOOGLE_PRIVATE_CREATOR_BLOCK_TAG,
    ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG,
    ingest_const.DICOMTagKeywords.OOF_SCORE_PRIVATE_TAG,
    ingest_const.DICOMTagKeywords.PUBSUB_MESSAGE_ID_TAG,
    ingest_const.DICOMTagKeywords.INGEST_FILENAME_TAG,
])
_DICOM_ALREADY_INGESTED_TAGS = frozenset([
    pydicom.tag.Tag(address) for address in DICOM_ALREADY_INGESTED_TAG_ADDRESSES
])


class UnexpectedDicomMetadataError(Exception):
  pass


def _contains_already_ingested_dicom_tags(
    metadata_tags: Set[str], log: Optional[Mapping[str, Any]] = None
) -> bool:
  """Tests if DICOM metadata tag address indicates DICOM was ingested by DPAS.

  Args:
    metadata_tags: Set of DICOM tag addresses.
    log: Structured logging entry.

  Returns:
    True if DICOM JSON metadata indicates DICOM was ingested by DPAS.
  """
  result = bool(
      metadata_tags.intersection(DICOM_ALREADY_INGESTED_TAG_ADDRESSES)
  )
  cloud_logging_client.debug(
      'DICOM instance has been processed by the transformation pipeline.'
      if result
      else 'DICOM instance not processed by the transformation pipeline.',
      log,
  )
  return result


class IngestDicom(ingest_base.IngestBase):
  """Ingest existing DICOM instance, adding relevant metadata."""

  def __init__(
      self,
      dicom_store_triggered_ingest: bool,
      override_study_uid_with_metadata: bool,
      metadata_client: metadata_storage_client.MetadataStorageClient,
      ingest_buckets: Optional[ingest_base.GcsIngestionBuckets] = None,
  ):
    super().__init__(ingest_buckets, metadata_client)
    self._dicom_store_triggered_ingest = dicom_store_triggered_ingest
    self._override_study_uid_with_metadata = override_study_uid_with_metadata
    self._dcm = None

  def _read_dicom(self, path: str) -> pydicom.FileDataset:
    """Reads DICOM file.

    Args:
      path: Path to DICOM file.

    Returns:
      Pydicom dataset.

    Raises:
      ingest_base.GenDicomFailedError: If unable to read DICOM.
    """
    try:
      with pydicom.dcmread(path, force=False) as dcm:
        return dcm
    except (
        pydicom.errors.InvalidDicomError,
        TypeError,
        FileNotFoundError,
    ) as exp:
      raise ingest_base.GenDicomFailedError(
          f'Failed to read DICOM from: {path}.',
          ingest_const.ErrorMsgs.INVALID_DICOM,
      ) from exp

  def _determine_dicom_slideid(
      self,
      dcm: pydicom.Dataset,
      dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
  ) -> str:
    """Determines slide id for DICOM instance.

    Args:
      dcm: Pydicom dataset.
      dicom_gen: File payload to convert into DICOM.

    Returns:
      SlideID as string.

    Raises:
      ingest_base.GenDicomFailedError: Error resulting from inability to
        determine slide id
    """
    # Attempt to get slide id from barcode tag in DICOM file.
    current_exception = None
    if not self._dicom_store_triggered_ingest:
      try:
        # Attempt to get slide id from ingested file filename.
        slide_id = decode_slideid.get_slide_id_from_filename(
            dicom_gen, self.metadata_storage_client
        )
        cloud_logging_client.info(
            'Slide ID identified in ingested filename.',
            {
                ingest_const.LogKeywords.FILENAME: dicom_gen.localfile,
                ingest_const.LogKeywords.SOURCE_URI: dicom_gen.source_uri,
                ingest_const.LogKeywords.SLIDE_ID: slide_id,
            },
        )
        return slide_id
      except decode_slideid.SlideIdIdentificationError as exp:
        current_exception = exp
    if ingest_const.DICOMTagKeywords.BARCODE_VALUE in dcm:
      try:
        slide_id = decode_slideid.find_slide_id_in_metadata(
            dcm.BarcodeValue, self.metadata_storage_client
        )
        cloud_logging_client.info(
            'Slide ID identified in DICOM BarcodeValue tag.',
            {ingest_const.LogKeywords.SLIDE_ID: slide_id},
        )
        return slide_id
      except decode_slideid.SlideIdIdentificationError as exp:
        current_exception = decode_slideid.highest_error_level_exception(
            current_exception, exp
        )
    if current_exception is not None:
      raise ingest_base.GenDicomFailedError(
          'Failed to determine slide id.',
          str(current_exception),
      ) from current_exception
    raise ingest_base.GenDicomFailedError(
        'Failed to determine slide id.', ingest_const.ErrorMsgs.SLIDE_ID_MISSING
    )

  def _generate_metadata_free_slide_metadata(
      self,
      unused_slide_id: str,
      unused_dicom_client: dicom_store_client.DicomStoreClient,
  ) -> ingest_base.DicomMetadata:
    return ingest_base.generate_empty_slide_metadata()

  def _set_study_instance_uid_from_metadata(self) -> bool:
    """Returns True if Study instance UID should be generated from metadata.

    If slide is ingested using metadata free transformation or as a result of
    dicom store metadata update then the study uid should always come from the
    value embedded in the DICOM. Otherwise return the value in metadata
    study initalization flag.
    """
    if self.is_metadata_free_slide_id or self._dicom_store_triggered_ingest:
      return False
    return self._override_study_uid_with_metadata

  def _set_series_instance_uid_from_metadata(self) -> bool:
    """Returns True if Series instance UID should be generated from metadata.

    If slide is ingested using metadata free transformation or as a result of
    dicom store metadata update then the series uid should always come from the
    value embedded in the DICOM. Otherwise return value in metadata
    series initalization flag.
    """
    if self.is_metadata_free_slide_id or self._dicom_store_triggered_ingest:
      return False
    return ingest_flags.INIT_SERIES_INSTANCE_UID_FROM_METADATA_FLG.value

  def _update_metadata(
      self,
      dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
      dcm: pydicom.Dataset,
      slide_id: str,
      message_id: str,
      abstract_dicom_handler: abstract_dicom_generation.AbstractDicomGeneration,
  ) -> None:
    """Reads and updates metadata for DICOM instance.

    Args:
      dicom_gen: File payload to convert into DICOM.
      dcm: Pydicom dataset to add metadata.
      slide_id: DICOM slide ID.
      message_id: Polling client message id.
      abstract_dicom_handler: dicom generation handler.
    """
    sop_class_name = dcm[ingest_const.DICOMTagKeywords.SOP_CLASS_UID].repval
    dcm_json = self.get_slide_dicom_json_formatted_metadata(
        sop_class_name,
        slide_id,
        abstract_dicom_handler.dcm_store_client,
    ).dicom_json
    ingest_base.initialize_metadata_study_and_series_uids_for_dicom_triggered_ingestion(
        dcm.StudyInstanceUID,
        dcm.SeriesInstanceUID,
        dcm_json,
        abstract_dicom_handler,
        self._set_study_instance_uid_from_metadata(),
        self._set_series_instance_uid_from_metadata(),
    )
    dicom_json_util.merge_json_metadata_with_pydicom_ds(dcm, dcm_json)

    private_tags = abstract_dicom_generation.get_private_tags_for_gen_dicoms(
        dicom_gen,
        message_id,
        include_integest_filename_tag=not self._dicom_store_triggered_ingest,
    )
    dicom_private_tag_generator.DicomPrivateTagGenerator.add_dicom_private_tags(
        private_tags, dcm
    )
    dicom_util.add_general_metadata_to_dicom(dcm)
    dicom_util.add_missing_type2_dicom_metadata(dcm)

  def is_dicom_instance_already_ingested(
      self,
      ds_client: dicom_store_client.DicomStoreClient,
      study_instance_uid: str,
      series_instance_uid: str,
      sop_instance_uid: str,
  ) -> bool:
    """Test DICOM metadata to determine if DICOM instance has been processed.

       Ignore errors during metadata testing. Instance is not in store that
       may not indicate error, instance could be in backupstore. If errors
       occur, continue. Errors will be handled during metadata update.

    Args:
      ds_client: DICOM store client.
      study_instance_uid: DICOM Study Instance UID.
      series_instance_uid: DICOM Series Instance UID.
      sop_instance_uid: DICOM SOP Instance UID.

    Returns:
      True if instance metadata indicates instance was processed by the
      pipeline.

    Raises:
      UnexpectedDicomMetadataError: DICOM metadata contains more than 1 dataset.
    """
    log = {
        ingest_const.LogKeywords.STUDY_INSTANCE_UID: study_instance_uid,
        ingest_const.LogKeywords.SERIES_INSTANCE_UID: series_instance_uid,
        ingest_const.LogKeywords.SOP_INSTANCE_UID: sop_instance_uid,
    }
    try:
      dicom_json_metadata = ds_client.get_instance_tags_json(
          study_instance_uid,
          series_instance_uid,
          sop_instance_uid,
          DICOM_ALREADY_INGESTED_TAG_ADDRESSES,
      )
    except (json.JSONDecodeError, requests.HTTPError) as exp:
      # If unable to determine metadata pipleine may be in the process of
      # of processing DICOM. return False to continue processing
      cloud_logging_client.warning(
          'Error occurred querying DICOM store for instance metadata. Possibly '
          'due to DICOM instance metadata update in progress.',
          log,
          exp,
      )
      return False
    log[ingest_const.LogKeywords.METADATA] = dicom_json_metadata
    if not dicom_json_metadata:
      cloud_logging_client.warning(
          'Could not find metadata for DICOM instance. Possibly '
          'due to DICOM instance metadata update in progress.',
          log,
      )
      return False
    elif len(dicom_json_metadata) > 1:
      cloud_logging_client.warning(
          'DICOM store instance query returned more than one dataset.',
          log,
      )
      raise UnexpectedDicomMetadataError()
    dicom_instance_tags = set(dicom_json_metadata[0])
    log[ingest_const.LogKeywords.DICOM_TAGS] = dicom_instance_tags
    return _contains_already_ingested_dicom_tags(dicom_instance_tags, log)

  def is_dicom_file_already_ingested(self, dcm_path: str) -> bool:
    """Returns whether DICOM was already ingested by DPAS.

    Args:
      dcm_path: Path to DICOM file.
    """
    try:
      with pydicom.dcmread(
          dcm_path,
          defer_size='512 KB',
          force=False,
          specific_tags=_DICOM_ALREADY_INGESTED_TAGS,
      ) as dcm:
        # Specific Character Set tag is included if reading specific tags. Using
        # intersection to original set of specific tags to avoid this issue.
        # Intersection calculated between DICOM JSON formtted tags.
        dicom_instance_tags = set(
            [f'{tag.group:04X}{tag.element:04X}' for tag in dcm.keys()]
        )
        return _contains_already_ingested_dicom_tags(
            dicom_instance_tags,
            {ingest_const.LogKeywords.DICOM_TAGS: dicom_instance_tags},
        )
    except (pydicom.errors.InvalidDicomError, TypeError, FileNotFoundError):
      return False

  def init_handler_for_ingestion(self) -> None:
    super().init_handler_for_ingestion()
    self._dcm = None

  def get_slide_id(
      self,
      dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
      unused_abstract_dicom_handler: abstract_dicom_generation.AbstractDicomGeneration,
  ) -> str:
    """Returns lock to ensure transform processes only one instance of a slide at a time.

    Args:
      dicom_gen: File payload to generate into DICOM.
      unused_abstract_dicom_handler: Polling client receiving triggering pub/sub
        msg.

    Returns:
      Name of lock
    """
    try:
      self._dcm = self._read_dicom(dicom_gen.localfile)
      return self.set_slide_id(
          self._determine_dicom_slideid(self._dcm, dicom_gen), False
      )
    except ingest_base.GenDicomFailedError as exp:
      if (
          not self._dicom_store_triggered_ingest
          and ingest_flags.ENABLE_METADATA_FREE_INGESTION_FLG.value
      ):
        return self.set_slide_id(
            decode_slideid.get_metadata_free_slide_id(dicom_gen), True
        )
      dest_uri = self.log_and_get_failure_bucket_path(exp)
      raise ingest_base.DetermineSlideIDError(dicom_gen, dest_uri) from exp

  def generate_dicom(
      self,
      dicom_gen_dir: str,
      dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
      message_id: str,
      abstract_dicom_handler: abstract_dicom_generation.AbstractDicomGeneration,
  ) -> ingest_base.GenDicomResult:
    """Ingests existing DICOM instance.

    Corresponding CSV metadata and generated metadata are incorporated into
    DICOM instance.

    Args:
      dicom_gen_dir: Directory to generate DICOM files in.
      dicom_gen: File payload to convert into DICOM.
      message_id: pub/sub msg id.
      abstract_dicom_handler: Abstract_dicom pub/sub message handler calling
        generate_dicom.

    Returns:
      GenDicomResult describing generated DICOM

    Raises:
      ValueError: Slide ID is undefined.
    """
    dcm = self._dcm
    if self.slide_id is None:
      raise ValueError('Slide id is not set.')
    try:
      self._update_metadata(
          dicom_gen,
          dcm,
          self.slide_id,
          message_id,
          abstract_dicom_handler,
      )

      generated_dicom_path = os.path.join(dicom_gen_dir, _OUTPUT_DICOM_FILENAME)
      dcm.save_as(generated_dicom_path, write_like_original=False)
      dicom_gen.generated_dicom_files = [generated_dicom_path]
      upload_file_list = dicom_gen.generated_dicom_files
      dest_uri = self.ingest_succeeded_uri
    except ingest_base.GenDicomFailedError as exp:
      upload_file_list = []
      dest_uri = self.log_and_get_failure_bucket_path(exp)

    return ingest_base.GenDicomResult(
        dicom_gen,
        dest_uri,
        ingest_base.DicomInstanceIngestionSets(upload_file_list),
        generated_series_instance_uid=False,
    )
