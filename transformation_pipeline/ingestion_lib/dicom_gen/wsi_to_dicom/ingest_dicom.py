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
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_json_util
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_private_tag_generator
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import decode_slideid
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base


_OUTPUT_DICOM_FILENAME = 'output.dcm'
DICOM_ALREADY_INGESTED_TAG_ADDRESSES = frozenset([
    ingest_const.DICOMTagKeywords.DICOM_GOOGLE_PRIVATE_CREATOR_BLOCK_TAG,
    ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG,
    ingest_const.DICOMTagKeywords.OOF_SCORE_PRIVATE_TAG,
    ingest_const.DICOMTagKeywords.PUBSUB_MESSAGE_ID_TAG,
    ingest_const.DICOMTagKeywords.INGEST_FILENAME_TAG,
])
_DICOM_ALREADY_INGESTED_TAGS = frozenset(
    [
        pydicom.tag.Tag(address)
        for address in DICOM_ALREADY_INGESTED_TAG_ADDRESSES
    ]
)


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
  cloud_logging_client.logger().debug(
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
      ingest_buckets: Optional[ingest_base.GcsIngestionBuckets] = None,
  ):
    super().__init__(ingest_buckets)
    self._dicom_store_triggered_ingest = dicom_store_triggered_ingest
    self._override_study_uid_with_metadata = override_study_uid_with_metadata

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
      self, dcm: pydicom.Dataset, dicom_path: str
  ) -> str:
    """Determines slide id for DICOM instance.

    Args:
      dcm: Pydicom dataset.
      dicom_path: Filename to test for slide id.

    Returns:
      SlideID as string.

    Raises:
      ingest_base.GenDicomFailedError: Error resulting from inability to
        determine slide id
    """
    # Attempt to get slide id from barcode tag in DICOM file.
    current_exception = None
    if ingest_const.DICOMTagKeywords.BARCODE_VALUE in dcm:
      try:
        slide_id = decode_slideid.find_slide_id_in_metadata(
            dcm.BarcodeValue, self.metadata_storage_client
        )
        cloud_logging_client.logger().info(
            'Slide ID identified in DICOM BarcodeValue tag.',
            {ingest_const.LogKeywords.SLIDE_ID: slide_id},
        )
        return slide_id
      except decode_slideid.SlideIdIdentificationError as exp:
        current_exception = exp
    if not self._dicom_store_triggered_ingest:
      try:
        # Attempt to get slide id from ingested file filename.
        slide_id = decode_slideid.get_slide_id_from_filename(
            dicom_path,
            self.metadata_storage_client,
            current_exception,
        )
        cloud_logging_client.logger().info(
            'Slide ID identified in ingested filename.',
            {
                ingest_const.LogKeywords.filename: dicom_path,
                ingest_const.LogKeywords.SLIDE_ID: slide_id,
            },
        )
        return slide_id
      except decode_slideid.SlideIdIdentificationError as exp:
        current_exception = exp
    if current_exception is not None:
      raise ingest_base.GenDicomFailedError(
          'Failed to determine slide id.',
          str(current_exception),
      ) from current_exception
    raise ingest_base.GenDicomFailedError(
        'Failed to determine slide id.', ingest_const.ErrorMsgs.SLIDE_ID_MISSING
    )

  def _update_metadata(
      self,
      dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
      dcm: pydicom.Dataset,
      slide_id: str,
      message_id: str,
      dcm_store_client: dicom_store_client.DicomStoreClient,
  ) -> None:
    """Reads and updates metadata for DICOM instance.

    Args:
      dicom_gen: File payload to convert into DICOM.
      dcm: Pydicom dataset to add metadata.
      slide_id: DICOM slide ID.
      message_id: Polling client message id.
      dcm_store_client: DICOM store client.
    """
    sop_class_name = dcm[ingest_const.DICOMTagKeywords.SOP_CLASS_UID].repval
    dcm_schema = self._get_dicom_metadata_schema(sop_class_name)
    dcm_json, _ = self._get_slide_dicom_json_formatted_metadata(
        sop_class_name,
        slide_id,
        dcm_schema,
        dcm_store_client,
        test_metadata_for_missing_study_instance_uid=True,
    )
    if self._override_study_uid_with_metadata:
      # Ignore (i.e. do not override) SeriesInstanceUID and SOPInstanceUID only.
      tags_to_ignore = set([
          pydicom.tag.Tag(ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID),
          pydicom.tag.Tag(ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID),
      ])
    else:
      tags_to_ignore = dicom_json_util.UID_TRIPLE_TAGS
    dicom_json_util.merge_json_metadata_with_pydicom_ds(
        dcm, dcm_json, tags_to_ignore
    )

    private_tags = abstract_dicom_generation.get_private_tags_for_gen_dicoms(
        dicom_gen, message_id
    )
    dicom_private_tag_generator.DicomPrivateTagGenerator.add_dicom_private_tags(
        private_tags, dcm
    )
    dicom_util.add_general_metadata_to_dicom(dcm)

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
        ingest_const.LogKeywords.study_instance_uid: study_instance_uid,
        ingest_const.LogKeywords.SERIES_INSTANCE_UID: series_instance_uid,
        ingest_const.LogKeywords.sop_instance_uid: sop_instance_uid,
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
      cloud_logging_client.logger().warning(
          'Error occurred querying DICOM store for instance metadata. Possibly '
          'due to DICOM instance metadata update in progress.',
          log,
          exp,
      )
      return False
    log[ingest_const.LogKeywords.METADATA] = dicom_json_metadata
    if not dicom_json_metadata:
      cloud_logging_client.logger().warning(
          'Could not find metadata for DICOM instance. Possibly '
          'due to DICOM instance metadata update in progress.',
          log,
      )
      return False
    elif len(dicom_json_metadata) > 1:
      cloud_logging_client.logger().warning(
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
    """
    upload_file_list = []
    try:
      dcm = self._read_dicom(dicom_gen.localfile)
      slide_id = self._determine_dicom_slideid(dcm, dicom_gen.localfile)
      self._update_metadata(
          dicom_gen,
          dcm,
          slide_id,
          message_id,
          abstract_dicom_handler.dcm_store_client,
      )

      generated_dicom_path = os.path.join(dicom_gen_dir, _OUTPUT_DICOM_FILENAME)
      dcm.save_as(generated_dicom_path, write_like_original=False)
      dicom_gen.generated_dicom_files = [generated_dicom_path]
      upload_file_list = dicom_gen.generated_dicom_files
      dest_uri = self.ingest_succeeded_uri
    except ingest_base.GenDicomFailedError as exp:
      dest_uri = self.log_and_get_failure_bucket_path(exp)

    return ingest_base.GenDicomResult(
        dicom_gen,
        dest_uri,
        ingest_base.DicomInstanceIngestionSets(upload_file_list),
        generated_series_instance_uid=False,
    )
