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
"""Converts a flat image to VL Slide-Coordinates Microscopic Image DICOM."""

import os

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_private_tag_generator
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_slide_coordinates_microscopic_image
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import decode_slideid
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client


_OUTPUT_DICOM_FILENAME = 'output.dcm'


class IngestFlatImage(ingest_base.IngestBase):
  """Converts flat image to DICOM."""

  def __init__(
      self,
      ingest_buckets: ingest_base.GcsIngestionBuckets,
      metadata_client: metadata_storage_client.MetadataStorageClient,
  ):
    super().__init__(ingest_buckets, metadata_client)
    if ingest_flags.FLAT_IMAGES_VL_MICROSCOPIC_IMAGE_IOD_FLG.value:
      self._sop_class = ingest_const.DicomSopClasses.MICROSCOPIC_IMAGE
    else:
      self._sop_class = ingest_const.DicomSopClasses.SLIDE_COORDINATES_IMAGE

  def _generate_metadata_free_slide_metadata(
      self, slide_id: str, dicom_client: dicom_store_client.DicomStoreClient
  ) -> ingest_base.DicomMetadata:
    return ingest_base.generate_metadata_free_slide_metadata(
        slide_id, dicom_client
    )

  def _determine_flat_image_slideid(
      self, dicom_gen: abstract_dicom_generation.GeneratedDicomFiles
  ) -> str:
    """Determines slide id for flat image.

    Args:
      dicom_gen: File payload to convert into DICOM.

    Returns:
      SlideID as string.

    Raises:
      ingest_base.GenDicomFailedError: Error resulting from inability to
        determine slide id
    """
    try:
      slide_id = decode_slideid.get_slide_id_from_filename(
          dicom_gen,
          self.metadata_storage_client,
      )
      cloud_logging_client.info(
          'Slide ID identified in ingested filename.',
          {
              ingest_const.LogKeywords.FILENAME: dicom_gen.localfile,
              ingest_const.LogKeywords.SLIDE_ID: slide_id,
              ingest_const.LogKeywords.SOURCE_URI: dicom_gen.source_uri,
          },
      )
      return slide_id
    except decode_slideid.SlideIdIdentificationError as exp:
      raise ingest_base.GenDicomFailedError(
          'Could not find slide id in filename.', str(exp)
      ) from exp

  def get_slide_id(
      self,
      dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
      unused_abstract_dicom_handler: abstract_dicom_generation.AbstractDicomGeneration,
  ) -> str:
    """Returns slide id of ingested SVS.

    Args:
      dicom_gen: File payload to convert into full DICOM WSI image pyramid.
      unused_abstract_dicom_handler: Abstract_dicom pub/sub message handler
        calling generate_dicom.

    Raises:
      DetermineSlideIDError: Cannot determine slide id of ingested DICOM.
    """
    try:
      return self.set_slide_id(
          self._determine_flat_image_slideid(dicom_gen), False
      )
    except ingest_base.GenDicomFailedError as exp:
      if ingest_flags.ENABLE_METADATA_FREE_INGESTION_FLG.value:
        return self.set_slide_id(
            decode_slideid.get_metadata_free_slide_id(dicom_gen), True
        )
      dest_uri = self.log_and_get_failure_bucket_path(exp)
      raise ingest_base.DetermineSlideIDError(dicom_gen, dest_uri) from exp

  def _init_series_instance_uid_from_metadata(self) -> bool:
    """Returns True if Series instance UID should be generated from metadata.

    If slide is ingested using metadata free transformation then the series uid
    should always come from the value embedded in the DICOM. Otherwise return
    value in metadata series initalization flag.
    """
    if self.is_metadata_free_slide_id:
      return False
    return ingest_flags.INIT_SERIES_INSTANCE_UID_FROM_METADATA_FLG.value

  def generate_dicom(
      self,
      dicom_gen_dir: str,
      dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
      message_id: str,
      abstract_dicom_handler: abstract_dicom_generation.AbstractDicomGeneration,
  ) -> ingest_base.GenDicomResult:
    """Converts flat image to VL Slide-Coordinates Microscopic Image DICOM.

    Supports JPG image format only.

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
    generated_series_instance_uid = True
    if self.slide_id is None:
      raise ValueError('Slideid is not set.')
    try:
      dcm_json = self.get_slide_dicom_json_formatted_metadata(
          self._sop_class.name,
          self.slide_id,
          abstract_dicom_handler.dcm_store_client,
      ).dicom_json
      generated_series_instance_uid = ingest_base.initialize_metadata_study_and_series_uids_for_non_dicom_image_triggered_ingestion(
          abstract_dicom_handler,
          dicom_gen,
          dcm_json,
          self._init_series_instance_uid_from_metadata(),
      )
      dcm = dicom_slide_coordinates_microscopic_image.create_encapsulated_flat_image_dicom(
          dicom_gen.localfile,
          uid_generator.generate_uid(),
          self._sop_class,
          dcm_json,
      )

      private_tags = abstract_dicom_generation.get_private_tags_for_gen_dicoms(
          dicom_gen, message_id
      )
      dicom_private_tag_generator.DicomPrivateTagGenerator.add_dicom_private_tags(
          private_tags, dcm
      )
      dicom_util.add_general_metadata_to_dicom(dcm)

      generated_dicom_path = os.path.join(dicom_gen_dir, _OUTPUT_DICOM_FILENAME)
      dicom_util.add_missing_type2_dicom_metadata(dcm)
      dcm.save_as(generated_dicom_path, write_like_original=False)
      dicom_gen.generated_dicom_files = [generated_dicom_path]
      upload_file_list = dicom_gen.generated_dicom_files
      dest_uri = self.ingest_succeeded_uri
    except (
        dicom_slide_coordinates_microscopic_image.InvalidFlatImageError,
        ingest_base.GenDicomFailedError,
    ) as exp:
      upload_file_list = []
      dest_uri = self.log_and_get_failure_bucket_path(exp)

    return ingest_base.GenDicomResult(
        dicom_gen,
        dest_uri,
        ingest_base.DicomInstanceIngestionSets(upload_file_list),
        generated_series_instance_uid=generated_series_instance_uid,
    )
