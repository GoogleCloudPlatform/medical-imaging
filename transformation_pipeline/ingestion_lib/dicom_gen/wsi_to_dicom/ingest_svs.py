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
"""Converts WSI SVS to DICOM images."""

import os
from typing import Any, Dict, List, Mapping, Optional

import pydicom

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_private_tag_generator
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_dicom_gen
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_image_extractor
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import decode_slideid
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import wsi_pyramid_gen_config
from transformation_pipeline.ingestion_lib.dicom_util import pydicom_util


def _add_burned_in_annotation_and_specimen_label_in_image_if_not_def(
    dcm_file: pydicom.Dataset,
) -> None:
  pydicom_util.set_dataset_tag_value_if_undefined(
      dcm_file, ingest_const.DICOMTagKeywords.BURNED_IN_ANNOTATION, 'NO'
  )
  pydicom_util.set_dataset_tag_value_if_undefined(
      dcm_file, ingest_const.DICOMTagKeywords.SPECIMEN_LABEL_IN_IMAGE, 'NO'
  )


def add_metadata_to_wsi_dicom_files(
    gen_dicom: abstract_dicom_generation.GeneratedDicomFiles,
    private_tags: List[dicom_private_tag_generator.DicomPrivateTag],
    additional_wsi_metadata: pydicom.Dataset,
    svs_metadata: Mapping[str, pydicom.DataElement],
    dcm_json: Optional[Dict[str, Any]] = None,
) -> None:
  """Augment WSI gen DICOM.

     * Add content date & time
     * Add ImplementationClassUID
     * replace sopinstanceuid
     replacing uid with uids generated using common tooling so all uid are
     generated with same algorithm and can have optional generator defined
     prefix.

  Args:
    gen_dicom: Abstract_dicom_generation.GeneratedDicomFiles.
    private_tags: List of private tags to add to DICOM.
    additional_wsi_metadata: Additional metadata to merge with gen DICOM.
    svs_metadata: SVS metadata to merge with dicom.
    dcm_json: Dicom JSON to merge with generated DICOM.

  Raises:
    ancillary_image_extractor.TiffSeriesNotFoundError: Baseline Series not found
      in svs image.
  """
  if not gen_dicom.generated_dicom_files:
    return
  try:
    # Get ICC color profile from Baseline series
    icc = ancillary_image_extractor.image_icc(gen_dicom.localfile)
  except ancillary_image_extractor.TiffSeriesNotFoundError:
    _, extension = os.path.splitext(gen_dicom.localfile)
    if extension.lower() == '.svs':
      cloud_logging_client.error(
          'Could not find Baseline series in ingested image'
      )
      raise
    icc = None
  for dicom_path in gen_dicom.generated_dicom_files:
    try:
      dcm_file = pydicom.dcmread(dicom_path, force=False)
      _add_burned_in_annotation_and_specimen_label_in_image_if_not_def(dcm_file)
      dicom_util.set_all_defined_pydicom_tags(dcm_file, svs_metadata)
      dicom_util.add_default_optical_path_sequence(dcm_file, icc)
      dicom_util.add_default_total_pixel_matrix_origin_sequence_if_not_defined(
          dcm_file
      )
      dicom_util.add_openslide_dicom_properties(dcm_file, gen_dicom.localfile)
      dicom_util.add_metadata_to_generated_wsi_dicom(
          additional_wsi_metadata, dcm_json, private_tags, dcm_file
      )
      dicom_util.add_missing_type2_dicom_metadata(dcm_file)
      dicom_util.if_missing_create_encapsulated_frame_offset_table(dcm_file)
      dcm_file.save_as(dicom_path, write_like_original=True)
    except pydicom.errors.InvalidDicomError as exp:
      cloud_logging_client.error(
          'WSI-to-DICOM conversion created invalid DICOM file.',
          {ingest_const.LogKeywords.FILENAME: dicom_path},
          exp,
      )
      raise


class IngestSVS(ingest_base.IngestBase):
  """Converts WSI image to DICOM."""

  def __init__(
      self,
      ingest_buckets: ingest_base.GcsIngestionBuckets,
      is_oof_ingestion_enabled: bool,
      metadata_client: metadata_storage_client.MetadataStorageClient,
  ):
    super().__init__(ingest_buckets, metadata_client)
    self._is_oof_ingestion_enabled = is_oof_ingestion_enabled
    # state used across method calls for slide ingestion.
    self._ancillary_images = None

  def _determine_slideid(
      self,
      dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
      ancillary_images: List[ancillary_image_extractor.AncillaryImage],
  ) -> str:
    """Determines slide id for ingested DICOM.

    Args:
      dicom_gen: File payload to convert into DICOM.
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
          dicom_gen, self.metadata_storage_client
      )
      cloud_logging_client.info(
          'Slide ID identified in file name.',
          {
              ingest_const.LogKeywords.FILENAME: dicom_gen.localfile,
              ingest_const.LogKeywords.SOURCE_URI: dicom_gen.source_uri,
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
      cloud_logging_client.info(
          'Slide ID identified in image barcode.',
          {ingest_const.LogKeywords.SLIDE_ID: slide_id},
      )
      return slide_id
    except decode_slideid.SlideIdIdentificationError as exp:
      raise ingest_base.GenDicomFailedError(
          'Failed to determine slide id.', str(exp)
      ) from exp

  def init_handler_for_ingestion(self) -> None:
    super().init_handler_for_ingestion()
    self._ancillary_images = None

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
    try:
      self._ancillary_images = (
          ancillary_image_extractor.get_ancillary_images_from_svs(
              dicom_gen.localfile, abstract_dicom_handler.img_dir
          )
      )
      return self.set_slide_id(
          self._determine_slideid(dicom_gen, self._ancillary_images), False
      )
    except (
        ingest_base.GenDicomFailedError,
        ancillary_image_extractor.TiffSeriesNotFoundError,
    ) as exp:
      if ingest_flags.ENABLE_METADATA_FREE_INGESTION_FLG.value:
        return self.set_slide_id(
            decode_slideid.get_metadata_free_slide_id(dicom_gen), True
        )
      raise ingest_base.DetermineSlideIDError(
          dicom_gen, self.log_and_get_failure_bucket_path(exp)
      ) from exp

  def _generate_metadata_free_slide_metadata(
      self, slide_id: str, dicom_client: dicom_store_client.DicomStoreClient
  ) -> ingest_base.DicomMetadata:
    return ingest_base.generate_metadata_free_slide_metadata(
        slide_id, dicom_client
    )

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
    """Converts downloaded wsi SVS image to full DICOM WSI Pyramid.

    Args:
      dicom_gen_dir: directory to generate DICOM files in.
      dicom_gen: File payload to convert into full DICOM WSI image pyramid.
      message_id: pub/sub msg id.
      abstract_dicom_handler: Abstract_dicom pub/sub message handler calling
        generate_dicom.

    Returns:
      GenDicomResult describing generated dicom

    Raises:
      ValueError: Slide ID is undefined.
    """
    generated_series_instance_uid = True
    pyramid_level_config = None
    if self.slide_id is None:
      raise ValueError('Slideid is not set.')
    try:
      wsi_dcm_json = self.get_slide_dicom_json_formatted_metadata(
          ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.name,
          self.slide_id,
          abstract_dicom_handler.dcm_store_client,
      ).dicom_json
      generated_series_instance_uid = ingest_base.initialize_metadata_study_and_series_uids_for_non_dicom_image_triggered_ingestion(
          abstract_dicom_handler,
          dicom_gen,
          wsi_dcm_json,
          self._init_series_instance_uid_from_metadata(),
      )

      pixel_spacing = wsi_pyramid_gen_config.get_openslide_pixel_spacing(
          dicom_gen.localfile
      )
      pyramid_level_config = self.get_downsamples_to_generate(
          pixel_spacing, self._is_oof_ingestion_enabled
      )

      dicom_gen.generated_dicom_files = self.convert_wsi_to_dicom(
          dicom_gen.localfile,
          pyramid_level_config,
          dicom_gen_dir,
          wsi_dcm_json,
      )

      if not dicom_gen.generated_dicom_files:
        cloud_logging_client.error('A error occurred converting to DICOM')
        raise ingest_base.GenDicomFailedError('dicom_conversion_failed')
      svs_metadata = dicom_util.get_svs_metadata(dicom_gen.localfile)
      private_tags = abstract_dicom_generation.get_private_tags_for_gen_dicoms(
          dicom_gen, message_id
      )

      additional_wsi_metadata = (
          dicom_util.get_additional_wsi_specific_dicom_metadata(
              abstract_dicom_handler.dcm_store_client,
              wsi_dcm_json,
          )
      )
      add_metadata_to_wsi_dicom_files(
          dicom_gen,
          private_tags,
          additional_wsi_metadata,
          svs_metadata,
          wsi_dcm_json,
      )
      additional_ancillary_dicom_metadata = dicom_util.get_pydicom_tags(
          dicom_gen.generated_dicom_files[0],
          ingest_const.DICOMTagKeywords.SHARED_FUNCTIONAL_GROUPS_SEQUENCE,
      )
      additional_ancillary_dicom_metadata.update(svs_metadata)
      upload_file_list = dicom_gen.generated_dicom_files
      force_upload_to_main_store_list = (
          ancillary_dicom_gen.generate_ancillary_dicom(
              dicom_gen,
              self._ancillary_images,
              wsi_dcm_json,
              private_tags,
              additional_wsi_metadata,
              additional_ancillary_dicom_metadata,
          )
      )
      dest_uri = self.ingest_succeeded_uri
    except (
        ingest_base.GenDicomFailedError,
        dicom_util.InvalidICCProfileError,
        ancillary_image_extractor.TiffSeriesNotFoundError,
        wsi_pyramid_gen_config.MissingPixelSpacingError,
    ) as exp:
      upload_file_list = []
      force_upload_to_main_store_list = []
      dest_uri = self.log_and_get_failure_bucket_path(exp)
    except pydicom.errors.InvalidDicomError as exp:
      upload_file_list = []
      force_upload_to_main_store_list = []
      dest_uri = self.log_and_get_failure_bucket_path(
          ingest_base.GenDicomFailedError(
              exp, ingest_const.ErrorMsgs.WSI_TO_DICOM_INVALID_DICOM_GENERATED
          )
      )

    return ingest_base.GenDicomResult(
        dicom_gen,
        dest_uri,
        ingest_base.DicomInstanceIngestionSets(
            upload_file_list,
            pyramid_level_config,
            force_upload_to_main_store_list,
        ),
        generated_series_instance_uid,
    )
