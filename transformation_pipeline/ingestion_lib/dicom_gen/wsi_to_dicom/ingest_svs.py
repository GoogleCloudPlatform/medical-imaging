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
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_dicom_gen
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_image_extractor
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base
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
    frame_of_ref_md: dicom_util.DicomFrameOfReferenceModuleMetadata,
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
    frame_of_ref_md: DICOM frame of reference module metadata.
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
      cloud_logging_client.logger().error(
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
      dicom_util.add_metadata_to_dicom(
          frame_of_ref_md, dcm_json, private_tags, dcm_file
      )
      dcm_file.save_as(dicom_path, write_like_original=True)
    except pydicom.errors.InvalidDicomError as exp:
      cloud_logging_client.logger().error(
          'WSI-to-DICOM conversion created invalid DICOM file.',
          {ingest_const.LogKeywords.filename: dicom_path},
          exp,
      )
      raise


class IngestSVS(ingest_base.IngestBase):
  """Converts WSI image to DICOM."""

  def __init__(
      self,
      ingest_buckets: ingest_base.GcsIngestionBuckets,
      is_oof_ingestion_enabled: bool,
  ):
    super().__init__(ingest_buckets)
    self._is_oof_ingestion_enabled = is_oof_ingestion_enabled

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
    """
    upload_file_list = []
    force_upload_to_main_store_list = []
    pyramid_level_config = None
    try:
      ancillary_images = (
          ancillary_image_extractor.get_ancillary_images_from_svs(
              dicom_gen.localfile, abstract_dicom_handler.img_dir
          )
      )
      wsi_dicom_schema = self._get_dicom_metadata_schema(
          ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.name
      )
      slide_id = self._determine_slideid(dicom_gen.localfile, ancillary_images)

      # Throws redis_client.CouldNotAcquireNonBlockingLockError if
      # context manager is redis_client.redis_client().non_blocking_lock
      # and lock cannot be acquired.
      #
      # Lock used to protects against other processes interacting with
      # dicom and or metadata stores prior to the data getting written into
      # dicomstore. It is safe to move the ingested bits to success folder
      # and acknowledge pub/sub msg in unlocked context.
      with self._get_context_manager(slide_id):
        wsi_dcm_json, _ = self._get_slide_dicom_json_formatted_metadata(
            ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.name,
            slide_id,
            wsi_dicom_schema,
            abstract_dicom_handler.dcm_store_client,
            test_metadata_for_missing_study_instance_uid=True,
        )

        result = self._get_study_series_uid_and_dcm_refs(
            abstract_dicom_handler.dcm_store_client,
            dicom_gen,
            slide_id,
            wsi_dcm_json,
            ingest_flags.INIT_SERIES_INSTANCE_UID_FROM_METADATA_FLG.value,
        )
        study_uid = result.study_instance_uid
        series_uid = result.series_instance_uid
        preexisting_dicoms_in_store = result.preexisting_dicoms_in_store

        pixel_spacing = wsi_pyramid_gen_config.get_openslide_pixel_spacing(
            dicom_gen.localfile
        )
        pyramid_level_config = self.get_downsamples_to_generate(
            pixel_spacing, self._is_oof_ingestion_enabled
        )

        dicom_gen.generated_dicom_files = self._convert_wsi_to_dicom(
            dicom_gen.localfile,
            pyramid_level_config,
            dicom_gen_dir,
            study_uid=study_uid,
            series_uid=series_uid,
        )

        if not dicom_gen.generated_dicom_files:
          cloud_logging_client.logger().error(
              'A error occurred converting to DICOM'
          )
          raise ingest_base.GenDicomFailedError('dicom_conversion_failed')
        svs_metadata = dicom_util.get_svs_metadata(dicom_gen.localfile)
        private_tags = (
            abstract_dicom_generation.get_private_tags_for_gen_dicoms(
                dicom_gen, message_id
            )
        )
        frame_of_ref_md = dicom_util.create_dicom_frame_of_ref_module_metadata(
            study_uid,
            series_uid,
            abstract_dicom_handler.dcm_store_client,
            preexisting_dicom_refs=preexisting_dicoms_in_store,
        )

        add_metadata_to_wsi_dicom_files(
            dicom_gen, private_tags, frame_of_ref_md, svs_metadata, wsi_dcm_json
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
                ancillary_images,
                study_uid,
                series_uid,
                wsi_dcm_json,
                private_tags,
                frame_of_ref_md,
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
      dest_uri = self.log_and_get_failure_bucket_path(exp)
    except pydicom.errors.InvalidDicomError as exp:
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
        True,
    )
