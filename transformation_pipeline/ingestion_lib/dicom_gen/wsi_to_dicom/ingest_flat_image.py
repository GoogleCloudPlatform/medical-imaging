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
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import decode_slideid
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base


_OUTPUT_DICOM_FILENAME = 'output.dcm'


class IngestFlatImage(ingest_base.IngestBase):
  """Converts flat image to DICOM."""

  def __init__(self, ingest_buckets: ingest_base.GcsIngestionBuckets):
    super().__init__(ingest_buckets)
    if ingest_flags.FLAT_IMAGES_VL_MICROSCOPIC_IMAGE_IOD_FLG.value:
      self._sop_class = ingest_const.DicomSopClasses.MICROSCOPIC_IMAGE
    else:
      self._sop_class = ingest_const.DicomSopClasses.SLIDE_COORDINATES_IMAGE

  def _determine_flat_image_slideid(self, filename: str) -> str:
    """Determines slide id for flat image.

    Args:
      filename: Filename to test for slide id.

    Returns:
      SlideID as string.

    Raises:
      ingest_base.GenDicomFailedError: Error resulting from inability to
        determine slide id
    """
    try:
      slide_id = decode_slideid.get_slide_id_from_filename(
          filename, self.metadata_storage_client
      )
      cloud_logging_client.logger().info(
          'Slide ID identified in ingested filename.',
          {
              ingest_const.LogKeywords.filename: filename,
              ingest_const.LogKeywords.SLIDE_ID: slide_id,
          },
      )
      return slide_id
    except decode_slideid.SlideIdIdentificationError as exp:
      raise ingest_base.GenDicomFailedError(
          'Could not find slide id in filename.', str(exp)
      ) from exp

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
    """
    upload_file_list = []
    try:
      dcm_schema = self._get_dicom_metadata_schema(self._sop_class.name)
      slide_id = self._determine_flat_image_slideid(dicom_gen.localfile)
      dcm_json, _ = self._get_slide_dicom_json_formatted_metadata(
          self._sop_class.name,
          slide_id,
          dcm_schema,
          abstract_dicom_handler.dcm_store_client,
          test_metadata_for_missing_study_instance_uid=True,
      )

      result = self._get_study_series_uid_and_dcm_refs(
          abstract_dicom_handler.dcm_store_client, dicom_gen, slide_id, dcm_json
      )
      study_uid = result.study_instance_uid
      series_uid = result.series_instance_uid
      instance_uid = uid_generator.generate_uid()
      dcm = dicom_slide_coordinates_microscopic_image.create_encapsulated_flat_image_dicom(
          dicom_gen.localfile,
          study_uid,
          series_uid,
          instance_uid,
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
      dcm.save_as(generated_dicom_path, write_like_original=False)
      dicom_gen.generated_dicom_files = [generated_dicom_path]
      upload_file_list = dicom_gen.generated_dicom_files
      dest_uri = self.ingest_succeeded_uri
    except (
        dicom_slide_coordinates_microscopic_image.InvalidFlatImageError,
        ingest_base.GenDicomFailedError,
    ) as exp:
      dest_uri = self.log_and_get_failure_bucket_path(exp)

    return ingest_base.GenDicomResult(
        dicom_gen,
        dest_uri,
        ingest_base.DicomInstanceIngestionSets(upload_file_list),
        generated_series_instance_uid=True,
    )
