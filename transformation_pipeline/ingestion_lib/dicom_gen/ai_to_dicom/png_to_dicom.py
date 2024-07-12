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
"""Converts PNG OOF inference results to DICOM."""

import datetime
import os
from typing import List, Optional

from google.cloud import pubsub_v1
import pydicom

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import abstract_polling_client
from transformation_pipeline.ingestion_lib import cloud_storage_client
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_general_equipment
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_private_tag_generator
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_secondary_capture
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen import ingestion_dicom_store_urls
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.pubsub_msgs import abstract_pubsub_msg
from transformation_pipeline.ingestion_lib.pubsub_msgs import inference_pubsub_msg


class AiPngtoDicomSecondaryCapture(
    abstract_dicom_generation.AbstractDicomGeneration
):
  """Converts PNG image from OOF inference result to DICOM."""

  def __init__(
      self,
      dicom_store_web_path: str,
      use_oof_legacy_pipeline: bool = True,
  ):
    self._use_oof_legacy_pipeline = use_oof_legacy_pipeline
    self._dicom_store_to_clean = (
        ingestion_dicom_store_urls.normalize_dicom_store_url(
            ingest_flags.DICOM_STORE_TO_CLEAN_FLG.value, dicom_web=True
        )
    )
    self._current_msg = None
    if not self._dicom_store_to_clean:
      cloud_logging_client.info(
          f'ENV {ingest_const.EnvVarNames.DICOM_STORE_TO_CLEAN} is undefined. '
          'DICOM ingested specifically for OOF will remain in the store.'
      )
    else:
      cloud_logging_client.info(
          'DICOM ingested specifically for OOF will be deleted.',
          {
              ingest_const.EnvVarNames.DICOM_STORE_TO_CLEAN: (
                  self._dicom_store_to_clean
              )
          },
      )
    super().__init__(dicom_store_web_path)

  def _delete_instance_from_dicom_store(
      self, dicomweb_path: str, study_uid: str, series_uid: str
  ) -> None:
    """Deletes series used for OOF pipeline from DICOM Store.

    Args:
      dicomweb_path: DICOM webpath of store to delete from.
      study_uid: Study uid of series to delete.
      series_uid: Series uid of series to delete.

    Returns:
      None
    """
    # Create a new client for DICOM store to clean up.
    client = dicom_store_client.DicomStoreClient(dicomweb_path=dicomweb_path)
    client.delete_series_from_dicom_store(study_uid, series_uid)

  def decode_pubsub_msg(
      self, msg: pubsub_v1.types.ReceivedMessage
  ) -> inference_pubsub_msg.InferencePubSubMsg:
    """Returns pub/sub message decoder for OOF result ingestion.

    Args:
      msg: pubsub msg

    Returns:
      implementation of InferencePubSubMsg
    """
    self._current_msg = inference_pubsub_msg.InferencePubSubMsg(
        msg, self._use_oof_legacy_pipeline
    )
    cloud_logging_client.info(
        'Decoded OOF pub/sub msg.',
        {ingest_const.LogKeywords.URI: self._current_msg.uri},
    )
    return self._current_msg

  def _convert_png_to_dicom(
      self,
      image_path: str,
      output_path: str,
      study_uid: str,
      series_uid: str,
      dcm_metadata: Optional[pydicom.Dataset] = None,
      instances: Optional[
          List[dicom_secondary_capture.DicomReferencedInstance]
      ] = None,
      private_tags: Optional[
          List[dicom_private_tag_generator.DicomPrivateTag]
      ] = None,
  ) -> List[str]:
    """Runs DicomSecondaryCaptureBuilder to build DICOM instance of image.

    Args:
      image_path: Path to image to be converted.
      output_path: Path to save generated DICOM in.
      study_uid: UID of study to create the DICOM instance.
      series_uid: UID of the series to create the DICOM instance.
      dcm_metadata: Optional DICOM metadata to embed with image.
      instances: Optional List of referenced WSI instances.
      private_tags: Optional DICOM private tags to be created and added to file
        Stored as {tag_name : dicom_private_tag_generator.DicomPrivateTag}

    Returns:
      List of paths to generated Dicoms.
    """
    sc_instance_uid = uid_generator.generate_uid()

    # Generate secondary capture dicom
    cloud_logging_client.info('Building DICOM.')
    dataset = (
        dicom_secondary_capture.create_raw_dicom_secondary_capture_from_img(
            image_path,
            study_uid,
            series_uid,
            sc_instance_uid,
            dcm_metadata=dcm_metadata,
            reference_instances=instances,
            private_tags=private_tags,
        )
    )
    dataset.ImageType = 'DERIVED\\PRIMARY\\AI_RESULT'
    dicom_general_equipment.add_ingest_general_equipment(dataset)
    filename = f'{image_path}.dcm'
    if output_path:
      _, filename = os.path.split(filename)
      filename = os.path.join(output_path, filename)
    dataset.save_as(filename, False)
    cloud_logging_client.info(
        'Saving Dicom.', {ingest_const.LogKeywords.FILENAME: filename}
    )

    generated_dicom_files = []
    with os.scandir(output_path) as it:
      for entry in it:
        if entry.name.endswith('.dcm'):
          generated_dicom_files.append(entry.path)

    return generated_dicom_files

  def handle_unexpected_exception(
      self,
      msg: abstract_pubsub_msg.AbstractPubSubMsg,
      ingest_file: Optional[abstract_dicom_generation.GeneratedDicomFiles],
      exp: Exception,
  ):
    """See base class."""
    cloud_logging_client.critical(
        'An unexpected exception occurred during OOF ingestion.',
        {
            ingest_const.LogKeywords.URI: msg.uri,
            ingest_const.LogKeywords.PUBSUB_MESSAGE_ID: msg.message_id,
        },
        exp,
    )

  def get_slide_transform_lock(
      self,
      unused_ingest_file: abstract_dicom_generation.GeneratedDicomFiles,
      unused_polling_client: abstract_polling_client.AbstractPollingClient,
  ) -> abstract_dicom_generation.TransformationLock:
    """Returns lock to ensure transform processes only one instance of a slide at a time.

    Args:
      unused_ingest_file: File payload to generate into DICOM.
      unused_polling_client: Polling client receiving triggering pub/sub msg.

    Returns:
      Transformation pipeline lock.
    """
    study_uid = self._current_msg.study_uid
    series_uid = self._current_msg.series_uid
    slide_id_lock = ingest_const.RedisLockKeywords.ML_TRIGGERED_INGESTION % (
        study_uid,
        series_uid,
    )
    return abstract_dicom_generation.TransformationLock(slide_id_lock)

  def generate_dicom_and_push_to_store(
      self,
      transform_lock: abstract_dicom_generation.TransformationLock,
      ingest_file: abstract_dicom_generation.GeneratedDicomFiles,
      polling_client: abstract_polling_client.AbstractPollingClient,
  ):
    """Converts downloaded wsi image to DICOM.

    Args:
      transform_lock: Transformation pipeline lock.
      ingest_file: File payload to generate into DICOM.
      polling_client: Polling client receiving triggering pub/sub msg.
    """
    cloud_logging_client.info('Starting OOF ML DICOM ingestion.')
    # Parse message publish_time
    current_msg = self._current_msg
    publish_datetime = datetime.datetime.fromtimestamp(
        current_msg.publish_time.nanosecond, datetime.timezone.utc
    )
    publish_date = publish_datetime.date()
    publish_time = publish_datetime.timetz()

    # Set DICOM metadata
    ds = pydicom.Dataset()
    ds.SecondaryCaptureDeviceManufacturer = ingest_const.SC_MANUFACTURER_NAME
    ds.DateOfSecondaryCapture = publish_date.isoformat().replace('-', '')
    ds.TimeOfSecondaryCapture = publish_time.isoformat('microseconds').replace(
        ':', ''
    )
    ds.SecondaryCaptureDeviceManufacturersModelName = current_msg.model_name
    ds.SecondaryCaptureDeviceSoftwareVersions = current_msg.model_version

    # Pass oof score to be created as private tag with value stored as
    # Decimal String (DS) Clip oof store to keep in DS VR type length limts
    oof_score = current_msg.whole_slide_score
    private_tags = abstract_dicom_generation.get_private_tags_for_gen_dicoms(
        ingest_file, current_msg.message_id
    )
    try:
      oof_score = str(round(float(oof_score), 3))
      private_tags.append(
          dicom_private_tag_generator.DicomPrivateTag(
              ingest_const.DICOMTagKeywords.OOF_SCORE_PRIVATE_TAG,
              'DS',
              oof_score,
          )
      )
      cloud_logging_client.info(
          'Adding rounded OOF score',
          {
              'msg_oof_score': str(current_msg.whole_slide_score),
              'rounded_score': str(oof_score),
          },
      )
    except ValueError:
      pass
    if not private_tags:
      cloud_logging_client.warning(
          'OOF Result without oof score',
          {'pub/msg_oof_score': str(current_msg.whole_slide_score)},
      )

    dicom_gen_dir = os.path.join(self.root_working_dir, 'gen_dicom')
    os.mkdir(dicom_gen_dir)
    ingest_file.generated_dicom_files = self._convert_png_to_dicom(
        ingest_file.localfile,
        dicom_gen_dir,
        current_msg.study_uid,
        current_msg.series_uid,
        dcm_metadata=ds,
        instances=current_msg.instances,
        private_tags=private_tags,
    )
    # URI to copy ingested images to at pipeline completion.
    # None = not copied & images will be deleted following ingest.
    ingest_file.destination_uri = None

    if not ingest_file.generated_dicom_files:
      cloud_logging_client.error('An error occurred converting to DICOM.')
      polling_client.nack()
      return
    if not self.validate_redis_lock_held(transform_lock):
      polling_client.nack(
          retry_ttl=ingest_flags.TRANSFORMATION_LOCK_RETRY_FLG.value
      )
      return
    dst_metadata = {
        'pubsub_message_id': str(polling_client.current_msg.message_id)
    }
    ingest_results = self.dcm_store_client.upload_to_dicom_store(
        ingest_file.generated_dicom_files,
        dicom_store_client.DiscoverExistingSeriesOptions.USE_STUDY_AND_SERIES,
        dst_metadata,
    )
    ingested_dicoms = ingest_results.ingested
    previously_ingested_dicoms = ingest_results.previously_ingested

    oof_source_dicom_store = (
        ingestion_dicom_store_urls.normalize_dicom_store_url(
            self._current_msg.dicomstore_path, dicom_web=True
        )
    )
    # Check if clean up is needed and clean up instances used for ML.
    if oof_source_dicom_store == self._dicom_store_to_clean:
      source_dcm_in_main_store = self._current_msg.additional_params.get(
          ingest_const.OofPassThroughKeywords.SOURCE_DICOM_IN_MAIN_STORE, True
      )
      if source_dcm_in_main_store:
        cloud_logging_client.warning(
            'ML ingestion appears to be incorrectly configured to delete'
            ' DICOM from the main store. OOF DICOM will not be deleted. To'
            ' fix: set env'
            f' {ingest_const.EnvVarNames.DICOM_STORE_TO_CLEAN} to the OOF'
            ' DICOM store.'
        )
      else:
        self._delete_instance_from_dicom_store(
            oof_source_dicom_store,
            self._current_msg.study_uid,
            self._current_msg.series_uid,
        )

    if ingested_dicoms or previously_ingested_dicoms:
      # very last step is to delete blob from ingest bucket.
      if not cloud_storage_client.del_blob(
          uri=ingest_file.source_uri, ignore_file_not_found=True
      ):
        # if delete failed retry
        polling_client.nack()
        return
    polling_client.ack()
    if ingested_dicoms:
      self.log_debug_url(
          viewer_debug_url=self._viewer_debug_url,
          ingested_dicom=ingested_dicoms[0],
      )
