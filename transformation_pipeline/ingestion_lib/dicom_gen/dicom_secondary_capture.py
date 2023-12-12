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
"""Builder to generate DICOM from secondary capture IOD."""

import dataclasses
import json
import math
import time
from typing import Any, Dict, List, Optional, Tuple

import cv2
from hcls_imaging_ml_toolkit import dicom_builder
from hcls_imaging_ml_toolkit import dicom_json
from hcls_imaging_ml_toolkit import tag_values
from hcls_imaging_ml_toolkit import tags
import pydicom

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_private_tag_generator
from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard


# Accepted character set.
_ISO_CHARACTER_SET = 'ISO_IR 192'


@dataclasses.dataclass(frozen=True)
class DicomReferencedInstance:
  """Defines a DICOM Referenced Instance by Class uid and Instance uid."""

  referenced_sop_class_uid: str
  referenced_sop_instance_uid: str


class DicomSecondaryCaptureBuilder(dicom_builder.DicomBuilder):
  """Extension of DicomBuilder for specific generation of RGB Secondary Capture instances."""

  def BuildJsonInstanceFromJpg(
      self,
      image: Optional[bytes],
      image_size: Tuple[int, int],
      metadata_json: Dict[str, Any],
      study_uid: str,
      series_uid: str,
      instance_uid: str,
      photo_metric_intrep: str = 'RGB',
  ) -> dicom_json.ObjectWithBulkData:  # pytype: disable=invalid-annotation
    """Builds a DICOM Secondary Capture from JPG.

    Args:
      image: Image bytes of JPG.
      image_size: Tuple representating image dimensions as height, width.
      metadata_json: Dict of tags (including study-level information) to add.
      study_uid: UID of study to create the SC in.
      series_uid: UID of the series to create the SC in.
      instance_uid: UID of the SC instance.
      photo_metric_intrep: PHOTOMETRIC_INTERPRETATION defaults to RGB

    Returns:
      DICOM JSON Object containing JSON and bulk data of the JPG Secondary
      Capture.
    """

    dicom_json.Insert(
        metadata_json, tags.SOP_CLASS_UID, tag_values.SECONDARY_CAPTURE_CUID
    )
    dicom_json.Insert(metadata_json, tags.STUDY_INSTANCE_UID, study_uid)
    dicom_json.Insert(metadata_json, tags.SERIES_INSTANCE_UID, series_uid)
    dicom_json.Insert(
        metadata_json, tags.SPECIFIC_CHARACTER_SET, _ISO_CHARACTER_SET
    )
    dicom_json.Insert(metadata_json, tags.SOP_INSTANCE_UID, instance_uid)

    if image is None:
      bulkdata = []
    else:
      # Assures URI is unique.
      uri = f'{study_uid}/{series_uid}/{instance_uid}'
      metadata_json[tags.PIXEL_DATA.number] = {
          'vr': tags.PIXEL_DATA.vr,
          'BulkDataURI': uri,
      }
      bulkdata = [
          dicom_json.DicomBulkData(
              uri=uri, data=image, content_type='image/jpeg; transfer-syntax=""'
          )
      ]
    dicom_json.Insert(metadata_json, tags.PLANAR_CONFIGURATION, 0)
    dicom_json.Insert(
        metadata_json, tags.PHOTOMETRIC_INTERPRETATION, photo_metric_intrep
    )
    dicom_json.Insert(metadata_json, tags.SAMPLES_PER_PIXEL, 3)
    dicom_json.Insert(metadata_json, tags.ROWS, image_size[0])
    dicom_json.Insert(metadata_json, tags.COLUMNS, image_size[1])
    dicom_json.Insert(metadata_json, tags.BITS_ALLOCATED, 8)
    dicom_json.Insert(metadata_json, tags.BITS_STORED, 8)
    dicom_json.Insert(metadata_json, tags.HIGH_BIT, 7)
    dicom_json.Insert(metadata_json, tags.PIXEL_REPRESENTATION, 0)

    return dicom_json.ObjectWithBulkData(metadata_json, bulkdata)


def _add_current_time_to_dicom(ds: pydicom.FileDataset):
  """Adds current UCT date time to as DICOM content time.

  Args:
    ds: pydicom dataset
  """
  current_time_sec = time.time()
  time_str = time.strftime('%H%M%S', time.gmtime(current_time_sec))
  time_fraction = current_time_sec - math.floor(current_time_sec)
  time_fraction = math.floor(time_fraction * 1000000)
  ds.ContentTime = f'{time_str}.{time_fraction}'
  ds.ContentDate = time.strftime('%Y%m%d', time.gmtime(current_time_sec))


def _add_instance_refs_to_dicom(
    ds: pydicom.FileDataset,
    series_uid: str,
    instances: Optional[List[DicomReferencedInstance]] = None,
):
  """Add referenced instances to dataset.

  Args:
    ds: pydicom dataset
    series_uid: series uid to reference =
    instances: List of dicom instances to reference.
  """
  if instances:
    instance_ref_list = []
    for i in instances:
      instance_ds = pydicom.dataset.Dataset()
      instance_ds.ReferencedSOPClassUID = i.referenced_sop_class_uid
      instance_ds.ReferencedSOPInstanceUID = i.referenced_sop_instance_uid
      instance_ref_list.append(instance_ds)
    series_ref = pydicom.dataset.Dataset()
    series_ref.SeriesInstanceUID = series_uid
    series_ref.ReferencedInstanceSequence = pydicom.sequence.Sequence(
        instance_ref_list
    )
    ds.ReferencedSeriesSequence = pydicom.sequence.Sequence([series_ref])


def _log_create_sec_capture(
    capture_type: str,
    source: str,
    study_uid: str,
    series_uid: str,
    instance_uid: Optional[str] = None,
    instance_number: Optional[str] = None,
):
  cloud_logging_client.logger().info(
      'Creating DICOM secondary capture',
      {
          'type': capture_type,
          'image_source': source,
          ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID: study_uid,
          ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: series_uid,
          ingest_const.DICOMTagKeywords.SOP_INSTANCE_UID: str(instance_uid),
          ingest_const.DICOMTagKeywords.INSTANCE_NUMBER: str(instance_number),
      },
  )


def create_raw_dicom_secondary_capture_from_img(
    image_path: str,
    study_uid: str,
    series_uid: str,
    instance_uid: str,
    instance_number: Optional[str] = None,
    dcm_json: Optional[Dict[str, Any]] = None,
    instances: Optional[List[DicomReferencedInstance]] = None,
    private_tags: Optional[
        List[dicom_private_tag_generator.DicomPrivateTag]
    ] = None,
) -> pydicom.Dataset:
  """Creates a dicom secondary capture for a image, stores image bytes in raw.

  Args:
    image_path: path to image
    study_uid: DICOM study uid for image
    series_uid: DICOM series uid for image
    instance_uid: DICOM instance_uid for image
    instance_number: Dicom instance number for image
    dcm_json: Optional Dicom metadata to embed in image
    instances: Optional List of referenced WSI instances
    private_tags: Optional Dicom private tags to be created and added to file
      Stored as {tag_name : dicom_private_tag_generator.DicomPrivateTag}

  Returns:
    Pydicom dataset representing secondary capture
  """
  # get image bytes and shape
  _log_create_sec_capture(
      'create_raw_dicom_secondary_capture_from_img',
      image_path,
      study_uid,
      series_uid,
      instance_uid,
      instance_number,
  )

  im = cv2.imread(image_path)
  if im.shape[2] == 3:
    im = cv2.cvtColor(im, cv2.COLOR_RGB2BGR)
  elif im.shape[2] == 4:
    im = cv2.cvtColor(im, cv2.COLOR_RGBA2BGR)
  image_shape = im.shape[:2]

  if dcm_json is None:
    dcm_json = dict()
  sec_capture_dicom = DicomSecondaryCaptureBuilder()
  dicom_builder_result = sec_capture_dicom.BuildJsonInstanceFromJpg(
      None, image_shape, dcm_json, study_uid, series_uid, instance_uid, 'RGB'
  )
  dcm_json = dicom_builder_result.dicom_dict
  base_ds = pydicom.Dataset.from_json(json.dumps(dcm_json))

  # Initialize secondary capture dicom header
  # 'Secondary Capture Image Storage'
  # Image bytes Implicit VR Endian: Default Transfer Syntax for DICOM
  # RAW bytes image.

  file_meta = pydicom.dataset.FileMetaDataset()
  sop_class_id = dicom_standard.dicom_standard_util().get_sop_classname_uid(
      'Secondary Capture Image Storage'
  )
  file_meta.MediaStorageSOPClassUID = sop_class_id
  file_meta.TransferSyntaxUID = ingest_const.EXPLICIT_VR_LITTLE_ENDIAN
  file_meta.MediaStorageSOPInstanceUID = instance_uid
  file_meta.ImplementationClassUID = ingest_const.SC_IMPLEMENTATION_CLASS_UID
  # end todo

  ds = pydicom.dataset.FileDataset(
      '', base_ds, file_meta=file_meta, preamble=b'\0' * 128
  )

  # Initialize content date time
  _add_current_time_to_dicom(ds)
  if instance_number is not None:
    ds.InstanceNumber = instance_number

  # define additional image tags
  ds.is_little_endian = True
  ds.is_implicit_VR = False
  ds.NumberOfFrames = 1
  ds.LossyImageCompression = '00'  # Image is not lossy

  # Technically not part of SC IOD adding to match wsi images
  rows, columns = image_shape
  ds.TotalPixelMatrixRows = rows
  ds.TotalPixelMatrixColumns = columns
  ds.PixelData = im.tobytes()

  # Add private tags
  if private_tags:
    dicom_private_tag_generator.DicomPrivateTagGenerator.add_dicom_private_tags(
        private_tags, ds
    )

  _add_instance_refs_to_dicom(ds, series_uid, instances)
  return ds


def create_encapsulated_jpeg_dicom_secondary_capture(
    jpg_image_path: str,
    study_uid: str,
    series_uid: str,
    instance_uid: str,
    instance_number: Optional[str] = None,
    dcm_json: Optional[Dict[str, Any]] = None,
    instances: Optional[List[DicomReferencedInstance]] = None,
    private_tags: Optional[
        List[dicom_private_tag_generator.DicomPrivateTag]
    ] = None,
) -> pydicom.Dataset:
  """Embedd jpeg bits directing in dicom secondary capture (no recompression).

  Args:
    jpg_image_path: path to image
    study_uid: DICOM study uid for image
    series_uid: DICOM series uid for image
    instance_uid: DICOM instance_uid for image
    instance_number: Dicom instance number for image
    dcm_json: Optional icom metadata to embed in image
    instances: Optional List of referenced WSI instances
    private_tags: Optional Dicom private tags to be created and added to file
      Stored as {tag_name : dicom_private_tag_generator.DicomPrivateTag}

  Returns:
    Pydicom dataset representing secondary capture
  """
  _log_create_sec_capture(
      'create_encapsulated_jpeg_dicom_secondary_capture',
      jpg_image_path,
      study_uid,
      series_uid,
      instance_uid,
      instance_number,
  )
  # get image shape
  img = cv2.imread(jpg_image_path)
  image_shape = img.shape[:2]
  del img

  # get image bytes
  with open(jpg_image_path, 'rb') as img:
    image_bytes = img.read()

  if dcm_json is None:
    dcm_json = dict()
  sec_capture_dicom = DicomSecondaryCaptureBuilder()
  dicom_builder_result = sec_capture_dicom.BuildJsonInstanceFromJpg(
      None,
      image_shape,
      dcm_json,
      study_uid,
      series_uid,
      instance_uid,
      'YBR_FULL_422',
  )
  dcm_json = dicom_builder_result.dicom_dict
  base_ds = pydicom.Dataset.from_json(json.dumps(dcm_json))

  # Initialize secondary capture dicom header
  # 'Secondary Capture Image Storage'
  # Image bytes JPEG Baseline (Process 1):
  # Default Transfer Syntax for Lossy JPEG 8-bit Image Compression
  file_meta = pydicom.dataset.FileMetaDataset()
  sop_class_id = dicom_standard.dicom_standard_util().get_sop_classname_uid(
      'Secondary Capture Image Storage'
  )
  file_meta.MediaStorageSOPClassUID = sop_class_id
  file_meta.TransferSyntaxUID = ingest_const.DicomImageTransferSyntax.JPEG_LOSSY
  file_meta.MediaStorageSOPInstanceUID = instance_uid
  file_meta.ImplementationClassUID = ingest_const.SC_IMPLEMENTATION_CLASS_UID
  # end todo

  ds = pydicom.dataset.FileDataset(
      '', base_ds, file_meta=file_meta, preamble=b'\0' * 128
  )

  # Initialize content date time
  _add_current_time_to_dicom(ds)
  if instance_number is not None:
    ds.InstanceNumber = instance_number

  # define additional image tags
  ds.is_little_endian = True
  ds.is_implicit_VR = False
  ds.NumberOfFrames = 1
  ds.LossyImageCompression = '01'  # Image is a lossy jpg
  ds.LossyImageCompressionMethod = 'ISO_10918_1'
  rows, columns = image_shape
  ds.LossyImageCompressionRatio = str(
      round((rows * columns * 3) / len(image_bytes), 2)
  )
  # Technically not part of SC IOD adding to match wsi images
  ds.TotalPixelMatrixRows = rows
  ds.TotalPixelMatrixColumns = columns
  pixel_data = pydicom.encaps.encapsulate([image_bytes])
  if len(pixel_data) % 2:
    pixel_data += b'\x00'
  ds.PixelData = pixel_data
  ds['PixelData'].is_undefined_length = True

  # Add private tags
  if private_tags:
    dicom_private_tag_generator.DicomPrivateTagGenerator.add_dicom_private_tags(
        private_tags, ds
    )

  _add_instance_refs_to_dicom(ds, series_uid, instances)
  return ds
