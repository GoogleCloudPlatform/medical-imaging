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
import math
import time
from typing import List, Optional, Tuple

import cv2
import numpy as np
import PIL.Image
import pydicom

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_private_tag_generator


@dataclasses.dataclass(frozen=True)
class DicomReferencedInstance:
  """Defines a DICOM Referenced Instance by Class uid and Instance uid."""

  referenced_sop_class_uid: str
  referenced_sop_instance_uid: str
  implementation_version_name: str = ingest_const.IMPLEMENTATION_VERSION_NAME


class UnsupportedDICOMSecondaryCaptureImageError(Exception):

  def __init__(self):
    super().__init__(
        'Unsupported DICOM secondary capture image.',
        ingest_const.ErrorMsgs.UNSUPPORTED_DICOM_SECONDARY_CAPTURE_IMAGE,
    )


def _add_current_time_to_dicom(ds: pydicom.Dataset):
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
    ds: pydicom.Dataset,
    series_uid: str,
    instances: Optional[List[DicomReferencedInstance]] = None,
):
  """Add referenced instances to dataset.

  Args:
    ds: pydicom dataset
    series_uid: series uid to reference =
    instances: List of dicom instances to reference.
  """
  if not instances:
    return
  instance_ref_list = []
  for instance in instances:
    instance_ds = pydicom.dataset.Dataset()
    instance_ds.ReferencedSOPClassUID = instance.referenced_sop_class_uid
    instance_ds.ReferencedSOPInstanceUID = instance.referenced_sop_instance_uid
    instance_ds.ImplementationVersionName = instance.implementation_version_name
    instance_ref_list.append(instance_ds)
  series_ref = pydicom.dataset.Dataset()
  series_ref.SeriesInstanceUID = series_uid
  series_ref.ReferencedInstanceSequence = pydicom.sequence.Sequence(
      instance_ref_list
  )
  ds.ReferencedSeriesSequence = pydicom.sequence.Sequence([series_ref])


def _build_dicom_instance(
    pixel_data: bytes,
    image_size: Tuple[int, int],
    samples_per_pixel: int,
    dcm_metadata: Optional[pydicom.Dataset],
    study_uid: str,
    series_uid: str,
    instance_uid: str,
    photo_metric_intrep: str,
    instance_number: Optional[int],
    reference_instances: Optional[List[DicomReferencedInstance]],
    private_tags: Optional[List[dicom_private_tag_generator.DicomPrivateTag]],
) -> pydicom.FileDataset:
  """Builds a DICOM Secondary Capture from JPG.

  Args:
    pixel_data: Image bytes.
    image_size: Tuple representating image dimensions as height, width.
    samples_per_pixel: Number of color channels per pixel.
    dcm_metadata: DICOM tags (including study-level information) to add.
    study_uid: UID of study to create the SC in.
    series_uid: UID of the series to create the SC in.
    instance_uid: UID of the SC instance.
    photo_metric_intrep: PHOTOMETRIC_INTERPRETATION defaults to RGB
    instance_number: Dicom instance number for image.
    reference_instances: Optional List of referenced WSI instances.
    private_tags: Optional Dicom private tags to be created and added to file
      Stored as {tag_name : dicom_private_tag_generator.DicomPrivateTag}.

  Returns:
    DICOM JSON Object containing JSON and bulk data of the JPG Secondary
    Capture.
  """
  ds = dcm_metadata if dcm_metadata is not None else pydicom.Dataset()
  ds.SOPClassUID = ingest_const.DicomSopClasses.SECONDARY_CAPTURE_IMAGE.uid
  ds.StudyInstanceUID = study_uid
  ds.SeriesInstanceUID = series_uid
  ds.SOPInstanceUID = instance_uid
  ds.SamplesPerPixel = samples_per_pixel
  ds.Columns, ds.Rows = image_size
  ds.PlanarConfiguration = 0
  ds.PhotometricInterpretation = photo_metric_intrep
  ds.BitsAllocated = 8
  ds.BitsStored = 8
  ds.HighBit = 7
  ds.PixelRepresentation = 0
  ds.NumberOfFrames = 1
  # Initialize content date time
  _add_current_time_to_dicom(ds)
  if len(pixel_data) % 2:
    pixel_data += b'\x00'
  ds.PixelData = pixel_data
  file_meta = pydicom.dataset.FileMetaDataset()
  file_meta.MediaStorageSOPClassUID = ds.SOPClassUID
  file_meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID
  # Technically not part of SC IOD adding to match wsi images
  ds.TotalPixelMatrixRows = ds.Rows
  ds.TotalPixelMatrixColumns = ds.Columns
  if instance_number is not None:
    ds.InstanceNumber = str(instance_number)
  # Add private tags
  if private_tags:
    dicom_private_tag_generator.DicomPrivateTagGenerator.add_dicom_private_tags(
        private_tags, ds
    )
  _add_instance_refs_to_dicom(ds, series_uid, reference_instances)
  file_meta.ImplementationClassUID = ingest_const.SC_IMPLEMENTATION_CLASS_UID
  file_meta.ImplementationVersionName = ingest_const.IMPLEMENTATION_VERSION_NAME
  # end todo
  fd = pydicom.dataset.FileDataset(
      '', ds, file_meta=file_meta, preamble=b'\0' * 128
  )
  fd.is_little_endian = True
  fd.is_implicit_VR = False
  return fd


def _log_create_sec_capture(
    capture_type: str,
    source: str,
    study_uid: str,
    series_uid: str,
    instance_uid: Optional[str] = None,
    instance_number: Optional[int] = None,
):
  cloud_logging_client.info(
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
    instance_number: Optional[int] = None,
    dcm_metadata: Optional[pydicom.Dataset] = None,
    reference_instances: Optional[List[DicomReferencedInstance]] = None,
    private_tags: Optional[
        List[dicom_private_tag_generator.DicomPrivateTag]
    ] = None,
) -> pydicom.FileDataset:
  """Creates a dicom secondary capture for a image, stores image bytes in raw.

  Args:
    image_path: path to image
    study_uid: DICOM study uid for image
    series_uid: DICOM series uid for image
    instance_uid: DICOM instance_uid for image
    instance_number: Dicom instance number for image
    dcm_metadata: Optional Dicom metadata to embed in image
    reference_instances: Optional List of referenced WSI instances
    private_tags: Optional Dicom private tags to be created and added to file
      Stored as {tag_name : dicom_private_tag_generator.DicomPrivateTag}

  Returns:
    Pydicom dataset representing secondary capture

  Raises:
    UnsupportedDICOMSecondaryCaptureImageError: PIL image mode is not supported.
  """
  _log_create_sec_capture(
      'create_raw_dicom_secondary_capture_from_img',
      image_path,
      study_uid,
      series_uid,
      instance_uid,
      instance_number,
  )
  # get image bytes and shape
  with PIL.Image.open(image_path) as img:
    image_shape = img.size
    im = np.asarray(img)  # Expect pixels decoded in Monochrome or RGB byte
    samples_per_pixel = 3  # order.
    if img.mode == 'L':
      samples_per_pixel = 1
    elif img.mode == 'RGBA':
      im = cv2.cvtColor(im, cv2.COLOR_RGBA2RGB)
    elif img.mode == 'HSV':
      im = cv2.cvtColor(im, cv2.COLOR_HSV2RGB_FULL)
    elif img.mode == 'LAB':
      im = cv2.cvtColor(im, cv2.COLOR_Lab2RGB)
    elif img.mode != 'RGB':
      cloud_logging_client.error(f'Unsupported image mode: {img.mode}')
      raise UnsupportedDICOMSecondaryCaptureImageError()
  # Initialize secondary capture dicom header
  # 'Secondary Capture Image Storage'
  ds = _build_dicom_instance(
      im.tobytes(),
      image_shape,
      samples_per_pixel,
      dcm_metadata,
      study_uid,
      series_uid,
      instance_uid,
      'RGB',
      instance_number,
      reference_instances,
      private_tags,
  )
  ds.file_meta.TransferSyntaxUID = (
      ingest_const.DicomImageTransferSyntax.EXPLICIT_VR_LITTLE_ENDIAN
  )
  ds.LossyImageCompression = '00'  # Image is encoded as RAW RGB bytes
  return ds


def create_encapsulated_jpeg_dicom_secondary_capture(
    jpg_image_path: str,
    study_uid: str,
    series_uid: str,
    instance_uid: str,
    instance_number: Optional[int] = None,
    dcm_metadata: Optional[pydicom.Dataset] = None,
    reference_instances: Optional[List[DicomReferencedInstance]] = None,
    private_tags: Optional[
        List[dicom_private_tag_generator.DicomPrivateTag]
    ] = None,
) -> pydicom.FileDataset:
  """Embed jpeg bits directing in dicom secondary capture (no recompression).

  Args:
    jpg_image_path: path to image.
    study_uid: DICOM study uid for image.
    series_uid: DICOM series uid for image.
    instance_uid: DICOM instance_uid for image.
    instance_number: Dicom instance number for image.
    dcm_metadata: Optional Dicom dataset metadata to embed in image.
    reference_instances: Optional List of referenced WSI instances.
    private_tags: Optional Dicom private tags to be created and added to file
      Stored as {tag_name : dicom_private_tag_generator.DicomPrivateTag}.

  Returns:
    Pydicom dataset representing secondary capture

  Raises:
    UnsupportedDICOMSecondaryCaptureImageError: PIL format of image passed !=
      'JPEG' or PIL image mode is not supported.
  """
  _log_create_sec_capture(
      'create_encapsulated_jpeg_dicom_secondary_capture',
      jpg_image_path,
      study_uid,
      series_uid,
      instance_uid,
      instance_number,
  )
  with PIL.Image.open(jpg_image_path) as img:
    image_shape = img.size
    if img.format != 'JPEG':
      cloud_logging_client.error(f'Unsupported image format: {img.format}')
      raise UnsupportedDICOMSecondaryCaptureImageError()
    if img.mode == 'L':
      samples_per_pixel = 1
    elif img.mode == 'RGB':
      samples_per_pixel = 3
    else:
      cloud_logging_client.error(f'Unsupported image mode: {img.mode}')
      raise UnsupportedDICOMSecondaryCaptureImageError()
  # get image bytes
  with open(jpg_image_path, 'rb') as img:
    image_bytes = img.read()

  ds = _build_dicom_instance(
      pydicom.encaps.encapsulate([image_bytes]),
      image_shape,
      samples_per_pixel,
      dcm_metadata,
      study_uid,
      series_uid,
      instance_uid,
      'YBR_FULL_422',
      instance_number,
      reference_instances,
      private_tags,
  )
  # Image bytes JPEG Baseline (Process 1):
  # Default Transfer Syntax for Lossy JPEG 8-bit Image Compression
  ds.file_meta.TransferSyntaxUID = (
      ingest_const.DicomImageTransferSyntax.JPEG_LOSSY
  )
  ds.LossyImageCompression = '01'  # Image is a lossy jpg
  ds.LossyImageCompressionMethod = 'ISO_10918_1'
  ds.LossyImageCompressionRatio = str(
      round((ds.Rows * ds.Columns * ds.SamplesPerPixel) / len(image_bytes), 2)
  )
  ds['PixelData'].is_undefined_length = True
  return ds
