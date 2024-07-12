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
"""Builder to generate VL Slide-Coordinates Microscopic Image DICOM."""

from collections.abc import Mapping, Sequence
import dataclasses
import datetime
import io
import json
import os
from typing import Any, Optional

import PIL
import pydicom
import tifffile

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_json_util
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_image_extractor
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util

_MAX_IMAGE_SIZE = 65535
_SUPPORTED_IMAGE_FORMATS = frozenset(['JPEG', 'PNG', 'TIFF'])

# DICOM related constants.
_DICOM_IMAGE_TYPE_ORIGINAL = 'ORIGINAL\\PRIMARY'
_DICOM_IMAGE_TYPE_DERIVED = 'DERIVED\\PRIMARY'

# TIF related constants.
_TIF_X_RESOLUTION_TAG = 282
_TIF_Y_RESOLUTION_TAG = 283
_TIF_RESOLUTION_UNIT_TAG = 296
_TIF_DATE_TIME_TAG = 306
_TIF_DATE_TIME_FORMAT = '%Y:%m:%d %H:%M:%S'


class InvalidFlatImageError(Exception):
  pass


@dataclasses.dataclass
class _TifTagsDicomMetadata:
  """TIF tags metadata relevant to VL Slide-Coordinates DICOM."""

  column_spacing: Optional[float] = None
  row_spacing: Optional[float] = None
  acquisition_datetime: Optional[datetime.datetime] = None


@dataclasses.dataclass
class _ImageDicomMetadata:
  """Image metadata relevant to VL Slide-Coordinates DICOM."""

  image_bytes: bytes
  width: int
  height: int
  photometric_interpretation: str
  samples_per_pixel: int
  compression_ratio: str
  image_type: str
  icc_profile: bytes
  transfer_syntax: str
  tif_tags: _TifTagsDicomMetadata


def _is_opaque(image) -> bool:
  """Returns true if all alpha values are 0xFF (i.e. image is opaque)."""
  try:
    alpha = image.getchannel('A')
  except ValueError:
    return True  # Image doesn't have alpha channel.
  return all([i == 255 for i in alpha.getdata()])


def _convert_resolution_to_spacing(
    resolution: Sequence[int], resolution_unit: tifffile.TIFF.RESUNIT
) -> Optional[float]:
  """Converts resolution to pixel spacing.

  Args:
    resolution: number of pixels per resolution unit.
    resolution_unit: the unit of measurement for the given resolution.

  Returns:
    Space between pixels in mm. None if unable to determine spacing.
  """
  if len(resolution) != 2:  # Invalid resolution value.
    return None
  if resolution_unit == tifffile.TIFF.RESUNIT.CENTIMETER:
    unit_to_mm = 10
  elif resolution_unit == tifffile.TIFF.RESUNIT.INCH:
    unit_to_mm = 25.4
  else:  # Resolution unit unspecified.
    return None
  return resolution[1] * unit_to_mm / resolution[0]


def _extract_tif_tags_metadata(tif_path: str) -> _TifTagsDicomMetadata:
  """Extracts TIF tags metadata relevant to VL Slide-Coordinates DICOM.

  Args:
    tif_path: Path to a TIF image.

  Returns:
    TIF tags metadata for VL Slide-Coordinates DICOM.
  """
  metadata = _TifTagsDicomMetadata()
  with tifffile.TiffFile(tif_path) as tif:
    if not tif.pages:
      cloud_logging_client.warning(f'TIF image missing pages: {tif_path}.')
      return metadata

    tags = tif.pages[0].tags

    if (
        _TIF_X_RESOLUTION_TAG in tags
        and _TIF_Y_RESOLUTION_TAG in tags
        and _TIF_RESOLUTION_UNIT_TAG in tags
    ):
      x_resolution = tags[_TIF_X_RESOLUTION_TAG].value
      y_resolution = tags[_TIF_Y_RESOLUTION_TAG].value
      resolution_unit = tags[_TIF_RESOLUTION_UNIT_TAG].value
      metadata.column_spacing = _convert_resolution_to_spacing(
          x_resolution, resolution_unit
      )
      metadata.row_spacing = _convert_resolution_to_spacing(
          y_resolution, resolution_unit
      )
      if not metadata.column_spacing or not metadata.row_spacing:
        cloud_logging_client.warning(
            f'Invalid resolution TIF tags in {tif_path}.'
        )

    if _TIF_DATE_TIME_TAG in tags:
      datetime_value = tags[_TIF_DATE_TIME_TAG].value
      try:
        metadata.acquisition_datetime = datetime.datetime.strptime(
            datetime_value, _TIF_DATE_TIME_FORMAT
        )
      except ValueError:
        cloud_logging_client.warning(
            f'Failed to parse acquisition datetime TIF tag in {tif_path}: '
            f'{datetime_value}.'
        )

  return metadata


def _create_image_dicom_metadata(image_path: str) -> _ImageDicomMetadata:
  """Creates image metadata for VL Slide-Coordinates DICOM.

  Args:
    image_path: Path to an image.

  Returns:
    Image metadata for VL Slide-Coordinates DICOM.

  Raises:
    InvalidFlatImageError: If invalid flat image (e.g. invalid path, error
      reading image, unsupported image format or dimensions, etc).
  """
  try:
    with PIL.Image.open(image_path) as img:
      img.load()
  except Exception as exp:
    raise InvalidFlatImageError(
        f'Failed to open image: {image_path}.',
        ingest_const.ErrorMsgs.FLAT_IMAGE_FAILED_TO_OPEN,
    ) from exp
  if img.format not in _SUPPORTED_IMAGE_FORMATS:
    raise InvalidFlatImageError(
        f'Unexpected image format {img.format} in {image_path}.',
        ingest_const.ErrorMsgs.FLAT_IMAGE_UNEXPECTED_FORMAT,
    )
  if img.width > _MAX_IMAGE_SIZE or img.height > _MAX_IMAGE_SIZE:
    raise InvalidFlatImageError(
        (
            f'Unexpected image dimensions {img.width} x {img.height} '
            f'in {image_path}. Expected <= {_MAX_IMAGE_SIZE}.'
        ),
        ingest_const.ErrorMsgs.FLAT_IMAGE_UNEXPECTED_DIMENSIONS,
    )

  tif_tags = _TifTagsDicomMetadata()
  if img.format == 'PNG':
    if img.mode == 'RGBA' and _is_opaque(img):
      img = img.convert(mode='RGB')
    with io.BytesIO() as jpg_bytes:
      img.save(jpg_bytes, format='JPEG2000', irreversible=False)
      image_bytes = jpg_bytes.getvalue()
      with PIL.Image.open(jpg_bytes) as img:
        img.load()
    image_type = _DICOM_IMAGE_TYPE_DERIVED
    transfer_syntax = ingest_const.DicomImageTransferSyntax.JPEG_2000
  elif img.format == 'TIFF':
    if img.mode == 'RGBA' and _is_opaque(img):
      img = img.convert(mode='RGB')
      img.save(image_path)
    ancillary_image_path = os.path.splitext(image_path)[0] + '.jpg'
    ancillary_image = ancillary_image_extractor.AncillaryImage(
        path=ancillary_image_path
    )
    if not ancillary_image_extractor.extract_jpg_image(
        image_path,
        ancillary_image,
        convert_to_jpeg_2000=True,
    ):
      raise InvalidFlatImageError(
          f'Failed to convert TIF to JPG: {image_path}',
          ingest_const.ErrorMsgs.FLAT_IMAGE_FAILED_TO_CONVERT_TIF_TO_JPG,
      )
    with PIL.Image.open(ancillary_image.path) as img:
      img.load()
    with open(ancillary_image.path, 'rb') as img_file:
      image_bytes = img_file.read()
    if ancillary_image.extracted_without_decompression:
      image_type = _DICOM_IMAGE_TYPE_ORIGINAL
      transfer_syntax = ingest_const.DicomImageTransferSyntax.JPEG_LOSSY
    else:
      image_type = _DICOM_IMAGE_TYPE_DERIVED
      transfer_syntax = ingest_const.DicomImageTransferSyntax.JPEG_2000
    tif_tags = _extract_tif_tags_metadata(image_path)
  else:
    with open(image_path, 'rb') as img_file:
      image_bytes = img_file.read()
    image_type = _DICOM_IMAGE_TYPE_ORIGINAL
    transfer_syntax = ingest_const.DicomImageTransferSyntax.JPEG_LOSSY

  if img.mode not in ['RGB', 'RGBA', '1', 'L']:
    raise InvalidFlatImageError(
        f'Unexpected image mode {img.mode} in {image_path}.',
        ingest_const.ErrorMsgs.FLAT_IMAGE_UNEXPECTED_PIXEL_MODE,
    )

  if img.mode in ['RGB', 'RGBA']:
    if transfer_syntax == ingest_const.DicomImageTransferSyntax.JPEG_LOSSY:
      photometric_interpretation = 'YBR_FULL_422'
    else:
      photometric_interpretation = 'RGB'
    samples_per_pixel = 3
  else:
    photometric_interpretation = 'MONOCHROME2'
    samples_per_pixel = 1

  compression_ratio = str(
      round(
          (img.width * img.height * samples_per_pixel)
          / os.path.getsize(image_path),
          2,
      )
  )

  return _ImageDicomMetadata(
      image_bytes,
      img.width,
      img.height,
      photometric_interpretation,
      samples_per_pixel,
      compression_ratio,
      image_type,
      icc_profile=img.info.get('icc_profile'),
      transfer_syntax=transfer_syntax,
      tif_tags=tif_tags,
  )


def _create_dicom(
    image_path: str,
    metadata: _ImageDicomMetadata,
    study_uid: str,
    series_uid: str,
    instance_uid: str,
    sop_class: ingest_const.DicomSopClass,
    dcm_json: Optional[Mapping[str, Any]] = None,
) -> pydicom.Dataset:
  """Creates flat image DICOM using image metadata.

  Args:
    image_path: Path to original image.
    metadata: Image DICOM metadata.
    study_uid: DICOM study UID for the image.
    series_uid: DICOM series UID for the image.
    instance_uid: DICOM instance UID for the image.
    sop_class: DICOM SOP Class to use for the image.
    dcm_json: Optional DICOM metadata to embed in the DICOM instance.

  Returns:
    Pydicom dataset representing DICOM instance.
  """
  if dcm_json is None:
    dcm_json = {}

  base_ds = pydicom.Dataset.from_json(json.dumps(dcm_json))

  file_meta = pydicom.dataset.FileMetaDataset()
  file_meta.ImplementationClassUID = ingest_const.SCMI_IMPLEMENTATION_CLASS_UID
  file_meta.ImplementationVersionName = ingest_const.IMPLEMENTATION_VERSION_NAME
  file_meta.MediaStorageSOPClassUID = sop_class.uid
  file_meta.MediaStorageSOPInstanceUID = instance_uid
  file_meta.TransferSyntaxUID = metadata.transfer_syntax

  ds = pydicom.dataset.FileDataset(
      filename_or_obj='',
      dataset=base_ds,
      file_meta=file_meta,
      preamble=b'\0' * 128,
  )
  ds.is_little_endian = True
  ds.is_implicit_VR = False
  if (
      metadata.transfer_syntax
      == ingest_const.DicomImageTransferSyntax.JPEG_LOSSY
  ):
    ds.LossyImageCompression = '01'  # Image subjected to lossy compression.
    ds.LossyImageCompressionMethod = (
        ingest_const.DicomImageCompressionMethod.JPEG_LOSSY
    )
    ds.LossyImageCompressionRatio = metadata.compression_ratio
  else:  # Lossless JPEG 2000
    ds.LossyImageCompression = '00'  # Image not subjected to lossy compression.
  ds.NumberOfFrames = 1
  ds.Modality = 'SM'
  ds.PlanarConfiguration = 0
  ds.PatientOrientation = ''
  ds.StudyInstanceUID = study_uid
  ds.SeriesInstanceUID = series_uid
  ds.SOPInstanceUID = instance_uid
  ds.SOPClassUID = sop_class.uid
  ds.Rows = metadata.height
  ds.Columns = metadata.width
  # See table 8.2.1 for JPEG pixel data related attributes in
  # https://dicom.nema.org/medical/dicom/current/output/chtml/part05/sect_8.2.html#sect_8.2
  ds.PixelRepresentation = 0
  ds.BitsAllocated = 8
  ds.BitsStored = 8
  ds.HighBit = 7
  ds.PhotometricInterpretation = metadata.photometric_interpretation
  ds.SamplesPerPixel = metadata.samples_per_pixel
  ds.ImageType = metadata.image_type
  ds.PixelData = pydicom.encaps.encapsulate([metadata.image_bytes])
  ds.FrameOfReferenceUID = uid_generator.generate_uid()
  try:
    dicom_util.add_default_optical_path_sequence(ds, metadata.icc_profile)
  except dicom_util.InvalidICCProfileError as exp:
    cloud_logging_client.warning(
        f'Failed to add ICC profile to {image_path}', exp
    )

  if metadata.tif_tags.column_spacing and metadata.tif_tags.row_spacing:
    # DICOM Standard represents the pixel spacing as a decimal string. The
    # total length of the string is required to be less than 16 bytes.
    # Pixel Spacing
    # https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.8.12.html#table_C.8-77
    #
    # DS VR Code (Limit to 16 characters)
    # https://dicom.nema.org/medical/dicom/current/output/chtml/part05/sect_6.2.html
    ds.PixelSpacing = [
        str(metadata.tif_tags.column_spacing)[:16],
        str(metadata.tif_tags.row_spacing)[:16],
    ]
  if metadata.tif_tags.acquisition_datetime:
    dicom_util.set_acquisition_date_time(
        ds, metadata.tif_tags.acquisition_datetime
    )

  return ds


def create_encapsulated_flat_image_dicom(
    image_path: str,
    instance_uid: str,
    sop_class: ingest_const.DicomSopClass,
    dcm_json: Mapping[str, Any],
) -> pydicom.Dataset:
  """Creates VL Slide-Coordinates Microscopic Image DICOM with embedded image.

  Args:
    image_path: Path to an image.
    instance_uid: DICOM instance UID for the image.
    sop_class: DICOM SOP Class to use for the image.
    dcm_json: DICOM metadata to embed in the DICOM instance.

  Returns:
    Pydicom dataset representing DICOM instance.

  Raises:
    InvalidFlatImageError: If invalid flat image (e.g. invalid path, error
      reading image, unsupported image format or dimensions, etc).
  """
  study_uid = dicom_json_util.get_study_instance_uid(dcm_json)
  series_uid = dicom_json_util.get_series_instance_uid(dcm_json)
  img = _create_image_dicom_metadata(image_path)
  return _create_dicom(
      image_path, img, study_uid, series_uid, instance_uid, sop_class, dcm_json
  )
