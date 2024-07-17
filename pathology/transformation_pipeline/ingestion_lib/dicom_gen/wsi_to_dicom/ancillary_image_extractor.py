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
"""Extracts series images (macro, thumbnail, label)  from svs file."""

import dataclasses
import os
from typing import Any, Generator, List, Optional

import cv2
import numpy as np
import openslide
import PIL
import pydicom
import tifffile

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingested_dicom_file_ref


# Setting JPG quality to 95 for thumbnail, label, macro images
# High image quality and compression.
_JPEG_QUALITTY = 95
_TIFF_JPEG_COMPRESSION = 7
_TIFF_RGB_PHOTOMETRIC = 2
_JPEG_TRANSFER_SYNTAX = ingest_const.DicomImageTransferSyntax.JPEG_LOSSY
_JPEG2000_TRANSFER_SYNTAX = (
    '1.2.840.10008.1.2.4.51',
    '1.2.840.10008.1.2.4.90',
    '1.2.840.10008.1.2.4.91',
)
_RGB = 'RGB'
_YBR_FULL_422 = 'YBR_FULL_422'
_LABEL = 'LABEL'
_OVERVIEW = 'OVERVIEW'
_THUMBNAIL = 'THUMBNAIL'


class TiffSeriesNotFoundError(Exception):
  pass


class ExtractingAncillaryImageToNonJpegImageError(Exception):
  pass


@dataclasses.dataclass
class AncillaryImage:
  """Container class holds metadata for extracted ancillary images."""

  path: str
  photometric_interpretation: str = ''
  extracted_without_decompression: bool = False


def _get_icc_tag(
    svs_file_path: str,
    series_name: str,
    error_if_series_not_found: bool = False,
    return_first_if_one_series: bool = False,
) -> Optional[bytes]:
  """Returns ICC profile embedded in TIFF series.

  Args:
    svs_file_path: path to SVS file.
    series_name: name of tiff series
    error_if_series_not_found: optional flag, if true raises error if series not
      found.
    return_first_if_one_series: if true return the ICC profile stored on the
      first series if there is only one series.

  Returns:
    embedded icc profile (bytes) or None if not found.

  Raises:
    TiffSeriesNotFoundError: if series not found and error_if_series_not_found
  """
  if not ingest_flags.EMBED_ICC_PROFILE_FLG.value or not svs_file_path:
    return None
  try:
    with tifffile.TiffFile(svs_file_path) as tiff:
      if return_first_if_one_series and len(tiff.series) == 1:
        # if conditions for return_first_if_one_series are true
        # override provided series_name with series_name
        # of first series to return series icc profile
        # regardless of series name parameter.
        # Purpose to extend icc profile extraction to generic
        # tiff files with single series
        series_name = tiff.series[0].name
      for series in tiff.series:
        if series.name == series_name and series.pages:
          for tg in series.pages[0].tags:
            if tg.name == 'InterColorProfile':
              return tg.value
          return None
      if error_if_series_not_found:
        raise TiffSeriesNotFoundError(
            f'TIFF series not found: {series_name} in file {svs_file_path}',
            ingest_const.ErrorMsgs.MISSING_TIFF_SERIES,
        )
    return None
  except tifffile.tifffile.TiffFileError:
    return None


def _extract_jpeg_directly(
    tiff: tifffile.TiffFile,
    series: tifffile.TiffPageSeries,
    image: AncillaryImage,
) -> bool:
  """Extract tiff series jpeg image without decompression.

     * Decoding and encoding bytes streams which are already valid
       jpeg is a noop.
     * TiffFile Routines incorrectly decode some RGB encoded jpegs.

  Args:
    tiff: tifffile extracting image from.
    series: tifffile series to extract image from
    image: AncillaryImage to extract.

  Returns:
    True if image written.
  """
  cloud_logging_client.info(f'Extracting  {image.path} without decompression.')
  page = series.pages[0]
  offset = page.dataoffsets[0]
  bytecount = page.databytecounts[0]
  fh = tiff.filehandle
  fh.seek(offset)
  with open(image.path, 'wb') as outfile:
    outfile.write(bytearray(fh.read(bytecount)))
  if page.photometric.value == _TIFF_RGB_PHOTOMETRIC:
    image.photometric_interpretation = _RGB
  else:
    image.photometric_interpretation = _YBR_FULL_422
  image.extracted_without_decompression = True
  return True


def _extract_jpeg_using_tifffile(
    series: tifffile.TiffPageSeries,
    image: AncillaryImage,
    convert_to_jpeg_2000: bool,
) -> bool:
  """Extract TIFF series image as JPEG using tifffile routines.

     Used to extract non-JPEG encoded images from tifffiles.

  Args:
    series: tifffile series to extract image from.
    image: AncillaryImage to extract.
    convert_to_jpeg_2000: whether to convert to JPEG 2000 instead of JPEG.

  Returns:
    True if image written.
  """
  output_path = image.path
  cloud_logging_client.info(f'Decoding {output_path} using tifffile.')
  series_bytes = series.asarray()
  if series_bytes is None:
    return False
  if convert_to_jpeg_2000:
    try:
      image.photometric_interpretation = _RGB
      PIL.Image.fromarray(series_bytes).save(image.path)
      image.extracted_without_decompression = False
      return True
    except (ValueError, OSError) as exp:
      cloud_logging_client.error(f'Failed to write to {output_path}.', exp)
      return False
  if cv2.imwrite(
      output_path,
      cv2.cvtColor(series_bytes, cv2.COLOR_RGB2BGR),
      [int(cv2.IMWRITE_JPEG_QUALITY), _JPEG_QUALITTY],
  ):
    image.photometric_interpretation = _YBR_FULL_422
    image.extracted_without_decompression = False
    return True
  cloud_logging_client.error(f'Failed to write to {output_path}.')
  return False


def _extract_ancillary_image_with_openslide(
    file_path: str,
    image: AncillaryImage,
    openslide_key: str,
) -> bool:
  """Extracts ancillary image using Openslide methods.

  Args:
    file_path: File path to OpenSlide compatible input.
    image: AncillaryImage to populate with extracted image.
    openslide_key: String key for image to extract.

  Returns:
    True if image extracted and AncillaryImage populated.
  """
  try:
    with openslide.OpenSlide(file_path) as imaging:
      pil_img = imaging.associated_images.get(openslide_key)
  except openslide.OpenSlideError:
    return False

  if pil_img is None:
    return False
  if pil_img.mode not in ('RGBA', 'RGB', 'L'):
    cloud_logging_client.error(
        'Ancillary image is encoded in unexpected format.',
        {
            ingest_const.LogKeywords.FILENAME: file_path,
            'image_mode': pil_img.mode,
            'image_size': pil_img.size,
            'openslide_key': openslide_key,
        },
    )
    return False
  raw_img_bytes = np.asarray(pil_img)
  if pil_img.mode == 'RGBA':
    raw_img_bytes = raw_img_bytes[..., :3]
  if pil_img.mode == 'L':
    raw_img_bytes = np.stack(
        [raw_img_bytes, raw_img_bytes, raw_img_bytes], axis=-1
    )
  else:
    raw_img_bytes = cv2.cvtColor(raw_img_bytes, cv2.COLOR_RGB2BGR)
  cv2.imwrite(
      image.path, raw_img_bytes, [int(cv2.IMWRITE_JPEG_QUALITY), _JPEG_QUALITTY]
  )
  image.photometric_interpretation = _YBR_FULL_422
  image.extracted_without_decompression = False
  return True


def extract_jpg_image(
    tiff_file_path: str,
    image: AncillaryImage,
    series_name: Optional[str] = None,
    convert_to_jpeg_2000: bool = False,
) -> bool:
  """Extracts JPG image from a TIFF file.

  Args:
    tiff_file_path: path to svs file.
    image: AncillaryImage to extract.
    series_name: (Optional) name of series image to return.
    convert_to_jpeg_2000: whether to convert to JPEG 2000 in case of non-JPEG
      images.

  Returns:
    True if image extracted and saved to ancillary image path.

  Raises:
    ExtractingAncillaryImageToNonJpegImageError: output file name is not JPG.
  """
  basepath, extension = os.path.splitext(image.path)
  # DICOM supports encapsulation of a limited set of imaging types.
  # See Dicom transfer syntax. Limit image generation to jpeg.
  if extension.lower() not in ('.jpeg', '.jpg'):
    raise ExtractingAncillaryImageToNonJpegImageError()

  try:
    with tifffile.TiffFile(tiff_file_path) as tiff:
      for series in tiff.series:
        if series_name and series.name != series_name:
          continue
        if (
            len(series.pages) == 1
            and series.pages[0].compression.value == _TIFF_JPEG_COMPRESSION
            and series.pages[0].jpegtables is None
        ):
          # If image is JPEG encoded and defined without external JPEG
          # table decode. If JPEG table is present, then
          # JPEG would need to be recreated, see wsi-to-dicom C++ for
          # example of this. Extracting JPEG bytes directly.
          # Note: tifffile decoder has issues with some photometric RGB
          # encoded images. If decoding is needed use OpenCV.
          return _extract_jpeg_directly(tiff, series, image)

        # More general case.
        # decode/decompress image using tiff file image decoder.
        # used for LZW compressed images etc.
        if convert_to_jpeg_2000:
          image.path = f'{basepath}.jp2'
        return _extract_jpeg_using_tifffile(series, image, convert_to_jpeg_2000)
  except tifffile.TiffFileError:
    pass
  return False


def macro_icc(svs_file_path: str) -> Optional[bytes]:
  return _get_icc_tag(svs_file_path, 'Macro')


def label_icc(svs_file_path: str) -> Optional[bytes]:
  return _get_icc_tag(svs_file_path, 'Label')


def thumbnail_icc(svs_file_path: str) -> Optional[bytes]:
  return _get_icc_tag(svs_file_path, 'Thumbnail')


def image_icc(svs_file_path: str) -> Optional[bytes]:
  """Returns ICC profile for WSI image.

  Args:
    svs_file_path: path to svs image.

  Raises:
    TiffSeriesNotFoundError: if Baseline not found
  """
  return _get_icc_tag(
      svs_file_path,
      'Baseline',
      error_if_series_not_found=True,
      return_first_if_one_series=True,
  )


def _ancillary_image(
    file_path: str,
    image: AncillaryImage,
    series_name: str,
    openslide_key: str,
) -> bool:
  """Extracts ancillary image from SVS file and saves it to img_path file.

  Args:
    file_path: path to svs file
    image: AncillaryImage for macro image extraction
    series_name: SVS Tiff file series name.
    openslide_key: OpenSlide accessory image key.

  Returns:
    True if image extracted
  """
  if extract_jpg_image(file_path, image, series_name):
    cloud_logging_client.info(
        f'File has a {openslide_key} image; extracting directly.'
    )
    return True
  if _extract_ancillary_image_with_openslide(file_path, image, openslide_key):
    cloud_logging_client.info(
        f'File has a {openslide_key} image; extracting using Openslide.'
    )
    return True
  cloud_logging_client.info(f'File is missing the {openslide_key} image')
  return False


def macro_image(svs_file_path: str, macro: AncillaryImage) -> bool:
  """Extracts macro image from SVS file and saves it to img_path file.

  Args:
    svs_file_path: path to svs file
    macro: AncillaryImage for macro image extraction

  Returns:
    True if image extracted
  """
  return _ancillary_image(svs_file_path, macro, 'Macro', 'macro')


def label_image(svs_file_path: str, label: AncillaryImage) -> bool:
  """Extracts label image from SVS file and saves it to img_path file.

  Args:
    svs_file_path: path to svs file
    label: AncillaryImage for label image extraction

  Returns:
    True if image extracted
  """
  return _ancillary_image(svs_file_path, label, 'Label', 'label')


def thumbnail_image(svs_file_path: str, thumbnail: AncillaryImage) -> bool:
  """Extracts thumbnail image from SVS file and saves it to img_path file.

  Args:
    svs_file_path: path to svs file
    thumbnail: AncillaryImage for thumbnail image extraction

  Returns:
    True if image extracted
  """
  return _ancillary_image(svs_file_path, thumbnail, 'Thumbnail', 'thumbnail')


def get_ancillary_images_from_svs(
    svs_path: str, ancillary_image_dir: str
) -> List[AncillaryImage]:
  """Extracts ancillary images from svs.

  Args:
    svs_path: path to svs file.
    ancillary_image_dir: drir to write extracted images to

  Returns:
    List of paths to extracted ancillary images.
  """
  # Extract Secondary capture images.
  macro_file_path = os.path.join(ancillary_image_dir, 'macro.jpg')
  thumbnail_file_path = os.path.join(ancillary_image_dir, 'thumbnail.jpg')
  label_file_path = os.path.join(ancillary_image_dir, 'label.jpg')
  # Validation order corresponds to order images may be tested for barcodes.
  ancillary_images = []
  label = AncillaryImage(label_file_path)
  if label_image(svs_path, label):
    ancillary_images.append(label)
  macro = AncillaryImage(macro_file_path)
  if macro_image(svs_path, macro):
    ancillary_images.append(macro)
  thumbnail = AncillaryImage(thumbnail_file_path)
  if thumbnail_image(svs_path, thumbnail):
    ancillary_images.append(thumbnail)
  return ancillary_images


def _get_first_encapsulated_frame(
    encapsulated_frames: Generator[bytes, Any, None], image_type: str
) -> bytes:
  """Return first element in frame generator or throw.

  Args:
    encapsulated_frames: Pydicom encapsulated frame generator.
    image_type: Image type extracting from DICOM.

  Returns:
    Bytes stored in first frame.

  Raises:
    ValueError: Encapsulated frame number != 1.
  """
  encapsulated_frame = None
  try:
    for frame in encapsulated_frames:
      if encapsulated_frame is not None:
        raise ValueError('More than one encapsulated frame.')
      encapsulated_frame = frame
    if encapsulated_frame is None:
      raise ValueError('Missing encapsulated frame.')
    return encapsulated_frame
  except (ValueError, EOFError) as exp:
    raise ValueError(
        f'Error extracting ancillary {image_type} image from DICOM.'
    ) from exp


def _extract_ancillary_dicom_image(
    wsi_image: ingested_dicom_file_ref.IngestDicomFileRef, image_path: str
) -> AncillaryImage:
  """Extracts individual ancillary image from a WSI DICOM ref.

  Args:
    wsi_image: Ingested DICOM file reference to extract DICOM from.
    image_path: Image path to extract ancilllary image to.

  Returns:
    AncillaryImages extracted.

  Raises:
    ValueError: DICOM encapsulated frame number != 1
  """
  _, extension = os.path.splitext(os.path.basename(image_path))
  if extension != '.jpg':
    raise ValueError('Invalid output filename. Expects jpg file extension.')
  if wsi_image.transfer_syntax == _JPEG_TRANSFER_SYNTAX:
    cv2_image_decode_type = 'jpeg'
  elif wsi_image.transfer_syntax in _JPEG2000_TRANSFER_SYNTAX:
    cv2_image_decode_type = 'jpeg2000'
  else:
    cv2_image_decode_type = ''
  if cv2_image_decode_type:
    with pydicom.dcmread(wsi_image.source) as ds:
      try:
        number_of_frames = ds.NumberOfFrames
      except AttributeError:
        number_of_frames = 1
      encoded_frames = pydicom.encaps.generate_pixel_data_frame(
          ds.PixelData, number_of_frames
      )
    encoded_frame = _get_first_encapsulated_frame(
        encoded_frames, cv2_image_decode_type
    )
    decoded_frame = cv2.imdecode(
        np.frombuffer(encoded_frame, dtype=np.uint8), flags=1
    )
    cv2.imwrite(
        image_path,
        decoded_frame,
        [int(cv2.IMWRITE_JPEG_QUALITY), _JPEG_QUALITTY],
    )
    return AncillaryImage(image_path, _YBR_FULL_422, False)
  # RAW
  with pydicom.dcmread(wsi_image.source) as ds:
    pixel_data = ds.pixel_array
  pixel_data = cv2.cvtColor(pixel_data, cv2.COLOR_BGR2RGB)
  cv2.imwrite(
      image_path, pixel_data, [int(cv2.IMWRITE_JPEG_QUALITY), _JPEG_QUALITTY]
  )
  return AncillaryImage(image_path, _YBR_FULL_422, False)


def get_ancillary_images_from_dicom(
    wsi_images: List[ingested_dicom_file_ref.IngestDicomFileRef], directory: str
) -> List[AncillaryImage]:
  """Extract thumbnail, macro, and label images from WSI DICOM.

  Args:
    wsi_images: List of Ingested DICOM File References.
    directory: Directory to save extracted images to.

  Returns:
    List of AncillaryImages extracted.
  """
  label = None
  overview = None
  thumbnail = None
  for wsi_image in wsi_images:
    if (
        _LABEL in wsi_image.image_type.upper()
        or _LABEL in wsi_image.frame_type.upper()
    ):
      label = _extract_ancillary_dicom_image(
          wsi_image, os.path.join(directory, 'label.jpg')
      )
      continue
    if (
        _OVERVIEW in wsi_image.image_type.upper()
        or _OVERVIEW in wsi_image.frame_type.upper()
    ):
      overview = _extract_ancillary_dicom_image(
          wsi_image, os.path.join(directory, 'macro.jpg')
      )
      continue
    if (
        _THUMBNAIL in wsi_image.image_type.upper()
        or _THUMBNAIL in wsi_image.frame_type.upper()
    ):
      thumbnail = _extract_ancillary_dicom_image(
          wsi_image, os.path.join(directory, 'thumbnail.jpg')
      )
      continue
  # Images searched in list order. Order matters.
  image_list = []
  if label is not None:
    image_list.append(label)
  if overview is not None:
    image_list.append(overview)
  if thumbnail is not None:
    image_list.append(thumbnail)
  return image_list
