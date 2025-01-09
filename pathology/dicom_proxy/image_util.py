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
"""Image encoding, decoding, and downsampling utility."""
import dataclasses
import io
import math
import typing
from typing import Any, Optional, Union

import cv2
import imagecodecs
import numpy as np
import PIL.Image

from pathology.dicom_proxy import dicom_proxy_flags
from pathology.dicom_proxy import enum_types
from pathology.shared_libs.logging_lib import cloud_logging_client

# Types
_Interpolation = enum_types.Interpolation
_Compression = enum_types.Compression

# Image Compression Quality Consts
_MIN_QUALITY: int = 1
_DEFAULT_QUALITY: int = 75
_MAX_QUALITY: int = 100


@dataclasses.dataclass
class PILImage:
  image: Any


class ImageEncodingDoesNotSupportEmbeddingICCProfileError(Exception):
  pass


class JpegxlToJpegTranscodeError(Exception):
  pass


def transcode_jpxl_to_jpeg(img: bytes) -> bytes:
  """Transcodes jpegxl image to jpeg compression.

  Args:
    img: jpegxl image bytes that were transcoded from jpeg.

  Returns:
    jpeg image bytes.

  Raises:
    JpegxlToJpegTranscodeError: Transcoding back to jpeg failed.
  """
  try:
    return imagecodecs.jpegxl_decode_jpeg(img, numthreads=1)
  except ValueError as exp:
    raise JpegxlToJpegTranscodeError() from exp


def transcode_jpeg_to_jpxl(img: bytes) -> bytes:
  return imagecodecs.jpegxl_encode_jpeg(img, numthreads=1)


def decode_image_bytes(
    frame: bytes, compression_hint: Optional[_Compression]
) -> np.ndarray:
  """Decode compressed image bytes to BRG image.

  Args:
    frame: Raw image bytes (compressed blob).
    compression_hint: Compression format bytes encoded in; currently unused.

  Returns:
    Decompressed image.
  """
  if compression_hint == _Compression.JPEG_TRANSCODED_TO_JPEGXL:
    # Transcode JPEGXL to JPEG and decode with JPEG pipeline.
    try:
      frame = transcode_jpxl_to_jpeg(frame)
      compression_hint = _Compression.JPEG
    except JpegxlToJpegTranscodeError:
      # Raises Value Error if transcoding failes.
      return cv2.cvtColor(
          imagecodecs.jpegxl_decode(frame, numthreads=1), cv2.COLOR_RGB2BGR
      )
  if compression_hint == _Compression.JPEGXL:
    return cv2.cvtColor(
        imagecodecs.jpegxl_decode(frame, numthreads=1), cv2.COLOR_RGB2BGR
    )
  if compression_hint != _Compression.JPEG2000:
    # Prefer PIL for decoding JPEG 2000
    result = cv2.imdecode(
        np.frombuffer(frame, dtype=np.uint8), cv2.IMREAD_COLOR
    )
    if result is not None:
      return result
    # OpenCV implementation in docker supports decoding Jpeg2000. However,
    # internal cv2 implementation does not. Fail over to decode Jpeg2000
    # using PIL. Log warning. This code is expected to be hit in internal tests
    # but not in the deployed Docker where OpenCV is built to decode Jpeg2000.
    cloud_logging_client.warning(
        'Cannot decode image bytes using OpenCV attempting to decode using PIL'
    )
  with io.BytesIO(frame) as buffer:
    with PIL.Image.open(buffer) as image:
      if image.mode != 'RGB':
        # if image is not RGB convert to RGB. OpenCV converts images to RGB.
        image = image.convert(mode='RGB')
      # Return image in BGR ordering to mimic OpenCV byte ordering.
      return cv2.cvtColor(np.asarray(image), cv2.COLOR_BGR2RGB)


def get_cv2_interpolation_padding(
    interpolation: Optional[_Interpolation] = None,
) -> int:
  """Returns edge padding required for seamless downsampling.

  Args:
    interpolation: Image downsampling interpolation algorithm (CV2).

  Returns:
    Downsampled image edge padding in pixels.
  """
  if interpolation in (_Interpolation.CUBIC, _Interpolation.LANCZOS4):
    # Algorithm's interpolate between neighboring pixels to improve downsample
    # image quality.  LANCZOS4 interpolates over 8x8 closest pixels.
    # Cubic is 4x4.
    # https://docs.opencv.org/3.4/da/d54/group__imgproc__transform.html#gga5bb5a1fea74ea38e1a5445ca803ff121ac6c578caa97f2d00f82bac879cf3c781
    #
    # Pad downsampled region by 8 pixels to ensure seamless downsampled images.
    # Note at actual pixel padding will be a factor of this to ensure
    # padding does not alter pixels region downsampled or the proportions of
    # the downsampled image.
    return 8
  return 0


def _get_cv2_interpolation(
    interpolation: Optional[_Interpolation] = None,
) -> int:
  """Returns CV2 interpolation algorithm specified.

  Args:
    interpolation: Image downsampling interpolation algorithm.

  Returns:
    CV2 interpolation enum.
  """
  if interpolation == _Interpolation.NEAREST:
    return cv2.INTER_NEAREST
  if interpolation == _Interpolation.LINEAR:
    return cv2.INTER_LINEAR
  if interpolation == _Interpolation.CUBIC:
    return cv2.INTER_CUBIC
  if interpolation == _Interpolation.AREA:
    return cv2.INTER_AREA
  if interpolation == _Interpolation.LANCZOS4:
    return cv2.INTER_LANCZOS4
  return cv2.INTER_AREA


def downsample_image(
    image: np.ndarray,
    downsample: float,
    dest_width: int,
    dest_height: int,
    interpolation: _Interpolation,
) -> np.ndarray:
  """Returns downsampled image.

  Args:
    image: RAW decompressed image memory.
    downsample: Factor to downsample image.
    dest_width: Width of the downsampled image.
    dest_height: Height of the downsampled image.
    interpolation: Algorithm to use to downsample image.

  Returns:
    Raw downsampled image.
  """
  source_height, source_width, _ = image.shape
  return cv2.resize(
      image,
      (
          max(int(source_width / downsample), dest_width, 1),
          max(int(source_height / downsample), dest_height, 1),
      ),
      _get_cv2_interpolation(interpolation),
  )


def _opencv_to_pil_image(img: np.ndarray) -> PILImage:
  """Converts OpenCV formatted image; byte swaps color images.

  Args:
    img: OpenCV formatted byte array

  Returns:
    PIL image; for color swaps R <-> B bytes;
  """
  # Copy is perf optmization. bgr2rgb swaps bytes. Make sure source array is
  # not transformed. Copy once.
  copy = True  # default behavior is for bgr2rgb to return copy of imaging.
  shape = img.shape
  if len(shape) == 2:
    # Monochrome
    mode = 'L'
  elif len(shape) == 3 and shape[2] == 1:
    # Monochrome
    mode = 'L'
    # np.squeeze creates a copy of array, we don't need to make an additional.
    img = np.squeeze(img, axis=2)
    copy = False
  else:
    # image loaded with opencv swap from BGR to RGB
    mode = 'RGB'
  return PILImage(PIL.Image.fromarray(bgr2rgb(img, copy=copy), mode=mode))


def _encode_png(
    img: Union[np.ndarray, PILImage], icc_profile: Optional[bytes]
) -> bytes:
  """Returns image encoded using PNG compression.

  Args:
    img: RAW decompressed image memory.
    icc_profile: ICC Color profile bytes to encode in image.

  Returns:
    PNG encoded image.
  """
  if isinstance(img, np.ndarray):
    # If image is in BGR OpenCV save using Opencv
    if not icc_profile:
      return cv2.imencode('.png', img)[1].tobytes()
    img = _opencv_to_pil_image(img)
  # If image is PIL RGB save using PIL
  with io.BytesIO() as buffer:
    img.image.save(buffer, format='PNG', icc_profile=icc_profile)
    return buffer.getvalue()


def _encode_gif(img: Union[np.ndarray, PILImage]) -> bytes:
  """Returns image encoded using GIF compression.

  Args:
    img: RAW decompressed image memory.

  Returns:
    GIF encoded image.
  """
  if isinstance(img, PILImage):
    # If PIL image save
    img2 = img.image
  else:
    img2 = _opencv_to_pil_image(img).image
  with io.BytesIO() as buffer:
    # Save to GIF using PIL
    img2.save(buffer, format='GIF')
    return buffer.getvalue()


def bgr2rgb(img: np.ndarray, copy: bool = False) -> np.ndarray:
  """Converts image between BGR and RGB byte ordering.

  Args:
    img: Image byte array.
    copy: True returns copy of image bytes (new allocation; default = False)

  Returns:
    returns R <-> B byte swapped image; NOP for monochrome.
  """
  if copy:
    img = img.copy()
  shape = img.shape
  if len(shape) == 3 and shape[2] == 3:
    cv2.cvtColor(img, cv2.COLOR_BGR2RGB, dst=img)
  return img


def _encode_jpeg(
    img: Union[np.ndarray, PILImage], quality: int, icc_profile: Optional[bytes]
) -> bytes:
  """Returns image encoded using JPEG compression.

  Args:
    img: RAW decompressed image memory.
    quality: Image compression quality (1 - 100).
    icc_profile: bytes.

  Returns:
    JPEG encoded image.
  """
  is_instance_pil = isinstance(img, PILImage)
  if (
      is_instance_pil
      or dicom_proxy_flags.JPEG_ENCODER_FLG.value.strip().upper() == 'PIL'
      or icc_profile
  ):
    if is_instance_pil:
      # If image is PIL RGB
      img = typing.cast(PILImage, img)
      pil_img = img.image
    else:
      # If image is opencv, convert from OPENCV BGR to RGB and generate PIL
      # image.
      pil_img = _opencv_to_pil_image(img).image
    # Save RGB image to JPEG
    # PIL currently supports saveing JPEG with subsampling=0 for higher quality
    # OpenCV does not.
    with io.BytesIO() as buffer:
      pil_img.save(
          buffer,
          format='JPEG',
          subsampling=0,
          quality=quality,
          icc_profile=icc_profile,
      )
      return buffer.getvalue()
  else:
    # Image is OPENCV BGR and PIL is not enabled save using OpenCV
    return cv2.imencode('.jpg', img, [int(cv2.IMWRITE_JPEG_QUALITY), quality])[
        1
    ].tobytes()


def _encode_webp(
    img: Union[np.ndarray, PILImage], quality: int, icc_profile: Optional[bytes]
) -> bytes:
  """Returns image encoded using WEBP compression.

  Args:
    img: RAW decompressed image memory.
    quality: Image compression quality (1 - 100).
    icc_profile: ICC Color profile bytes to encode in image.

  Returns:
    WEBP encoded image.
  """
  if isinstance(img, np.ndarray):
    if not icc_profile:
      # If image is OpenCV BGR image save using OpenCV
      return cv2.imencode(
          '.webp', img, [int(cv2.IMWRITE_WEBP_QUALITY), quality]
      )[1].tobytes()
    img = _opencv_to_pil_image(img)
  # If image is PIL RGB image save using PIL
  with io.BytesIO() as buffer:
    img.image.save(
        buffer, format='WEBP', quality=quality, icc_profile=icc_profile
    )
    return buffer.getvalue()


def _encode_jpegxl(img: Union[np.ndarray, PILImage], quality: int) -> bytes:
  """Returns image encoded using jpegxl compression.

  Args:
    img: RAW decompressed image memory.
    quality: Image compression quality (1 - 100).

  Returns:
    JPEGXL encoded image.
  """
  if isinstance(img, np.ndarray):
    img = _opencv_to_pil_image(img)
  return imagecodecs.jpegxl_encode(np.asarray(img), level=quality, numthreads=1)


def encode_image(
    img: Union[np.ndarray, PILImage],
    compression: _Compression,
    quality: Optional[int],
    icc_profile: Optional[bytes],
) -> Union[bytes, np.ndarray]:
  """Returns image encoded using compression.

  Args:
    img: RAW decompressed image memory PIL = RGB byte order Numpy(OpenCV) = BGR.
    compression: Image compression algorithm
    quality: Image compression quality (1 - 100; default 75). Image quality is
      used by (JPEG and WEBP formats only). Lossless formats such as PNG do not
      have a quality parameter.
    icc_profile: ICC Color profile bytes to encode in image.

  Returns:
    Compressed image.

  Raises:
    ValueError: Unrecognized compression.
    ImageEncodingDoesNotSupportEmbeddingICCProfileError: Image encoding does not
     support ICCProfile.
  """
  if quality is None:
    quality = _DEFAULT_QUALITY
  quality = max(_MIN_QUALITY, min(quality, _MAX_QUALITY))

  if compression == _Compression.JPEG:
    return _encode_jpeg(img, quality, icc_profile)
  if compression == _Compression.WEBP:
    return _encode_webp(img, quality, icc_profile)
  if compression == _Compression.PNG:
    return _encode_png(img, icc_profile)
  if compression == _Compression.GIF:
    if icc_profile is not None and icc_profile:
      raise ImageEncodingDoesNotSupportEmbeddingICCProfileError()
    return _encode_gif(img)
  if (
      compression == _Compression.JPEGXL
      or compression == _Compression.JPEG_TRANSCODED_TO_JPEGXL
  ):
    return _encode_jpegxl(img, quality)
  if compression == _Compression.RAW:
    # Return bytes in RGB byte order regardless of input source
    if isinstance(img, PILImage):
      img = img.image
      return img.tobytes()
    return bgr2rgb(img, copy=True).tobytes()
  if compression == _Compression.NUMPY:
    # return numpy array in BGR byte order regardless of source
    if isinstance(img, np.ndarray):
      return img
    else:
      img = np.array(img.image)
      # Convert byte ordering into OpenCV byte ordering
      return bgr2rgb(img)
  raise ValueError('Unhandled Compression')


class GaussianImageFilter:
  """Image filter to apply prior to downsampling to remove aliasing artifacts."""

  def __init__(self, downsample: float, interpolation: _Interpolation):
    """GaussianImageFilter constructor.

    Args:
      downsample: Factor to downsample image by.
      interpolation: Interpolation method, disable if area interpolation.
    """
    self._downsample = 1 if interpolation == _Interpolation.AREA else downsample

  @property
  def _kernel_size(self) -> int:
    """Returns image filter kernel size."""
    if self._downsample <= 1:
      return 0
    return 1 + (2 * self.image_padding)

  @property
  def image_padding(self) -> int:
    """Returns image padding required for filter."""
    if self._downsample <= 1:
      return 0
    return int(math.ceil(3 * self._sigma))

  @property
  def _sigma(self) -> float:
    """Returns guassian filter sigma."""
    # clamp sigma at 128 to avoid numerical error
    return max(0.0, min((self._downsample - 1) / 2.0, 128.0))

  def filter(self, img: np.ndarray) -> np.ndarray:
    """Returns filtered image.

    Args:
      img: Unfiltered image.

    Returns:
      filtered image.
    """
    if self._downsample <= 1:
      return img
    ks = self._kernel_size
    return cv2.GaussianBlur(img, (ks, ks), self._sigma)
