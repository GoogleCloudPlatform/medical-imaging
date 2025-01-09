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
r"""Command Line tool Converts WSI *.dcm DICOM into JPEGXL DICOM.

Run from commandline:
  python3 convert_to_jxl.py --input /cns/path/to/input.dcm \
      --output /cns/path/to/output.dcm --quality 75 --transcode True
"""
import concurrent.futures
import functools
import io
from typing import Iterator, Sequence

from absl import app
from absl import flags
import imagecodecs
import numpy as np
import PIL
import pydicom

_JPEG_BASELINE = '1.2.840.10008.1.2.4.50'
_JPEGXL_LOSSLESS = '1.2.840.10008.1.2.4.110'
_JPEGXL_JPEG = '1.2.840.10008.1.2.4.111'
_JPEGXL = '1.2.840.10008.1.2.4.112'

_PYDICOM_MAJOR_VERSION = int((pydicom.__version__).split('.')[0])

_UNENCAPSULATED_TRANSFER_SYNTAXES = frozenset([
    '1.2.840.10008.1.2.1',  # 	Explicit VR Little Endian
    '1.2.840.10008.1.2',  # Implicit VR Endian: Default Transfer Syntax
    '1.2.840.10008.1.2.1.99',  # Deflated Explicit VR Little Endian
    '1.2.840.10008.1.2.2',  # Explicit VR Big Endian
])

INPUT_DICOM_INSTANCE_FLAG = flags.DEFINE_string(
    'input', None, 'Input DICOM instance.'
)
OUTPUT_DICOM_INSTANCE_FLAG = flags.DEFINE_string(
    'output', None, 'Output DICOM instance.'
)
quality_flag = flags.DEFINE_integer('quality', 75, 'JPGXL quality.')
transcode_jpeg_flag = flags.DEFINE_boolean(
    'transcode', True, 'Transcode JPEG to JPEGXL.'
)


def _generate_frames(buffer: bytes, number_of_frames: int) -> Iterator[bytes]:
  # pytype: disable=module-attr
  if _PYDICOM_MAJOR_VERSION <= 2:
    return pydicom.encaps.generate_pixel_data_frame(
        buffer, int(number_of_frames)
    )
  else:
    return pydicom.encaps.generate_frames(
        buffer, number_of_frames=int(number_of_frames)
    )
  # pytype: enable=module-attr


def _encode_jpg(img: bytes) -> bytes:
  return imagecodecs.jpegxl_encode_jpeg(img, numthreads=4)


def _jpegxl_encode_raw(quality: int, decoded_image: np.ndarray) -> bytes:
  return imagecodecs.jpegxl_encode(decoded_image, level=quality, numthreads=4)


def _jpegxl_encode_encapsulated(quality: int, img: bytes) -> bytes:
  with PIL.Image.open(io.BytesIO(img)) as im:
    decoded_image = np.asarray(im)
  return _jpegxl_encode_raw(quality, decoded_image)


def _gen_unencapsulated_frames(
    ds: pydicom.dataset.Dataset,
) -> Iterator[np.ndarray]:
  for frame in ds.pixel_array:
    yield frame


def transform_dicom_pixel_data(unused_args: Sequence[str]) -> None:
  """Generates a PNG of wsi-dicom image and saves to disk in working dir.

  Args:
    unused_args: Command line arguments. Unused.
  """
  input_filename = INPUT_DICOM_INSTANCE_FLAG.value
  output_filename = OUTPUT_DICOM_INSTANCE_FLAG.value
  quality = quality_flag.value
  transcode_jpeg = transcode_jpeg_flag.value
  if output_filename is None:
    output_filename = input_filename
  with pydicom.dcmread(input_filename) as ds:
    if ds.DimensionOrganizationType != 'TILED_FULL':
      raise ValueError(
          'DimensionOrganizationType is not TILED_FULL. '
          'Only TILED_FULL is supported.'
      )
    if 'ConcatenationUID' in ds:
      raise ValueError(
          'ConcatenationUID is present. Concatenation is not supported.'
      )
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
      if transcode_jpeg and ds.file_meta.TransferSyntaxUID == _JPEG_BASELINE:
        jxl_frames = executor.map(
            _encode_jpg,
            _generate_frames(ds.PixelData, ds.NumberOfFrames),
        )
        transfer_syntax_uid = _JPEGXL_JPEG
      elif ds.file_meta.TransferSyntaxUID in _UNENCAPSULATED_TRANSFER_SYNTAXES:
        frames = _gen_unencapsulated_frames(ds)
        jxl_frames = executor.map(
            functools.partial(_jpegxl_encode_raw, quality), frames
        )
        transfer_syntax_uid = _JPEGXL if quality < 100 else _JPEGXL_LOSSLESS
      else:
        jxl_frames = executor.map(
            functools.partial(_jpegxl_encode_encapsulated, quality),
            _generate_frames(ds.PixelData, ds.NumberOfFrames),
        )
        transfer_syntax_uid = _JPEGXL if quality < 100 else _JPEGXL_LOSSLESS
    if 'LossyImageCompression' not in ds or ds.LossyImageCompression == '00':
      ds.LossyImageCompression = (
          '00' if transfer_syntax_uid == _JPEGXL_LOSSLESS else '01'
      )
    ds.PixelData = pydicom.encaps.encapsulate(list(jxl_frames))
    ds.LossyImageCompressionMethod = 'ISO_18181_1'
    ds.LossyImageCompressionRatio = (
        ds.SamplesPerPixel
        * (ds.BitsAllocated / 8)
        * ds.TotalPixelMatrixColumns
        * ds.TotalPixelMatrixRows
        * ds.TotalPixelMatrixFocalPlanes
    ) / len(ds.PixelData)
    ds.file_meta.TransferSyntaxUID = transfer_syntax_uid
    # pylint: disable=unexpected-keyword-arg
    if _PYDICOM_MAJOR_VERSION <= 2:
      ds.is_implicit_VR = False
      ds.is_little_endian = True
      ds.save_as(output_filename, write_like_original=False)
    else:
      ds.save_as(
          output_filename,
          enforce_file_format=False,
          little_endian=ds.file_meta.TransferSyntaxUID != '1.2.840.10008.1.2.2',
          implicit_vr=ds.file_meta.TransferSyntaxUID == '1.2.840.10008.1.2',
          force_encoding=True,
      )
    # pylint: enable=unexpected-keyword-arg


if __name__ == '__main__':
  app.run(transform_dicom_pixel_data)
