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
"""Test if OpenCV build succeeded."""

import os
import tempfile

import numpy as np
from PIL import Image

try:
  import cv2  # pylint: disable=g-import-not-at-top
except ImportError:
  print('ERROR OpenCV lib not installed in python3.')
  exit(1)

# Const
_EXPECTED_LIB_JPEG_TURBO = ' /opt/libjpeg-turbo/lib64/libjpeg.a (ver 62)'
_EXPECTED_LIB_JPEG2000_DECODER = 'OpenJPEG (ver 2.5.2)'

lib_jpeg_turbo_installed = None
jpeg2000_decoder_installed = None
build_info = cv2.getBuildInformation().split('\n')
error_output = []
success_output = []
for line in build_info:
  if 'JPEG:' in line and (
      lib_jpeg_turbo_installed is None or not lib_jpeg_turbo_installed
  ):
    lib_jpeg_turbo_installed = _EXPECTED_LIB_JPEG_TURBO in line
    if not lib_jpeg_turbo_installed:
      error_output.append(
          'Error OpenCV not using expected version of LibJPEGTurbo'
      )
      error_output.append(
          f'  Expected: {_EXPECTED_LIB_JPEG_TURBO}\n   Using: {line}\n'
      )
  if 'JPEG 2000:' in line and (
      jpeg2000_decoder_installed is None or not jpeg2000_decoder_installed
  ):
    jpeg2000_decoder_installed = _EXPECTED_LIB_JPEG2000_DECODER in line
    if not jpeg2000_decoder_installed:
      error_output.append(
          'Error OpenCV not using expected version of JPEG2000 decoder'
      )
      error_output.append(
          f'  Expected: {_EXPECTED_LIB_JPEG2000_DECODER}\n   Using: {line}\n'
      )

if lib_jpeg_turbo_installed is None:
  error_output.append('Error OpenCV not configured with jpeg codec.\n')
if jpeg2000_decoder_installed is None:
  error_output.append('Error OpenCV not configured with JPEG2000 codec.\n')

if not error_output:
  success_output.append(
      'Test Passed: OpenCV Python build using expected libraries.'
  )
  # test read/write jpeg2000
  tst_img = np.zeros((500, 500, 3), dtype=np.uint8)
  with tempfile.TemporaryDirectory() as tmp:
    imgpth = os.path.join(tmp, 'test.jp2')
    cv2.imwrite(imgpth, tst_img)
    img = Image.open(imgpth)
    if img.format == 'JPEG2000':
      success_output.append('Test Passed: OpenCV write JPEG 2000 (jp2).')
    else:
      error_output.append(
          'Error: Could not write JPEG 2000 file from python using opencv.'
      )

if error_output:
  print('\n'.join(error_output))
if success_output:
  print('\n'.join(success_output))
exit(1 if error_output else 0)
