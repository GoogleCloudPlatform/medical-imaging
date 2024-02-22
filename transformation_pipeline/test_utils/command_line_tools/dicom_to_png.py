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
"""Command Line tool Converts WSI *.dcm DICOM in local directory to PNG.

Run from commandline:
  python3 dicom_to_png.py
"""
import logging
import multiprocessing
import os

import cv2
import numpy as np
import pydicom


def gen_dicom(filename: str):
  """Generates a PNG of wsi-dicom image and saves to disk in working dir.

  Args:
    filename: Filename in working directory
  """
  if not filename.lower().endswith('.dcm'):
    return
  with pydicom.dcmread(filename) as ds:
    wsi_width = ds.TotalPixelMatrixColumns
    wsi_height = ds.TotalPixelMatrixRows
    untruncated_frame_width = ds.Columns
    untruncated_frame_height = ds.Rows
    mem = np.zeros((wsi_height, wsi_width, 3), dtype=np.uint8)
    frame_left = 0
    frame_top = 0
    if len(ds.pixel_array.shape) == 3:
      num_frames = 1
    else:
      num_frames = ds.NumberOfFrames
    encoded_frames = pydicom.encaps.generate_pixel_data_frame(
        ds.PixelData, num_frames
    )
    for pixel_data in encoded_frames:
      frame_bottom = min(frame_top + untruncated_frame_height, wsi_height)
      frame_height = frame_bottom - frame_top
      frame_right = min(frame_left + untruncated_frame_width, wsi_width)
      frame_width = frame_right - frame_left
      # decodes encapsulation from jpeg or jpeg 2000 to raw.
      # use opencv to decode pixel array instead of pydicom as workaround
      # for bugs in pydicom jpeg decoding
      decoded_frame = cv2.imdecode(
          np.frombuffer(pixel_data, dtype=np.uint8), flags=1
      )
      svs_patch = np.asarray(decoded_frame)
      mem[frame_top:frame_bottom, frame_left:frame_right, :] = svs_patch[
          :frame_height, :frame_width, :
      ]
      frame_left += untruncated_frame_width
      if frame_left >= wsi_width:
        frame_left = 0
        frame_top += untruncated_frame_height
  outfile_name, _ = os.path.splitext(filename)
  outfile_name = f'{outfile_name}.png'
  logging.info('Generating: %s', outfile_name)
  cv2.imwrite(outfile_name, mem)


if __name__ == '__main__':
  with multiprocessing.Pool(5) as pool:
    pool.map(gen_dicom, os.listdir())
