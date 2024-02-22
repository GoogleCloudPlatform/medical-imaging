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
"""Barcode reading utils for DPAS slide label images."""

import base64
import collections
import os
import subprocess
import tempfile
import traceback
from typing import List, MutableMapping, Set

import cv2
from googleapiclient import discovery as cloud_discovery
from googleapiclient import errors as google_api_errors
import numpy as np

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags


class CloudVisionHttpError(Exception):
  pass


def _segment_barcode(image_path: str, segmented_image_path: str):
  """Segment the area of barcode from a label image.

  Args:
    image_path: image file path.
    segmented_image_path: segmented image file path if succeed.

  Raises:
    CloudVisionHttpError: if cannot connect to Cloud Vision
    ValueError: if segmentation fails.
  """
  vision_service = cloud_discovery.build('vision', 'v1', cache_discovery=False)

  with open(image_path, 'rb') as image_file:
    content = image_file.read()
  request = vision_service.images().annotate(
      body={
          'requests': [{
              'image': {'content': base64.b64encode(content).decode('UTF-8')},
              'features': [{
                  'type': 'OBJECT_LOCALIZATION',
                  'maxResults': 2,
              }],
          }]
      }
  )
  try:
    response = request.execute()
  except google_api_errors.HttpError as exp:
    raise CloudVisionHttpError(
        'Exception occurred calling cloud vision\n'
        f'exception: {traceback.format_exc()}'
    ) from exp

  try:
    annotations = response['responses'][0]['localizedObjectAnnotations']
  except KeyError as exp:
    raise ValueError(
        'Unexpected cloud vision API response\n'
        f'exception: {traceback.format_exc()}'
    ) from exp

  if not annotations:
    raise ValueError(f'No objects detected in image {image_path}')

  barcode_annotation = None
  for annotation in annotations:
    # The 2D barcode is sometimes recognized as 1D barcode.
    # Overrides the annotation if 2D barcode annotation is found.
    if annotation['name'] == '1D barcode':
      barcode_annotation = annotation
    if annotation['name'] == '2D barcode':
      barcode_annotation = annotation
      break
  if barcode_annotation is None:
    raise ValueError(f'No barcodes detected in image {image_path}')

  original_image = cv2.imread(image_path)
  original_height, original_width, _ = original_image.shape
  try:
    bounding_poly = [
        [v['x'] * original_width, v['y'] * original_height]
        for v in barcode_annotation['boundingPoly']['normalizedVertices']
    ]
  except KeyError as exp:
    cloud_logging_client.warning(
        'Segmentation results missing expected key.', exp
    )
    raise ValueError(f'No barcodes detected in image {image_path}') from exp
  x, y, width, height = cv2.boundingRect(np.array(bounding_poly).astype(int))

  # Padding to avoid edge cut off.
  # 0.1 is just a magic number works well in our dataset.
  padding = 0.1
  y -= int(padding * height)
  x -= int(padding * width)
  height += int(2 * padding * height)
  width += int(2 * padding * width)
  # Clip padded coordinates to imaging.
  y = max(y, 0)
  x = max(x, 0)
  y_max = min(y + height, original_image.shape[0])
  x_max = min(x + width, original_image.shape[1])
  cropped = original_image[y:y_max, x:x_max]
  if cropped.size == 0:
    cloud_logging_client.warning(
        'Segmentation dimensions are zero.',
        dict(x=x, width=width, y=y, height=height),
    )
    raise ValueError(f'No barcodes detected in image {image_path}')
  cv2.imwrite(segmented_image_path, cropped)


def _zxing_read_barcode(image_path: str, try_harder: bool = False) -> str:
  """Read barcode using zxing command line.

  Args:
    image_path: path to the image file.
    try_harder: take more time to try and find barcode

  Returns:
    decoded barcode content if success.

  Raises:
    ValueError if failed.
  """
  if not os.path.isfile(image_path):
    raise ValueError(f'Loading "{image_path}" failed')

  cli_params = (
      [ingest_flags.ZXING_CLI_FLG.value, '--try-harder', image_path]
      if try_harder
      else [ingest_flags.ZXING_CLI_FLG.value, image_path]
  )
  try:
    result = subprocess.run(
        cli_params,
        check=True,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
  except subprocess.CalledProcessError as exp:
    raise ValueError(f'{image_path} barcode decoding failed.') from exp
  # The zxing cli always returns 0, detect error by stderr.
  if result.stderr or result.stdout == 'decoding failed\n':
    raise ValueError(f'{image_path} barcode decoding failed {result.stderr}')

  # Output of zxing contains new lines.
  return result.stdout.strip('\n')


def _barcode_read_effort_escalation(barcode_image_path: str) -> str:
  """Reads barcode in image, firsy normal reader effort then aggressive effort.

  Args:
    barcode_image_path: path to image

  Returns:
    Barcode value

  Raises:
    ValueError: cannot segment barcode with aggressive effort.
  """
  try:
    return _zxing_read_barcode(barcode_image_path, try_harder=False)
  except ValueError:
    cloud_logging_client.info(
        f'Could not segment barcode in {barcode_image_path} now trying harder.'
    )
    return _zxing_read_barcode(barcode_image_path, try_harder=True)


def _segment_and_read_bc(image_file_path: str) -> str:
  """Segments and read image barcode.

  Args:
    image_file_path: Path to image

  Returns:
    Barcode value as string or None if barcode cannot be segmented

  Raises:
    ValueError: if segmented barcode cannot be read
  """
  with tempfile.TemporaryDirectory('segmented_barcode') as segment_barcode_dir:
    _, image_filename = os.path.split(image_file_path)
    if ingest_flags.DISABLE_CLOUD_VISION_BARCODE_SEG_FLG.value:
      cloud_logging_client.warning(
          'Barcode segmentation disabled; DISABLE_CLOUD_VISION_BARCODE_SEG '
          '= True'
      )
    else:
      try:
        segmented_barcode_image_path = os.path.join(
            segment_barcode_dir, image_filename
        )
        _segment_barcode(image_file_path, segmented_barcode_image_path)
        return _barcode_read_effort_escalation(segmented_barcode_image_path)
      except CloudVisionHttpError as exp:
        cloud_logging_client.error('Error calling Cloud Vision API', exp)
      except ValueError:
        cloud_logging_client.info(
            f'Could not segment barcode in {image_file_path}.'
        )

    # Could not read segmented segment bar code.
    # Try to read barcode from whole image.
    return _barcode_read_effort_escalation(image_file_path)


def read_barcode_in_files(
    images_to_read_barcodes: List[str],
) -> MutableMapping[str, Set[str]]:
  """Finds barcodes in files.

  Args:
    images_to_read_barcodes: List of images to read

  Returns:
    Ordered Dict of Barcode values and files containing found value
  """
  barcode_values_found = collections.OrderedDict()
  if ingest_flags.DISABLE_BARCODE_DECODER_FLG.value:
    cloud_logging_client.warning(
        'Files not tested for barcodes; DISABLE_BARCODE_DECODER = True.'
    )
    return barcode_values_found

  files_with_no_barcodes = set()
  for image_file_path in images_to_read_barcodes:
    if not os.path.isfile(image_file_path):
      continue
    try:
      barcode = _segment_and_read_bc(image_file_path)
      barcode_set = barcode_values_found.get(barcode)
      if barcode_set is None:
        barcode_set = set()
        barcode_values_found[barcode] = barcode_set
      cloud_logging_client.info(
          'Found segmented barcode',
          {'barcode': barcode, 'filename': image_file_path},
      )
      barcode_set.add(image_file_path)
    except ValueError:
      files_with_no_barcodes.add(image_file_path)
  files_with_no_barcodes = ', '.join(files_with_no_barcodes)
  if not barcode_values_found:
    cloud_logging_client.error(
        f'Could not decode barcode in file: {files_with_no_barcodes}'
    )
    return barcode_values_found
  barcode_count = len(barcode_values_found)
  if barcode_count > 1:
    cloud_logging_client.warning(
        f'Multiple barcodes (N={barcode_count}) found.', barcode_values_found
    )
    cloud_logging_client.warning(
        f'Could not decode barcode in: {files_with_no_barcodes}'
    )
  return barcode_values_found
