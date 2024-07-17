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
"""Read ingestion downsampling layer generation config."""
import json
import math
import os
from typing import Any, List, Optional, Set

import openslide
import pydicom
import yaml

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import ingest_const


class MissingPixelSpacingError(Exception):
  pass


class IngestPyramidConfigError(Exception):
  pass


class _IngestPyramidConfigFileTypeError(IngestPyramidConfigError):
  pass


class _IngestPyramidConfigValidationError(IngestPyramidConfigError):
  pass


class _IngestPyramidConfigFileNotFoundError(IngestPyramidConfigError):
  pass


class _IngestPyramidConfigJsonFormatError(IngestPyramidConfigError):
  pass


class _IngestPyramidConfigYamlFormatError(IngestPyramidConfigError):
  pass


def _test_downsample_value(val: Any) -> bool:
  if not (isinstance(val, int) or isinstance(val, float)):
    return False
  if int(val) <= 0:
    return False
  return True


def _is_config_valid(decoded_config: Any) -> bool:
  """Test if pyramid definition is correctly formatted.

  Expected format:
    PixelSpacing (mm/pixel): List[int] Downsamples or single int

    example configuration for (40x, 10x , & 2.5x):
      # config for 40x = 0.00025
      0.00025 : [1, 8, 32]
      # config for 10x = 0.001
      0.001   : [1, 8]
      # config for 2.5x = 0.004
      0.004   : 1

  Args:
    decoded_config: YAML or JSON parsed into Python structures.

  Returns:
      True if correctly formatted.
  """
  if not isinstance(decoded_config, dict) or not decoded_config:
    return False
  for key, value in decoded_config.items():
    if isinstance(key, str):
      # Error if keys are strings that cannot be decoded to floats.
      # Detect errors in JSON
      try:
        float(key)
      except ValueError:
        return False
    elif not (isinstance(key, float) or isinstance(key, int)):
      return False
    if isinstance(value, list) or isinstance(value, set):
      for item in value:
        if not _test_downsample_value(item):
          return False
      continue
    if not _test_downsample_value(value):
      return False
  return True


def get_dicom_pixel_spacing(filename: str) -> float:
  """Returns pixel spacing of DICOM instance or None.

  Args:
    filename: Path to file to read.

  Returns:
    Pixel spacing in mm/pixel.

  Raises:
    MissingPixelSpacingError if pixel spacing is missing.
  """
  with pydicom.dcmread(filename, defer_size='512 KB') as ds:
    try:
      func_group_seq = ds.SharedFunctionalGroupsSequence[0]
      rows, columns = func_group_seq.PixelMeasuresSequence[0].PixelSpacing
      return float(max(rows, columns))
    except (IndexError, AttributeError) as _:
      try:
        rows = float(ds.ImagedVolumeHeight) / float(ds.TotalPixelMatrixRows)
        columns = float(ds.ImagedVolumeWidth) / float(
            ds.TotalPixelMatrixColumns
        )
        return float(max(rows, columns))
      except (ZeroDivisionError, AttributeError) as exp:
        raise MissingPixelSpacingError(
            f'Could not determine pixel spacing in DICOM: {filename}.',
            ingest_const.ErrorMsgs.MISSING_PIXEL_SPACING,
        ) from exp


def get_openslide_pixel_spacing(filename: str) -> float:
  """Returns pixel spacing of openslide readable file or None.

  Args:
    filename: Path to file to read.

  Returns:
    Pixel spacing in mm/pixel.

  Raises:
    MissingPixelSpacingError if pixel spacing is missing.
  """
  try:
    with openslide.OpenSlide(filename) as open_sld:
      # Convert from microns per pixel to mm per pixel.
      rows = float(open_sld.properties[openslide.PROPERTY_NAME_MPP_Y]) / 1000.0
      columns = (
          float(open_sld.properties[openslide.PROPERTY_NAME_MPP_X]) / 1000.0
      )
      return float(max(rows, columns))
  except (openslide.OpenSlideError, AttributeError) as exp:
    raise MissingPixelSpacingError(
        f'Could not determine pixel spacing in image: {filename}.',
        ingest_const.ErrorMsgs.MISSING_PIXEL_SPACING,
    ) from exp


def _compute_relative_downsamples(
    source_imaging_pixel_spacing, required_pixel_spacing: List[float]
) -> Set[int]:
  """Returns downsample factors* required to generate required_pixel_spacing.

  * WSI imaging is commonly downsampled at 50% (e.g., 40x, 20x, 10x, 5x, etc.).
  This models standard adjustments in microscope magnifications.
  Downsample factors returned here are are closest power of 2 downsample to
  achieve desired pixel spacing. If for example a 30x downsample is required
  to create the precise pixel spacing this function will return 2^5 or 32 for
  OOF magnifcations this will reduce the number of levels generated and as
  levels generated for the main pyramid will likely be reused.

  Args:
    source_imaging_pixel_spacing: Pixel spacing (mm/px) of source imaging.
    required_pixel_spacing: List of lower resolution pixel spacings.

  Returns:
    Set of downsamples required to generate lower resolution imaging.

  Raises:
    ZeroDivisionError: source_imaging_pixel_spacing == 0
  """
  required_downsamples = set()
  for px_spacing in required_pixel_spacing:
    # Returns closest power of 2 to achieve desired pixel spacing.  See comment
    # above.
    if px_spacing < source_imaging_pixel_spacing:
      cloud_logging_client.warning(
          'Source imaging pixel spacing is > '
          'requested downsample. Downsample will not be returned.',
          {
              'source_imaging_pixel_spacing': source_imaging_pixel_spacing,
              'requested_downsample_pixel_spacing': px_spacing,
          },
      )
      continue
    try:
      ds = int(
          pow(2, round(math.log2(px_spacing / source_imaging_pixel_spacing), 0))
      )
    except ZeroDivisionError as exp:
      cloud_logging_client.error('Source imaging pixel spacing is zero.', exp)
      raise
    required_downsamples.add(ds)
  return required_downsamples


def get_oof_downsample_levels(pixel_spacing: float) -> Set[int]:
  """Computes imaging downsampled factors necessary to run OOF."""
  # Pixel spacing for 20x and 1.25x in mm/px
  pixel_spacing_for_20x_mag = 0.0005
  pixel_spacing_for_one_and_quarter_x = 0.008
  return _compute_relative_downsamples(
      pixel_spacing,
      [pixel_spacing_for_20x_mag, pixel_spacing_for_one_and_quarter_x],
  )


def _read_yaml(config_path: str) -> Any:
  """Reads YAML file and returns contents."""
  try:
    with open(config_path, 'rt') as infile:
      return yaml.safe_load(infile)
  except FileNotFoundError as exp:
    msg = 'Error reading ingestion pyramid generation config.'
    cloud_logging_client.critical(
        msg, {'ingest_pyramid_layer_configuration': config_path}, exp
    )
    raise _IngestPyramidConfigFileNotFoundError(msg) from exp
  except yaml.YAMLError as exp:
    msg = 'Error parsing ingestion pyramid generation config.'
    cloud_logging_client.critical(
        msg, {'ingest_pyramid_layer_configuration': config_path}, exp
    )
    raise _IngestPyramidConfigYamlFormatError(msg) from exp


def _read_json(config_path: str) -> Any:
  """Reads JSON file and returns contents."""
  try:
    with open(config_path, 'rt') as infile:
      return json.load(infile)
  except FileNotFoundError as exp:
    msg = 'Error reading ingestion pyramid generation config.'
    cloud_logging_client.critical(
        msg, {'ingest_pyramid_layer_configuration': config_path}, exp
    )
    raise _IngestPyramidConfigFileNotFoundError(msg) from exp
  except json.JSONDecodeError as exp:
    msg = 'Error parsing ingestion pyramid generation config.'
    cloud_logging_client.critical(
        msg, {'ingest_pyramid_layer_configuration': config_path}, exp
    )
    raise _IngestPyramidConfigJsonFormatError(msg) from exp


class WsiPyramidGenerationConfig:
  """Image Pyramid ingestion configuration."""

  def __init__(self):
    config_path = (
        ingest_flags.INGESTION_PYRAMID_LAYER_GENERATION_CONFIG_PATH_FLG.value.strip()
    )
    self._downsample_config = None
    if not config_path:
      cloud_logging_client.info((
          'Ingestion pyramid generation config is undefined. '
          'Defaulting to full pyramid generation.'
      ))
      return
    _, ext = os.path.splitext(config_path)
    ext = ext.lower()
    if ext == '.yaml':
      self._downsample_config = _read_yaml(config_path)
    elif ext == '.json':
      self._downsample_config = _read_json(config_path)
    else:
      msg = (
          'Unknown pyramid generation config file type. File does not end in'
          ' ".json" or ".yaml"'
      )
      cloud_logging_client.error(
          msg,
          {
              'ingest_pyramid_layer_configuration': config_path,
              'file_extension_found': ext,
          },
      )
      raise _IngestPyramidConfigFileTypeError(msg)

    cloud_logging_client.info(
        'Loaded ingestion pyramid generation config.',
        {
            'ingest_pyramid_layer_configuration': config_path,
            'config_definition': str(self._downsample_config),
        },
    )

    if not _is_config_valid(self._downsample_config):
      msg = 'Ingestion pyamid config does not define dict of int lists.'
      cloud_logging_client.error(
          msg,
          {
              'ingest_pyramid_layer_configuration': config_path,
              'config_definition': str(self._downsample_config),
          },
      )
      raise _IngestPyramidConfigValidationError(msg)

  def get_downsample(self, pixel_spacing: float) -> Optional[Set[int]]:
    """Returns set of image downsampling factors to generate for an image.

    Args:
      pixel_spacing: Pixel spacing of image.

    Returns:
      Set of downsampling factors.
    """
    if self._downsample_config is None:
      return None
    found_spacing = -1
    smallest_spacing = None
    downsample = []
    for spacing, value in self._downsample_config.items():
      spacing = float(spacing)
      if (spacing <= pixel_spacing) and (spacing >= found_spacing):
        downsample = value
        found_spacing = spacing
      elif found_spacing == -1 and (
          smallest_spacing is None or smallest_spacing > spacing
      ):
        smallest_spacing = spacing
        downsample = value
    if isinstance(downsample, set):
      return downsample
    if isinstance(downsample, list):
      return set(downsample)
    return {downsample}
