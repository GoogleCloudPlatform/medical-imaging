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
"""Dicom File Reference for WSI images."""
import functools
from typing import Any, List, Mapping

import pydicom

from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_file_ref


class WSIDicomFileRef(dicom_file_ref.DicomFileRef):
  """Dicom File Reference for WSI images."""

  def __init__(self):
    super().__init__()
    self.define_tag(ingest_const.DICOMTagKeywords.IMAGE_TYPE)
    self.define_tag(ingest_const.DICOMTagKeywords.FRAME_TYPE)
    self.define_tag(ingest_const.DICOMTagKeywords.BITS_ALLOCATED)
    self.define_tag(ingest_const.DICOMTagKeywords.BITS_STORED)
    self.define_tag(ingest_const.DICOMTagKeywords.HIGH_BIT)
    self.define_tag(ingest_const.DICOMTagKeywords.PIXEL_REPRESENTATION)
    self.define_tag(ingest_const.DICOMTagKeywords.SAMPLES_PER_PIXEL)
    self.define_tag(ingest_const.DICOMTagKeywords.PLANAR_CONFIGURATION)
    self.define_tag(ingest_const.DICOMTagKeywords.ACCESSION_NUMBER)
    self.define_tag(ingest_const.DICOMTagKeywords.SERIES_NUMBER)
    self.define_tag(ingest_const.DICOMTagKeywords.BARCODE_VALUE)
    self.define_tag(ingest_const.DICOMTagKeywords.INSTANCE_NUMBER)
    self.define_tag(ingest_const.DICOMTagKeywords.PHOTOMETRIC_INTERPRETATION)
    self.define_tag(ingest_const.DICOMTagKeywords.IMAGED_VOLUME_WIDTH)
    self.define_tag(ingest_const.DICOMTagKeywords.IMAGED_VOLUME_HEIGHT)
    self.define_tag(ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_COLUMNS)
    self.define_tag(ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_ROWS)
    self.define_tag(ingest_const.DICOMTagKeywords.NUMBER_OF_FRAMES)
    self.define_tag(ingest_const.DICOMTagKeywords.COLUMNS)
    self.define_tag(ingest_const.DICOMTagKeywords.ROWS)
    self.define_tag(ingest_const.DICOMTagKeywords.DIMENSION_ORGANIZATION_TYPE)
    self.define_tag(ingest_const.DICOMTagKeywords.MODALITY)
    self.define_tag(ingest_const.DICOMTagKeywords.SPECIMEN_LABEL_IN_IMAGE)
    self.define_tag(ingest_const.DICOMTagKeywords.BURNED_IN_ANNOTATION)
    self.define_tag(ingest_const.DICOMTagKeywords.CONCATENATION_UID)
    self.define_tag(ingest_const.DICOMTagKeywords.FRAME_OF_REFERENCE_UID)
    self.define_tag(ingest_const.DICOMTagKeywords.POSITION_REFERENCE_INDICATOR)
    self.define_tag(ingest_const.DICOMTagKeywords.PYRAMID_UID)
    self.define_tag(ingest_const.DICOMTagKeywords.PYRAMID_LABEL)
    self.define_tag(ingest_const.DICOMTagKeywords.PYRAMID_DESCRIPTION)
    self.define_tag(
        ingest_const.DICOMTagKeywords.CONCATENATION_FRAME_OFFSET_NUMBER
    )
    self.define_tag(ingest_const.DICOMTagKeywords.IN_CONCATENATION_NUMBER)
    self.define_tag(
        ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_FOCAL_PLANES
    )
    self.define_private_tag(ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG)

  @property
  def position_reference_indicator(self) -> str:
    return self.get_tag_value(
        ingest_const.DICOMTagKeywords.POSITION_REFERENCE_INDICATOR
    )

  @property
  def frame_of_reference_uid(self) -> str:
    return self.get_tag_value(
        ingest_const.DICOMTagKeywords.FRAME_OF_REFERENCE_UID
    )

  @property
  def concatenation_uid(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.CONCATENATION_UID)

  @property
  def concatenation_frame_offset_number(self) -> str:
    return self.get_tag_value(
        ingest_const.DICOMTagKeywords.CONCATENATION_FRAME_OFFSET_NUMBER
    )

  @property
  def in_concatenation_number(self) -> str:
    return self.get_tag_value(
        ingest_const.DICOMTagKeywords.IN_CONCATENATION_NUMBER
    )

  @property
  def image_type(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.IMAGE_TYPE)

  @property
  def frame_type(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.FRAME_TYPE)

  @property
  def bits_allocated(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.BITS_ALLOCATED)

  @property
  def bits_stored(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.BITS_STORED)

  @property
  def high_bit(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.HIGH_BIT)

  @property
  def pixel_representation(self) -> str:
    return self.get_tag_value(
        ingest_const.DICOMTagKeywords.PIXEL_REPRESENTATION
    )

  @property
  def samples_per_pixel(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.SAMPLES_PER_PIXEL)

  @property
  def planar_configuration(self) -> str:
    return self.get_tag_value(
        ingest_const.DICOMTagKeywords.PLANAR_CONFIGURATION
    )

  @property
  def accession_number(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.ACCESSION_NUMBER)

  @property
  def series_number(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.SERIES_NUMBER)

  @property
  def barcode_value(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.BARCODE_VALUE)

  @property
  def instance_number(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.INSTANCE_NUMBER)

  @property
  def photometric_interpretation(self) -> str:
    return self.get_tag_value(
        ingest_const.DICOMTagKeywords.PHOTOMETRIC_INTERPRETATION
    )

  @property
  def imaged_volume_width(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.IMAGED_VOLUME_WIDTH)

  @property
  def imaged_volume_height(self) -> str:
    return self.get_tag_value(
        ingest_const.DICOMTagKeywords.IMAGED_VOLUME_HEIGHT
    )

  @property
  def total_pixel_matrix_columns(self) -> str:
    return self.get_tag_value(
        ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_COLUMNS
    )

  @property
  def total_pixel_matrix_focal_planes(self) -> str:
    return self.get_tag_value(
        ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_FOCAL_PLANES
    )

  @property
  def total_pixel_matrix_rows(self) -> str:
    return self.get_tag_value(
        ingest_const.DICOMTagKeywords.TOTAL_PIXEL_MATRIX_ROWS
    )

  @property
  def number_of_frames(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.NUMBER_OF_FRAMES)

  @property
  def columns(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.COLUMNS)

  @property
  def rows(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.ROWS)

  @property
  def hash(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG)

  @property
  def dimension_organization_type(self) -> str:
    return self.get_tag_value(
        ingest_const.DICOMTagKeywords.DIMENSION_ORGANIZATION_TYPE
    )

  @property
  def modality(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.MODALITY)

  @property
  def specimen_label_in_image(self) -> str:
    return self.get_tag_value(
        ingest_const.DICOMTagKeywords.SPECIMEN_LABEL_IN_IMAGE
    )

  @property
  def burned_in_annotation(self) -> str:
    return self.get_tag_value(
        ingest_const.DICOMTagKeywords.BURNED_IN_ANNOTATION
    )

  @property
  def pyramid_uid(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.PYRAMID_UID)

  @property
  def pyramid_label(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.PYRAMID_LABEL)

  @property
  def pyramid_description(self) -> str:
    return self.get_tag_value(ingest_const.DICOMTagKeywords.PYRAMID_DESCRIPTION)


def init_wsi_dicom_file_ref_from_file(fname: str) -> WSIDicomFileRef:
  return dicom_file_ref.init_from_file(fname, WSIDicomFileRef())


def init_wsi_dicom_file_ref_from_json(
    file_json: Mapping[str, Any]
) -> WSIDicomFileRef:
  return dicom_file_ref.init_from_json(file_json, WSIDicomFileRef())


def init_from_pydicom_dataset(
    file_path: str, ds: pydicom.Dataset
) -> WSIDicomFileRef:
  return dicom_file_ref.init_from_loaded_file(file_path, ds, WSIDicomFileRef())


@functools.cache
def get_wsi_dicom_file_ref_tag_address_list() -> List[str]:
  """Returns list of tag address for tags needed to init WSIDICOMFileRef."""
  return [address[2:] for address in WSIDicomFileRef().tag_addresses()]
