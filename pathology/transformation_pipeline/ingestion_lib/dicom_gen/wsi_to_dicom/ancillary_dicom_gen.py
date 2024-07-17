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
"""Generate Ancillary Image (Thumbnail, Marco, Label) DICOM."""

import os
from typing import Any, Dict, List, Mapping, Optional

import cv2
import pydicom

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_private_tag_generator
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_image_extractor
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util
from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard


AncillaryImage = ancillary_image_extractor.AncillaryImage


def _gen_ancillary_dicom_instance(
    svs_path: str,
    image: AncillaryImage,
    ds: pydicom.Dataset,
    description: str,
    instance_number: str,
    wsi_dcm_json: Dict[str, Any],
    private_tags: List[dicom_private_tag_generator.DicomPrivateTag],
    additional_wsi_metadata: pydicom.Dataset,
) -> str:
  """Converts jpg(label, thumbnail and macro images) into wsi-dicom.

  Args:
    svs_path: path to SVS file which images were generated from
    image: AncillaryImage to generate instance from
    ds: reference DICOM dataset to modify.
    description: Description of instance.
    instance_number: DICOM instance number.
    wsi_dcm_json: metadata mapping schema.
    private_tags: private tags to add to DICOMS.
    additional_wsi_metadata: Additional metadata to merge with gen DICOM.

  Returns:
     Path to file name of generated DICOM file.
  """
  image_path = image.path
  # get image shape
  image_shape = cv2.imread(image_path).shape[:2]

  # get image bytes
  with open(image_path, 'rb') as img:
    image_bytes = img.read()

  # Initialize VL Whole Slide Microscopy Image Storage header
  # 'Secondary Capture Image Storage'
  # Image bytes Implicit VR Endian: Default Transfer Syntax for DICOM
  # RAW bytes image.
  file_meta = pydicom.dataset.FileMetaDataset()
  sop_class_id = dicom_standard.dicom_standard_util().get_sop_classname_uid(
      'VL Whole Slide Microscopy Image Storage'
  )
  file_meta.MediaStorageSOPClassUID = sop_class_id
  file_meta.TransferSyntaxUID = ingest_const.DicomImageTransferSyntax.JPEG_LOSSY
  # end todo
  ds = pydicom.dataset.FileDataset(
      '', ds, file_meta=file_meta, preamble=b'\0' * 128
  )
  ds.SOPClassUID = sop_class_id
  ds.InstanceNumber = instance_number

  # define additional image tags
  ds.is_little_endian = True
  ds.is_implicit_VR = False  # Required for correct handling of private tags

  rows, columns = image_shape
  # Compute effective compression ratio.
  ds.LossyImageCompressionRatio = str(
      round((rows * columns * 3) / len(image_bytes), 2)
  )
  ds.TotalPixelMatrixRows = rows
  ds.TotalPixelMatrixColumns = columns
  ds.Rows = rows
  ds.Columns = columns

  ds.PhotometricInterpretation = image.photometric_interpretation
  if image.extracted_without_decompression:
    ds.DerivationDescription = (
        'Image bytes extracted from SVS file without decompression and '
        'ecapsulated in DICOM; image unchanged.'
    )
  else:
    ds.DerivationDescription = (
        'Image uncompressed from SVS file and saved '
        'using lossy jpeg compression (quality: 95%).'
    )

  pixel_data = pydicom.encaps.encapsulate([image_bytes])
  # pad pixel data to even number of bytes
  ds.PixelData = dicom_util.pad_bytes_to_even_length(pixel_data)
  ds['PixelData'].is_undefined_length = True
  if 'label' == description:
    image_type = 'ORIGINAL\\PRIMARY\\LABEL\\NONE'
    icc = ancillary_image_extractor.label_icc(svs_path)
    ds.SpecimenLabelInImage = 'YES'
  elif 'thumbnail' == description:
    image_type = 'ORIGINAL\\PRIMARY\\THUMBNAIL\\NONE'
    icc = ancillary_image_extractor.thumbnail_icc(svs_path)
    ds.SpecimenLabelInImage = 'NO'
  elif 'macro' == description:
    image_type = 'ORIGINAL\\PRIMARY\\OVERVIEW\\NONE'
    icc = ancillary_image_extractor.macro_icc(svs_path)
    ds.SpecimenLabelInImage = 'YES'
  else:
    image_type = f'ORIGINAL\\PRIMARY\\{description.upper()}\\NONE'
    icc = None
    ds.SpecimenLabelInImage = 'NO'
    cloud_logging_client.error(
        'Unexpected ancillary image type', {'type': description}
    )
  ds.ImageType = image_type
  if 'OpticalPathSequence' in ds:
    # Force optical path sequence to be written.
    # Pre-existing opptical path sequence in ds may not include icc profile.
    # fix: b/239442285
    del ds['OpticalPathSequence']
  dicom_util.add_default_optical_path_sequence(ds, icc)
  dicom_util.add_default_total_pixel_matrix_origin_sequence_if_not_defined(ds)
  dicom_util.add_metadata_to_generated_wsi_dicom(
      additional_wsi_metadata, wsi_dcm_json, private_tags, ds
  )
  dicom_util.add_missing_type2_dicom_metadata(ds)
  filename = f'{image_path}.dcm'
  ds.save_as(filename, write_like_original=False)
  return filename


def generate_ancillary_dicom(
    gen_dicom: abstract_dicom_generation.GeneratedDicomFiles,
    ancillary_images: Optional[List[AncillaryImage]],
    wsi_dcm_json: Dict[str, Any],
    private_tags: List[dicom_private_tag_generator.DicomPrivateTag],
    additional_wsi_metadata: pydicom.Dataset,
    metadata: Mapping[str, pydicom.DataElement],
) -> List[str]:
  """Generates svs ancillary DICOM images (thumbnail, macro, label).

     Images saved using whole slide imaging IOD.

  Args:
    gen_dicom: Structure containing generated dicom, source svs file, hash, etc.
    ancillary_images: List of ancillary images to generate DICOMS.
    wsi_dcm_json: Metadata mapping schema.
    private_tags: Private tags to add to DICOMS.
    additional_wsi_metadata: Additional metadata to merge with gen DICOM.
    metadata: Metadata to merge with DICOMS.

  Returns:
    List of paths to DICOM files generated.
  """
  if ancillary_images is None or not ancillary_images:
    return []
  ancillary_dicom_files = []
  svs_path = gen_dicom.localfile

  # Initialize Base DICOM tags that are common
  # across all ancillary images.
  reference_dcm = pydicom.Dataset()
  reference_dcm.Modality = 'SM'
  reference_dcm.SamplesPerPixel = 3
  reference_dcm.PlanarConfiguration = 0
  reference_dcm.BitsAllocated = 8
  reference_dcm.BitsStored = 8
  reference_dcm.HighBit = 7
  reference_dcm.PixelRepresentation = 0
  reference_dcm.RepresentativeFrameNumber = 1
  reference_dcm.ImageOrientationSlide = '0\\-1\\0\\-1\\0\\0'
  reference_dcm.BurnedInAnnotation = 'NO'
  reference_dcm.NumberOfFrames = 1
  reference_dcm.DimensionOrganizationType = ingest_const.TILED_FULL
  # All images are lossy JPEGS
  reference_dcm.LossyImageCompression = '01'
  reference_dcm.LossyImageCompressionMethod = 'ISO_10918_1'
  dicom_util.set_all_defined_pydicom_tags(reference_dcm, metadata)
  gen_wsi_file_count = len(gen_dicom.generated_dicom_files) + 1
  for index, image in enumerate(ancillary_images):
    instance_number = index + gen_wsi_file_count
    fname = os.path.basename(image.path)
    instance_description, _ = os.path.splitext(fname)
    dcm_file = _gen_ancillary_dicom_instance(
        svs_path,
        image,
        reference_dcm,
        instance_description,
        str(instance_number),
        wsi_dcm_json,
        private_tags,
        additional_wsi_metadata,
    )
    ancillary_dicom_files.append(dcm_file)
    cloud_logging_client.info(f'Gen DICOM: {instance_description}')
  return ancillary_dicom_files
