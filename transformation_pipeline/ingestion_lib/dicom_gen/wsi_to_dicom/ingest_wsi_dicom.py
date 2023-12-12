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
"""Converts WSI DICOM image to DICOM Pyramid."""
import dataclasses
import os
import shutil
import tempfile
from typing import Any, Dict, List, Optional, Tuple
import zipfile

import pydicom

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import hash_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import abstract_dicom_generation
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_file_ref
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_json_util
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_private_tag_generator
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_schema_util
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ancillary_image_extractor
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import decode_slideid
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import dicom_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingest_base
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingested_dicom_file_ref
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import metadata_storage_client
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import validate_ingested_dicom
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import wsi_pyramid_gen_config
from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard

# DICOM tag keywords
_NUMBER_OF_OPTICAL_PATHS = 'NumberOfOpticalPaths'
_IMAGE_ORIENTATION_SLIDE = 'ImageOrientationSlide'
_EXTENDED_DEPTH_OF_FIELD = 'ExtendedDepthOfField'
_FOCUS_METHOD = 'FocusMethod'
_STUDY_ID = 'StudyID'
_SERIES_NUMBER = 'SeriesNumber'
_DEVICE_SERIAL_NUMBER = 'DeviceSerialNumber'
_ACQUISITION_DATETIME = 'AcquisitionDateTime'
_ACQUISITION_DURATION = 'AcquisitionDuration'
_ACQUISITION_DEVICE_PROCESSING_DESCRIPTION = (
    'AcquisitionDeviceProcessingDescription'
)
_RECOMMENDED_ABSENT_PIXEL_CIE_LAB_VALUE = 'RecommendedAbsentPixelCIELabValue'
_TOTAL_PIXEL_MATRIX_ORIGIN_SEQUENCE = 'TotalPixelMatrixOriginSequence'
_ACQUISITION_CONTEXT_SEQUENCE = 'AcquisitionContextSequence'
_ACQUISITION_CONTEXT_DESCRIPTION = 'AcquisitionContextDescription'
_VOLUMETRIC_PROPERTIES = 'VolumetricProperties'
_NUMBER_OF_FOCAL_PLANES = 'NumberOfFocalPlanes'
_DISTANCE_BETWEEN_FOCAL_PLANES = 'DistanceBetweenFocalPlanes'
_TOTAL_PIXEL_MATRIX_FOCAL_PLANES = 'TotalPixelMatrixFocalPlanes'
_PRESENTATION_LUT_SHAPE = 'PresentationLUTShape'
_RESCALE_INTERCEPT = 'RescaleIntercept'
_RESCALE_SLOPE = 'RescaleSlope'
_LABEL_TEXT = 'LabelText'

# ICC_Profile not included. Handled separately.
_ADDITIONAL_DICOM_TAGS_TO_COPY_TO_GENERATED_INSTANCES = (
    ingest_const.DICOMTagKeywords.OPTICAL_PATH_SEQUENCE,
    _NUMBER_OF_OPTICAL_PATHS,
    _IMAGE_ORIENTATION_SLIDE,
    _EXTENDED_DEPTH_OF_FIELD,
    _FOCUS_METHOD,
    _STUDY_ID,
    _SERIES_NUMBER,
    _DEVICE_SERIAL_NUMBER,
    _ACQUISITION_DATETIME,
    _ACQUISITION_DURATION,
    _ACQUISITION_DEVICE_PROCESSING_DESCRIPTION,
    _RECOMMENDED_ABSENT_PIXEL_CIE_LAB_VALUE,
    _TOTAL_PIXEL_MATRIX_ORIGIN_SEQUENCE,
    _ACQUISITION_CONTEXT_SEQUENCE,
    _ACQUISITION_CONTEXT_DESCRIPTION,
    _VOLUMETRIC_PROPERTIES,
    _NUMBER_OF_FOCAL_PLANES,
    _DISTANCE_BETWEEN_FOCAL_PLANES,
    _TOTAL_PIXEL_MATRIX_FOCAL_PLANES,
    _PRESENTATION_LUT_SHAPE,
    _RESCALE_INTERCEPT,
    _RESCALE_SLOPE,
    _LABEL_TEXT,
)

_UNKOWN_SOPCLASS_UID = 'Unknown SOPClassUID'


def _unarchive_zipfile(zip_file_path: str, out_directory: str) -> List[str]:
  """Unarchives zip files to directory.

  Args:
    zip_file_path: Path to zip file.
    out_directory: Directory to unarchive files to.

  Returns:
    List of files unarchived or passed file path if not zip file.
  """
  if not zipfile.is_zipfile(zip_file_path):
    cloud_logging_client.logger().info(
        'File is not zip file. Atttempting to process as DICOM.',
        {'filename': zip_file_path},
    )
    return [zip_file_path]
  file_list = []
  cloud_logging_client.logger().info('Decompressing zip file.')
  with tempfile.TemporaryDirectory() as unarchive_temp_path:
    with zipfile.ZipFile(zip_file_path, allowZip64=True) as zip_archive:
      for zip_file_info in zip_archive.infolist():
        if zip_file_info.is_dir():
          cloud_logging_client.logger().warning(
              'Zip file contains directories; directories ignored.'
          )
          continue
        if zip_file_info.file_size == 0:
          cloud_logging_client.logger().warning(
              'Ingoring zipped file. File contained 0 bytes',
              {'filename': zip_file_info.filename},
          )
          continue
        # Extract file to temp directory and move to desired location.
        extracted_file = zip_archive.extract(zip_file_info, unarchive_temp_path)
        outfile = os.path.join(out_directory, f'file_{len(file_list)}.dcm')
        shutil.move(extracted_file, outfile)
        file_list.append(outfile)
  cloud_logging_client.logger().info(
      'Decompressed files.', {'decompressed_file_list': str(file_list)}
  )
  return file_list


def get_dicom_filerefs_list(
    file_paths: List[str],
) -> List[ingested_dicom_file_ref.IngestDicomFileRef]:
  """Returns list of DICOM files from file list.

  Args:
    file_paths: List of paths to files

  Returns:
    List of IngestDicomFileRef to DICOM files.

  Raises:
    ingest_base.GenDicomFailedError: Ingestion does not contain any DICOM
  """
  file_ref_list = []
  for path in file_paths:
    try:
      file_ref = ingested_dicom_file_ref.load_ingest_dicom_fileref(path)
      file_ref_list.append(file_ref)
      cloud_logging_client.logger().info(
          'Received DICOM file instance in ingestion payload.',
          {'filename': path},
          file_ref.dict(),
      )
    except pydicom.errors.InvalidDicomError as exp:
      cloud_logging_client.logger().warning(
          'Received Non-DICOM file in ingestion payload.  Ignoring file.',
          {'filename': path},
          exp,
      )
  if not file_ref_list:
    raise ingest_base.GenDicomFailedError(
        'Did not receive any DICOM files in ingestion payload.',
        ingest_const.ErrorMsgs.WSI_DICOM_PAYLOAD_MISSING_DICOM_FILES,
    )
  return file_ref_list


def _wsi_image_type_sort_key(
    ref: ingested_dicom_file_ref.IngestDicomFileRef,
) -> int:
  """Sorting key for WSI image type.

     Sort order.
       ORIGINAL, PRIMARY, VOLUME
       ORIGINAL, SECONDARY, VOLUME
       DERIVED, PRIMARY, VOLUME
       DERIVED, SECONDARY, VOLUME
       LABEL, THUMBNAIL, OVERVIEW images.

  Args:
    ref: DicomFileRef to return image_type sort order.

  Returns:
    Sort order as int
  """
  if ingest_const.OVERVIEW in ref.image_type:
    value = 4000
  elif ingest_const.THUMBNAIL in ref.image_type:
    value = 3000
  elif ingest_const.LABEL in ref.image_type:
    value = 2000
  else:
    value = 1
  if ingest_const.DERIVED in ref.image_type:  # Alternative ORIGINAL = 0
    value += 100
  if ingest_const.SECONDARY in ref.image_type:  # Alternative PRIMARY = 0
    value += 10
  return value


def _other_instance_number_sort_key(
    ref: ingested_dicom_file_ref.IngestDicomFileRef,
) -> int:
  """Sorting key for none-wsi DICOM.

     Returns instance number or number smaller than allowed by DICOM spec for
     instance number tag.

  Args:
    ref: DicomFileRef to return image_type sort order.

  Returns:
    Sort order as int
  """
  try:
    return int(ref.instance_number)
  except ValueError:
    return -(
        2**35
    )  # Number smaller than anything representable in DICOM IS VR.


def _dcmref_instance_key(
    ref: ingested_dicom_file_ref.IngestDicomFileRef,
) -> Tuple[str, str, str]:
  """Returns DICOM UID triple as a Tuple."""
  return (ref.study_instance_uid, ref.series_instance_uid, ref.sop_instance_uid)


def _pydicom_instance_key(dcm: pydicom.Dataset) -> Tuple[str, str, str]:
  """Returns DICOM UID triple as a Tuple."""
  return (dcm.StudyInstanceUID, dcm.SeriesInstanceUID, dcm.SOPInstanceUID)


class _DicomInstanceNumberAssignmentUtil:
  """Helper class assigns instance numbers to ingested DICOM a gen DICOM.

  Assigns WSI images instance numbers in ascending order. Highest number
  to highest magnification. Instance numbers for label, overview, and
  thumbnail images follow. Non pyramid images follow these and re-mapped only
  if necessary.
  """

  def __init__(self):
    self._dicom_wsi_ref_list = []
    self._other_dicom_ref_list = []
    self._instance_number_dict = {}
    self._done_adding_instances = False

  def add_inst(self, ref_lst: List[ingested_dicom_file_ref.IngestDicomFileRef]):
    """Adds DICOM reference to mapping.

    Args:
      ref_lst: List[Dicom reference to add to internal state]

    Raises:
      ValueError: if method called after get_instance_number.
    """
    if self._done_adding_instances:
      raise ValueError(
          'Cannot add additional instances after calling get_instance_number.'
      )
    for ref in ref_lst:
      if (
          ref.sop_class_uid
          == ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.uid
      ):
        self._dicom_wsi_ref_list.append(ref)
      else:
        self._other_dicom_ref_list.append(ref)

  def _get_dicom_ref_pixel_area(
      self, ref: ingested_dicom_file_ref.IngestDicomFileRef
  ) -> int:
    return int(ref.total_pixel_matrix_columns) * int(
        ref.total_pixel_matrix_rows
    )

  def _generate_instance_number_dict(self):
    """Generates internal instance number mapping dictionary.

    Dictionary maps[DICOM uid triple] to instance number

    WSI images are sorted first by width (total_pixel_matrix_columns) and
    then by image type. WSI images are numbered starting at 1. Instance
    numbers are incremented when image dimensions or image type changes.
    This will number SOP from concatenated instances with the same instance
    number.

    Non-WSI DICOM instances are renumbered only if absolutely necessary (i.e.
    if a WSI received a instance number that was previously allocated to
    non-WSI DICOM).
    """
    if not self._dicom_wsi_ref_list and not self._other_dicom_ref_list:
      return
    instance_number = 1
    if self._dicom_wsi_ref_list:  # iterate over WSI instances
      # sort WSI DICOM instances first by the width of the instance.
      self._dicom_wsi_ref_list.sort(
          reverse=True, key=self._get_dicom_ref_pixel_area
      )
      # next sort the DICOM by image type. Python sorting is in place
      # i.e. relative position is preserved
      self._dicom_wsi_ref_list.sort(key=_wsi_image_type_sort_key)
      prior_ref = self._dicom_wsi_ref_list[0]
      # Now renumber the instances. Instance number is incremented if
      # width or image type changes.
      prior_key = (prior_ref.image_type, prior_ref.total_pixel_matrix_columns)
      for ref in self._dicom_wsi_ref_list:
        test_key = (ref.image_type, ref.total_pixel_matrix_columns)
        if test_key != prior_key:
          instance_number += 1
          prior_key = test_key
        # store the tuples instance number in the dict so it can be looked up.
        self._instance_number_dict[_dcmref_instance_key(ref)] = instance_number

    # Iterate over non-WSI DICOM
    if self._other_dicom_ref_list:
      # Sort these DICOM by instance number, smallest to largest.
      self._other_dicom_ref_list.sort(key=_other_instance_number_sort_key)
      mapped_instance = None
      for ref in self._other_dicom_ref_list:
        # Get the instance number previously assigned to the DICOM.
        # If it doesn't have one thats ok. Don't assign anything to DICOM.
        # Instance numbers are not technically required.
        try:
          ref_instance_num = int(ref.instance_number)
        except ValueError:  # if instance_number is not set do not set.
          self._instance_number_dict[_dcmref_instance_key(ref)] = None
          continue
        # if instance number is set then determine if we are seeing a new one
        # or mapping one we have seen
        if ref_instance_num != mapped_instance:  # if it's a new number and its
          mapped_instance = ref_instance_num  # bigger than what we are
          if ref_instance_num > instance_number:  # mapping use the new value
            instance_number = ref_instance_num  # instances sorted sml -> lrg
          else:
            instance_number += 1  # otherwise increment the instance count
        # Assign the non-WSI instance its instance number.
        self._instance_number_dict[_dcmref_instance_key(ref)] = instance_number

  def get_instance_number(self, dcm: pydicom.Dataset) -> Optional[int]:
    """Returns instance number for pydicom.Dataset."""
    if not self._instance_number_dict:
      self._generate_instance_number_dict()
      self._done_adding_instances = True
    return self._instance_number_dict[_pydicom_instance_key(dcm)]


@dataclasses.dataclass
class DeDuplicatedDicom:
  dicom_file_refs: List[ingested_dicom_file_ref.IngestDicomFileRef]

  @property
  def file_paths(self) -> List[str]:
    return [ref.source for ref in self.dicom_file_refs]  # pylint: disable=not-an-iterable


def _remove_duplicate_generated_dicom(
    generate_dicoms: List[str],
    ingested_files: List[ingested_dicom_file_ref.IngestDicomFileRef],
) -> DeDuplicatedDicom:
  """Removes generated dicom which were already represented.

  Args:
    generate_dicoms: List of file paths to dicom to tests.
    ingested_files: List of ingested files to filter against.

  Returns:
    List of paths to de-duplicated DICOM file references and paths.
  """
  de_duplicated_dicom_list = []
  for path in generate_dicoms:
    gen_file_ref = dicom_file_ref.init_from_file(
        path, ingested_dicom_file_ref.IngestDicomFileRef()
    )
    # DICOM generated in C++ code have not been updated to contain
    # source hash tags or DPAS prefixed instance UIDs. Normally the absence of
    # both of these would cause DPAS to treat the DICOM as an externally
    # generated DICOM and use the study, series, instance uid to test
    # equality. In this case, this test is incorrect and the DICOM should be
    # tested as an internally generated DICOM. The instances hash and instance
    # are be updated later in conjunction with other metadata modifications
    # (performance, to avoid multiple read/writes).
    #
    # ignore_source_image_hash_tag = True
    # The parameter makes is_wsidcmref_in_list less specific, and does not
    # the generated dicom's source image's be generated from the same binary.
    # TLDR, Function can evaluate to true, for two DICOM with hash !=.
    # Ignoring Study Instance UID to fix: b/240621864
    if dicom_store_client.is_dpas_dicom_wsidcmref_in_list(
        gen_file_ref,
        ingested_files,
        ignore_source_image_hash_tag=True,
        ignore_tags=[ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID],
    ):
      cloud_logging_client.logger().info(
          (
              'Generated DICOM instance duplicates ingested instance. '
              'Ingested instance preferred. Ignoring duplicate generated '
              'instance.'
          ),
          gen_file_ref.dict(),
      )
      continue
    de_duplicated_dicom_list.append(gen_file_ref)
  return DeDuplicatedDicom(de_duplicated_dicom_list)


def _determine_slideid(
    barcode_value: str,
    filename: str,
    wsi_image_filerefs: List[ingested_dicom_file_ref.IngestDicomFileRef],
    metadata: metadata_storage_client.MetadataStorageClient,
) -> str:
  """Determine slideid for ingested dicom.

  Args:
    barcode_value: Barcode value defined in tag in DICOM.
    filename: Filename to test for slide id.
    wsi_image_filerefs: List of validated dicom files to test for barcode.
    metadata: Slide metadata storage client.

  Returns:
    SlideID as string

  Raises:
    GenDICOMFailedError: Error resulting from inability to determine slide id
  """

  # First use SlideId in DICOM
  # Next use SlideId in filename
  # Next decode images
  # attempt to get slide id from barcode tag in dicom file.
  try:
    slide_id = decode_slideid.find_slide_id_in_metadata(barcode_value, metadata)
    cloud_logging_client.logger().info(
        'Slide ID identified in DICOM BarcodeValue tag.',
        {ingest_const.LogKeywords.SLIDE_ID: slide_id},
    )
    return slide_id
  except decode_slideid.SlideIdIdentificationError as exp:
    current_exception = exp
  # attempt to get slide id from ingested file filename
  try:
    slide_id = decode_slideid.get_slide_id_from_filename(
        filename, metadata, current_exception
    )
    cloud_logging_client.logger().info(
        'Slide ID identified in file name.',
        {
            ingest_const.LogKeywords.filename: filename,
            ingest_const.LogKeywords.SLIDE_ID: slide_id,
        },
    )
    return slide_id
  except decode_slideid.SlideIdIdentificationError as exp:
    current_exception = exp

  # attempt to get slide id from barcodes in ancillary images
  with tempfile.TemporaryDirectory() as working_dir:
    ancillary_images = (
        ancillary_image_extractor.get_ancillary_images_from_dicom(
            wsi_image_filerefs, working_dir
        )
    )
    try:
      slide_id = decode_slideid.get_slide_id_from_ancillary_images(
          ancillary_images, metadata, current_exception
      )
      cloud_logging_client.logger().info(
          'Slide ID identified in image barcode.',
          {ingest_const.LogKeywords.SLIDE_ID: slide_id},
      )
      return slide_id
    except decode_slideid.SlideIdIdentificationError as exp:
      raise ingest_base.GenDicomFailedError(
          'Could not decode barcode.', str(exp)
      ) from exp


def _get_icc_profile(ds: pydicom.FileDataset) -> Optional[bytes]:
  """Returns ICC profile from pydicom dataset.

  Args:
    ds: Pydicom dataset.

  Raises:
    ValueError: More than one optical sequence defined.

  Returns:
    ICC Profile
  """
  if ingest_const.DICOMTagKeywords.OPTICAL_PATH_SEQUENCE in ds:
    if len(ds.OpticalPathSequence) == 1:
      seq_element = ds.OpticalPathSequence[0]
      if ingest_const.DICOMTagKeywords.ICC_PROFILE in seq_element:
        return seq_element.ICCProfile
    if len(ds.OpticalPathSequence) > 1:
      raise ValueError(
          'DICOM with more than one optical sequence not supported.'
      )
  if ingest_const.DICOMTagKeywords.ICC_PROFILE in ds:
    return ds.ICCProfile
  return None


def _add_correct_optical_path_sequence(
    ds: pydicom.FileDataset, reference_icc_profile: Optional[bytes] = None
) -> None:
  """Corrects ICC Color Space or adds default optical path sequence if missing.

  Args:
    ds: Pydicom.dataset to correct.
    reference_icc_profile: Optional reference ICC profile to add to dicom

  Raises:
    InvalidICCProfileError: Raised if ICC profile cannot be decoded.
    ValueError: More than one optical sequence defined.
  """
  if dicom_util.has_optical_path_sequence(ds):
    dicom_util.add_icc_colorspace_if_not_defined(ds)
  else:
    # Optical sequence is missing
    if reference_icc_profile is None:
      # if ref ICC profile is not defined then try to find one in the DICOM
      reference_icc_profile = _get_icc_profile(ds)
    dicom_util.add_default_optical_path_sequence(ds, reference_icc_profile)


def _add_metadata_to_generated_dicom_files(
    source_dicom_ref: ingested_dicom_file_ref.IngestDicomFileRef,
    gen_dicom: List[str],
    private_tags: List[dicom_private_tag_generator.DicomPrivateTag],
    instance_number_util: _DicomInstanceNumberAssignmentUtil,
    dcm_json: Optional[Dict[str, Any]],
    study_instance_uid: str,
    frame_of_ref_md: Optional[dicom_util.DicomFrameOfReferenceModuleMetadata],
) -> List[str]:
  """Adds metadata to generated WSI DICOM.

  Args:
    source_dicom_ref: IngestDicomFileRef to DICOM images generated from.
    gen_dicom: List of paths to generated DICOM instances.
    private_tags: List of private tags to add to DICOM.
    instance_number_util: Instance number assignment utility class.
    dcm_json: Dicom JSON to merge with generated DICOM.
    study_instance_uid: DICOM Study Instance UID to set generated DICOM to.
    frame_of_ref_md: DICOM frame of reference module metadata.

  Returns:
    List of paths to DICOM files.
  """
  # private hash tag should only be included in dicoms generated fully by
  # ingestion.
  source_hash = hash_util.sha512hash(source_dicom_ref.source)
  generated_dicom_private_tags = [
      dicom_private_tag_generator.DicomPrivateTag(
          ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG, 'LT', source_hash
      )
  ]
  generated_dicom_private_tags.extend(private_tags)
  dicom_path_list = []
  with pydicom.dcmread(
      source_dicom_ref.source, defer_size='512 KB'
  ) as reference_dcm:
    reference_icc_profile = _get_icc_profile(reference_dcm)
    dcm_tags = dicom_util.get_pydicom_tags(
        reference_dcm, _ADDITIONAL_DICOM_TAGS_TO_COPY_TO_GENERATED_INSTANCES
    )
  for dicom_path in gen_dicom:
    try:
      dcm_file = pydicom.dcmread(dicom_path, force=False)
      dcm_file.BurnedInAnnotation = source_dicom_ref.burned_in_annotation
      dcm_file.SpecimenLabelInImage = source_dicom_ref.specimen_label_in_image
      # Undefined tags, mapped value is None, are not set.
      dicom_util.set_defined_pydicom_tags(
          dcm_file,
          dcm_tags,
          _ADDITIONAL_DICOM_TAGS_TO_COPY_TO_GENERATED_INSTANCES,
      )
      dcm_file.InstanceNumber = instance_number_util.get_instance_number(
          dcm_file
      )
      _add_correct_optical_path_sequence(dcm_file, reference_icc_profile)
      dicom_util.add_metadata_to_dicom(
          frame_of_ref_md, dcm_json, generated_dicom_private_tags, dcm_file
      )

      # It is intended that the line below will overwrite study uid definition
      # in metadata schema.
      dcm_file.StudyInstanceUID = study_instance_uid
      dcm_file.save_as(dicom_path, write_like_original=True)
      dicom_path_list.append(dicom_path)
    except pydicom.errors.InvalidDicomError as exp:
      cloud_logging_client.logger().error(
          'WSI-to-DICOM conversion created invalid DICOM file.',
          {ingest_const.LogKeywords.filename: dicom_path},
          exp,
      )
      raise
  return dicom_path_list


def _add_metadata_to_ingested_wsi_dicom(
    wsi_dicom_filerefs: List[ingested_dicom_file_ref.IngestDicomFileRef],
    private_tags: List[dicom_private_tag_generator.DicomPrivateTag],
    instance_number_util: _DicomInstanceNumberAssignmentUtil,
    study_instance_uid: str,
    dcm_json: Optional[Dict[str, Any]],
    frame_of_ref_md: Optional[dicom_util.DicomFrameOfReferenceModuleMetadata],
    highest_magnification_image: Optional[
        ingested_dicom_file_ref.IngestDicomFileRef
    ] = None,
) -> List[str]:
  """Adds metadata to ingested WSI DICOM.

  Internally generated DICOM filenames identify the images downsampling
  represented via the file name, e.g., "downsample-{downsample}-". Externally
  generated DICOM are renamed here to identify the downsampling represented
  by the imaging. This enables these DICOM instances to be processed using
  similar pipeline mechanisms as the internally generated ones.

  Args:
    wsi_dicom_filerefs: List of references to WSI dicom ingested in zip.
    private_tags: List of private tags to add to DICOM.
    instance_number_util: Instance number assignment utility class.
    study_instance_uid: DICOM Study Instance UID to set ingested DICOM to.
    dcm_json: DICOM JSON to merge with generated DICOM.
    frame_of_ref_md: DICOM frame of reference module metadata.
    highest_magnification_image: Highest mag WSI imaging, used to calculate
      image downsampling.

  Returns:
    List of paths to DICOM files.
  """
  dicom_path_list = []
  for wsi_ref in wsi_dicom_filerefs:
    try:
      dcm_file = pydicom.dcmread(wsi_ref.source, force=True)
      # if dicom defined with implicit vr endian transfer syntax set to
      # explicit VR little endian
      if dcm_file.file_meta.TransferSyntaxUID == '1.2.840.10008.1.2':
        dcm_file.file_meta.TransferSyntaxUID = '1.2.840.10008.1.2.1'
      dcm_file.is_implicit_VR = False
      dcm_file.InstanceNumber = instance_number_util.get_instance_number(
          dcm_file
      )
      # frame of fref uid not defined for non-wsi dicom
      if frame_of_ref_md is not None:
        dicom_util.set_wsi_frame_of_ref_metadata(dcm_file, frame_of_ref_md)
      dicom_private_tag_generator.DicomPrivateTagGenerator.add_dicom_private_tags(
          private_tags, dcm_file
      )
      _add_correct_optical_path_sequence(dcm_file)
      dicom_json_util.merge_json_metadata_with_pydicom_ds(dcm_file, dcm_json)
      # It is intended that the line below will overwrite study uid definition
      # in metadata schema.
      dcm_file.StudyInstanceUID = study_instance_uid
      # Initialize content date time
      dicom_util.set_content_date_time_to_now(dcm_file)
      dicom_util.dicom_general_equipment.add_ingest_general_equipment(dcm_file)
      if highest_magnification_image is None:
        dcm_file.save_as(wsi_ref.source, write_like_original=True)
        dicom_path_list.append(wsi_ref.source)
        continue
      # Save image using file name that identifies image downsampling.
      dir_name, base_name = os.path.split(wsi_ref.source)

      ancillary_image_types = (
          ingest_const.OVERVIEW,
          ingest_const.THUMBNAIL,
          ingest_const.LABEL,
      )
      if any(
          [
              ancillary_image_type in wsi_ref.image_type
              for ancillary_image_type in ancillary_image_types
          ]
      ):
        cloud_logging_client.logger().info(
            (
                'Skipping downsample determination. DICOM instance describes '
                'ancillary image.'
            ),
            {'tested_dicom_instance': wsi_ref.dict()},
        )
        dcm_file.save_as(wsi_ref.source, write_like_original=True)
        dicom_path_list.append(wsi_ref.source)
        continue
      if not wsi_ref.imaged_volume_width or not wsi_ref.imaged_volume_height:
        cloud_logging_client.logger().info(
            (
                'Skipping downsample determination. DICOM instance does not'
                ' have physical dimensions.'
            ),
            {'tested_dicom_instance': wsi_ref.dict()},
        )
        dcm_file.save_as(wsi_ref.source, write_like_original=True)
        dicom_path_list.append(wsi_ref.source)
        continue
      try:
        ds_width = int(
            int(highest_magnification_image.total_pixel_matrix_columns)
            / int(wsi_ref.total_pixel_matrix_columns)
        )
        ds_height = int(
            int(highest_magnification_image.total_pixel_matrix_rows)
            / int(wsi_ref.total_pixel_matrix_rows)
        )
      except (ZeroDivisionError, ValueError) as exp:
        cloud_logging_client.logger().warning(
            'Could not determine DICOM instance downsampling.',
            {
                'highest_mag_instance': highest_magnification_image.dict(),
                'tested_dicom_instance': wsi_ref.dict(),
            },
            exp,
        )
        dcm_file.save_as(wsi_ref.source, write_like_original=True)
        dicom_path_list.append(wsi_ref.source)
        continue
      downsample = max(1, min(ds_width, ds_height))
      downsample_name = f'downsample-{downsample}-{base_name}'
      file_path = os.path.join(dir_name, downsample_name)
      dcm_file.save_as(file_path, write_like_original=True)
      cloud_logging_client.logger().debug(
          'Re-naming ingested DICOM to identify instance downsampling.',
          {
              'old_file_name': base_name,
              'new_file_name': downsample_name,
              'highest_mag_instance': highest_magnification_image.dict(),
              'tested_dicom_instance': wsi_ref.dict(),
          },
      )
      dicom_path_list.append(file_path)
    except pydicom.errors.InvalidDicomError as exp:
      cloud_logging_client.logger().error(
          'Adding metadata to ingested wsi-DICOM instance.', wsi_ref.dict(), exp
      )
      raise
  return dicom_path_list


def _add_metadata_to_other_ingested_dicom(
    dicom_filerefs: List[ingested_dicom_file_ref.IngestDicomFileRef],
    private_tags: List[dicom_private_tag_generator.DicomPrivateTag],
    instance_number_util: _DicomInstanceNumberAssignmentUtil,
    csv_metadata: dicom_schema_util.PandasMetadataTableWrapper,
    metadata_client: metadata_storage_client.MetadataStorageClient,
    default_metadata: Optional[Dict[str, Any]],
    study_instance_uid: str,
) -> List[str]:
  """Adds metadata to ingested DICOM (not WSI).

  Args:
    dicom_filerefs: List of references to non-WSI dicom ingested in zip.
    private_tags: List of private tags to add to DICOM.
    instance_number_util: Instance number assignment utility class.
    csv_metadata: To merge with DICOM.
    metadata_client: Metadata_storage_client to read schema SOPClassID specific
      schema from.
    default_metadata: Default JSON WSI dicom metadata. Used if SOPClassUID
      specific metadata not found.
    study_instance_uid: DICOM Study Instance UID to set other ingested DICOM to.

  Returns:
    List of paths to DICOM files.

  Raises:
    GenDicomFailedError: Error occures applyinig DICOM metadata mapping.
  """
  metadata_cache = {}
  dicom_path_list = []
  frame_of_ref_metadata = None  # frame of ref uid not defined for no-wsi dicom
  for dicom_ref in dicom_filerefs:
    metadata = metadata_cache.get(dicom_ref.sop_class_uid)
    if metadata is None:
      classname = _UNKOWN_SOPCLASS_UID
      try:
        classname = dicom_standard.dicom_standard_util().get_sop_classid_name(
            dicom_ref.sop_class_uid
        )
        wsi_dicom_schema = metadata_client.get_dicom_schema(
            {'SOPClassUID_Name': classname}
        )
        metadata = dicom_schema_util.get_json_dicom(
            classname, csv_metadata, wsi_dicom_schema
        )
        del wsi_dicom_schema
      except metadata_storage_client.MetadataSchemaExceptionError as exp:
        raise ingest_base.GenDicomFailedError(
            f'Invalid {classname} DICOM metadata schema.',
            ingest_const.ErrorMsgs.INVALID_METADATA_SCHEMA,
        ) from exp

      except (
          dicom_schema_util.DICOMSchemaTagError,
          dicom_schema_util.DICOMSchemaError,
      ) as exp:
        raise ingest_base.GenDicomFailedError(
            f'Invalid {classname} DICOM metadata schema.',
            ingest_const.ErrorMsgs.INVALID_METADATA_SCHEMA,
        ) from exp
      except dicom_schema_util.DICOMSpecMetadataError as exp:
        raise ingest_base.GenDicomFailedError(
            'Error in internal representation of DICOM standard.',
            ingest_const.ErrorMsgs.INVALID_DICOM_STANDARD_REPRESENTATION,
        ) from exp
      except dicom_schema_util.MissingRequiredMetadataValueError as exp:
        raise ingest_base.GenDicomFailedError(
            'Missing metadata for required DICOM tag.',
            ingest_const.ErrorMsgs.MISSING_METADATA_FOR_REQUIRED_DICOM_TAG,
        ) from exp
      if metadata is None:
        metadata = default_metadata
      metadata_cache[dicom_ref.sop_class_uid] = metadata
    dicom_path_list.extend(
        _add_metadata_to_ingested_wsi_dicom(
            [dicom_ref],
            private_tags,
            instance_number_util,
            study_instance_uid,
            metadata,
            frame_of_ref_metadata,
        )
    )
  return dicom_path_list


class IngestWsiDicom(ingest_base.IngestBase):
  """Converts WSI DICOM image to DICOM Pyramid."""

  def __init__(
      self,
      ingest_buckets: ingest_base.GcsIngestionBuckets,
      is_oof_ingestion_enabled: bool,
      override_study_uid_with_metadata: bool,
  ):
    super().__init__(ingest_buckets)
    self._is_oof_ingestion_enabled = is_oof_ingestion_enabled
    self._override_study_uid_with_metadata = override_study_uid_with_metadata
    self.default_wsi_conv_params = f'{self.default_wsi_conv_params} --readDICOM'

  def generate_dicom(
      self,
      dicom_gen_dir: str,
      dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
      message_id: str,
      abstract_dicom_handler: abstract_dicom_generation.AbstractDicomGeneration,
  ) -> ingest_base.GenDicomResult:
    """Converts downloaded WSI DICOM to full DICOM WSI Pyramid.

    Args:
      dicom_gen_dir: Directory to generate DICOM files in.
      dicom_gen: File payload to convert into full DICOM WSI image pyramid.
      message_id: pub/sub msg id.
      abstract_dicom_handler: Abstract_dicom pub/sub message handler calling
        generate_dicom.

    Returns:
      GenDicomResult describing generated DICOM.
    """
    upload_file_list = []
    force_upload_to_main_store_list = []
    pyramid_level_config = None
    dicom_gen.hash = None
    try:
      unzip_dir = os.path.join(abstract_dicom_handler.img_dir, 'archive')
      os.mkdir(unzip_dir)
      dicom_file_list = _unarchive_zipfile(dicom_gen.localfile, unzip_dir)

      dicom_file_list = get_dicom_filerefs_list(dicom_file_list)

      dicom_file_info = validate_ingested_dicom.validate_dicom_files(
          dicom_file_list
      )

      wsi_dicom_schema = self._get_dicom_metadata_schema(
          ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.name
      )
      slide_id = _determine_slideid(
          dicom_file_info.barcode_value,
          dicom_gen.localfile,
          dicom_file_info.wsi_image_filerefs,
          self.metadata_storage_client,
      )

      # Throws redis_client.CouldNotAcquireNonBlockingLockError if
      # context manager is redis_client.redis_client().non_blocking_lock
      # and lock cannot be acquired.
      #
      # Lock used to protects against other processes interacting with
      # dicom and or metadata stores prior to the data getting written into
      # dicomstore. It is safe to move the ingested bits to success folder
      # and acknowledge pub/sub msg in unlocked context.
      with self._get_context_manager(slide_id):
        wsi_dcm_json, csv_metadata = (
            self._get_slide_dicom_json_formatted_metadata(
                ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.name,
                slide_id,
                wsi_dicom_schema,
                abstract_dicom_handler.dcm_store_client,
                test_metadata_for_missing_study_instance_uid=self._override_study_uid_with_metadata,
            )
        )
        if not self._override_study_uid_with_metadata:
          dicom_json_util.set_study_instance_uid_in_metadata(
              wsi_dcm_json, dicom_file_info.study_uid
          )

        # Extra validation DICOM will provide StudyInstanceUID.
        # StudyInstanceUID in DICOM must match value in metadata.
        try:
          study_uid = dicom_json_util.get_study_instance_uid(wsi_dcm_json)
        except dicom_json_util.MissingStudyUIDInMetadataError as exp:
          raise ingest_base.GenDicomFailedError(
              f'Missing Study UID in metadata: {slide_id}',
              ingest_const.ErrorMsgs.MISSING_STUDY_UID,
          ) from exp

        if self._override_study_uid_with_metadata:
          cloud_logging_client.logger().info(
              (
                  'Overriding DICOM Study Instance UID defined in DICOM with'
                  ' value in metadata'
              ),
              {
                  'old_study_instance_uid': dicom_file_info.study_uid,
                  'new_study_instance_uid': study_uid,
              },
          )
          dicom_file_info.study_uid = study_uid

        if study_uid != dicom_file_info.study_uid:
          raise ingest_base.GenDicomFailedError(
              'Metadata study_uid != study_uid in DICOM',
              ingest_const.ErrorMsgs.WSI_DICOM_STUDY_UID_METADATA_DICOM_MISMATCH,
          )

        # get series uid from ingested dicom
        series_uid = dicom_file_info.series_uid
        cloud_logging_client.logger().info(
            'Ingesting images into series',
            {ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID: series_uid},
        )

        pixel_spacing = wsi_pyramid_gen_config.get_dicom_pixel_spacing(
            dicom_file_info.original_image.source
        )
        pyramid_level_config = self.get_downsamples_to_generate(
            pixel_spacing, self._is_oof_ingestion_enabled
        )

        dicom_gen.generated_dicom_files = self._convert_wsi_to_dicom(
            dicom_file_info.original_image.source,
            pyramid_level_config,
            dicom_gen_dir,
            study_uid=study_uid,
            series_uid=series_uid,
        )
        if not dicom_gen.generated_dicom_files:
          raise ingest_base.GenDicomFailedError(
              'An error occurred converting to DICOM.',
              ingest_const.ErrorMsgs.WSI_TO_DICOM_CONVERSION_FAILED,
          )

        # Create DICOM ref for generated dicom.
        de_duplicated_gen_dicom = _remove_duplicate_generated_dicom(
            dicom_gen.generated_dicom_files, dicom_file_info.wsi_image_filerefs
        )
        dicom_gen.generated_dicom_files = de_duplicated_gen_dicom.file_paths

        instance_number_util = _DicomInstanceNumberAssignmentUtil()
        instance_number_util.add_inst(de_duplicated_gen_dicom.dicom_file_refs)
        instance_number_util.add_inst(dicom_file_info.wsi_image_filerefs)
        instance_number_util.add_inst(dicom_file_info.other_dicom_filerefs)

        private_tags = (
            abstract_dicom_generation.get_private_tags_for_gen_dicoms(
                dicom_gen, message_id
            )
        )

        frame_of_ref_metadata = (
            dicom_util.create_dicom_frame_of_ref_module_metadata(
                study_uid,
                series_uid,
                abstract_dicom_handler.dcm_store_client,
                dicom_file_info.original_image,
            )
        )

        # Add metadata to DICOM
        upload_file_list = _add_metadata_to_generated_dicom_files(
            dicom_file_info.original_image,
            dicom_gen.generated_dicom_files,
            private_tags,
            instance_number_util,
            wsi_dcm_json,
            dicom_file_info.study_uid,
            frame_of_ref_metadata,
        )

        # The ancillary images, Label, macro, thumbnail, and wsi image submitted
        # by the customer as part of the DICOM ingestion payload are always
        # uploaded to the main DICOM Store.
        force_upload_to_main_store_list = _add_metadata_to_ingested_wsi_dicom(
            dicom_file_info.wsi_image_filerefs,
            private_tags,
            instance_number_util,
            dicom_file_info.study_uid,
            wsi_dcm_json,
            frame_of_ref_metadata,
            highest_magnification_image=dicom_file_info.original_image,
        )
        upload_file_list.extend(force_upload_to_main_store_list)

        # Any non-wsi DICOM images provided by the customer are always uploaded
        # to the main DICOM store.
        force_upload_to_main_store_list.extend(
            _add_metadata_to_other_ingested_dicom(
                dicom_file_info.other_dicom_filerefs,
                private_tags,
                instance_number_util,
                csv_metadata,
                self.metadata_storage_client,
                wsi_dcm_json,
                dicom_file_info.study_uid,
            )
        )
        dest_uri = self.ingest_succeeded_uri
    except (
        ingest_base.GenDicomFailedError,
        dicom_util.InvalidICCProfileError,
        wsi_pyramid_gen_config.MissingPixelSpacingError,
    ) as exp:
      dest_uri = self.log_and_get_failure_bucket_path(exp)
    except pydicom.errors.InvalidDicomError as exp:
      dest_uri = self.log_and_get_failure_bucket_path(
          ingest_base.GenDicomFailedError(
              exp, ingest_const.ErrorMsgs.WSI_TO_DICOM_INVALID_DICOM_GENERATED
          )
      )

    return ingest_base.GenDicomResult(
        dicom_gen,
        dest_uri,
        ingest_base.DicomInstanceIngestionSets(
            upload_file_list,
            pyramid_level_config,
            force_upload_to_main_store_list,
        ),
        False,
    )
