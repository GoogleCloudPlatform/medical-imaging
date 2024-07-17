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
from typing import Any, Dict, List, Mapping, Optional, Tuple
import zipfile

import pydicom

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
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
from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard_util


_VL_WHOLE_SLIDE_MICROSCOPY_IOD_MODULES_TO_COPY_FROM_PRIMARY_TO_RESAMPLED = {
    'Acquisition Context',
    'Clinical Trial Series',
    'Clinical Trial Study',
    'Clinical Trial Subject',
    'Enhanced General Equipment',
    'General Acquisition',
    'General Equipment',
    'General Series',
    'General Study',
    'Microscope Slide Layer Tile Organization',
    'Optical Path',
    'Patient',
    'Patient Study',
    'Slide Label',
    'Specimen',
    'Whole Slide Microscopy Image',
    'Whole Slide Microscopy Series',
}


def _unarchive_zipfile(zip_file_path: str, out_directory: str) -> List[str]:
  """Unarchives zip files to directory.

  Args:
    zip_file_path: Path to zip file.
    out_directory: Directory to unarchive files to.

  Returns:
    List of files unarchived or passed file path if not zip file.
  """
  if not zipfile.is_zipfile(zip_file_path):
    cloud_logging_client.info(
        'File is not zip file. Atttempting to process as DICOM.',
        {'filename': zip_file_path},
    )
    return [zip_file_path]
  file_list = []
  cloud_logging_client.info('Decompressing zip file.')
  with tempfile.TemporaryDirectory() as unarchive_temp_path:
    with zipfile.ZipFile(zip_file_path, allowZip64=True) as zip_archive:
      for zip_file_info in zip_archive.infolist():
        if zip_file_info.is_dir():
          cloud_logging_client.warning(
              'Zip file contains directories; directories ignored.'
          )
          continue
        if zip_file_info.file_size == 0:
          cloud_logging_client.warning(
              'Ingoring zipped file. File contained 0 bytes',
              {'filename': zip_file_info.filename},
          )
          continue
        # Extract file to temp directory and move to desired location.
        extracted_file = zip_archive.extract(zip_file_info, unarchive_temp_path)
        outfile = os.path.join(out_directory, f'file_{len(file_list)}.dcm')
        shutil.move(extracted_file, outfile)
        file_list.append(outfile)
  cloud_logging_client.info(
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
    except pydicom.errors.InvalidDicomError as exp:
      cloud_logging_client.warning(
          'Received Non-DICOM file in ingestion payload.  Ignoring file.',
          {'filename': path},
          exp,
      )
      continue
    except ingested_dicom_file_ref.DicomIngestError as exp:
      cloud_logging_client.warning(
          'Unable to determine transfer syntax of dicom file in ingestion'
          ' payload.  Ignoring file.',
          {'filename': path},
          exp,
      )
      continue
    file_ref_list.append(file_ref)
    cloud_logging_client.info(
        'Received DICOM file instance in ingestion payload.',
        {'filename': path},
        file_ref.dict(),
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
  generate_dicom_file_refs = [
      dicom_file_ref.init_from_file(
          path, ingested_dicom_file_ref.IngestDicomFileRef()
      )
      for path in generate_dicoms
  ]
  cloud_logging_client.debug(
      'Removing generated downsampled DICOM instances that duplicate provided'
      ' DICOM instances.',
      {
          ingest_const.LogKeywords.PIPELINE_GENERATED_DOWNSAMPLE_DICOM_INSTANCE: [
              ing.dict() for ing in generate_dicom_file_refs
          ],
          ingest_const.LogKeywords.DICOM_INSTANCES_TRIGGERING_TRANSFORM_PIPELINE: [
              ing.dict() for ing in ingested_files
          ],
      },
  )
  for gen_file_ref in generate_dicom_file_refs:
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
    if not dicom_store_client.is_dpas_dicom_wsidcmref_in_list(
        gen_file_ref,
        ingested_files,
        ignore_source_image_hash_tag=True,
        ignore_tags=[
            ingest_const.DICOMTagKeywords.STUDY_INSTANCE_UID,
            ingest_const.DICOMTagKeywords.SERIES_INSTANCE_UID,
        ],
    ):
      de_duplicated_dicom_list.append(gen_file_ref)
  return DeDuplicatedDicom(de_duplicated_dicom_list)


def _determine_slideid(
    barcode_value: str,
    dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
    wsi_image_filerefs: List[ingested_dicom_file_ref.IngestDicomFileRef],
    metadata: metadata_storage_client.MetadataStorageClient,
) -> str:
  """Determine slideid for ingested dicom.

  Args:
    barcode_value: Barcode value defined in tag in DICOM.
    dicom_gen: File payload to convert into DICOM.
    wsi_image_filerefs: List of validated dicom files to test for barcode.
    metadata: Slide metadata storage client.

  Returns:
    SlideID as string

  Raises:
    GenDICOMFailedError: Error resulting from inability to determine slide id
  """
  # First use SlideId in filename
  # First use SlideId in DICOM
  # Next decode images

  # attempt to get slide id from ingested file filename
  try:
    slide_id = decode_slideid.get_slide_id_from_filename(dicom_gen, metadata)
    cloud_logging_client.info(
        'Slide ID identified in file name.',
        {
            ingest_const.LogKeywords.FILENAME: dicom_gen.localfile,
            ingest_const.LogKeywords.SOURCE_URI: dicom_gen.source_uri,
            ingest_const.LogKeywords.SLIDE_ID: slide_id,
        },
    )
    return slide_id
  except decode_slideid.SlideIdIdentificationError as exp:
    current_exception = exp

  # attempt to get slide id from barcode tag in dicom file.
  try:
    slide_id = decode_slideid.find_slide_id_in_metadata(barcode_value, metadata)
    cloud_logging_client.info(
        'Slide ID identified in DICOM BarcodeValue tag.',
        {ingest_const.LogKeywords.SLIDE_ID: slide_id},
    )
    return slide_id
  except decode_slideid.SlideIdIdentificationError as exp:
    current_exception = decode_slideid.highest_error_level_exception(
        current_exception, exp
    )

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
      cloud_logging_client.info(
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


def _get_additional_tag_keywords_to_copy() -> List[str]:
  """Returns DICOM keywords to copy to generated DICOM images."""
  iod_name = dicom_standard_util.IODName(
      ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.name
  )
  return dicom_standard.dicom_iod_dataset_util().get_root_level_iod_tag_keywords(
      iod_name,
      _VL_WHOLE_SLIDE_MICROSCOPY_IOD_MODULES_TO_COPY_FROM_PRIMARY_TO_RESAMPLED,
  )


def _add_metadata_to_generated_dicom_files(
    source_dicom_ref: ingested_dicom_file_ref.IngestDicomFileRef,
    gen_dicom: List[str],
    private_tags: List[dicom_private_tag_generator.DicomPrivateTag],
    instance_number_util: _DicomInstanceNumberAssignmentUtil,
    dcm_json: Mapping[str, Any],
    additional_wsi_metadata: pydicom.Dataset,
) -> List[str]:
  """Adds metadata to generated WSI DICOM.

  Args:
    source_dicom_ref: IngestDicomFileRef to DICOM images generated from.
    gen_dicom: List of paths to generated DICOM instances.
    private_tags: List of private tags to add to DICOM.
    instance_number_util: Instance number assignment utility class.
    dcm_json: Dicom JSON to merge with generated DICOM.
    additional_wsi_metadata: Additional metadata to merge with gen DICOM.

  Returns:
    List of paths to DICOM files.
  """
  # Private hash tag should only be included in dicoms generated fully by
  # ingestion to enable the function is_dpas_dicom_wsidcmref_in_list in the
  # DicomStoreClient to differentiate between instances that were created by
  # DPAS and those which were externally generated (not by the transform
  # pipeline).
  source_hash = hash_util.sha512hash(source_dicom_ref.source)
  generated_dicom_private_tags = [
      dicom_private_tag_generator.DicomPrivateTag(
          ingest_const.DICOMTagKeywords.HASH_PRIVATE_TAG, 'LT', source_hash
      )
  ]
  generated_dicom_private_tags.extend(private_tags)
  dicom_path_list = []
  additional_tags_to_copy = _get_additional_tag_keywords_to_copy()
  with pydicom.dcmread(
      source_dicom_ref.source, defer_size='512 KB'
  ) as reference_dcm:
    reference_icc_profile = _get_icc_profile(reference_dcm)
    dcm_tags = dicom_util.get_pydicom_tags(
        reference_dcm, additional_tags_to_copy
    )
  for dicom_path in gen_dicom:
    try:
      dcm_file = pydicom.dcmread(dicom_path, force=False)
      dcm_file.BurnedInAnnotation = source_dicom_ref.burned_in_annotation
      dcm_file.SpecimenLabelInImage = source_dicom_ref.specimen_label_in_image
      if (
          ingest_const.DICOMTagKeywords.IMAGE_ORIENTATION_SLIDE in dcm_file
          and ingest_const.DICOMTagKeywords.IMAGE_ORIENTATION_SLIDE in dcm_tags
      ):
        # if image orientation was defined in the input image prefer that.
        del dcm_file[ingest_const.DICOMTagKeywords.IMAGE_ORIENTATION_SLIDE]
      # Undefined tags, mapped value is None, are not set.
      dicom_util.set_defined_pydicom_tags(
          dcm_file,
          dcm_tags,
          additional_tags_to_copy,
          overwrite_existing_values=False,
      )
      dcm_file.InstanceNumber = instance_number_util.get_instance_number(
          dcm_file
      )
      _add_correct_optical_path_sequence(dcm_file, reference_icc_profile)
      dicom_util.add_default_total_pixel_matrix_origin_sequence_if_not_defined(
          dcm_file
      )
      dicom_util.add_metadata_to_generated_wsi_dicom(
          additional_wsi_metadata,
          dcm_json,
          generated_dicom_private_tags,
          dcm_file,
      )
      dicom_util.add_missing_type2_dicom_metadata(dcm_file)
      dicom_util.if_missing_create_encapsulated_frame_offset_table(dcm_file)
      dcm_file.save_as(dicom_path, write_like_original=True)
      dicom_path_list.append(dicom_path)
    except pydicom.errors.InvalidDicomError as exp:
      cloud_logging_client.error(
          'WSI-to-DICOM conversion created invalid DICOM file.',
          {ingest_const.LogKeywords.FILENAME: dicom_path},
          exp,
      )
      raise
  return dicom_path_list


def _common_init_for_externally_generated_dicom_instance(
    dcm_file: pydicom.FileDataset,
    private_tags: List[dicom_private_tag_generator.DicomPrivateTag],
    instance_number_util: _DicomInstanceNumberAssignmentUtil,
    dcm_json: Mapping[str, Any],
) -> None:
  """Comon initalization for extenrally generated DICOM files."""
  # if dicom defined with implicit vr endian transfer syntax set to
  # explicit VR little endian
  if (
      dcm_file.file_meta.TransferSyntaxUID
      == ingest_const.DicomImageTransferSyntax.IMPLICIT_VR_LITTLE_ENDIAN
  ):
    dcm_file.file_meta.TransferSyntaxUID = (
        ingest_const.DicomImageTransferSyntax.EXPLICIT_VR_LITTLE_ENDIAN
    )
  dcm_file.is_implicit_VR = False
  dcm_file.InstanceNumber = instance_number_util.get_instance_number(dcm_file)
  dicom_private_tag_generator.DicomPrivateTagGenerator.add_dicom_private_tags(
      private_tags, dcm_file
  )
  dicom_json_util.merge_json_metadata_with_pydicom_ds(dcm_file, dcm_json)
  # Initialize content date time & General Equipment
  dicom_util.add_general_metadata_to_dicom(dcm_file)


def _add_metadata_to_ingested_wsi_dicom(
    wsi_dicom_filerefs: List[ingested_dicom_file_ref.IngestDicomFileRef],
    private_tags: List[dicom_private_tag_generator.DicomPrivateTag],
    instance_number_util: _DicomInstanceNumberAssignmentUtil,
    dcm_json: Mapping[str, Any],
    additional_wsi_metadata: pydicom.Dataset,
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
    dcm_json: DICOM JSON to merge with generated DICOM.
    additional_wsi_metadata: Additional metadata to merge with gen DICOM.
    highest_magnification_image: Highest mag WSI imaging, used to calculate
      image downsampling.

  Returns:
    List of paths to DICOM files.
  """
  dicom_path_list = []
  for wsi_ref in wsi_dicom_filerefs:
    try:
      dcm_file = pydicom.dcmread(wsi_ref.source, force=True)
      _add_correct_optical_path_sequence(dcm_file)
      dicom_util.add_default_total_pixel_matrix_origin_sequence_if_not_defined(
          dcm_file
      )
      dicom_util.copy_pydicom_dataset(additional_wsi_metadata, dcm_file)
      _common_init_for_externally_generated_dicom_instance(
          dcm_file, private_tags, instance_number_util, dcm_json
      )
      dicom_util.init_undefined_wsi_imaging_type1_tags(dcm_file)
      dicom_util.add_missing_type2_dicom_metadata(dcm_file)
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
      if any([
          ancillary_image_type in wsi_ref.image_type
          for ancillary_image_type in ancillary_image_types
      ]):
        cloud_logging_client.info(
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
        cloud_logging_client.info(
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
        cloud_logging_client.warning(
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
      cloud_logging_client.debug(
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
      cloud_logging_client.error(
          'Adding metadata to ingested wsi-DICOM instance.', wsi_ref.dict(), exp
      )
      raise
  return dicom_path_list


class IngestWsiDicom(ingest_base.IngestBase):
  """Converts WSI DICOM image to DICOM Pyramid."""

  def __init__(
      self,
      ingest_buckets: ingest_base.GcsIngestionBuckets,
      is_oof_ingestion_enabled: bool,
      override_study_uid_with_metadata: bool,
      metadata_client: metadata_storage_client.MetadataStorageClient,
  ):
    super().__init__(ingest_buckets, metadata_client)
    self._is_oof_ingestion_enabled = is_oof_ingestion_enabled
    self._override_study_uid_with_metadata = override_study_uid_with_metadata
    self._dicom_file_info = None

  def init_handler_for_ingestion(self) -> None:
    super().init_handler_for_ingestion()
    self._dicom_file_info = None

  def get_slide_id(
      self,
      dicom_gen: abstract_dicom_generation.GeneratedDicomFiles,
      abstract_dicom_handler: abstract_dicom_generation.AbstractDicomGeneration,
  ) -> str:
    """Returns slide id of ingested SVS.

    Args:
      dicom_gen: File payload to convert into full DICOM WSI image pyramid.
      abstract_dicom_handler: Abstract_dicom pub/sub message handler calling
        generate_dicom.

    Raises:
      DetermineSlideIDError: Cannot determine slide id of ingested DICOM.
    """
    dicom_gen.hash = None
    unzip_dir = os.path.join(abstract_dicom_handler.img_dir, 'archive')
    os.mkdir(unzip_dir)
    dicom_file_list = _unarchive_zipfile(dicom_gen.localfile, unzip_dir)
    try:
      dicom_file_list = get_dicom_filerefs_list(dicom_file_list)
      dicom_file_info = validate_ingested_dicom.validate_dicom_files(
          dicom_file_list
      )
      self._dicom_file_info = dicom_file_info
    except ingested_dicom_file_ref.DicomIngestError as exp:
      dest_uri = self.log_and_get_failure_bucket_path(exp)
      user_msg = 'Invalid DICOM input.'
      failure_bucket_error_msg = str(exp.args[1] if len(exp.args) == 2 else exp)
      raise ingest_base.DetermineSlideIDError(
          dicom_gen,
          dest_uri,
          user_msg=user_msg,
          failure_bucket_error_msg=failure_bucket_error_msg,
      )
    try:
      slide_id = _determine_slideid(
          dicom_file_info.barcode_value,
          dicom_gen,
          dicom_file_info.wsi_image_filerefs,
          self.metadata_storage_client,
      )
      return self.set_slide_id(slide_id, False)
    except (ingest_base.GenDicomFailedError,) as exp:
      if ingest_flags.ENABLE_METADATA_FREE_INGESTION_FLG.value:
        return self.set_slide_id(
            decode_slideid.get_metadata_free_slide_id(dicom_gen), True
        )
      dest_uri = self.log_and_get_failure_bucket_path(exp)
      raise ingest_base.DetermineSlideIDError(dicom_gen, dest_uri) from exp

  def _get_non_wsi_sop_class_metadata(
      self,
      sop_class_uid: str,
      dicom_client: dicom_store_client.DicomStoreClient,
      metadata_row: dicom_schema_util.MetadataTableWrapper,
      wsi_metadata: Mapping[str, Any],
  ) -> Optional[Dict[str, Any]]:
    classname = dicom_standard.dicom_standard_util().get_sop_classid_name(
        sop_class_uid
    )
    if classname is None:
      return None
    try:
      metadata = self.get_slide_dicom_json_formatted_metadata(
          classname, self.slide_id, dicom_client, metadata_row
      ).dicom_json
      study_instance_uid = dicom_json_util.get_study_instance_uid(wsi_metadata)
      series_instance_uid = dicom_json_util.get_series_instance_uid(
          wsi_metadata
      )
      dicom_json_util.set_study_instance_uid_in_metadata(
          metadata, study_instance_uid
      )
      dicom_json_util.set_series_instance_uid_in_metadata(
          metadata, series_instance_uid
      )
      # remove SOP Class UID in metadata to ensure metadata does not overwrite
      # value in DICOM.
      dicom_json_util.remove_sop_class_uid_from_metadata(metadata)
      return metadata
    except ingest_base.GenDicomFailedError:
      return None

  def _add_metadata_to_other_ingested_dicom(
      self,
      dicom_filerefs: List[ingested_dicom_file_ref.IngestDicomFileRef],
      private_tags: List[dicom_private_tag_generator.DicomPrivateTag],
      instance_number_util: _DicomInstanceNumberAssignmentUtil,
      metadata_table_row: dicom_schema_util.MetadataTableWrapper,
      wsi_metadata: Mapping[str, Any],
      dicom_client: dicom_store_client.DicomStoreClient,
  ) -> List[str]:
    """Adds metadata to ingested DICOM (not WSI).

    Args:
      dicom_filerefs: List of references to non-WSI dicom ingested in zip.
      private_tags: List of private tags to add to DICOM.
      instance_number_util: Instance number assignment utility class.
      metadata_table_row: To merge with DICOM.
      wsi_metadata: Default JSON WSI dicom metadata. Used if SOPClassUID
        specific metadata not found.
      dicom_client: DICOM store client used to correct missing study instance
        uid.

    Returns:
      List of paths to DICOM files.

    Raises:
      GenDicomFailedError: Error occures applyinig DICOM metadata mapping.
    """
    metadata_cache = {}
    dicom_path_list = []
    for dicom_ref in dicom_filerefs:
      metadata = metadata_cache.get(dicom_ref.sop_class_uid)
      if metadata is None:
        metadata = self._get_non_wsi_sop_class_metadata(
            dicom_ref.sop_class_uid,
            dicom_client,
            metadata_table_row,
            wsi_metadata,
        )
        if metadata is None:
          cloud_logging_client.warning(
              'Could not identify SOPClassUID specfic metadata using VL Whole'
              ' Slide Microscopy Image formatted metadata.',
              {ingest_const.LogKeywords.SOP_CLASS_UID: dicom_ref.sop_class_uid},
          )
          metadata = wsi_metadata
        metadata_cache[dicom_ref.sop_class_uid] = metadata
      dicom_path = dicom_ref.source
      try:
        with pydicom.dcmread(dicom_path, force=True) as dcm_file:
          _common_init_for_externally_generated_dicom_instance(
              dcm_file, private_tags, instance_number_util, metadata
          )
          dicom_util.add_missing_type2_dicom_metadata(dcm_file)
          dcm_file.save_as(dicom_path, write_like_original=True)
          dicom_path_list.append(dicom_path)
      except pydicom.errors.InvalidDicomError as exp:
        cloud_logging_client.error(
            'Adding metadata to ingested DICOM instance.', dicom_ref.dict(), exp
        )
        raise
    return dicom_path_list

  def _generate_metadata_free_slide_metadata(
      self,
      unused_slide_id: str,
      unused_dicom_client: dicom_store_client.DicomStoreClient,
  ) -> ingest_base.DicomMetadata:
    return ingest_base.generate_empty_slide_metadata()

  def _set_study_instance_uid_from_metadata(self) -> bool:
    """Returns True if Study instance UID should be generated from metadata.

    If slide is ingested using metadata free transformation then the study uid
    should always come from the value embedded in the DICOM. Otherwise return
    the value in metadata study initalization flag.
    """
    if self.is_metadata_free_slide_id:
      return False
    return self._override_study_uid_with_metadata

  def _set_series_instance_uid_from_metadata(self) -> bool:
    """Returns True if Series instance UID should be generated from metadata.

    If slide is ingested using metadata free transformation then the series uid
    should always come from the value embedded in the DICOM. Otherwise return
    the value in metadata series initalization flag.
    """
    if self.is_metadata_free_slide_id:
      return False
    return ingest_flags.INIT_SERIES_INSTANCE_UID_FROM_METADATA_FLG.value

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

    Raises:
      ValueError: Slide ID is undefined.
    """
    pyramid_level_config = None
    dicom_file_info = self._dicom_file_info
    if self.slide_id is None:
      raise ValueError('Slideid is not set.')
    try:
      dcm_metadata = self.get_slide_dicom_json_formatted_metadata(
          ingest_const.DicomSopClasses.WHOLE_SLIDE_IMAGE.name,
          self.slide_id,
          abstract_dicom_handler.dcm_store_client,
      )
      wsi_dcm_json = dcm_metadata.dicom_json
      wsi_table_row = dcm_metadata.metadata_table_row

      ingest_base.initialize_metadata_study_and_series_uids_for_dicom_triggered_ingestion(
          dicom_file_info.study_uid,
          dicom_file_info.series_uid,
          wsi_dcm_json,
          abstract_dicom_handler,
          self._set_study_instance_uid_from_metadata(),
          self._set_series_instance_uid_from_metadata(),
      )
      # The absence of an original_image indicates that ingested DICOM
      # does not contain an original/primary/volume or derived/primary/volume
      # image. Downsampled images will not be generated.
      # However, other DICOM present in the payload will be ingested.
      if dicom_file_info.original_image is None:
        dicom_gen.generated_dicom_files = []
        cloud_logging_client.warning(
            'DICOM ingestion payload does not contain either an'
            ' ORIGINAL\\PRIMARY\\VOLUME or DERIVED\\PRIMARY\\VOLUME.'
            ' Downsampled pyramid layers will not be generated for this'
            ' ingestion.'
        )
      else:
        pixel_spacing = wsi_pyramid_gen_config.get_dicom_pixel_spacing(
            dicom_file_info.original_image.source
        )
        pyramid_level_config = self.get_downsamples_to_generate(
            pixel_spacing, self._is_oof_ingestion_enabled
        )

        dicom_gen.generated_dicom_files = self.convert_wsi_to_dicom(
            dicom_file_info.original_image.source,
            pyramid_level_config,
            dicom_gen_dir,
            wsi_dcm_json,
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
      private_tags = abstract_dicom_generation.get_private_tags_for_gen_dicoms(
          dicom_gen, message_id
      )
      additional_wsi_metadata = (
          dicom_util.get_additional_wsi_specific_dicom_metadata(
              abstract_dicom_handler.dcm_store_client,
              wsi_dcm_json,
              dicom_file_info.wsi_image_filerefs,
          )
      )
      # Add metadata to DICOM
      if dicom_file_info.original_image is None:
        upload_file_list = []
      else:
        upload_file_list = _add_metadata_to_generated_dicom_files(
            dicom_file_info.original_image,
            dicom_gen.generated_dicom_files,
            private_tags,
            instance_number_util,
            wsi_dcm_json,
            additional_wsi_metadata,
        )
      # The ancillary images, Label, macro, thumbnail, and wsi image submitted
      # by the customer as part of the DICOM ingestion payload are always
      # uploaded to the main DICOM Store.
      force_upload_to_main_store_list = _add_metadata_to_ingested_wsi_dicom(
          dicom_file_info.wsi_image_filerefs,
          private_tags,
          instance_number_util,
          wsi_dcm_json,
          additional_wsi_metadata,
          highest_magnification_image=dicom_file_info.original_image,
      )
      upload_file_list.extend(force_upload_to_main_store_list)
      force_upload_to_main_store_list.extend(
          self._add_metadata_to_other_ingested_dicom(
              dicom_file_info.other_dicom_filerefs,
              private_tags,
              instance_number_util,
              wsi_table_row,
              wsi_dcm_json,
              abstract_dicom_handler.dcm_store_client,
          )
      )
      dest_uri = self.ingest_succeeded_uri
    except (
        ingest_base.GenDicomFailedError,
        dicom_util.InvalidICCProfileError,
        wsi_pyramid_gen_config.MissingPixelSpacingError,
    ) as exp:
      upload_file_list = []
      force_upload_to_main_store_list = []
      dest_uri = self.log_and_get_failure_bucket_path(exp)
    except pydicom.errors.InvalidDicomError as exp:
      upload_file_list = []
      force_upload_to_main_store_list = []
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
