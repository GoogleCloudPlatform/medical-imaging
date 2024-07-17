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
r"""Trigger transformation pipeline using test imaging with synthetic metadata.

Trigger the transformation pipeline in Cloud using imaging stored in a
cloud storage bucket. Tool assumes example metadata mapping schemas are being
used. Generated imaging can be returned to facilitate additional testing.
--------------------------------------------------------------------------------
Getting started from the open source  repo:

Requires application default credentials:
$ gcloud auth application-default login

If running directly using python3 then set python path to include current
working directory.
$ PYTHONPATH="${PYTHONPATH}:."

Execute directly with python interpreter from the root directory of the source
repo (e.g., python3 ./transformation_pipeline/test_utils/ingest_data_generator/
  synthetic_ingest_data_generator.py) or execute using Bazel.

Example 1): Transform specific imaging to DICOM WSI using transformation
  pipeline. In this example source imaging locations in GCS are referenced
  directly using --wsi_images. For WSI dicom imaging that is defined by more
  than one image, e.g. (40x acquisition, slide label, thumbnail, etc) then
  the path should reference the directory which contains the desired DICOMS.
  DICOMS from multiple slides should not be intermixed in the same directory.

$ python3 transformation_pipeline/test_utils/ingest_data_generator/synthetic_ingest_data_generator.py \
    --gcs_input_project=<GCS_PROJECT_STORING_IMAGING> \
    --gcs_output_metadata_uri=gs://<TRANSFORM_PIPELINE_METADATA_INGESTION_BUCKET> \
    --gcs_output_images_uri=gs://<TRANSFORM_PIPELINE_IMAGE_INGESTION_BUCKET> \
    --dicom_store_output_images_uri=https://healthcare.googleapis.com/v1/projects/<DICOM_STORE_GCP_PROJECT>/locations/<DICOM_STORE_LOCATION>/datasets/<DICOM_STORE_DATASET>/dicomStores/<DICOM_STORE_NAME>/dicomWeb \
    --wsi_images=gs://<BUCKET_PATH>/example1.svs,gs://<BUCKET_PATH>/example2.svs \
    --output_type=gcs \
    --cleanup_artifacts=False

Example 2): Transform untiled imaging to untiled microscopy DICOM using the
  transformation pipeline. The only difference between Example 1 and 2 is that
  example 2 uses --flat_images to define the paths to reference imaging.
  Reference imaging is expected to be JPEG, PNG, or untiled tiff imaging.

$ python3 transformation_pipeline/test_utils/ingest_data_generator/synthetic_ingest_data_generator.py \
    --gcs_input_project=<GCS_PROJECT_STORING_IMAGING> \
    --gcs_output_metadata_uri=gs://<TRANSFORM_PIPELINE_METADATA_INGESTION_BUCKET> \
    --gcs_output_images_uri=gs://<TRANSFORM_PIPELINE_IMAGE_INGESTION_BUCKET> \
    --dicom_store_output_images_uri=https://healthcare.googleapis.com/v1/projects/<DICOM_STORE_GCP_PROJECT>/locations/<DICOM_STORE_LOCATION>/datasets/<DICOM_STORE_DATASET>/dicomStores/<DICOM_STORE_NAME>/dicomWeb \
    --wsi_images=gs://<BUCKET_PATH>/example1.jpg,gs://<BUCKET_PATH>/example2.png \
    --output_type=gcs \
    --cleanup_artifacts=False

Example 3): Transform a set of images to DICOM. This example will ingest a set
  of images to DICOM. As an example the imaging in the CAMELYON17 grand
  challenge could be placed within a bucket in cloud and either the whole
  dataset or a subset of the dataset could be ingested and transformed to DICOM.
  The location of of the imaging in cloud is defined using the JSON file
  (datasets_path) and the datasets to ingest are defined by
  name using the datasets parameter.

$ python3 transformation_pipeline/test_utils/ingest_data_generator/synthetic_ingest_data_generator.py \
    --gcs_input_project=<GCS_PROJECT_STORING_IMAGING> \
    --gcs_output_metadata_uri=gs://<TRANSFORM_PIPELINE_METADATA_INGESTION_BUCKET> \
    --gcs_output_images_uri=gs://<TRANSFORM_PIPELINE_IMAGE_INGESTION_BUCKET> \
    --dicom_store_output_images_uri=https://healthcare.googleapis.com/v1/projects/<DICOM_STORE_GCP_PROJECT>/locations/<DICOM_STORE_LOCATION>/datasets/<DICOM_STORE_DATASET>/dicomStores/<DICOM_STORE_NAME>/dicomWeb \
    --output_type=gcs \
    --datasets=<MY_FIRST_DATASET>,<MY_SECOND_DATASET> \
    --datasets_path=<LOCAL_PATH_TO_JSON_FILE_DEFINING_DATASETS> \
    --cleanup_artifacts=False

Commonly Used Optional Flags:
  --max_num_images=1
    The maximum number of images to ingest when ingesting from a dataset. The
    example will ingest a single image from a dataset.

  --largest_images
    When ingesting from a dataset ingest largest imaging (by file size) first. Use
    with --max_num_images to selectively transform the largest imaging.

  --smallest_images
    When ingesting from a dataset ingest smallest imaging (by file size) first.
    Use with --max_num_images to selectively transform the smallest imaging.

  --cleanup_artifacts=True
    Delete artifacts from DICOM store at conclusion of test ingestion. Enable to
    test ingestion without permanently adding imaging to the DICOM store.

  --cleanup_artifacts=False
    Leave imaging in the DICOM store at conclusion of test.

  --verify_results_bucket=gs://PATH_TO_GCS_BUCKET_DICOM_FILES_WRITTEN_TO
    If the transformation pipeline is configured to mirror imaging ingested into
    the dicom store to GCS, the path to location in GCS that imaging is mirrored
    to can be set here. Testing tool will validate that ingested DICOM are
    copied to GCS.

  --viewer_base_url=https://VIEWER_DOMAIN/dpas/viewer?series=projects/DICOM_STORE_GCP_PROJECT/locations/DICOM_STORE_LOCATION/datasets/DICOM_STORE_DATASET/dicomStores/DICOM_STORE_NAME/dicomWeb \
    Defines a base url to for google zero foot print web viewer. If defined will
    show url which can be used to immediatly visualize transformed imaging.

  --big_query_table_id=GCP_PROJECT_NAME.BIG_QUERY_DATASET.TABLE_NAME
    Defines the big query table to ingest metadata into. Use if transformation
    pipeline is configured to read metadata from Big Query.

--------------------------------------------------------------------------------
Dataset Source Definition Configuration

The location of test datasets can be described in a JSON formatted file.
By default the tool will look for a file located within the tools running
directory with the name "default_datasets.json"

Configuration File formatting.
The configuration file may start with comment header lines, lines prefixed with
#. Following this, the file is expected to contain a JSON formatted dictionary
# of one or more datasets. The keys for the entries in the dictionary are
are the names used to identify the dataset. The dataset key is expected to map
to a dictionary that defines "name", "path", and "is_wsi" key value pairs.
The "name" key maps to a string that defines a descriptive name for the dataset.
The "path" key maps to a string that defines the location of the dataset in a
cloud storage bucket; gs style formatted path. The "is_wsi" key maps to a
boolean that if true indicates the referenced imaging describes WSI imaging
(e.g., Openslide compatiable imaging) or if false defines untiled imaging
(e.g., jpg, png, or DICOM imaging defined using the VL Microscopic Image IOD or
VL Slide-Coordinates Microscopic Image IOD)

A example json follows:

# Testing dataset maping
{
  "large_dataset": {
    "name": "NAME_OF_DATASET",
    "path": "gs://my_test_data",
    "is_wsi": true
  }
}
"""

import abc
import base64
import collections
import dataclasses
import enum
import functools
import json
import os
import random
import shutil
import tempfile
import time
from typing import List, Mapping, Optional, Set
import zipfile

from absl import app
from absl import flags
from absl import logging
from absl.testing import flagsaver
import google.api_core
from google.cloud import bigquery
from google.cloud import storage
import pandas
import pydicom
import requests

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import cloud_storage_client
from transformation_pipeline.ingestion_lib import csv_util
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_store_client
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator

_DATA_DIR = os.path.dirname(__file__)


class OutputType(enum.Enum):
  LOCAL = 1
  GCS = 2
  DICOM_STORE = 3


# Data generation configuration flags
LARGEST_IMAGES_FLG = flags.DEFINE_boolean(
    'largest_images', False, 'Non-Random. Prefer to import largest images.'
)
SMALLEST_IMAGES_FLG = flags.DEFINE_boolean(
    'smallest_images', False, 'Non-Random. Prefer to import smallest images.'
)
MINIMAL_METADATA_FLG = flags.DEFINE_boolean(
    'minimal_metadata', False, 'Import data with minimal metadata.'
)
ZIP_DICOM_FLG = flags.DEFINE_boolean(
    'zip_dicom',
    True,
    (
        'Transform DICOM imaging described by single instance in a zip file.'
        ' Flag is only meaningful for slides described by a single DICOM.'
        ' Multi-instance slides require zip packaging.'
    ),
)
DATA_MULTIPLIER_FLG = flags.DEFINE_integer(
    'data_multiplier', 1, 'How many times to replicate data to be ingested.'
)
RANDOM_SEED_FLG = flags.DEFINE_integer(
    'random_seed', None, 'Seed for random number generator.'
)
IDENTIFY_DICOM_BY_FILENAME_FLG = flags.DEFINE_boolean(
    'identify_dicom_by_filename',
    False,
    'True = Identify slide in filename; False = BarcodeValueTag.',
)
WHOLE_FILENAME_PRIMARY_KEY_FLG = flags.DEFINE_boolean(
    'whole_file_name_metadata_primary_key',
    False,
    'False = identify by part of file name, True = identfy by whole name',
)
MAX_STUDY_PER_PATIENT_FLG = flags.DEFINE_integer(
    'max_studies_per_patient', 5, 'Max number of studies per patient.'
)
MIN_IMAGE_PER_PATIENT_FLG = flags.DEFINE_integer(
    'min_images_per_patient',
    1,
    'Min images per patient. Must be less than total number of images.',
)
MAX_IMAGE_PER_PATIENT_FLG = flags.DEFINE_integer(
    'max_images_per_patient', 65, 'Max number of images per patient.'
)
MAX_NUM_IMAGES_FLG = flags.DEFINE_integer(
    'max_num_images',
    None,
    'Maximum number of images to ingest. All images used if None.',
)
GENERATE_STUDY_INSTANCE_UID_FLG = flags.DEFINE_boolean(
    'generate_study_instance_uid',
    True,
    'Metadata will include study instance uid.',
)
GENERATE_ACCESSION_NUMBER_FLG = flags.DEFINE_boolean(
    'generate_accession_number',
    True,
    'Metadata will include acession number.',
)

GENERATE_SERIES_INSTANCE_UID_FLG = flags.DEFINE_boolean(
    'generate_series_instance_uid',
    False,
    'Generate metadata with series instance uid.',
)

WRITE_BARCODE_VALUE_METADATA_FLG = flags.DEFINE_boolean(
    'write_barcode_value_metadata',
    True,
    'Write barcode value metadata.',
)
WRITE_METADATA_FLG = flags.DEFINE_boolean(
    'write_metadata', True, 'Upload metadata.'
)

# Input flags
DATASETS_FLG = flags.DEFINE_list(
    'datasets',
    [],
    (
        'Comma-separated list of datasets to test. See datasets below for '
        'possible values.'
    ),
)

_DEFAULT_DATASETS_PATH = os.path.join(_DATA_DIR, 'default_datasets.json')

DATASET_SOURCE_FLG = flags.DEFINE_string(
    'datasets_path',
    _DEFAULT_DATASETS_PATH if os.path.isfile(_DEFAULT_DATASETS_PATH) else '',
    'File path to JSON formated dataset configuration file',
)

WSI_IMAGES_FLG = flags.DEFINE_list(
    'wsi_images', [], 'Comma-separated list of WSI in GCS to ingest.'
)
FLAT_IMAGES_FLG = flags.DEFINE_list(
    'flat_images', [], 'Comma-separated list of flat images in GCS to ingest.'
)
GCS_INPUT_PROJECT_FLG = flags.DEFINE_string(
    'gcs_input_project',
    '',
    'GCS project to use for reading input imaging.',
)

# Output flags
OUTPUT_TYPE_FLG = flags.DEFINE_enum_class(
    'output_type',
    OutputType.LOCAL,
    OutputType,
    'How to output images. See flags below for each option.',
)
LOCAL_OUTPUT_DIR_FLG = flags.DEFINE_string(
    'local_output_dir', None, 'Local output directory.'
)
GCP_OUTPUT_PROJECT_FLG = flags.DEFINE_string(
    'gcp_output_project',
    None,
    'GCP project to use for writing output data.',
)
GCS_OUTPUT_METADATA_URI_FLG = flags.DEFINE_string(
    'gcs_output_metadata_uri',
    '',
    'GCS output metadata URI.',
)
BIG_QUERY_TABLE_ID_FLG = flags.DEFINE_string(
    'big_query_table_id',
    '',
    'Big Query Table ID to write metadata. '
    'Format={GCP_PROJECT_NAME}.{BIG_QUERY_DATASET}.{TABLE_NAME}',
)
GCS_OUTPUT_IMAGES_URI_FLG = flags.DEFINE_string(
    'gcs_output_images_uri',
    '',
    'GCS output images URI.',
)
DICOM_STORE_OUTPUT_IMAGES_URI_FLG = flags.DEFINE_string(
    'dicom_store_output_images_uri',
    '',
    'DICOM store output images URI.',
)
VERIFY_RESULTS_BUCKET_FLG = flags.DEFINE_string(
    'verify_results_bucket',
    '',
    (
        'Optional, gs:// style path to GCS bucket ingested instances are copied'
        ' to. Verification is skipped if empty.'
    ),
)
VERIFY_RESULTS_TIMEOUT_SECONDS_FLG = flags.DEFINE_integer(
    'verify_results_timeout_seconds', 600, 'Timeout for results verification.'
)
VIEWER_BASE_URL_FLG = flags.DEFINE_string(
    'viewer_base_url',
    '',
    'If defined, viewer URLs of successfully ingested instances are logged.',
)
CLEANUP_ARTIFACTS_FLG = flags.DEFINE_boolean(
    'cleanup_artifacts',
    True,
    (
        'Whether to cleanup artifacts generated for and during ingestion. '
        'Cleans up GCS metadata, as well as DICOM instances in GCS copy '
        'bucket and DICOM Store. '
        'Reuses --verify_results_bucket flag for GCS copy bucket and '
        '--dicom_store_output_images_uri flag for DICOM Store path.'
    ),
)
CLEANUP_ARTIFACTS_DELAY_SECONDS_FLG = flags.DEFINE_integer(
    'cleanup_artifacts_delay_seconds',
    60,
    (
        'Wait time before cleaning up artifacts. Can be used for '
        'e.g. for having time to test OOF as well.'
    ),
)
INCLUDE_SLIDEID_IN_METADATA_FLG = flags.DEFINE_boolean(
    'include_slideid_in_metadata',
    True,
    (
        'Set to False to omit SlideID metadata in CSV. SlideID is '
        'stored in DICOM Type 1 tag. Use this to test ability of '
        'ingestion to block ingestion of slides without type 1 metadata.'
    ),
)
LOG_UPLOADED_FILES_FLG = flags.DEFINE_string(
    'log_uploaded_images_to', '', 'Set to file name to log uploaded images to.'
)

SET_PATIENT_ID_TO_SLIDE_URI_FLG = flags.DEFINE_boolean(
    'set_patient_id_to_slide_uri', False, 'Patient id to slide image uri.'
)


CURRENT_TIME_STR = time.strftime('%Y_%m_%d_%H:%M:%S', time.gmtime())
HASH_PRIVATE_TAG = '30211001'
OUTPUT_CSV_FILENAME = f'synth_meta_gen_utc_{CURRENT_TIME_STR}.csv'
DICOM_EXTENSIONS = frozenset(['.dcm', '.dicom'])
WSI_IMAGE_EXTENSIONS = frozenset(['.svs', '.tif', '.ndpi'])


@dataclasses.dataclass(frozen=True)
class Dataset:
  """Dataset for ingestion."""

  name: str
  path: str
  is_wsi: bool


class GenMethods:
  """Generic methods for generating ingest metadata data."""

  person_counter = 0
  specimen_counter = 0

  @classmethod
  def _int_to_bytes(cls, size: int, val: int) -> bytes:
    return val.to_bytes(size, 'little').rstrip(b'\x00')

  @classmethod
  def gen_caseid(cls) -> str:
    """Returns CaseID."""
    cls.specimen_counter += 1
    if cls.specimen_counter > 0xFF:
      time.sleep(0.5)  # Sleep to ensure that time based part changes.
      cls.specimen_counter = 1
    time_bytes = cls._int_to_bytes(10, int(time.time() * 1000.0))
    counter_bytes = cls._int_to_bytes(1, cls.specimen_counter)
    caseid_bytes = b'\x00'.join([time_bytes, counter_bytes])
    uid = base64.b32encode(caseid_bytes).rstrip(b'=').decode('utf-8')
    return f'GO-{uid}'

  @classmethod
  def gen_patient_id(cls) -> str:
    """Returns PatientID."""
    ms = int(time.time() * 1000.0)
    cls.person_counter += 1
    parts = [str(cls.person_counter)]
    while ms > 0:
      parts.append(str(ms % 1000))
      ms = int(ms / 1000)
    parts.reverse()
    return 'PN-' + '-'.join(parts)

  @classmethod
  def gen_name(cls, sex: str) -> str:
    """Returns DICOM formatted Patient name typically reflective of sex.

    Args:
      sex: str 'M' or 'F'

    Returns:
      Patient Name
    """
    names = csv_util.read_csv(os.path.join(_DATA_DIR, 'names.csv'))
    if sex == 'M':
      firstnames = names['Boys_Names']
    else:
      firstnames = names['Girls_Names']
    row = random.randint(0, firstnames.shape[0] - 1)
    first_name = firstnames[row]
    return f'GenLast^{first_name}'

  @classmethod
  def gen_date(cls, start_date: Optional[str] = None) -> str:
    """Returns Dicom formatted Date.

    Args:
       start_date: optional starting date, if suppled returns date within 100Y
         of start

    Returns:
      DICOM formatted date as str.
    """

    if start_date:
      start_year = int(start_date[:4])
      year = start_year + random.randint(1, 100)
    else:
      start_year = 1200
      end_year = 1800
      year = random.randint(start_year, end_year)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return f'{year}{month:02}{day:02}'

  @classmethod
  def gen_time(cls) -> str:
    """Returns DICOM formatted time."""
    thour = random.randint(0, 23)
    tmin = random.randint(0, 59)
    tsec = random.randint(0, 59)
    return f'{thour:02}{tmin:02}{tsec:02}'

  @classmethod
  def gen_age(cls, dob: str, sample_date: str) -> str:
    """Returns age in Y between two DICOM dates."""
    birth = int(dob[:4])
    collection = int(sample_date[:4])
    age = collection - birth
    return f'{age:03}Y'


class Slide:
  """Generates synthetic data representing tissue sections aka slides."""

  def __init__(self):
    self._stain = random.choice([
        'Bielschowsky',
        'zz External STAINED',
        'CD30 uA5h',
        'Congo Red',
        'Calponin uV8',
        'TRAP',
        'Tetracrhome',
        'H&E',
        'Alcian Blue',
        'ALK uO1z',
        'AB-PAS',
        'Fontana',
        'Arginase uV',
        'BOB1 uA',
    ])

  @property
  def stain(self) -> str:
    """Returns stain applied to a slide."""
    return self._stain


class Block:
  """Generates synthetic data representing tissue blocks."""

  def __init__(self, block_code: str, slide_count: int):
    self._block_code = block_code
    self._material_type = random.choice(
        ['PBLOCK', 'CONBLOCK', 'CONSSL', 'CONUSL']
    )
    self._slides = [Slide() for _ in range(slide_count)]

  @property
  def code(self) -> str:
    """Returns code identifying the block."""
    return self._block_code

  @property
  def material_type(self) -> str:
    """Returns the material type of the block."""
    return self._material_type

  @property
  def slides(self) -> List[Slide]:
    """Returns slides cut from a block."""
    return self._slides


class Specimen:
  """Represents a specimen from which blocks are made."""

  def __init__(self, person_dob: str, image_count: int, case_id: str):
    self._case_id = case_id
    self._requesting_physician = GenMethods.gen_name(random.choice(['M', 'F']))
    self._department = 'MCR GENERAL AP PATHDX'
    self._uid = uid_generator.generate_uid()
    self._specimen_collection_date = GenMethods.gen_date(person_dob)
    self._specimen_collection_time = GenMethods.gen_time()

    mod_list = ['proximal', 'distal', 'medial']
    dir_list = ['left', 'right', '']
    object_list = ['nodule', 'cyst', 'lesion', 'polyp', 'tumor', '']
    specimen_lst = []
    for organ in [
        'Kidney',
        'Liver',
        'Colon',
        'Stomach',
        'Intestin',
        'Bone',
        'Brain',
        'Ear',
        'Galbladder',
        'Heart',
        'Lung',
        'Eye',
        'Skin',
    ]:
      specimen_lst.append(('Pathology Consultation', f'{organ} test', organ))
      modifer_list = [organ, random.choice(mod_list)]
      dir_choice = random.choice(dir_list)
      if dir_choice:
        modifer_list.append(dir_choice)
      obj = random.choice(object_list)
      if obj:
        modifer_list.append(obj)
      modifer = ' '.join(modifer_list)
      specimen_lst.append(
          (f'{organ} biopsy', modifer, f'{organ}{random.randint(1,20)}')
      )
    description, modifier, specimen_type = random.choice(specimen_lst)
    self._specimen_description = description
    self._specimen_modifier = modifier
    self._specimen_type = specimen_type

    test_id, test_name = random.choice([
        ('PATHC', 'Pathology Consult'),
        ('APWT', 'Surgical Pathology'),
        ('INDPC', 'Independent Pathology Consult'),
        ('OPATHC', 'Oncology Pathology Consult'),
        ('RPATHC', 'Radiology Pathology Consult'),
        ('EPATHC', 'Encrinology Pathology Consult'),
    ])
    self._test_id = test_id
    self._test_name = test_name
    self._blocks = []

    block_count = random.randint(1, min(image_count, 26))
    images_available = image_count - block_count
    for block_index in range(block_count):
      block_code = chr(ord('A') + block_index)
      if block_index < block_count - 1:
        slide_count = random.randint(1, 1 + images_available)
      else:
        slide_count = 1 + images_available
      images_available -= slide_count - 1
      self.blocks.append(Block(block_code, slide_count))

  @property
  def case_id(self) -> str:
    """Returns the Case ID for the specimen."""
    return self._case_id

  @property
  def requesting_physician(self) -> str:
    """Returns name of physician requesting tissue biposy."""
    return self._requesting_physician

  @property
  def department(self) -> str:
    """Returns name of physician's dept requesting tissue biposy."""
    return self._department

  @property
  def uid(self) -> str:
    """Returns uid of tissue."""
    return self._uid

  @property
  def collection_date(self) -> str:
    """Returns date tissue collected; (YYYYMMDD)."""
    return self._specimen_collection_date

  @property
  def collection_time(self) -> str:
    """Returns time tissue collected; HHMMSS (24 hr)."""
    return self._specimen_collection_time

  @property
  def specimen_description(self) -> str:
    """Returns description of tissue."""
    return self._specimen_description

  @property
  def specimen_modifier(self) -> str:
    """Returns description of tissue."""
    return self._specimen_modifier

  @property
  def specimen_type(self) -> str:
    """Returns type of tissue."""
    return self._specimen_type

  @property
  def test_id(self) -> str:
    """Returns ID of test."""
    return self._test_id

  @property
  def test_name(self) -> str:
    """Returns Name of test."""
    return self._test_name

  @property
  def blocks(self) -> List[Block]:
    """Returns List of blocks cut from tissue."""
    return self._blocks


class Study:
  """Represents a study consisting of a collections of specimens."""

  def __init__(self, person_dob: str, image_count: int):
    self._uid = (
        uid_generator.generate_uid()
        if GENERATE_STUDY_INSTANCE_UID_FLG.value
        else ''
    )
    self._specimen = []
    case_id = GenMethods.gen_caseid()
    while image_count > 0:
      specimen_image_count = random.randint(1, image_count)
      self._specimen.append(Specimen(person_dob, specimen_image_count, case_id))
      image_count -= specimen_image_count

  @property
  def uid(self) -> str:
    """Returns DICOM UID of study."""
    return self._uid

  @property
  def specimen(self) -> List[Specimen]:
    """Returns List of specimen in study."""
    return self._specimen


class ImageRefGCPClientCache:
  """Global client cache for ImageRef.bytes."""

  name = None
  client = None


@dataclasses.dataclass
class ImageRef:
  """Path to GCP images."""

  gcp_project: str
  image_uris: List[str]
  dataset_name: str
  size_b: Optional[int]
  is_dicom: bool
  is_wsi: bool

  @property
  def bytes(self) -> Optional[int]:
    """Returns size in bytes of file."""
    if self.size_b is None:
      self.size_b = 0
      for image_uri in self.image_uris:
        blob = storage.Blob.from_string(image_uri)
        if blob.size is None:
          if ImageRefGCPClientCache.name != self.gcp_project:
            ImageRefGCPClientCache.name = self.gcp_project
            ImageRefGCPClientCache.client = storage.Client(
                project=self.gcp_project
            )
          blob.reload(client=ImageRefGCPClientCache.client)
        self.size_b += blob.size
    return self.size_b


@dataclasses.dataclass
class SlideImagingAndMetadata:
  image_paths: List[ImageRef]
  metadata: Mapping[str, List[str]]


class Person:
  """Represents a person on which a collection of studies have been done."""

  def __init__(self, studies: int, image_count: int):
    studies = min(studies, image_count)
    self._sex = random.choice(['F', 'M'])
    self._name = GenMethods.gen_name(self._sex)
    self._dob = GenMethods.gen_date()
    self._patient_id = GenMethods.gen_patient_id()
    self._studies = []
    images_available = image_count - studies
    for idx in range(studies):
      if idx != studies - 1:
        image_count = random.randint(1, images_available + 1)
      else:
        image_count = images_available + 1
      images_available -= image_count - 1
      self._studies.append(Study(self._dob, image_count))

  @property
  def sex(self) -> str:
    """Returns Sex of person; M/F."""
    return self._sex

  @property
  def name(self) -> str:
    """Returns persons name, in DICOM format."""
    return self._name

  @property
  def dob(self) -> str:
    """Returns persons date of birth in DICOM format."""
    return self._dob

  @property
  def patient_id(self) -> str:
    """Returns persons id in DICOM format."""
    return self._patient_id

  def _find_image(self, image_paths: List[ImageRef], gsuri: str) -> ImageRef:
    for image_ref in image_paths:
      for image_uri in image_ref.image_uris:
        if image_uri.endswith(gsuri):
          return image_ref
    raise ValueError(f'Error {gsuri} not found.')

  def _get_study_img(
      self, study_images: Set[str], image_paths: List[ImageRef]
  ) -> Optional[ImageRef]:
    """Get images for study, ensuring unique paths.

    Args:
      study_images: set tracking images added to a study.
      image_paths: list of images.

    Returns:
      First image found which is not a dupe for the study.
    """
    while True:
      if not image_paths:
        return None
      img = image_paths.pop()
      if img.bytes == 0:
        logging.warning('Skipping image with 0 bytes: %s', img.image_uris[0])
        continue
      img_str = f'{img.gcp_project}{img.image_uris[0]}'
      if img_str in study_images:
        logging.warning(
            'Skipping image already in study: %s', img.image_uris[0]
        )
        continue
      logging.info(
          'Queuing image (%s; size: %d bytes)', img.image_uris[0], img.bytes
      )
      study_images.add(img_str)
      return img

  def optional_md(self, val: str) -> str:
    """Excludes optional metadata if minimal_metadata defined.

    Args:
      val: Value as string.

    Returns:
      Empty string if FLAGS.minimal_metadata otherwise returns value.
    """
    if MINIMAL_METADATA_FLG.value:
      return ''
    return val

  def get_slides(self, image_paths: List[ImageRef]) -> SlideImagingAndMetadata:
    """Returns a collection of slides for the person.

    Args:
      image_paths: List of paths for all images.

    Returns:
      Mapping representing the slide imaging and metadata for people.
    """

    patient_data = collections.OrderedDict()
    for key in [
        'Metadata Primary Key',
        'Bar Code Value',
        'Patient Name',
        'Patient ID',
        'Patient DOB',
        'Patient Sex',
        'Specimen Collection Date',
        'Specimen Collection Time',
        'Patient Age on Collection Date',
        'Slide ID',
        'Test ID',
        'Test Name',
        'Institution Name',
        'Department',
        'Requesting Physician',
        'Material ID',
        'Specimen Description',
        'Specimen Modifier',
        'Material Type',
        'Specimen Type',
        'Stain',
        'Block Code',
        'Specimen Code',
        'Case ID',
        'Study Instance UID',
        'Specimen UID',
        'ICD Version',
        'DIAGNOSIS_CODE1',
        'DIAGNOSIS_NAME1',
        'DIAGNOSIS_CODE2',
        'DIAGNOSIS_NAME2',
        'DIAGNOSIS_CODE3',
        'DIAGNOSIS_NAME3',
        'DIAGNOSIS_CODE4',
        'DIAGNOSIS_NAME4',
        'SNOMED_VERSION',
        'SNOMED_CODE1',
        'SNOMED_DESCRIPTION1',
        'SNOMED_CODE2',
        'SNOMED_DESCRIPTION2',
        'SNOMED_CODE3',
        'SNOMED_DESCRIPTION3',
        'SNOMED_CODE4',
        'SNOMED_DESCRIPTION4',
    ]:
      patient_data[key] = list()
    if GENERATE_SERIES_INSTANCE_UID_FLG.value:
      patient_data['Series Instance UID'] = list()
    selected_slides_imaging_paths = []
    for study in self._studies:
      study_image_memory = set()
      for specimen_number, specimen in enumerate(study.specimen):
        for block in specimen.blocks:
          for slide_number, slide in enumerate(block.slides):
            img = self._get_study_img(study_image_memory, image_paths)
            if not img:  # No images left to add in study.
              break
            selected_slides_imaging_paths.append(img)

            case_id = specimen.case_id
            block_code = block.code

            specimen_code = block_code[0]
            material_id = '-'.join(
                [case_id, str(specimen_number + 1), block_code]
            )
            slide_id = '-'.join([material_id, str(slide_number + 1)])
            patient_data['Metadata Primary Key'].append(slide_id)
            patient_data['Bar Code Value'].append(
                slide_id if WRITE_BARCODE_VALUE_METADATA_FLG.value else ''
            )
            patient_data['Patient Name'].append(self.optional_md(self.name))
            patient_data['Patient ID'].append(self.optional_md(self.patient_id))
            patient_data['Patient DOB'].append(self.optional_md(self.dob))
            patient_data['Patient Sex'].append(self.optional_md(self.sex))
            patient_data['Specimen Collection Date'].append(
                self.optional_md(specimen.collection_date)
            )
            patient_data['Specimen Collection Time'].append(
                self.optional_md(specimen.collection_time)
            )
            patient_data['Patient Age on Collection Date'].append(
                self.optional_md(
                    GenMethods.gen_age(self.dob, specimen.collection_date)
                )
            )
            patient_data['Slide ID'].append(
                slide_id if INCLUDE_SLIDEID_IN_METADATA_FLG.value else ''
            )
            patient_data['Test ID'].append(self.optional_md(specimen.test_id))
            patient_data['Test Name'].append(
                self.optional_md(specimen.test_name)
            )

            patient_data['Institution Name'].append(
                self.optional_md(img.dataset_name)
            )
            patient_data['Department'].append(
                self.optional_md(img.image_uris[0])
            )

            patient_data['Requesting Physician'].append(
                self.optional_md(specimen.requesting_physician)
            )
            patient_data['Material ID'].append(material_id)
            patient_data['Specimen Description'].append(
                self.optional_md(specimen.specimen_description)
            )
            patient_data['Specimen Modifier'].append(
                self.optional_md(specimen.specimen_modifier)
            )
            patient_data['Material Type'].append(
                self.optional_md(block.material_type)
            )
            patient_data['Specimen Type'].append(
                self.optional_md(specimen.specimen_type)
            )
            patient_data['Stain'].append(self.optional_md(slide.stain))
            patient_data['Block Code'].append(self.optional_md(block_code))
            patient_data['Specimen Code'].append(
                self.optional_md((specimen_code))
            )
            patient_data['Case ID'].append(
                case_id if GENERATE_ACCESSION_NUMBER_FLG.value else ''
            )
            patient_data['Study Instance UID'].append(study.uid)
            if GENERATE_SERIES_INSTANCE_UID_FLG.value:
              patient_data['Series Instance UID'].append(
                  uid_generator.generate_uid()
              )
            patient_data['Specimen UID'].append(specimen.uid)

            icd_list = [
                (
                    'Z12.83',
                    'Encounter for screening for malignant neoplasm of skin',
                ),
                ('Z85.820', 'Personal history of malignant melanoma of skin'),
                ('R23.8', 'Other skin changes'),
                ('R23.8TEST_VERY_LONG_CODE', 'Other skin changes'),
                ('R23.9TEST_VERY_LONG_CODE', 'Other skin changes'),
                ('R23.7TEST_VERY_LONG_CODE', 'Other skin changes'),
                ('L82.1', 'Other seborrheic keratosis'),
                ('', ''),
            ]
            random.shuffle(icd_list)
            patient_data['ICD Version'].append(self.optional_md('ICD 10'))
            for index in range(1, 5):
              code, name = icd_list.pop()
              patient_data[f'DIAGNOSIS_CODE{index}'].append(
                  self.optional_md(code)
              )
              patient_data[f'DIAGNOSIS_NAME{index}'].append(
                  self.optional_md(name)
              )

            snowmed_list = [
                ('M-80702', 'IEC - Intraepidermal, carcinoma'),
                ('M-80702', 'Intraepidermal carcinoma, NOS'),
                ('M-80702', 'Intraepidermal carcinoma, NOS'),
                ('M-87422', "Hutchinson's melanotic, freckle\\, NOS"),
                ('M-87422', "Hutchinson's melanotic, freckle, NOS"),
                ('M-87422', "[M]Hutchinson's melanotic, freckle"),
                ('T-02007', 'Skin of, thigh'),
                ('T-02007_TEST_VERY_LONG_CODE', 'Skin of, thigh'),
                ('T-02121', 'Skin structure, of cheek'),
                ('', ''),
                ('', ''),
                ('', ''),
            ]
            random.shuffle(snowmed_list)
            patient_data['SNOMED_VERSION'].append(self.optional_md('SNOMED CT'))
            for index in range(1, 5):
              code, name = snowmed_list.pop()
              patient_data[f'SNOMED_CODE{index}'].append(self.optional_md(code))
              patient_data[f'SNOMED_DESCRIPTION{index}'].append(
                  self.optional_md(name)
              )

    return SlideImagingAndMetadata(selected_slides_imaging_paths, patient_data)


@functools.cache
def _load_dataset_description() -> Mapping[str, Dataset]:
  """Loads dataset(s) def from JSON file returns as map."""
  path = DATASET_SOURCE_FLG.value
  if not path:
    msg = f'Undefined dataset configuration file; {path}.'
    logging.error(msg)
    raise ValueError(msg)
  if not os.path.isfile(path):
    msg = f'Dataset configuration file not found; {path}.'
    logging.error(msg)
    raise ValueError(msg)
  with open(path, 'rt') as infile:
    try:
      dataset = json.load(infile)
    except json.JSONDecodeError as exp:
      msg = f'Dataset configuration file does not contain valid json; {path}.'
      logging.error(msg)
      raise exp
  if not isinstance(dataset, dict):
    msg = f'Dataset configuration file is empty; {path}.'
    logging.error(msg)
    raise ValueError(msg)
  return_dict = {}
  for key, value in dataset.items():
    if not isinstance(value, dict) or not value:
      msg = f'Invalid dataset configuration file.; {path}.'
      logging.error(msg)
      raise ValueError(msg)
    value = {key.lower().strip(): val for key, val in value.items()}
    ds = Dataset(
        value.get('name', key), value.get('path'), value.get('is_wsi', True)
    )
    if (
        isinstance(key, str)
        and isinstance(ds.is_wsi, bool)
        and isinstance(ds.name, str)
        and isinstance(ds.path, str)
        and ds.name
        and ds.path
    ):
      return_dict[key.strip().lower()] = ds
    else:
      msg = f'Dataset configuration formatted unexpectedly; {path}.'
      logging.error(msg)
      raise ValueError(msg)
  return return_dict


def get_images() -> List[ImageRef]:
  """Returns the list of input images.

  Optionally replicates data and orders by file size based on flags.

  Returns:
    List of ImageRef
  """
  dataset_map = _load_dataset_description()
  datasets = [dataset_map[d.strip().lower()] for d in DATASETS_FLG.value]
  for wsi_image in WSI_IMAGES_FLG.value:
    datasets.append(Dataset(name='gcs_file_wsi', path=wsi_image, is_wsi=True))
  for flat_image in FLAT_IMAGES_FLG.value:
    datasets.append(
        Dataset(name='gcs_file_flat', path=flat_image, is_wsi=False)
    )

  image_refs = []
  client = storage.Client(project=GCS_INPUT_PROJECT_FLG.value)
  for dataset in datasets:
    logging.info('Fetching images for dataset: %s', dataset.name)
    bk = storage.Bucket.from_string(dataset.path)
    bl = storage.Blob.from_string(dataset.path)
    blob_name = bl.name.rstrip('/')
    dicom_found = dict()
    for prefix in [f'{blob_name}/', blob_name]:
      blob_list = list(client.list_blobs(bucket_or_name=bk, prefix=prefix))
      if blob_list:
        break
    for path in blob_list:
      _, ext = os.path.splitext(path.name)
      ext = ext.lower()
      is_dicom = ext in DICOM_EXTENSIONS
      if not dataset.is_wsi or ext in WSI_IMAGE_EXTENSIONS:
        logging.info('Found: %s', path.name)
        image_refs.append(
            ImageRef(
                GCS_INPUT_PROJECT_FLG.value,
                [f'gs://{bk.name}/{path.name}'],
                dataset.name,
                size_b=None,
                is_dicom=is_dicom,
                is_wsi=dataset.is_wsi,
            )
        )
      elif dataset.is_wsi and is_dicom:
        dicom_parent_dir = os.path.dirname(path.name)
        dicom_path = f'gs://{bk.name}/{dicom_parent_dir}'
        if dicom_path in dicom_found:
          dicom_found[dicom_path].image_uris.append(
              f'gs://{bk.name}/{path.name}'
          )
        else:
          logging.info('Found: %s', dicom_parent_dir)
          img_ref = ImageRef(
              GCS_INPUT_PROJECT_FLG.value,
              [f'gs://{bk.name}/{path.name}'],
              dataset.name,
              size_b=None,
              is_dicom=is_dicom,
              is_wsi=True,
          )
          image_refs.append(img_ref)
          dicom_found[dicom_path] = img_ref

  if not image_refs:
    raise ValueError('No images found.')
  logging.info('Found: %d images.', len(image_refs))

  if DATA_MULTIPLIER_FLG.value > 1:
    logging.info('Replicating images %d times.', DATA_MULTIPLIER_FLG.value)
    image_refs = DATA_MULTIPLIER_FLG.value * image_refs

  if LARGEST_IMAGES_FLG.value:
    logging.info('Reordering images based on size (largest first).')
    image_refs = sorted(
        image_refs, key=lambda imgref: imgref.bytes, reverse=False
    )
  elif SMALLEST_IMAGES_FLG.value:
    logging.info('Reordering images based on size (smallest first).')
    image_refs = sorted(
        image_refs, key=lambda imgref: imgref.bytes, reverse=True
    )
  else:
    logging.info('Shuffling images.')
    random.shuffle(image_refs)

  if (
      image_refs
      and MAX_NUM_IMAGES_FLG.value
      and MAX_NUM_IMAGES_FLG.value < len(image_refs)
  ):
    logging.info(
        'Limiting number of images to %d (out of %d).',
        MAX_NUM_IMAGES_FLG.value,
        len(image_refs),
    )
    image_refs = image_refs[: MAX_NUM_IMAGES_FLG.value]

  return image_refs


def update_dcm(
    barcode: str,
    study_uid: str,
    series_uid: str,
    sop_instance_uid: str,
    dcm: pydicom.Dataset,
):
  """Updates relevant values in DICOM instance."""
  dcm.BarcodeValue = '' if IDENTIFY_DICOM_BY_FILENAME_FLG.value else barcode
  dcm.StudyInstanceUID = study_uid
  dcm.SeriesInstanceUID = series_uid
  dcm.file_meta.MediaStorageSOPInstanceUID = sop_instance_uid
  dcm.SOPInstanceUID = sop_instance_uid
  if HASH_PRIVATE_TAG in dcm:
    del dcm[HASH_PRIVATE_TAG]


def download_and_update_dcms(
    image_uris: List[str],
    filename_prefix: str,
    barcode: str,
    study_uid: str,
    series_uid: str,
    use_dpas_uid_generator: bool,
    output_dir: str,
) -> List[str]:
  """Downloads and modifies DICOM instances, writing to output directory."""
  dicom_paths = []
  for img_uri in image_uris:
    dicom_path = os.path.join(
        output_dir, f'{filename_prefix}dcm_{time.time()}.dcm'
    )
    cloud_storage_client.download_to_container(img_uri, dicom_path)
    dcm = pydicom.dcmread(dicom_path)
    if use_dpas_uid_generator:
      sop_instance_uid = uid_generator.generate_uid()
    else:
      sop_instance_uid = pydicom.uid.generate_uid()
    update_dcm(barcode, study_uid, series_uid, sop_instance_uid, dcm)
    dcm.save_as(dicom_path, write_like_original=False)
    dicom_paths.append(dicom_path)
  return dicom_paths


class AbstractImageSourceHandle(metaclass=abc.ABCMeta):

  @abc.abstractmethod
  def get_file_path(self) -> str:
    pass

  @abc.abstractmethod
  def retrieve_file_bytes(self) -> None:
    pass


class EmptyImageSourceHandle(AbstractImageSourceHandle):

  def get_file_path(self) -> str:
    return ''

  def retrieve_file_bytes(self) -> None:
    return None


class GenericImageSourceHandle(AbstractImageSourceHandle):
  """Retrieves Generic imaging."""

  def __init__(
      self, metadata_primary_key: str, output_dir: str, image_path: ImageRef
  ):
    filename = os.path.basename(image_path.image_uris[0])
    self._image_path = image_path
    if WRITE_METADATA_FLG.value:
      self._output_path = os.path.join(
          output_dir, f'{metadata_primary_key}_{filename}'
      )
    else:
      ext = os.path.splitext(filename)[1]
      self._output_path = os.path.join(
          output_dir, f'{metadata_primary_key}_{time.time()}{ext}'
      )

  def get_file_path(self) -> str:
    return self._output_path

  def retrieve_file_bytes(self) -> None:
    """Downloads bytes into file."""
    cloud_storage_client.reset_storage_client(self._image_path.gcp_project)
    cloud_storage_client.download_to_container(
        self._image_path.image_uris[0], self._output_path
    )


class DicomImageSourceHandle(AbstractImageSourceHandle):
  """Retrieves DICOM imaging."""

  def __init__(
      self,
      metadata_primary_key: str,
      study_uid: str,
      output_dir: str,
      image_path: ImageRef,
      dicom_store_ingest: bool,
      zip_wsi_dicom: bool,
  ):
    self._study_uid = study_uid if study_uid else uid_generator.generate_uid()
    self._dicom_store_ingest = dicom_store_ingest
    self._output_dir = output_dir
    self._image_path = image_path
    self._metadata_primary_key = metadata_primary_key
    self._filename_prefix = (
        f'{metadata_primary_key}_'
        if IDENTIFY_DICOM_BY_FILENAME_FLG.value
        else ''
    )
    if not image_path.image_uris:
      self._output_path = ''
      self._suffix = ''
      return
    if image_path.is_wsi and (zip_wsi_dicom or len(image_path.image_uris) != 1):
      self._suffix = 'zip'
    else:
      self._suffix = 'dcm'
    self._output_path = os.path.join(
        output_dir, f'{self._filename_prefix}dcm_{time.time()}.{self._suffix}'
    )

  def get_file_path(self) -> str:
    return self._output_path

  def retrieve_file_bytes(self) -> None:
    """Downloads bytes into file."""
    series_uid = uid_generator.generate_uid()
    use_dpas_uid_generator = False if self._dicom_store_ingest else True
    dicom_paths = download_and_update_dcms(
        self._image_path.image_uris,
        self._filename_prefix,
        self._metadata_primary_key,
        self._study_uid,
        series_uid,
        use_dpas_uid_generator,
        self._output_dir,
    )
    # Required ZIP packaging flag ignored if WSI DICOM set does not contain 1
    # image.
    if self._suffix == 'zip':
      with zipfile.ZipFile(
          self._output_path,
          mode='w',
          compression=zipfile.ZIP_STORED,
          allowZip64=True,
      ) as myzip:
        for dicom_path in dicom_paths:
          myzip.write(dicom_path)
          os.remove(dicom_path)
      return
    if len(dicom_paths) > 1:
      raise ValueError('Multiple DICOM images found.')
    # Non-WSI DICOM or unzipped WSI DICOM.
    shutil.copy(dicom_paths[0], self._output_path)
    os.remove(dicom_paths[0])


def retrieve_image_source_handle(
    metadata_primary_key: str,
    study_uid: str,
    image_path: ImageRef,
    output_dir: str,
    dicom_store_ingest: bool,
    zip_wsi_dicom: bool,
) -> AbstractImageSourceHandle:
  """Downloads and prepares images for DPAS ingestion, writing to output dir."""
  if dicom_store_ingest and not image_path.is_dicom:
    logging.warning('Ignoring non-DICOM image: %s', image_path)
    return EmptyImageSourceHandle()

  if not image_path.is_dicom:
    return GenericImageSourceHandle(
        metadata_primary_key, output_dir, image_path
    )
  return DicomImageSourceHandle(
      metadata_primary_key,
      study_uid,
      output_dir,
      image_path,
      dicom_store_ingest,
      zip_wsi_dicom,
  )


def get_gcs_output_metadata_path():
  return os.path.join(GCS_OUTPUT_METADATA_URI_FLG.value, OUTPUT_CSV_FILENAME)


def write_output_metadata(metadata_path: str, storage_client: storage.Client):
  metadata_blob = storage.Blob.from_string(
      get_gcs_output_metadata_path(), client=storage_client
  )
  metadata_blob.upload_from_filename(metadata_path, client=storage_client)


def write_metadata_to_bq(metadata_path: str):
  """Write CSV metadata to BigQuery Table."""
  metadata_csv = csv_util.read_csv(metadata_path)
  metadata_csv.rename(
      columns={name: name.replace(' ', '_') for name in metadata_csv.columns},
      inplace=True,
  )
  schema = [
      bigquery.SchemaField(name, 'STRING', mode='NULLABLE')
      for name in metadata_csv.columns
  ]
  bq_table_id = BIG_QUERY_TABLE_ID_FLG.value
  with bigquery.Client() as bq_client:
    try:
      table = bq_client.get_table(bq_table_id)
    except google.api_core.exceptions.NotFound:
      table = bigquery.Table(bq_table_id, schema=schema)
      table = bq_client.create_table(table)
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = bq_client.load_table_from_dataframe(
        metadata_csv, table, job_config=job_config
    )
    job.result()


class Writer(metaclass=abc.ABCMeta):
  """Writer for DPAS ingestion files."""

  @abc.abstractmethod
  def write_metadata(self, metadata_path: str):
    pass

  @abc.abstractmethod
  def write_image(self, image_path: str):
    pass


class LocalWriter(Writer):
  """Writes DPAS ingestion files locally."""

  def __init__(self):
    self._output_dir = LOCAL_OUTPUT_DIR_FLG.value

  def write_metadata(self, metadata_path: str):
    shutil.copy(
        metadata_path, os.path.join(self._output_dir, OUTPUT_CSV_FILENAME)
    )

  def write_image(self, image_path: str):
    shutil.copy(
        image_path, os.path.join(self._output_dir, os.path.basename(image_path))
    )


class GcsWriter(Writer):
  """Writes DPAS ingestion files to GCS and metadata to GCS or BQ if set."""

  def __init__(self):
    self._images_bucket = GCS_OUTPUT_IMAGES_URI_FLG.value
    self._gcp_project = GCP_OUTPUT_PROJECT_FLG.value
    self._storage_client = storage.Client(GCP_OUTPUT_PROJECT_FLG.value)
    cloud_storage_client.reset_storage_client(self._gcp_project)

  def write_metadata(self, metadata_path: str):
    if BIG_QUERY_TABLE_ID_FLG.value:
      write_metadata_to_bq(metadata_path)
    else:
      write_output_metadata(metadata_path, self._storage_client)

  def write_image(self, image_path: str):
    cloud_storage_client.upload_blob_to_uri(
        image_path, self._images_bucket, os.path.basename(image_path)
    )


class DicomStoreWriter(Writer):
  """Writes DPAS ingestion files to GCS/BQ (metadata) and DICOM Store (images)."""

  def __init__(self):
    self._dicom_store_client = dicom_store_client.DicomStoreClient(
        dicomweb_path=DICOM_STORE_OUTPUT_IMAGES_URI_FLG.value
    )
    self._gcp_project = GCP_OUTPUT_PROJECT_FLG.value
    self._storage_client = storage.Client(GCP_OUTPUT_PROJECT_FLG.value)
    cloud_storage_client.reset_storage_client(self._gcp_project)

  def write_metadata(self, metadata_path: str):
    if BIG_QUERY_TABLE_ID_FLG.value:
      write_metadata_to_bq(metadata_path)
    else:
      write_output_metadata(metadata_path, self._storage_client)

  def write_image(self, image_path: str):
    try:
      upload_result = self._dicom_store_client.upload_to_dicom_store(
          dicom_paths=[image_path],
          discover_existing_series_option=(
              dicom_store_client.DiscoverExistingSeriesOptions.IGNORE
          ),
          copy_to_bucket_enabled=False,
      )
    except (requests.HTTPError, dicom_store_client.DicomUploadToGcsError):
      logging.error('Error uploading to DICOM store.', exc_info=True)
      return
    if upload_result.previously_ingested:
      logging.error('Unable to upload to DICOM store: DICOM already in store.')


def verify_results(
    study_instance_uids: List[str],
    viewer_base_url: Optional[str] = None,
):
  """Verifies DICOM instances are successfully ingested.

  Checks GCS copy bucket for instances with given study UID. Optionally logs
  viewer urls for successfully ingested instances.

  Args:
    study_instance_uids: study UIDs to look for.
    viewer_base_url: (Optional) viewer base URL to use for logging.
  """
  if not study_instance_uids:
    logging.info('No images to verify, skipping.')
    return
  paths_to_verify = collections.deque([
      os.path.join(VERIFY_RESULTS_BUCKET_FLG.value, study_uid)
      for study_uid in study_instance_uids
  ])
  gcs_image_uri_to_viewer_url = {}
  if viewer_base_url:
    gcs_image_uri_to_viewer_url = {
        paths_to_verify[
            i
        ]: f'{viewer_base_url}/studies/{study_instance_uids[i]}'
        for i in range(len(paths_to_verify))
    }
  num_paths_to_verify_before_sleep = len(paths_to_verify)
  succeeded = []
  start_time = time.time()
  gcs_client = storage.Client(GCP_OUTPUT_PROJECT_FLG.value)
  while paths_to_verify:
    logging.log_every_n_seconds(
        logging.INFO,
        (
            f'Verifying ingestion results; ingested: {len(succeeded)}, '
            f'remaining: {len(paths_to_verify)}.'
        ),
        10,
    )
    if time.time() - start_time > VERIFY_RESULTS_TIMEOUT_SECONDS_FLG.value:
      logging.warning('Timeout while waiting for ingestion to finish.')
      break
    # Sleep every full iteration on remaining paths before checking them again.
    if num_paths_to_verify_before_sleep <= 0:
      time.sleep(10)
      num_paths_to_verify_before_sleep = len(paths_to_verify)
    image_uri = paths_to_verify.popleft()
    num_paths_to_verify_before_sleep -= 1
    # Verify image(s) was copied to success bucket.
    bk = storage.Bucket.from_string(image_uri)
    bl = storage.Blob.from_string(image_uri)
    if list(gcs_client.list_blobs(bucket_or_name=bk, prefix=f'{bl.name}/')):
      succeeded.append(image_uri)
    else:
      paths_to_verify.append(image_uri)

  logging.info('Could not verify: %s', paths_to_verify)
  logging.info('Successfully ingested: %s', succeeded)
  if viewer_base_url:
    for image_uri in succeeded:
      logging.info('Viewer URL: %s', gcs_image_uri_to_viewer_url[image_uri])


def _wait(time_seconds: int) -> None:
  try:
    time.sleep(time_seconds)
    logging.info('Done waiting.')
  except KeyboardInterrupt:
    logging.info('Skipping wait.')


def cleanup_artifacts(study_instance_uids: List[str], primary_keys: List[str]):
  """Cleans up artifacts generated by ingestion test.

  Namely, deletes:
  * Metadata CSV written to ingestion GCS metadata bucket
  * DICOM instances written to ingestion GCS copy bucket
  * DICOM instances written to ingestion DICOM Store

  Args:
    study_instance_uids: study UIDs for which related resources in DICOM Store
      and GCS copy bucket are deleted.
    primary_keys: List of primary keys values to delete from Big Query Table.
  """
  # Remove duplicates.
  study_instance_uids = set(study_instance_uids)
  primary_keys = set(primary_keys)

  if CLEANUP_ARTIFACTS_DELAY_SECONDS_FLG.value > 0:
    logging.info(
        'Waiting for %d seconds before cleaning up artifacts. '
        'Press CTRL+C to skip wait.',
        CLEANUP_ARTIFACTS_DELAY_SECONDS_FLG.value,
    )
    _wait(CLEANUP_ARTIFACTS_DELAY_SECONDS_FLG.value)

  logging.info('Cleaning up metadata CSV...')
  cloud_storage_client.reset_storage_client(GCP_OUTPUT_PROJECT_FLG.value)
  cloud_storage_client.del_blob(get_gcs_output_metadata_path())

  bq_table_id = BIG_QUERY_TABLE_ID_FLG.value
  if bq_table_id:
    logging.info('Cleaning up metadata in BigQuery...')
    with bigquery.Client() as bq_client:
      for primary_key in primary_keys:
        del_row_query = (
            f'Delete from {bq_table_id} where'
            f' Metadata_Primary_Key="{primary_key}"'
        )
        query_job = bq_client.query(del_row_query)
        query_job.result()  # Waits for statement to finish

  logging.info('Cleaning up studies in GCS copy bucket...')
  gcs_client = storage.Client(GCP_OUTPUT_PROJECT_FLG.value)
  for study in study_instance_uids:
    path = os.path.join(VERIFY_RESULTS_BUCKET_FLG.value, study)
    bk = storage.Bucket.from_string(path)
    bl = storage.Blob.from_string(path)
    for b in gcs_client.list_blobs(bucket_or_name=bk, prefix=f'{bl.name}/'):
      b.delete()
    logging.info('Deleted %s from GCS', path)

  logging.info('Cleaning up studies in DICOM Store...')
  dicomweb_path = DICOM_STORE_OUTPUT_IMAGES_URI_FLG.value
  dcm_client = dicom_store_client.DicomStoreClient(dicomweb_path)
  for study in study_instance_uids:
    path = os.path.join(dicomweb_path, 'studies', study)
    if dcm_client.delete_resource_from_dicom_store(path):
      logging.info('Deleted %s from DICOM Store', path)
    else:
      logging.info('Failed to delete %s from DICOM Store', path)


# Init cloud logging client, used to log via absl.
@flagsaver.flagsaver(
    pod_hostname='BADF00D',
    dicom_guid_prefix='1.3.6.1.4.1.11129.5.7.999',
    debug_logging_use_absl_logging=True,
)
def main(unused_argv) -> None:
  """Generates and ingests synthetic CSV and image data."""
  logging.set_stderrthreshold(logging.INFO)
  # Logger used from referenced transform pipeline modules.
  cloud_logging_client.info('Logger initialized.')
  random.seed(RANDOM_SEED_FLG.value)
  if not (DATASETS_FLG.value or WSI_IMAGES_FLG.value or FLAT_IMAGES_FLG.value):
    raise ValueError('Must specify input data to ingest.')
  if MIN_IMAGE_PER_PATIENT_FLG.value > MAX_IMAGE_PER_PATIENT_FLG.value:
    logging.info(
        'Invalid min/max images per patient: %d > %d',
        MIN_IMAGE_PER_PATIENT_FLG.value,
        MAX_IMAGE_PER_PATIENT_FLG.value,
    )
  if DATA_MULTIPLIER_FLG.value <= 0:
    raise ValueError('Data multiplier cannot be negative.')
  if DATA_MULTIPLIER_FLG.value > 1 and (
      LARGEST_IMAGES_FLG.value or SMALLEST_IMAGES_FLG.value
  ):
    raise ValueError('Cannot replicate data when sorting images by size.')
  if (
      OUTPUT_TYPE_FLG.value == OutputType.LOCAL
      and not LOCAL_OUTPUT_DIR_FLG.value
  ):
    raise ValueError('Missing local output flag.')
  if OUTPUT_TYPE_FLG.value == OutputType.GCS and not (
      GCP_OUTPUT_PROJECT_FLG.value
      or GCS_OUTPUT_METADATA_URI_FLG.value
      or GCS_OUTPUT_IMAGES_URI_FLG.value
  ):
    raise ValueError('Missing GCS output flag(s).')
  if (
      OUTPUT_TYPE_FLG.value == OutputType.DICOM_STORE
      and IDENTIFY_DICOM_BY_FILENAME_FLG.value
  ):
    raise ValueError(
        'Cannot identify DICOMs by filename when writing to DICOM store.'
    )
  if (
      OUTPUT_TYPE_FLG.value == OutputType.DICOM_STORE
      and not DICOM_STORE_OUTPUT_IMAGES_URI_FLG.value
  ):
    raise ValueError('Missing DICOM store output flag.')
  if CLEANUP_ARTIFACTS_FLG.value and not GCS_OUTPUT_METADATA_URI_FLG.value:
    raise ValueError('Missing GCS metadata URI, used for cleanup.')
  if CLEANUP_ARTIFACTS_FLG.value and not VERIFY_RESULTS_BUCKET_FLG.value:
    raise ValueError('Missing GCS copy bucket, used for cleanup.')
  if (
      CLEANUP_ARTIFACTS_FLG.value
      and not DICOM_STORE_OUTPUT_IMAGES_URI_FLG.value
  ):
    raise ValueError('Missing DICOM store output, used for cleanup')

  image_paths = get_images()
  selected_slide_imaging_paths = []
  metadata_datasets = []
  while image_paths:
    num_images = len(image_paths)
    min_images_per_patient = min(MIN_IMAGE_PER_PATIENT_FLG.value, num_images)
    max_images_per_patient = min(MAX_IMAGE_PER_PATIENT_FLG.value, num_images)
    image_count = random.randint(min_images_per_patient, max_images_per_patient)
    study_count = random.randint(
        1, min(MAX_STUDY_PER_PATIENT_FLG.value, image_count)
    )
    person = Person(study_count, image_count)
    imaging_and_metadata = person.get_slides(image_paths)
    selected_slide_imaging_paths.extend(imaging_and_metadata.image_paths)
    metadata_datasets.append(pandas.DataFrame(imaging_and_metadata.metadata))
  metadata = pandas.concat(metadata_datasets)

  with (
      tempfile.TemporaryDirectory() as tmp_metadata_dir,
      tempfile.TemporaryDirectory() as tmp_images_dir,
  ):
    dicom_store_ingest = False
    zip_wsi_dicom = ZIP_DICOM_FLG.value
    if OUTPUT_TYPE_FLG.value == OutputType.LOCAL:
      writer = LocalWriter()
    elif OUTPUT_TYPE_FLG.value == OutputType.GCS:
      writer = GcsWriter()
    elif OUTPUT_TYPE_FLG.value == OutputType.DICOM_STORE:
      writer = DicomStoreWriter()
      dicom_store_ingest = True
      zip_wsi_dicom = False
    else:
      raise ValueError(f'Unsupported output type: {OUTPUT_TYPE_FLG.value}')

    study_instance_uids_to_verify = []
    image_source_handles: List[AbstractImageSourceHandle] = []
    output_log_path = LOG_UPLOADED_FILES_FLG.value.strip()
    output_log_lines = []
    for row_index in range(metadata.shape[0]):
      row_metadata = metadata.iloc[row_index]
      study_instance_uid = row_metadata['Study Instance UID']
      image_ref = selected_slide_imaging_paths[row_index]
      logging.info('Downloading: %s', image_ref.image_uris)
      image_source_handle = retrieve_image_source_handle(
          row_metadata['Metadata Primary Key'],
          study_instance_uid,
          image_ref,
          tmp_images_dir,
          dicom_store_ingest,
          zip_wsi_dicom,
      )
      if not image_source_handle.get_file_path():
        continue
      image_source_handles.append(image_source_handle)
      if SET_PATIENT_ID_TO_SLIDE_URI_FLG.value:
        metadata['Patient ID'][row_index] = image_ref.image_uris[0]
      if WHOLE_FILENAME_PRIMARY_KEY_FLG.value:
        core_filename = os.path.splitext(
            os.path.basename(image_source_handle.get_file_path())
        )[0]
        row_metadata['Metadata Primary Key'] = core_filename
        if row_metadata['Bar Code Value']:
          row_metadata['Bar Code Value'] = core_filename

      # If study instance uid is not written in metadata it is returned as NAN.
      if isinstance(study_instance_uid, str):
        study_instance_uids_to_verify.append(study_instance_uid)
      if output_log_path:
        elements = [
            str(image_ref.gcp_project),
            image_ref.image_uris[0],
            str(study_instance_uid),
        ]
        if GENERATE_SERIES_INSTANCE_UID_FLG.value:
          elements.append(row_metadata['Series Instance UID'])
        if VIEWER_BASE_URL_FLG.value:
          elements.append(
              f'{VIEWER_BASE_URL_FLG.value}/studies/{study_instance_uid}'
          )
        output_log_lines.append(elements)
    tmp_metadata_csv_path = os.path.join(tmp_metadata_dir, 'test.csv')
    with open(tmp_metadata_csv_path, 'wt') as metaout:
      metaout.write(f'# File generated: UTC {CURRENT_TIME_STR}\n')
      metadata.to_csv(metaout, index=False, doublequote=False, escapechar='\\')
    if WRITE_METADATA_FLG.value:
      writer.write_metadata(tmp_metadata_csv_path)
    for image_source in image_source_handles:
      image_source.retrieve_file_bytes()
      path = image_source.get_file_path()
      logging.info('Writing: %s', path)
      writer.write_image(path)
      os.remove(path)
    if output_log_lines:
      header = ['gcp_project', 'source_image_uri', 'study_instance_uid']
      if GENERATE_SERIES_INSTANCE_UID_FLG.value:
        header.append('series_instance_uid')
      if VIEWER_BASE_URL_FLG.value:
        header.append('url')
      output_log_lines.insert(0, header)
      output_log_lines = [','.join(lines) for lines in output_log_lines]
      with open(output_log_path, 'wt') as output_log:
        output_log.write('\n'.join(output_log_lines))
    logging.info('Done writing %d images.', len(image_source_handles))

  if OUTPUT_TYPE_FLG.value != OutputType.LOCAL:
    if VERIFY_RESULTS_BUCKET_FLG.value:
      verify_results(study_instance_uids_to_verify, VIEWER_BASE_URL_FLG.value)
    if CLEANUP_ARTIFACTS_FLG.value:
      cleanup_artifacts(
          study_instance_uids_to_verify, metadata['Metadata Primary Key'].values
      )


if __name__ == '__main__':
  app.run(main)
