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
"""Script to create a test annotation dicom instance."""

import sys

from absl import flags
import pydicom

from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_util import dicom_test_util

ANNOTATOR_EMAIL = flags.DEFINE_string(
    'annotator_email',
    'pathologist.416279@gmail.com',
    'Annotator email for test annotation.',
)
OUTPUT_PATH = flags.DEFINE_string(
    'output_path', '/tmp/outdir/test.dcm', 'Path to save annotation to.'
)
STUDY_INSTANCE_UID = flags.DEFINE_string(
    'study_uid', '1.2.3.4', 'Study Instance UID for test annotation.'
)
SERIES_INSTANCE_UID = flags.DEFINE_string(
    'series_uid', '1.2.3.4.5', 'Series Instance UID for test annotation.'
)
SOP_INSTANCE_UID = flags.DEFINE_string(
    'sop_instance_uid', '1.2.3.4.5.6', 'SOP Instance UID for test annotation.'
)


FLAGS = flags.FLAGS
FLAGS(sys.argv)

base_ds = pydicom.dataset.Dataset()
operator_id = pydicom.dataset.Dataset()
person_id = pydicom.dataset.Dataset()
person_id.LongCodeValue = ANNOTATOR_EMAIL.value
person_id.CodeMeaning = 'Annotator'
operator_id.PersonIdentificationCodeSequence = [person_id]
base_ds.OperatorIdentificationSequence = [operator_id]

dcm = dicom_test_util.create_test_dicom_instance(
    filepath=OUTPUT_PATH.value,
    study=STUDY_INSTANCE_UID.value,
    series=SERIES_INSTANCE_UID.value,
    sop_instance_uid=SOP_INSTANCE_UID.value,
    sop_class_uid=ingest_const.DicomSopClasses.MICROSCOPY_ANNOTATION.uid,
    dcm_json=base_ds.to_json_dict(),
)
