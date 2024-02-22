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
"""Command Line tool: Uses DICOM store to render frame 0 of local DICOM file.

Run from commandline:
  python3 render_dcm.py dicomStoreWebPath my_dicom.dcm

dicomStoreWebPath formatting:
"https://healthcare.googleapis.com/v1/projects/{PROJECT}/locations/{LOCATION}/datasets/{DATASETS}/dicomStores/{DICOMSTORES}"


  Parameter: Path to DICOM file to render.
"""
import os
import subprocess
import sys

import pydicom

if __name__ == '__main__':
  # https://healthcare.googleapis.com/v1/projects/{PROJECT}/locations/{LOCATION}/datasets/{DATASETS}/dicomStores/{DICOMSTORES}
  dicom_store_path, dicom_path = sys.argv[1:3]
  dcm = pydicom.dcmread(dicom_path)
  study_uid = dcm.StudyInstanceUID
  series_uid = dcm.SeriesInstanceUID
  instance_uid = dcm.SOPInstanceUID

  dicom_store_web = f'{dicom_store_path}/dicomWeb'

  render_frame_query = (
      'curl -X GET  -H "Authorization: Bearer $(gcloud auth'
      ' application-default print-access-token)"  -H "Accept: image/jpeg"'
      f' "{dicom_store_web}/studies/{study_uid}/series/{series_uid}/instances/{instance_uid}/frames/1/rendered"'
      ' --output test.jpg'
  )
  subprocess.run([render_frame_query], shell=True, check=True)
  filename, extension = os.path.splitext(os.path.basename(dicom_path))
  os.rename('test.jpg', f'{filename}.jpg')
