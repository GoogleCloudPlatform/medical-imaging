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
"""Downloads DICOM instances from DICOM Store to working directory.

                              <req>      <req>      <opt>      <opt>
python3 download_dcm.py dicomStorePath StudyUID SeriesUID SOPInstanceUID

dicomStoreWebPath formatting:
"https://healthcare.googleapis.com/v1/projects/{PROJECT}/locations/{LOCATION}/datasets/{DATASETS}/dicomStores/{DICOMSTORES}"

If run with StudyInstanceUID parameter only & study contains multiple series.
will create a directory for each series and download each series instances into
the dir
"""

import json
import logging
import os
import subprocess
import sys

import pydicom

if __name__ == '__main__':
  # https://healthcare.googleapis.com/v1/projects/{PROJECT}/locations/{LOCATION}/datasets/{DATASETS}/dicomStores/{DICOMSTORES}
  dicom_store_path, study_uid = sys.argv[1:3]
  series_uid = sys.argv[3] if len(sys.argv) >= 5 else None
  user_specified_instance_uid = sys.argv[4] if len(sys.argv) >= 5 else None
  dicom_store_web = f'{dicom_store_path}/dicomWeb'
  series_query = (
      'curl -X GET  -H "Authorization: Bearer $(gcloud auth '
      'application-default print-access-token)" '
      f'"{dicom_store_web}/studies/{study_uid}/series?"'
  )

  if series_uid:
    series_set = set([series_uid])
  else:
    result = subprocess.run(
        [series_query], shell=True, capture_output=True, check=True
    )
    dcm_list = json.loads(result.stdout.decode('utf-8'))
    series_set = set()
    for dcm in dcm_list:
      SeriesInstanceUID = dcm.get('0020000E')
      if SeriesInstanceUID:
        series_set.add(SeriesInstanceUID['Value'][0])

  for series_uid in series_set:
    dst_dir = None
    if len(series_set) > 1:
      if not os.path.isdir(series_uid):
        os.mkdir(series_uid)
      dst_dir = series_uid

    if user_specified_instance_uid:
      instanceuid_list = [user_specified_instance_uid]
    else:
      instance_query = (
          'curl -X GET  -H "Authorization: Bearer $(gcloud auth '
          'application-default print-access-token)" '
          f'"{dicom_store_web}/studies/{study_uid}/series/{series_uid}/instances?"'
      )
      result = subprocess.run(
          [instance_query], shell=True, capture_output=True, check=True
      )
      dcm_list = json.loads(result.stdout.decode('utf-8'))
      instanceuid_list = []
      for dcm in dcm_list:
        SOPinstanceUID = dcm.get('00080018')
        if SOPinstanceUID:
          instanceuid_list.append(SOPinstanceUID['Value'][0])

    for instance_uid in instanceuid_list:
      get_instance_query = (
          'curl -X GET -H "Authorization: Bearer $(gcloud auth'
          ' application-default print-access-token)" -H "Accept:'
          ' application/dicom; transfer-syntax=*"'
          f' "{dicom_store_web}/studies/{study_uid}/series/{series_uid}/instances/{instance_uid}"'
          ' --output tmp.dcm'
      )
      subprocess.run([get_instance_query], shell=True, check=True)
      try:
        ds = pydicom.dcmread('tmp.dcm')
      except pydicom.errors.InvalidDicomError as exp:
        msg = [
            f'study_uid: {study_uid}',
            f'series_uid: {series_uid}',
            f'instance_uid: {instance_uid}',
            '',
            str(exp),
        ]
        logging.info('\n'.join(msg))
        continue
      if 'THUMBNAIL' in ds.ImageType:
        filename = 'thumbnail.dcm'
      elif 'LABEL' in ds.ImageType:
        filename = 'label.dcm'
      elif 'OVERVIEW' in ds.ImageType:
        filename = 'macro.dcm'
      else:
        num = ds.InstanceNumber
        filename = f'pyramid_{num}.dcm'
      if dst_dir:
        filename = os.path.join(dst_dir, filename)
      os.rename('tmp.dcm', filename)
