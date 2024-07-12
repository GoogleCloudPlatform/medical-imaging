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
#
# ==============================================================================
"""Constants used in tests."""

import dataclasses
import datetime

import ilm_config
import ilm_types

INSTANCE_0 = 'studies/1.22.333/series/1.44.555/instances/6.77.888'
INSTANCE_1 = 'studies/1.22.333/series/1.44.555/instances/6.77.999'
INSTANCE_2 = 'studies/1.22.333/series/1.44.555/instances/6.77.000'
INSTANCE_DISALLOW_LIST = 'studies/1.22.333/series/1.44.555/instances/9.99.999'

ILM_CONFIG = ilm_config.ImageLifecycleManagementConfig(
    dry_run=True,
    dicom_store_config=ilm_config.DicomStoreConfig(
        dicom_store_path='projects/my-proj/locations/my-location/datasets/my-ds-dataset/dicomStores/my-dicom-store',
        dicom_store_bigquery_table='my-proj.my_ds_dataset.my-ds-table',
        set_storage_class_max_num_instances=2,
        set_storage_class_timeout_min=60,
    ),
    logs_config=ilm_config.DataAccessLogsConfiguration(
        logs_bigquery_table=(
            'my-proj.ilm_dataset.cloudaudit_googleapis_com_data_access_*'
        ),
        log_entries_date_equal_or_after='20231120',
    ),
    instances_disallow_list=frozenset([INSTANCE_DISALLOW_LIST]),
    storage_class_config=ilm_config.StorageClassConfig(
        move_rules=[
            ilm_config.MoveRule(
                from_storage_class=ilm_config.StorageClass.STANDARD,
                to_storage_class=ilm_config.StorageClass.ARCHIVE,
                downgrade_conditions=[
                    ilm_config.ToLowerAvailabilityCondition(modality='MR'),
                    ilm_config.ToLowerAvailabilityCondition(
                        sop_class_uid='1.2.300'
                    ),
                ],
            ),
            ilm_config.MoveRule(
                from_storage_class=ilm_config.StorageClass.ARCHIVE,
                to_storage_class=ilm_config.StorageClass.STANDARD,
                upgrade_conditions=[
                    ilm_config.ToLowerAvailabilityCondition(modality='CR'),
                ],
            ),
        ],
    ),
    tmp_gcs_uri='gs://my-proj-bucket/tmp/',
    report_config=ilm_config.ReportConfiguration(
        summarized_results_report_gcs_uri='gs://my-proj-bucket/results-{}.txt',
    ),
)

ACCESS_METADATA = ilm_types.AccessMetadata([
    ilm_config.AccessCount(count=1.0, num_days=2),
    ilm_config.AccessCount(count=3.0, num_days=4),
    ilm_config.AccessCount(count=5.0, num_days=6),
    ilm_config.AccessCount(count=7.0, num_days=8),
])
INSTANCE_METADATA_ARCHIVE = ilm_types.InstanceMetadata(
    instance=INSTANCE_0,
    modality='MR',
    num_frames=10,
    pixel_spacing=None,
    sop_class_uid='1.2.300',
    acquisition_date=datetime.date(year=1899, month=12, day=28),
    content_date=datetime.date(year=1899, month=12, day=29),
    series_date=datetime.date(year=1899, month=12, day=30),
    study_date=datetime.date(year=1899, month=12, day=31),
    image_type=None,
    size_bytes=2048,
    storage_class=ilm_config.StorageClass.ARCHIVE,
    access_metadata=ACCESS_METADATA,
    num_days_in_current_storage_class=20,
)
INSTANCE_METADATA_STANDARD = dataclasses.replace(
    INSTANCE_METADATA_ARCHIVE, storage_class=ilm_config.StorageClass.STANDARD
)
