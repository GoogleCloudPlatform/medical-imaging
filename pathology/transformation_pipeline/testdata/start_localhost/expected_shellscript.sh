# Copyright 2024 Google LLC
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

docker rm localhost_transform_pipeline
docker run -it --init -e GOOGLE_APPLICATION_CREDENTIALS=/.config/gcloud/application_default_credentials.json -e GOOGLE_CLOUD_PROJECT=fake_gcp_project --mount type=bind,source=fake.json,target=/.config/gcloud/application_default_credentials.json,readonly -e LOCAL_UID=123 -e LOCAL_GID=456 -e POLL_IMAGE_INGESTION_DIR=False -e ENABLE_CLOUD_OPS_LOGGING=False -e INGESTION_PYRAMID_LAYER_GENERATION_CONFIG_PATH= -e ENABLE_METADATA_FREE_INGESTION=True -e ENABLE_CREATE_MISSING_STUDY_INSTANCE_UID=True -e GCS_INGEST_STUDY_INSTANCE_UID_SOURCE=DICOM -e TEST_WHOLE_FILENAME_AS_METADATA_PRIMARY_KEY=True -e INCLUDE_UPLOAD_BUCKET_PATH_IN_WHOLE_FILENAME_SLIDEID=False -e FILENAME_METADATA_PRIMARY_KEY_SPLIT_STR=_ -e METADATA_PRIMARY_KEY_REGEX=^[a-zA-Z0-9]+-[a-zA-Z0-9]+(-[a-zA-Z0-9]+)+ -e METADATA_PRIMARY_KEY_COLUMN_NAME=Bar Code Value -e ENABLE_BARCODE_DECODER=True -e ENABLE_CLOUD_VISION_BARCODE_SEGMENTATION=False -e BIG_QUERY_METADATA_TABLE= -e METADATA_UID_VALIDATION=LOG_WARNING -e METADATA_TAG_LENGTH_VALIDATION=LOG_WARNING -e REQUIRE_TYPE1_DICOM_TAG_METADATA_IS_DEFINED=False -e WSI2DCM_DICOM_FRAME_HEIGHT=256 -e WSI2DCM_DICOM_FRAME_WIDTH=256 -e WSI2DCM_COMPRESSION=JPEG -e WSI2DCM_JPEG_COMPRESSION_QUALITY=95 -e WSI2DCM_JPEG_COMPRESSION_SUBSAMPLING=SUBSAMPLE_444 -e EMBED_ICC_PROFILE=True -e WSI2DCM_PIXEL_EQUIVALENT_TRANSFORM=HIGHEST_MAGNIFICATION -e DICOM_GUID_PREFIX=1.3.6.1.4.1.11129.5.7 -e INGEST_IGNORE_ROOT_DIR=["cloud-ingest", "storage-transfer"] -e GCS_UPLOAD_IGNORE_FILE_EXT= -e DELETE_FILE_FROM_INGEST_BUCKET_AT_INGEST_SUCCESS_OR_FAILURE=True -e LOCALHOST_DICOM_STORE=/transform/dicom_store --mount type=bind,source=/mock_run/metadata_ingestion,target=/input_metadata --mount type=bind,source=/mock_run/image_ingestion,target=/input_imaging --mount type=bind,source=/mock_run/processed_images,target=/processed_imaging --mount type=bind,source=/transform/dicom_store,target=/mock_dicom_store --name localhost_transform_pipeline localhost
docker rm localhost_transform_pipeline