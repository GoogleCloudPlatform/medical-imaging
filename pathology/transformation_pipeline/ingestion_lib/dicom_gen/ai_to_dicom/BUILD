load("@pathology_cloud_deps//:requirements.bzl", "requirement")

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

licenses(["notice"])

package(
    default_applicable_licenses = ["//pathology:license"],
    default_visibility = ["//pathology/transformation_pipeline:__subpackages__"],
)

py_library(
    name = "png_to_dicom",
    srcs = [
        "png_to_dicom.py",
    ],
    srcs_version = "PY3",
    deps = [
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:abstract_polling_client",
        "//pathology/transformation_pipeline/ingestion_lib:cloud_storage_client",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_general_equipment",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_private_tag_generator",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_secondary_capture",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:ingestion_dicom_store_urls",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:abstract_pubsub_msg",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:inference_pubsub_msg",
        requirement("google-cloud-pubsub"),
        requirement("pydicom"),
    ],
)

py_test(
    name = "png_to_dicom_test",
    srcs = [
        "png_to_dicom_test.py",
    ],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":png_to_dicom",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        "//pathology/transformation_pipeline/ingestion_lib:cloud_storage_client",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib:mock_redis_client",
        "//pathology/transformation_pipeline/ingestion_lib:polling_client",
        "//pathology/transformation_pipeline/ingestion_lib:redis_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
        requirement("google-cloud-pubsub"),
        requirement("pydicom"),
    ],
)
