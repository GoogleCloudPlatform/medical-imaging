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
    default_visibility = ["//pathology:__subpackages__"],
)

filegroup(
    name = "testdata",
    srcs = glob(
        [
            "testdata/**",
            "testdata/bucket/*.*",
            "example/metadata_mapping_schemas/*.json",
        ],
    ),
)

py_binary(
    name = "gke_main",
    srcs = ["gke_main.py"],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":ingest_flags",
        "//pathology/shared_libs/build_version",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline/ingestion_lib:abstract_pubsub_message_handler",
        "//pathology/transformation_pipeline/ingestion_lib:polling_client",
        "//pathology/transformation_pipeline/ingestion_lib:redis_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:ingestion_dicom_store_urls",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/ai_to_dicom:png_to_dicom",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:ingest_dicom_store_handler",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:ingest_gcs_handler",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:ingestion_complete_oof_trigger_pubsub_topic",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:metadata_storage_client",
    ],
)

py_library(
    name = "gke_main_lib",
    srcs = ["gke_main.py"],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":ingest_flags",
        "//pathology/shared_libs/build_version",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline/ingestion_lib:abstract_pubsub_message_handler",
        "//pathology/transformation_pipeline/ingestion_lib:polling_client",
        "//pathology/transformation_pipeline/ingestion_lib:redis_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:ingestion_dicom_store_urls",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/ai_to_dicom:png_to_dicom",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:ingest_dicom_store_handler",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:ingest_gcs_handler",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:ingestion_complete_oof_trigger_pubsub_topic",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:metadata_storage_client",
    ],
)

filegroup(
    name = "license",
    srcs =
        [
            "ThirdParty_LICENSE.docker",
        ],
)

py_binary(
    name = "local_main",
    srcs = [
        "local_main.py",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":gke_main_lib",
        "//pathology/shared_libs/flags:flag_utils",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        "//pathology/shared_libs/test_utils/gcs_mock",
        "//pathology/shared_libs/test_utils/gcs_mock:gcs_pubsub_mock",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:metadata_storage_client",
    ],
)

py_library(
    name = "local_main_lib",
    srcs = [
        "local_main.py",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":gke_main_lib",
        "//pathology/shared_libs/flags:flag_utils",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        "//pathology/shared_libs/test_utils/gcs_mock",
        "//pathology/shared_libs/test_utils/gcs_mock:gcs_pubsub_mock",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:metadata_storage_client",
    ],
)

py_test(
    name = "local_main_test",
    srcs = [
        "local_main_test.py",
    ],
    data = [
        ":license",
        ":testdata",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":gke_main_lib",
        ":ingest_flags",
        ":local_main_lib",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:redis_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:dicom_util",
    ],
)

py_test(
    name = "gke_main_test",
    srcs = [
        "gke_main_test.py",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":gke_main_lib",
        ":ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:polling_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
    ],
)

py_library(
    name = "ingest_flags",
    srcs = ["ingest_flags.py"],
    deps = [
        requirement("absl-py"),
        "//pathology/shared_libs/flags:secret_flag_utils",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
    ],
)

py_test(
    name = "ingest_flags_test",
    srcs = ["ingest_flags_test.py"],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":ingest_flags",
    ],
)
