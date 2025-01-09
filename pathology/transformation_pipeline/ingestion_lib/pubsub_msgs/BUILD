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

package(default_visibility = ["//pathology/transformation_pipeline:__subpackages__"])

py_library(
    name = "abstract_pubsub_msg",
    srcs = [
        "abstract_pubsub_msg.py",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("google-api-core"),
        requirement("google-cloud-pubsub"),
    ],
)

py_library(
    name = "cloud_storage_pubsub_msg",
    srcs = [
        "cloud_storage_pubsub_msg.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":abstract_pubsub_msg",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("google-cloud-pubsub"),
    ],
)

py_library(
    name = "gcs_file_msg",
    srcs = [
        "gcs_file_msg.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":abstract_pubsub_msg",
        "//pathology/transformation_pipeline/ingestion_lib:cloud_storage_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
        requirement("google-api-core"),
        requirement("google-cloud-pubsub"),
        requirement("google-cloud-storage"),
    ],
)

py_library(
    name = "dicom_store_pubsub_msg",
    srcs = [
        "dicom_store_pubsub_msg.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":abstract_pubsub_msg",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        requirement("google-cloud-pubsub"),
    ],
)

py_test(
    name = "dicom_store_pubsub_msg_test",
    srcs = [
        "dicom_store_pubsub_msg_test.py",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":dicom_store_pubsub_msg",
        requirement("google-cloud-pubsub"),
    ],
)

py_library(
    name = "inference_pubsub_msg",
    srcs = ["inference_pubsub_msg.py"],
    deps = [
        ":abstract_pubsub_msg",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/ml/inference_pipeline:inference_pubsub_message",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_secondary_capture",
        requirement("google-cloud-pubsub"),
    ],
)

py_test(
    name = "inference_pubsub_msg_test",
    srcs = ["inference_pubsub_msg_test.py"],
    deps = [
        requirement("absl-py"),
        ":inference_pubsub_msg",
        "//pathology/shared_libs/ml/inference_pipeline:inference_pubsub_message",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_secondary_capture",
        requirement("google-cloud-pubsub"),
    ],
)

py_library(
    name = "ingestion_complete_pubsub",
    srcs = [
        "ingestion_complete_pubsub.py",
    ],
    srcs_version = "PY3",
    visibility = ["//pathology/transformation_pipeline:__subpackages__"],
    deps = [
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/ml/inference_pipeline:inference_pubsub_message",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:wsi_dicom_file_ref",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:transform_types",
        requirement("google-api-core"),
        requirement("google-cloud-pubsub"),
    ],
)

py_test(
    name = "ingestion_complete_pubsub_test",
    srcs = [
        "ingestion_complete_pubsub_test.py",
    ],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":ingestion_complete_pubsub",
        "//pathology/shared_libs/ml/inference_pipeline:inference_pubsub_message",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_test_util",
    ],
)
