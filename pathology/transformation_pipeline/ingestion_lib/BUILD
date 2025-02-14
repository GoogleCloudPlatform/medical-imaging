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

visibility = ["//pathology/transformation_pipeline:__subpackages__"]

package(
    default_applicable_licenses = ["//pathology:license"],
    default_visibility = visibility,
)

py_library(
    name = "python_lib",
    srcs = [
        "__init__.py",
    ],
    srcs_version = "PY3",
)

py_test(
    name = "cloud_storage_client_test",
    srcs = [
        "cloud_storage_client_test.py",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":cloud_storage_client",
        requirement("google-api-core"),
        requirement("google-cloud-storage"),
    ],
)

py_library(
    name = "ingest_const",
    srcs = [
        "ingest_const.py",
    ],
    srcs_version = "PY3",
    visibility = visibility + ["//pathology:__subpackages__"],
)

py_library(
    name = "ack_timeout_monitor",
    srcs = [
        "ack_timeout_monitor.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":ingest_const",
        ":redis_client",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:abstract_pubsub_msg",
        requirement("google-cloud-pubsub"),
    ],
)

py_test(
    name = "ack_timeout_monitor_test",
    srcs = [
        "ack_timeout_monitor_test.py",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":ack_timeout_monitor",
        ":ingest_const",
        ":mock_redis_client",
        ":redis_client",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:cloud_storage_pubsub_msg",
        requirement("google-cloud-pubsub"),
    ],
)

py_library(
    name = "cloud_storage_client",
    srcs = [
        "cloud_storage_client.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":hash_util",
        ":ingest_const",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("google-api-core"),
        requirement("google-cloud-storage"),
    ],
)

py_library(
    name = "abstract_polling_client",
    srcs = [
        "abstract_polling_client.py",
    ],
    deps = [
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:abstract_pubsub_msg",
    ],
)

py_library(
    name = "abstract_pubsub_message_handler",
    srcs = [
        "abstract_pubsub_message_handler.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":abstract_polling_client",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:abstract_pubsub_msg",
        requirement("google-cloud-pubsub"),
    ],
)

py_library(
    name = "polling_client",
    srcs = [
        "polling_client.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":abstract_polling_client",
        ":abstract_pubsub_message_handler",
        ":ack_timeout_monitor",
        ":cloud_storage_client",
        ":ingest_const",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:abstract_pubsub_msg",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:gcs_file_msg",
        requirement("google-api-core"),
        requirement("google-cloud-pubsub"),
    ],
)

py_test(
    name = "polling_client_test",
    srcs = [
        "polling_client_test.py",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":ack_timeout_monitor",
        ":polling_client",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:ingest_gcs_handler",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:metadata_storage_client",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:cloud_storage_pubsub_msg",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:gcs_file_msg",
        requirement("google-cloud-pubsub"),
    ],
)

py_library(
    name = "redis_client",
    srcs = [
        "redis_client.py",
    ],
    srcs_version = "PY3",
    deps = [
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        requirement("redis"),
    ],
)

py_library(
    name = "csv_util",
    srcs = [
        "csv_util.py",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("pandas"),
    ],
)

py_library(
    name = "hash_util",
    srcs = [
        "hash_util.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":ingest_const",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
    ],
)

py_test(
    name = "hash_util_test",
    srcs = [
        "hash_util_test.py",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":hash_util",
    ],
)

py_test(
    name = "csv_util_test",
    srcs = [
        "csv_util_test.py",
    ],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":csv_util",
        ":gen_test_util",
        requirement("pandas"),
    ],
)

py_test(
    name = "redis_client_test",
    srcs = [
        "redis_client_test.py",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":mock_redis_client",
        ":redis_client",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("redis"),
    ],
)

py_library(
    name = "mock_redis_client",
    srcs = [
        "mock_redis_client.py",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":redis_client",
        requirement("redis"),
    ],
)

py_library(
    name = "gen_test_util",
    srcs = [
        "gen_test_util.py",
    ],
    deps = [
        ":ingest_const",
        "//pathology/shared_libs/pydicom_version_util",
        requirement("pillow"),
        requirement("numpy"),
        requirement("openslide-python"),
        requirement("pydicom"),
    ],
)

py_library(
    name = "pubsub_util",
    srcs = [
        "pubsub_util.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":ingest_const",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("google-api-core"),
        requirement("google-cloud-pubsub"),
    ],
)

py_test(
    name = "pubsub_util_test",
    srcs = [
        "pubsub_util_test.py",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":pubsub_util",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("google-api-core"),
        requirement("google-auth"),
        requirement("google-cloud-pubsub"),
    ],
)
