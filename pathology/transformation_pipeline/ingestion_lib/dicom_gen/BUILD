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
    default_visibility = [
        "//pathology/dicom_proxy:__subpackages__",
        "//pathology/transformation_pipeline:__subpackages__",
    ],
)

py_library(
    name = "abstract_dicom_generation",
    srcs = [
        "abstract_dicom_generation.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":dicom_private_tag_generator",
        ":dicom_store_client",
        ":uid_generator",
        ":wsi_dicom_file_ref",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:abstract_polling_client",
        "//pathology/transformation_pipeline/ingestion_lib:abstract_pubsub_message_handler",
        "//pathology/transformation_pipeline/ingestion_lib:cloud_storage_client",
        "//pathology/transformation_pipeline/ingestion_lib:hash_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib:redis_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:dicom_util",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:abstract_pubsub_msg",
        requirement("google-cloud-pubsub"),
        requirement("requests"),
    ],
)

py_library(
    name = "dicom_general_equipment",
    srcs = [
        "dicom_general_equipment.py",
    ],
    srcs_version = "PY3",
    deps = [
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:pydicom_util",
        requirement("pydicom"),
    ],
)

py_test(
    name = "dicom_general_equipment_test",
    srcs = [
        "dicom_general_equipment_test.py",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":dicom_general_equipment",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("pydicom"),
    ],
)

py_library(
    name = "dicom_schema_util",
    srcs = [
        "dicom_schema_util.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":uid_generator",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_standard",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicomtag",
        requirement("pandas"),
    ],
)

py_library(
    name = "dicom_json_util",
    srcs = [
        "dicom_json_util.py",
    ],
    srcs_version = "PY3",
    deps = [
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        requirement("pydicom"),
    ],
)

py_library(
    name = "dicom_secondary_capture",
    srcs = ["dicom_secondary_capture.py"],
    srcs_version = "PY3",
    deps = [
        ":dicom_private_tag_generator",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        requirement("pillow"),
        requirement("opencv-python-headless"),
        requirement("numpy"),
        requirement("pydicom"),
    ],
)

py_test(
    name = "dicom_secondary_capture_test",
    srcs = [
        "dicom_secondary_capture_test.py",
    ],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        ":dicom_private_tag_generator",
        ":dicom_secondary_capture",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        requirement("pillow"),
        requirement("absl-py"),
        requirement("numpy"),
        requirement("pydicom"),
    ],
)

py_library(
    name = "dicom_slide_coordinates_microscopic_image",
    srcs = ["dicom_slide_coordinates_microscopic_image.py"],
    srcs_version = "PY3",
    deps = [
        ":dicom_json_util",
        ":uid_generator",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:ancillary_image_extractor",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:dicom_util",
        requirement("pillow"),
        requirement("pydicom"),
        requirement("tifffile"),
    ],
)

py_test(
    name = "dicom_slide_coordinates_microscopic_image_test",
    srcs = [
        "dicom_slide_coordinates_microscopic_image_test.py",
    ],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        ":dicom_slide_coordinates_microscopic_image",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:ancillary_image_extractor",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:dicom_util",
        requirement("pillow"),
        requirement("absl-py"),
        requirement("numpy"),
        requirement("pydicom"),
    ],
)

py_library(
    name = "uid_generator",
    srcs = [
        "uid_generator.py",
    ],
    srcs_version = "PY3",
    deps = [
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
    ],
)

py_test(
    name = "uid_generator_test",
    srcs = [
        "uid_generator_test.py",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":uid_generator",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
    ],
)

py_test(
    name = "dicom_schema_util_test",
    srcs = ["dicom_schema_util_test.py"],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":dicom_schema_util",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:csv_util",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
    ],
)

py_test(
    name = "dicom_json_util_test",
    srcs = ["dicom_json_util_test.py"],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":dicom_json_util",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_test_util",
        requirement("pydicom"),
    ],
)

py_test(
    name = "abstract_dicom_generation_test",
    srcs = ["abstract_dicom_generation_test.py"],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":abstract_dicom_generation",
        ":uid_generator",
        ":wsi_dicom_file_ref",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/transformation_pipeline/ingestion_lib:abstract_polling_client",
        "//pathology/transformation_pipeline/ingestion_lib:cloud_storage_client",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib:redis_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen/wsi_to_dicom:dicom_util",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_test_util",
        requirement("pydicom"),
    ],
)

py_library(
    name = "dicom_file_ref",
    srcs = [
        "dicom_file_ref.py",
    ],
    srcs_version = "PY3",
    deps = [
        "//pathology/shared_libs/test_utils/dicom_store_mock:dicom_store_mock_types",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_standard",
        requirement("pydicom"),
    ],
)

py_test(
    name = "dicom_file_ref_test",
    srcs = [
        "dicom_file_ref_test.py",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":dicom_file_ref",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/shared_libs/test_utils/dicom_store_mock:dicom_store_mock_types",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_test_util",
        requirement("pydicom"),
    ],
)

py_library(
    name = "dicom_store_client",
    srcs = [
        "dicom_store_client.py",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("google-auth"),
        ":dicom_json_util",
        ":wsi_dicom_file_ref",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:cloud_storage_client",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        requirement("pydicom"),
        requirement("requests"),
    ],
)

py_test(
    name = "dicom_store_client_test",
    srcs = [
        "dicom_store_client_test.py",
    ],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":dicom_store_client",
        ":wsi_dicom_file_ref",
        "//pathology/shared_libs/flags:flag_utils",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_test_util",
        requirement("pydicom"),
        requirement("requests"),
        requirement("requests_mock"),
    ],
)

py_library(
    name = "wsi_dicom_file_ref",
    srcs = [
        "wsi_dicom_file_ref.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":dicom_file_ref",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        requirement("pydicom"),
    ],
)

py_test(
    name = "wsi_dicom_file_ref_test",
    srcs = [
        "wsi_dicom_file_ref_test.py",
    ],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":wsi_dicom_file_ref",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        requirement("pydicom"),
    ],
)

py_library(
    name = "dicom_private_tag_generator",
    srcs = [
        "dicom_private_tag_generator.py",
    ],
    srcs_version = "PY3",
    deps = [
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        requirement("pydicom"),
    ],
)

py_test(
    name = "dicom_private_tag_generator_test",
    srcs = [
        "dicom_private_tag_generator_test.py",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":dicom_private_tag_generator",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_standard",
        requirement("pydicom"),
    ],
)

py_library(
    name = "ingestion_dicom_store_urls",
    srcs = [
        "ingestion_dicom_store_urls.py",
    ],
    srcs_version = "PY3",
    deps = [
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
    ],
)

py_test(
    name = "ingestion_dicom_store_urls_test",
    srcs = [
        "ingestion_dicom_store_urls_test.py",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":ingestion_dicom_store_urls",
    ],
)
