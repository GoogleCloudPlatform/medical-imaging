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
    name = "barcode_reader",
    srcs = ["barcode_reader.py"],
    srcs_version = "PY3",
    deps = [
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline:ingest_flags",
        requirement("opencv-python-headless"),
        requirement("google-api-python-client"),
        requirement("numpy"),
    ],
)

py_test(
    name = "barcode_reader_test",
    srcs = ["barcode_reader_test.py"],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":barcode_reader",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        requirement("opencv-python-headless"),
        requirement("google-api-python-client"),
    ],
)

py_library(
    name = "dicom_util",
    srcs = [
        "dicom_util.py",
    ],
    srcs_version = "PY3",
    deps = [
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_general_equipment",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_json_util",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_private_tag_generator",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:wsi_dicom_file_ref",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:pydicom_util",
        requirement("pillow"),
        "//pathology/ez_wsi_dicomweb:dicom_slide",
        requirement("openslide-python"),
        requirement("pydicom"),
    ],
)

py_test(
    name = "dicom_util_test",
    srcs = [
        "dicom_util_test.py",
    ],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":dicom_util",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:wsi_dicom_file_ref",
        requirement("numpy"),
        requirement("openslide-python"),
        requirement("pydicom"),
    ],
)

py_library(
    name = "gcs_storage_util",
    srcs = [
        "gcs_storage_util.py",
    ],
    srcs_version = "PY3",
    deps = [
        "//pathology/transformation_pipeline/ingestion_lib:cloud_storage_client",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:ingestion_complete_pubsub",
    ],
)

py_test(
    name = "gcs_storage_util_test",
    srcs = ["gcs_storage_util_test.py"],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":gcs_storage_util",
        "//pathology/transformation_pipeline/ingestion_lib:cloud_storage_client",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:ingestion_complete_pubsub",
    ],
)

py_library(
    name = "ancillary_dicom_gen",
    srcs = [
        "ancillary_dicom_gen.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":ancillary_image_extractor",
        ":dicom_util",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_private_tag_generator",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_standard",
        requirement("opencv-python-headless"),
        requirement("pydicom"),
    ],
)

py_library(
    name = "ingest_base",
    srcs = [
        "ingest_base.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":decode_slideid",
        ":metadata_storage_client",
        ":wsi_pyramid_gen_config",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_json_util",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_schema_util",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
        requirement("pydicom"),
        requirement("requests"),
    ],
)

py_test(
    name = "ingest_base_test",
    srcs = ["ingest_base_test.py"],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":ingest_base",
        ":metadata_storage_client",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        "//pathology/shared_libs/test_utils/gcs_mock",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:abstract_polling_client",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib:redis_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_json_util",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_private_tag_generator",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
        requirement("pydicom"),
        requirement("requests"),
    ],
)

py_library(
    name = "wsi_pyramid_gen_config",
    srcs = [
        "wsi_pyramid_gen_config.py",
    ],
    srcs_version = "PY3",
    deps = [
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        requirement("openslide-python"),
        requirement("pydicom"),
        requirement("PyYAML"),
    ],
)

py_test(
    name = "wsi_pyramid_gen_config_test",
    srcs = ["wsi_pyramid_gen_config_test.py"],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":wsi_pyramid_gen_config",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        requirement("pydicom"),
    ],
)

py_library(
    name = "ingest_dicom",
    srcs = [
        "ingest_dicom.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":decode_slideid",
        ":dicom_util",
        ":ingest_base",
        ":metadata_storage_client",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_json_util",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_private_tag_generator",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        requirement("pydicom"),
        requirement("requests"),
    ],
)

py_test(
    name = "ingest_dicom_test",
    srcs = [
        "ingest_dicom_test.py",
    ],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":decode_slideid",
        ":ingest_base",
        ":ingest_dicom",
        ":metadata_storage_client",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_test_util",
        requirement("pydicom"),
    ],
)

py_library(
    name = "ingest_wsi_dicom",
    srcs = [
        "ingest_wsi_dicom.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":ancillary_image_extractor",
        ":decode_slideid",
        ":dicom_util",
        ":ingest_base",
        ":ingested_dicom_file_ref",
        ":metadata_storage_client",
        ":validate_ingested_dicom",
        ":wsi_pyramid_gen_config",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:hash_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_file_ref",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_json_util",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_private_tag_generator",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_schema_util",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_standard",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_standard_util",
        requirement("pydicom"),
    ],
)

py_test(
    name = "ingest_wsi_dicom_test",
    srcs = [
        "ingest_wsi_dicom_test.py",
    ],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    srcs_version = "PY3",
    deps = [
        ":dicom_util",
        ":ingest_base",
        ":ingest_gcs_handler",
        ":ingest_wsi_dicom",
        ":ingested_dicom_file_ref",
        ":metadata_storage_client",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        "//pathology/shared_libs/test_utils/gcs_mock",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_file_ref",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_schema_util",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_test_util",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:pydicom_util",
        requirement("pydicom"),
    ],
)

py_library(
    name = "ingest_svs",
    srcs = [
        "ingest_svs.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":ancillary_dicom_gen",
        ":ancillary_image_extractor",
        ":decode_slideid",
        ":dicom_util",
        ":ingest_base",
        ":metadata_storage_client",
        ":wsi_pyramid_gen_config",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_private_tag_generator",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:pydicom_util",
        requirement("pydicom"),
    ],
)

py_library(
    name = "ingest_flat_image",
    srcs = [
        "ingest_flat_image.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":decode_slideid",
        ":dicom_util",
        ":ingest_base",
        ":metadata_storage_client",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_private_tag_generator",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_slide_coordinates_microscopic_image",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
    ],
)

py_test(
    name = "ingest_flat_image_test",
    srcs = [
        "ingest_flat_image_test.py",
    ],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":decode_slideid",
        ":dicom_util",
        ":ingest_base",
        ":ingest_flat_image",
        ":metadata_storage_client",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
        requirement("pydicom"),
    ],
)

py_library(
    name = "ingested_dicom_file_ref",
    srcs = [
        "ingested_dicom_file_ref.py",
    ],
    srcs_version = "PY3",
    deps = [
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_file_ref",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:wsi_dicom_file_ref",
        requirement("pydicom"),
    ],
)

py_test(
    name = "ingested_dicom_file_ref_test",
    srcs = [
        "ingested_dicom_file_ref_test.py",
    ],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":ingested_dicom_file_ref",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        requirement("pydicom"),
    ],
)

py_library(
    name = "metadata_storage_client",
    srcs = [
        "metadata_storage_client.py",
    ],
    srcs_version = "PY3",
    deps = [
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:csv_util",
        "//pathology/transformation_pipeline/ingestion_lib:hash_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_schema_util",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_standard",
        requirement("cachetools"),
        requirement("google-api-core"),
        requirement("google-cloud-bigquery"),
        requirement("google-cloud-storage"),
        requirement("pandas"),
    ],
)

py_library(
    name = "ancillary_image_extractor",
    srcs = [
        "ancillary_image_extractor.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":ingested_dicom_file_ref",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        requirement("pillow"),
        requirement("opencv-python-headless"),
        requirement("numpy"),
        requirement("openslide-python"),
        requirement("pydicom"),
        requirement("tifffile"),
    ],
)

py_test(
    name = "ancillary_image_extractor_test",
    srcs = [
        "ancillary_image_extractor_test.py",
    ],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    srcs_version = "PY3",
    deps = [
        ":ancillary_image_extractor",
        ":ingested_dicom_file_ref",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        requirement("pillow"),
        requirement("absl-py"),
        requirement("opencv-python-headless"),
        requirement("numpy"),
        requirement("openslide-python"),
        requirement("pydicom"),
        requirement("tifffile"),
    ],
)

py_library(
    name = "validate_ingested_dicom",
    srcs = [
        "validate_ingested_dicom.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":ingested_dicom_file_ref",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
        requirement("pydicom"),
    ],
)

py_test(
    name = "validate_ingested_dicom_test",
    srcs = ["validate_ingested_dicom_test.py"],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":ingest_wsi_dicom",
        ":ingested_dicom_file_ref",
        ":validate_ingested_dicom",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_test_util",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:pydicom_util",
        requirement("pydicom"),
    ],
)

py_test(
    name = "metadata_storage_client_test",
    srcs = ["metadata_storage_client_test.py"],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":metadata_storage_client",
        "//pathology/transformation_pipeline/ingestion_lib:csv_util",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_schema_util",
    ],
)

py_test(
    name = "ingest_svs_test",
    srcs = ["ingest_svs_test.py"],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":ancillary_image_extractor",
        ":barcode_reader",
        ":dicom_util",
        ":ingest_base",
        ":ingest_gcs_handler",
        ":ingest_svs",
        ":metadata_storage_client",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        "//pathology/shared_libs/test_utils/gcs_mock",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_general_equipment",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_test_util",
        requirement("pydicom"),
        requirement("requests"),
    ],
)

py_library(
    name = "ingest_gcs_handler",
    srcs = [
        "ingest_gcs_handler.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":gcs_storage_util",
        ":ingest_base",
        ":ingest_dicom",
        ":ingest_flat_image",
        ":ingest_svs",
        ":ingest_wsi_dicom",
        ":metadata_storage_client",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:abstract_polling_client",
        "//pathology/transformation_pipeline/ingestion_lib:cloud_storage_client",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:abstract_pubsub_msg",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:cloud_storage_pubsub_msg",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:ingestion_complete_pubsub",
        requirement("google-cloud-pubsub"),
        requirement("pydicom"),
        requirement("tifffile"),
    ],
)

py_test(
    name = "ingest_gcs_handler_test",
    srcs = ["ingest_gcs_handler_test.py"],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        requirement("google-auth"),
        ":gcs_storage_util",
        ":ingest_base",
        ":ingest_gcs_handler",
        ":metadata_storage_client",
        "//pathology/shared_libs/logging_lib:cloud_logging_client_instance",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        "//pathology/shared_libs/test_utils/gcs_mock",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:cloud_storage_client",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib:polling_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:wsi_dicom_file_ref",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_test_util",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:ingestion_complete_pubsub",
        requirement("google-cloud-pubsub"),
        requirement("pydicom"),
    ],
)

py_test(
    name = "ancillary_dicom_gen_test",
    srcs = ["ancillary_dicom_gen_test.py"],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":ancillary_dicom_gen",
        ":ancillary_image_extractor",
        ":dicom_util",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_general_equipment",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_test_util",
        requirement("pydicom"),
    ],
)

py_library(
    name = "transform_types",
    srcs = [
        "transform_types.py",
    ],
    srcs_version = "PY3",
    visibility = ["//pathology:__subpackages__"],
)

py_library(
    name = "decode_slideid",
    srcs = [
        "decode_slideid.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":ancillary_image_extractor",
        ":barcode_reader",
        ":metadata_storage_client",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
    ],
)

py_test(
    name = "decode_slideid_test",
    srcs = ["decode_slideid_test.py"],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    srcs_version = "PY3",
    deps = [
        ":ancillary_image_extractor",
        ":barcode_reader",
        ":decode_slideid",
        ":metadata_storage_client",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
    ],
)

py_library(
    name = "ingest_dicom_store_handler",
    srcs = [
        "ingest_dicom_store_handler.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":ingest_base",
        ":ingest_dicom",
        ":metadata_storage_client",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:abstract_polling_client",
        "//pathology/transformation_pipeline/ingestion_lib:cloud_storage_client",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:abstract_pubsub_msg",
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:dicom_store_pubsub_msg",
        requirement("google-cloud-pubsub"),
        requirement("requests"),
    ],
)

py_test(
    name = "ingest_dicom_store_handler_test",
    srcs = ["ingest_dicom_store_handler_test.py"],
    data = [
        "//pathology/transformation_pipeline:testdata",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":ingest_base",
        ":ingest_dicom",
        ":ingest_dicom_store_handler",
        ":metadata_storage_client",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        "//pathology/shared_libs/test_utils/gcs_mock",
        "//pathology/transformation_pipeline/ingestion_lib:cloud_storage_client",
        "//pathology/transformation_pipeline/ingestion_lib:gen_test_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib:polling_client",
        "//pathology/transformation_pipeline/ingestion_lib:redis_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:abstract_dicom_generation",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:wsi_dicom_file_ref",
        requirement("google-cloud-pubsub"),
        requirement("google-cloud-storage"),
        requirement("pydicom"),
        requirement("requests"),
    ],
)

py_library(
    name = "ingestion_complete_oof_trigger_pubsub_topic",
    srcs = [
        "ingestion_complete_oof_trigger_pubsub_topic.py",
    ],
    srcs_version = "PY3",
    deps = [
        "//pathology/transformation_pipeline:ingest_flags",
        "//pathology/transformation_pipeline/ingestion_lib:pubsub_util",
    ],
)

py_test(
    name = "ingestion_complete_oof_trigger_pubsub_topic_test",
    srcs = ["ingestion_complete_oof_trigger_pubsub_topic_test.py"],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":ingestion_complete_oof_trigger_pubsub_topic",
        "//pathology/transformation_pipeline/ingestion_lib:pubsub_util",
    ],
)
