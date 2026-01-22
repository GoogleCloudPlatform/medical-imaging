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

py_binary(
    name = "synthetic_ingest_data_generator",
    srcs = ["synthetic_ingest_data_generator.py"],
    data = [
        "names.csv",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/transformation_pipeline/ingestion_lib:cloud_storage_client",
        "//pathology/transformation_pipeline/ingestion_lib:csv_util",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:dicom_store_client",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:uid_generator",
        requirement("google-api-core"),
        requirement("google-cloud-bigquery"),
        requirement("google-cloud-storage"),
        requirement("pandas"),
        requirement("pydicom"),
        requirement("requests"),
    ],
)
