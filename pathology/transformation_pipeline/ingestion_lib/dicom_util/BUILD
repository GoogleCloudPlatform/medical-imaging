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

package(
    default_applicable_licenses = ["//pathology:license"],
    default_visibility = ["//pathology:__subpackages__"],
)

licenses(["notice"])

py_library(
    name = "dicom_standard_util",
    srcs = ["dicom_standard_util.py"],
    data = ["//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec:all_files"],
    srcs_version = "PY3",
)

py_library(
    name = "dicom_iod_util",
    srcs = ["dicom_iod_util.py"],
    srcs_version = "PY3",
    deps = [":dicom_standard_util"],
)

py_library(
    name = "dicom_standard",
    srcs = ["dicom_standard.py"],
    srcs_version = "PY3",
    deps = [
        ":dicom_iod_util",
        ":dicom_standard_util",
    ],
)

py_library(
    name = "dicomtag",
    srcs = ["dicomtag.py"],
    srcs_version = "PY3",
    deps = [
        ":dicom_standard",
    ],
)

py_test(
    name = "dicomtag_test",
    srcs = ["dicomtag_test.py"],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":dicomtag",
    ],
)

py_test(
    name = "dicom_standard_util_test",
    srcs = ["dicom_standard_util_test.py"],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":dicom_standard_util",
    ],
)

py_library(
    name = "pydicom_util",
    srcs = ["pydicom_util.py"],
    srcs_version = "PY3",
    deps = [
        ":dicom_iod_util",
        ":dicom_standard",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        requirement("pydicom"),
    ],
)

py_test(
    name = "pydicom_util_test",
    srcs = ["pydicom_util_test.py"],
    deps = [
        requirement("absl-py"),
        ":dicom_iod_util",
        ":dicom_standard_util",
        ":pydicom_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        requirement("pydicom"),
    ],
)

py_library(
    name = "dicom_test_util",
    srcs = ["dicom_test_util.py"],
    srcs_version = "PY3",
    deps = [
        ":dicom_standard",
        "//pathology/shared_libs/pydicom_version_util",
        "//pathology/transformation_pipeline/ingestion_lib:ingest_const",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_gen:wsi_dicom_file_ref",
        requirement("pydicom"),
    ],
)

py_test(
    name = "dicom_test_util_test",
    srcs = ["dicom_test_util_test.py"],
    deps = [
        requirement("absl-py"),
        ":dicom_test_util",
        requirement("pydicom"),
    ],
)

py_test(
    name = "dicom_iod_util_test",
    srcs = ["dicom_iod_util_test.py"],
    deps = [
        requirement("absl-py"),
        ":dicom_iod_util",
        ":dicom_standard_util",
    ],
)
