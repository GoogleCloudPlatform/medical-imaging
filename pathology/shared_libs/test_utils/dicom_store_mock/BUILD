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
        "//ilm:__subpackages__",
        "//pathology:__subpackages__",
    ],
)

py_library(
    name = "dicom_store_mock",
    srcs = [
        "dicom_store_mock.py",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":dicom_store_mock_types",
        ":dicom_uid_value_map",
        requirement("cachetools"),
        requirement("google-auth"),
        requirement("google-auth-httplib2"),
        requirement("httplib2"),
        requirement("pydicom"),
        requirement("requests"),
        requirement("requests_mock"),
        requirement("requests-toolbelt"),
    ],
)

py_library(
    name = "dicom_store_mock_types",
    srcs = [
        "dicom_store_mock_types.py",
    ],
    srcs_version = "PY3",
)

filegroup(
    name = "testdata",
    srcs = glob(
        ["testdata/*.*"],
    ),
)

py_test(
    name = "dicom_store_mock_test",
    srcs = [
        "dicom_store_mock_test.py",
    ],
    data = [
        ":testdata",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":dicom_store_mock",
        ":dicom_store_mock_types",
        requirement("google-auth-httplib2"),
        requirement("pydicom"),
        requirement("requests"),
        requirement("requests-toolbelt"),
    ],
)

py_library(
    name = "dicom_uid_value_map",
    srcs = [
        "dicom_uid_value_map.py",
    ],
    srcs_version = "PY3",
)

py_test(
    name = "dicom_uid_value_map_test",
    srcs = [
        "dicom_uid_value_map_test.py",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":dicom_uid_value_map",
    ],
)
