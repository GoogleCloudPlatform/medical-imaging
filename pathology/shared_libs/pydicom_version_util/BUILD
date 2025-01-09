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

py_library(
    name = "pydicom_version_util",
    srcs = [
        "pydicom_version_util.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":pydicom_v2_version_util",  # buildcleaner: keep
        ":pydicom_v3_version_util",  # buildcleaner: keep
        requirement("pydicom"),
    ],
)

py_library(
    name = "pydicom_v2_version_util",
    srcs = [
        "pydicom_v2_version_util.py",
    ],
    srcs_version = "PY3",
    visibility = ["//visibility:private"],
    deps = [
        requirement("pydicom"),
    ],
)

py_library(
    name = "pydicom_v3_version_util",
    srcs = [
        "pydicom_v3_version_util.py",
    ],
    srcs_version = "PY3",
    visibility = ["//visibility:private"],
    deps = [
        requirement("pydicom"),
    ],
)

filegroup(
    name = "testdata",
    srcs = glob(["testdata/**"]),
)

py_test(
    name = "pydicom_version_util_test",
    srcs = [
        "pydicom_version_util_test.py",
    ],
    data = [":testdata"],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":pydicom_version_util",
        requirement("pydicom"),
    ],
)
