# Copyright 2024 Google LLC
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

load("@pathology_cloud_deps//:requirements.bzl", "requirement")

licenses(["notice"])

package(
    default_applicable_licenses = ["//pathology:license"],
    default_visibility = ["//ilm:__subpackages__"],
)

py_library(
    name = "dicom_store_lib",
    srcs = ["dicom_store_lib.py"],
    deps = [
        ":pipeline_util",
        "//ilm:ilm_config",
        "//ilm:ilm_types",
        requirement("apache-beam"),
        requirement("google-auth"),
        requirement("google-cloud-bigquery"),
        requirement("google-cloud-storage"),
        requirement("requests"),
    ],
)

py_test(
    name = "dicom_store_lib_test",
    srcs = ["dicom_store_lib_test.py"],
    deps = [
        requirement("absl-py"),
        ":dicom_store_lib",
        ":pipeline_util",
        "//ilm:ilm_config",
        "//ilm:ilm_types",
        "//ilm/test_utils:test_const",
        requirement("google-cloud-bigquery"),
    ],
)

py_library(
    name = "logs_lib",
    srcs = ["logs_lib.py"],
    deps = [
        ":dicom_store_lib",
        ":pipeline_util",
        "//ilm:ilm_config",
        "//ilm:ilm_types",
        requirement("apache-beam"),
    ],
)

py_library(
    name = "heuristics",
    srcs = ["heuristics.py"],
    deps = [
        "//ilm:ilm_config",
        "//ilm:ilm_types",
        requirement("apache-beam"),
    ],
)

py_test(
    name = "heuristics_test",
    srcs = ["heuristics_test.py"],
    deps = [
        requirement("absl-py"),
        ":heuristics",
        "//ilm:ilm_config",
        "//ilm:ilm_types",
        "//ilm/test_utils:test_const",
    ],
)

py_test(
    name = "logs_lib_test",
    srcs = ["logs_lib_test.py"],
    deps = [
        requirement("absl-py"),
        ":dicom_store_lib",
        ":logs_lib",
        ":pipeline_util",
        "//ilm:ilm_config",
        "//ilm:ilm_types",
        "//ilm/test_utils:test_const",
    ],
)

py_library(
    name = "pipeline_util",
    srcs = ["pipeline_util.py"],
    deps = [
        "//ilm:ilm_config",
        "//ilm:ilm_types",
        requirement("google-cloud-storage"),
    ],
)

py_test(
    name = "pipeline_util_test",
    srcs = ["pipeline_util_test.py"],
    data = [
        "//ilm:testdata_files",
    ],
    deps = [
        requirement("absl-py"),
        ":pipeline_util",
        "//ilm:ilm_config",
        "//ilm:ilm_types",
        "//ilm/test_utils:test_const",
        "//pathology/shared_libs/test_utils/gcs_mock",
    ],
)
