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

filegroup(
    name = "testdata_files",
    srcs = glob(["testdata/**"]),
)

py_binary(
    name = "batch_pipeline_main",
    srcs = ["batch_pipeline_main.py"],
    deps = [
        "//ilm/ilm_lib:dicom_store_lib",
        "//ilm/ilm_lib:heuristics",
        "//ilm/ilm_lib:logs_lib",
        "//ilm/ilm_lib:pipeline_util",
        requirement("apache-beam"),
    ],
)

py_library(
    name = "ilm_config",
    srcs = ["ilm_config.py"],
    deps = [requirement("dataclasses_json")],
)

py_test(
    name = "ilm_config_test",
    srcs = ["ilm_config_test.py"],
    deps = [
        requirement("absl-py"),
        ":ilm_config",
    ],
)

py_library(
    name = "ilm_types",
    srcs = ["ilm_types.py"],
    deps = [
        ":ilm_config",
        requirement("dataclasses_json"),
    ],
)

py_test(
    name = "ilm_types_test",
    srcs = ["ilm_types_test.py"],
    deps = [
        requirement("absl-py"),
        ":ilm_config",
        ":ilm_types",
    ],
)
