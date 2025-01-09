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

py_binary(
    name = "dataflow_main",
    srcs = ["dataflow_main.py"],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline:gke_main_lib",
        "//pathology/transformation_pipeline:ingest_flags",
        requirement("apache-beam"),
    ],
)

py_library(
    name = "dataflow_main_lib",
    srcs = ["dataflow_main.py"],
    deps = [
        requirement("absl-py"),
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/transformation_pipeline:gke_main_lib",
        "//pathology/transformation_pipeline:ingest_flags",
        requirement("apache-beam"),
    ],
)

py_test(
    name = "dataflow_main_test",
    srcs = ["dataflow_main_test.py"],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":dataflow_main_lib",
        "//pathology/transformation_pipeline:gke_main_lib",
        requirement("apache-beam"),
    ],
)
