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

filegroup(
    name = "all_files",
    srcs = glob(["**"]),
)

py_library(
    name = "cloud_spanner_client",
    srcs = ["cloud_spanner_client.py"],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        requirement("google-cloud-spanner"),
        "//pathology/shared_libs/flags:secret_flag_utils",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
    ],
)
