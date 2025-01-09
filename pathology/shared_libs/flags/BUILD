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
    name = "flag_utils",
    srcs = [
        "flag_utils.py",
    ],
    srcs_version = "PY3",
)

py_library(
    name = "secret_flag_utils",
    srcs = [
        "secret_flag_utils.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":flag_utils",
        requirement("cachetools"),
        requirement("google-api-core"),
        requirement("google-cloud-secret-manager"),
    ],
)

py_test(
    name = "flag_utils_test",
    srcs = [
        "flag_utils_test.py",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":flag_utils",
    ],
)

py_test(
    name = "secret_flag_utils_test",
    srcs = [
        "secret_flag_utils_test.py",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":secret_flag_utils",
        requirement("google-api-core"),
        requirement("google-cloud-secret-manager"),
    ],
)
