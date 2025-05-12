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

package(default_visibility = ["//pathology/shared_libs/test_utils/gcs_mock:__subpackages__"])

py_library(
    name = "client_mock",
    srcs = ["client_mock.py"],
    deps = [
        ":gcs_mock_types",
        ":gcs_state_mock",
        requirement("google-api-core"),
        requirement("google-auth"),
        requirement("google-cloud-storage"),
        requirement("requests"),
    ],
)

py_library(
    name = "bucket_mock",
    srcs = ["bucket_mock.py"],
    deps = [
        ":client_mock",
        ":gcs_mock_types",
        ":gcs_mock_utils",
        requirement("google-api-core"),
        requirement("google-cloud-storage"),
    ],
)

py_library(
    name = "blob_mock",
    srcs = ["blob_mock.py"],
    deps = [
        ":blob_state_mock",
        ":client_mock",
        ":gcs_mock_constants",
        ":gcs_mock_types",
        ":gcs_mock_utils",
        requirement("google-cloud-storage"),
    ],
)

py_library(
    name = "gcs_state_mock",
    srcs = ["gcs_state_mock.py"],
    deps = [
        ":blob_state_mock",
        ":gcs_mock_constants",
        ":gcs_mock_types",
        ":gcs_mock_utils",
        requirement("google-api-core"),
        requirement("google-cloud-storage"),
    ],
)

py_library(
    name = "gcs_mock_constants",
    srcs = ["gcs_mock_constants.py"],
)

py_library(
    name = "gcs_mock_types",
    srcs = ["gcs_mock_types.py"],
    deps = [
        requirement("google-api-core"),
        requirement("google-cloud-storage"),
    ],
)

py_library(
    name = "blob_state_mock",
    srcs = ["blob_state_mock.py"],
)

py_test(
    name = "blob_mock_test",
    srcs = ["blob_mock_test.py"],
    deps = [
        requirement("absl-py"),
        ":blob_mock",
        ":blob_state_mock",
        ":bucket_mock",
        ":gcs_mock_constants",
        ":gcs_mock_types",
        ":gcs_test_utils",
        "//pathology/shared_libs/test_utils/gcs_mock",
        requirement("google-cloud-storage"),
    ],
)

py_test(
    name = "bucket_mock_test",
    srcs = ["bucket_mock_test.py"],
    deps = [
        requirement("absl-py"),
        ":bucket_mock",
        ":gcs_mock_types",
        ":gcs_test_utils",
        "//pathology/shared_libs/test_utils/gcs_mock",
        requirement("google-api-core"),
        requirement("google-cloud-storage"),
    ],
)

py_test(
    name = "client_mock_test",
    srcs = ["client_mock_test.py"],
    deps = [
        requirement("absl-py"),
        ":client_mock",
        ":gcs_mock_types",
        ":gcs_state_mock",
        ":gcs_test_utils",
        "//pathology/shared_libs/test_utils/gcs_mock",
        requirement("google-api-core"),
        requirement("google-cloud-storage"),
    ],
)

py_test(
    name = "gcs_state_mock_test",
    srcs = ["gcs_state_mock_test.py"],
    deps = [
        requirement("absl-py"),
        ":gcs_mock_types",
        ":gcs_state_mock",
        ":gcs_test_utils",
        "//pathology/shared_libs/test_utils/gcs_mock",
        requirement("google-api-core"),
        requirement("google-cloud-storage"),
    ],
)

py_test(
    name = "blob_state_mock_test",
    srcs = ["blob_state_mock_test.py"],
    deps = [
        requirement("absl-py"),
        ":blob_state_mock",
    ],
)

py_library(
    name = "gcs_test_utils",
    srcs = ["gcs_test_utils.py"],
    deps = [
        requirement("absl-py"),
    ],
)

py_library(
    name = "gcs_mock_utils",
    srcs = ["gcs_mock_utils.py"],
    deps = [
        ":gcs_mock_constants",
        ":gcs_mock_types",
    ],
)

py_test(
    name = "gcs_mock_utils_test",
    srcs = ["gcs_mock_utils_test.py"],
    deps = [
        requirement("absl-py"),
        ":gcs_mock_constants",
        ":gcs_mock_utils",
        requirement("google-cloud-storage"),
    ],
)

py_library(
    name = "transfer_manager_mock",
    srcs = ["transfer_manager_mock.py"],
    deps = [
        requirement("google-cloud-storage"),
    ],
)

py_test(
    name = "transfer_manager_mock_test",
    srcs = ["transfer_manager_mock_test.py"],
    deps = [
        requirement("absl-py"),
        "//pathology/shared_libs/test_utils/gcs_mock",
        requirement("google-cloud-storage"),
    ],
)
