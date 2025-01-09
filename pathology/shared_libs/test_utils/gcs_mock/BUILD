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
    name = "gcs_mock",
    srcs = ["gcs_mock.py"],
    deps = [
        requirement("absl-py"),
        "//pathology/shared_libs/test_utils/gcs_mock/gcs_mock_lib:blob_mock",
        "//pathology/shared_libs/test_utils/gcs_mock/gcs_mock_lib:bucket_mock",
        "//pathology/shared_libs/test_utils/gcs_mock/gcs_mock_lib:client_mock",
        "//pathology/shared_libs/test_utils/gcs_mock/gcs_mock_lib:gcs_mock_types",
        "//pathology/shared_libs/test_utils/gcs_mock/gcs_mock_lib:gcs_state_mock",
        requirement("google-cloud-storage"),
    ],
)

py_test(
    name = "gcs_mock_test",
    srcs = ["gcs_mock_test.py"],
    deps = [
        requirement("absl-py"),
        ":gcs_mock",
        requirement("google-cloud-storage"),
    ],
)

py_library(
    name = "gcs_pubsub_mock",
    srcs = [
        "gcs_pubsub_mock.py",
    ],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("google-api-core"),
        requirement("google-cloud-pubsub"),
    ],
)

py_test(
    name = "gcs_pubsub_mock_test",
    srcs = ["gcs_pubsub_mock_test.py"],
    deps = [
        requirement("absl-py"),
        ":gcs_pubsub_mock",
        requirement("google-cloud-pubsub"),
    ],
)
