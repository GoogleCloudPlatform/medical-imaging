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

default_visibility = [
    "//pathology/transformation_pipeline:__subpackages__",
    "//pathology/ml/inference_pipeline:__subpackages__",
]

package(
    default_applicable_licenses = ["//pathology:license"],
    default_visibility = default_visibility,
)

py_library(
    name = "inference_pubsub_message",
    srcs = ["inference_pubsub_message.py"],
    visibility = default_visibility + [
        "//pathology/transformation_pipeline/ingestion_lib/pubsub_msgs:__subpackages__",
    ],
    deps = [
        requirement("dataclasses_json"),
    ],
)
