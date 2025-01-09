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

package(default_visibility = ["//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec/util:__subpackages__"])

licenses(["unencumbered"])

py_library(
    name = "dicom_abstract_xml_parser",
    srcs = ["dicom_abstract_xml_parser.py"],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":dicom_iod_generator_exception",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util:dicom_standard_util",
    ],
)

py_library(
    name = "dicom_iod_generator_exception",
    srcs = ["dicom_iod_generator_exception.py"],
    srcs_version = "PY3",
)

py_library(
    name = "dicom_xml_core_parser",
    srcs = ["dicom_xml_core_parser.py"],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        ":dicom_iod_generator_exception",
    ],
)
