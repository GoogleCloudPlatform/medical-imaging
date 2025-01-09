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

licenses(["notice"])

py_library(
    name = "dicom_iod_uid_xml_parser",
    srcs = ["dicom_iod_uid_xml_parser.py"],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec/util/lib/util:dicom_abstract_xml_parser",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec/util/lib/util:dicom_xml_core_parser",
    ],
)

py_library(
    name = "dicom_iod_xml_parser",
    srcs = ["dicom_iod_xml_parser.py"],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec/util/lib/util:dicom_abstract_xml_parser",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec/util/lib/util:dicom_iod_generator_exception",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec/util/lib/util:dicom_xml_core_parser",
    ],
)

py_library(
    name = "dicom_tag_xml_parser",
    srcs = ["dicom_tag_xml_parser.py"],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec/util/lib/util:dicom_abstract_xml_parser",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec/util/lib/util:dicom_iod_generator_exception",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec/util/lib/util:dicom_xml_core_parser",
    ],
)
