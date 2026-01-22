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

licenses(["unencumbered"])

py_binary(
    name = "dicom_iod_generator",
    srcs = ["dicom_iod_generator.py"],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec/util/lib:dicom_iod_uid_xml_parser",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec/util/lib:dicom_iod_xml_parser",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec/util/lib:dicom_tag_xml_parser",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec/util/lib/util:dicom_iod_generator_exception",
    ],
)

py_test(
    name = "dicom_iod_generator_test",
    size = "small",
    srcs = ["dicom_iod_generator_test.py"],
    data = [
        "test_data/part3_tags_iod.xml",
        "test_data/part3_tags_mod1.xml",
        "test_data/part3_tags_mod2.xml",
        "test_data/part3_tags_mod3.xml",
        "test_data/part3_tags_mod4.xml",
        "test_data/part3_tags_table.xml",
        "test_data/part4_tags.xml",
        "test_data/part6_tags.xml",
        "test_data/part7_tags.xml",
    ],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec/util/lib:dicom_iod_uid_xml_parser",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec/util/lib:dicom_iod_xml_parser",
        "//pathology/transformation_pipeline/ingestion_lib/dicom_util/spec/util/lib:dicom_tag_xml_parser",
    ],
)
