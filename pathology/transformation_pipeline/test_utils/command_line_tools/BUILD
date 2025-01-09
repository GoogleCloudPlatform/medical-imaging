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

package(
    default_applicable_licenses = ["//pathology:license"],
    default_visibility = ["//visibility:private"],
)

py_binary(
    name = "dicom_to_png",
    srcs = ["dicom_to_png.py"],
    srcs_version = "PY3",
    deps = [
        "//pathology/shared_libs/pydicom_version_util",
        requirement("opencv-python-headless"),
        requirement("numpy"),
        requirement("pydicom"),
    ],
)

py_binary(
    name = "download_dcm",
    srcs = ["download_dcm.py"],
    srcs_version = "PY3",
    deps = [
        requirement("pydicom"),
    ],
)

py_binary(
    name = "print_dcm",
    srcs = ["print_dcm.py"],
    srcs_version = "PY3",
    deps = [
        requirement("pydicom"),
    ],
)

py_binary(
    name = "render_dcm",
    srcs = ["render_dcm.py"],
    srcs_version = "PY3",
    deps = [
        requirement("pydicom"),
    ],
)

py_binary(
    name = "crop_dicom_to_frame_edge",
    srcs = ["crop_dicom_to_frame_edge.py"],
    srcs_version = "PY3",
    deps = [
        requirement("absl-py"),
        "//pathology/shared_libs/pydicom_version_util",
        requirement("pydicom"),
    ],
)

py_binary(
    name = "convert_to_jxl",
    srcs = ["convert_to_jxl.py"],
    srcs_version = "PY3",
    deps = [
        requirement("pillow"),
        requirement("absl-py"),
        requirement("imagecodecs"),
        requirement("numpy"),
        requirement("pydicom"),
    ],
)
