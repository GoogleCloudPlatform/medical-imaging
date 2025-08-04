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

# ==================================================
# Files.
filegroup(
    name = "dicom_instance_frame_patch_util_test_images",
    srcs = glob([
        "testdata/dicom_instance_frame_patch_util/*.png",
    ]),
)

filegroup(
    name = "downsample_util_test_images",
    srcs = glob([
        "testdata/downsample_util/*.*",
        "testdata/dicom_instance/*.*",
    ]),
)

filegroup(
    name = "frame_retrieval_util_test_images",
    srcs = glob([
        "testdata/frame_retrieval_util/*.jpeg",
    ]),
)

filegroup(
    name = "image_util_test_images",
    srcs = glob([
        "testdata/*.jpeg",
        "testdata/*.png",
        "testdata/*.webp",
        "testdata/image_util/*.png",
        "testdata/image_util/*.gif",
        "testdata/image_util/*.jpeg",
        "testdata/image_util/*.webp",
    ]),
)

# ==================================================
# Libraries.
py_library(
    name = "annotations_util",
    srcs = ["annotations_util.py"],
    deps = [
        ":dicom_proxy_flags",
        ":dicom_store_util",
        ":dicom_tag_util",
        ":dicom_url_util",
        ":flask_util",
        ":proxy_const",
        ":user_auth_util",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        requirement("Flask"),
        requirement("pydicom"),
        requirement("requests"),
        requirement("requests-toolbelt"),
    ],
)

py_library(
    name = "metadata_augmentation",
    srcs = ["metadata_augmentation.py"],
    deps = [
        ":bulkdata_util",
        ":dicom_proxy_flags",
        ":dicom_store_util",
        ":dicom_url_util",
        ":flask_util",
        ":metadata_util",
        ":proxy_const",
        ":sparse_dicom_util",
        ":user_auth_util",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("Flask"),
    ],
)

py_library(
    name = "base_dicom_request_error",
    srcs = ["base_dicom_request_error.py"],
    deps = [
        requirement("Flask"),
        requirement("requests"),
    ],
)

py_library(
    name = "proxy_const",
    srcs = ["proxy_const.py"],
    deps = [
        ":enum_types",
        "//pathology/shared_libs/iap_auth_lib:auth",
    ],
)

py_library(
    name = "dicom_instance_request",
    srcs = ["dicom_instance_request.py"],
    deps = [
        ":dicom_url_util",
        ":enum_types",
        ":frame_retrieval_util",
        ":icc_color_transform",
        ":metadata_util",
        ":render_frame_params",
        ":user_auth_util",
    ],
)

py_library(
    name = "icc_color_transform",
    srcs = ["icc_color_transform.py"],
    deps = [
        ":enum_types",
        requirement("pillow"),
    ],
)

py_library(
    name = "color_conversion_util",
    srcs = ["color_conversion_util.py"],
    deps = [
        ":cache_enabled_type",
        ":dicom_proxy_flags",
        ":dicom_store_util",
        ":dicom_url_util",
        ":enum_types",
        ":execution_timer",
        ":icc_color_transform",
        ":icc_profile_metadata_cache",
        ":image_util",
        ":metadata_util",
        ":proxy_const",
        ":pydicom_single_instance_read_cache",
        ":redis_cache",
        ":user_auth_util",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("pillow"),
        requirement("cachetools"),
        "//pathology/ez_wsi_dicomweb:dicom_slide",
        requirement("numpy"),
    ],
)

py_library(
    name = "icc_profile_metadata_cache",
    srcs = ["icc_profile_metadata_cache.py"],
    deps = [
        ":dicom_proxy_flags",
        ":dicom_url_util",
        ":redis_cache",
        requirement("cachetools"),
    ],
)

py_library(
    name = "dicom_image_coordinate_util",
    srcs = [
        "dicom_image_coordinate_util.py",
    ],
    deps = [
        ":metadata_util",
    ],
)

py_library(
    name = "dicom_instance_frame_patch_util",
    srcs = ["dicom_instance_frame_patch_util.py"],
    deps = [
        ":dicom_image_coordinate_util",
        ":enum_types",
        ":image_util",
        ":metadata_util",
        requirement("numpy"),
    ],
)

py_library(
    name = "dicom_store_util",
    srcs = ["dicom_store_util.py"],
    deps = [
        ":base_dicom_request_error",
        ":dicom_proxy_flags",
        ":dicom_url_util",
        ":flask_util",
        ":logging_util",
        ":proxy_const",
        ":user_auth_util",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("Flask"),
        requirement("pydicom"),
        requirement("requests"),
    ],
)

py_library(
    name = "dicom_tag_util",
    srcs = [
        "dicom_tag_util.py",
    ],
    deps = [
    ],
)

py_library(
    name = "dicom_url_util",
    srcs = [
        "dicom_url_util.py",
    ],
    deps = [
        ":cache_enabled_type",
        ":dicom_proxy_flags",
        ":enum_types",
        ":render_frame_params",
        ":user_auth_util",
        requirement("dataclasses_json"),
    ],
)

py_library(
    name = "downsample_util",
    srcs = ["downsample_util.py"],
    deps = [
        ":color_conversion_util",
        ":dicom_image_coordinate_util",
        ":dicom_instance_frame_patch_util",
        ":dicom_instance_request",
        ":dicom_proxy_flags",
        ":enum_types",
        ":frame_retrieval_util",
        ":icc_color_transform",
        ":image_util",
        ":metadata_util",
        ":parameters_exceptions_and_return_types",
        ":proxy_const",
        ":render_frame_params",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("numpy"),
    ],
)

py_library(
    name = "enum_types",
    srcs = ["enum_types.py"],
)

py_library(
    name = "execution_timer",
    srcs = ["execution_timer.py"],
    deps = [
        ":dicom_proxy_flags",
        ":flask_util",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
    ],
)

py_library(
    name = "flask_util",
    srcs = ["flask_util.py"],
    deps = [
        ":cache_enabled_type",
        ":enum_types",
        "//pathology/shared_libs/flags:flag_utils",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("Flask"),
        requirement("pydicom"),
    ],
)

py_library(
    name = "frame_caching_util",
    srcs = ["frame_caching_util.py"],
    deps = [
        requirement("absl-py"),
        ":dicom_instance_request",
        ":dicom_proxy_flags",
        ":dicom_url_util",
        ":enum_types",
        ":frame_retrieval_util",
        ":metadata_util",
        ":redis_cache",
        ":render_frame_params",
        ":user_auth_util",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        "//pathology/shared_libs/pydicom_version_util",
        requirement("psutil"),
        requirement("pydicom"),
    ],
)

py_library(
    name = "frame_retrieval_util",
    srcs = ["frame_retrieval_util.py"],
    deps = [
        ":base_dicom_request_error",
        ":dicom_proxy_flags",
        ":dicom_url_util",
        ":enum_types",
        ":metadata_util",
        ":pydicom_single_instance_read_cache",
        ":redis_cache",
        ":render_frame_params",
        ":user_auth_util",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("requests"),
        requirement("requests-toolbelt"),
    ],
)

py_library(
    name = "image_util",
    srcs = ["image_util.py"],
    deps = [
        ":dicom_proxy_flags",
        ":enum_types",
        requirement("pillow"),
        requirement("opencv-python-headless"),
        "//pathology/ez_wsi_dicomweb:dicom_frame_decoder",
        requirement("imagecodecs"),
        requirement("numpy"),
    ],
)

py_library(
    name = "logging_util",
    srcs = [
        "logging_util.py",
    ],
    deps = [
        ":dicom_proxy_flags",
        ":flask_util",
        ":proxy_const",
        ":user_auth_util",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("Flask"),
    ],
)

py_library(
    name = "metadata_util",
    srcs = [
        "metadata_util.py",
    ],
    deps = [
        ":bulkdata_util",
        ":cache_enabled_type",
        ":dicom_proxy_flags",
        ":dicom_store_util",
        ":dicom_url_util",
        ":redis_cache",
        ":user_auth_util",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("cachetools"),
        requirement("dataclasses_json"),
        requirement("numpy"),
        requirement("pydicom"),
    ],
)

py_library(
    name = "parameters_exceptions_and_return_types",
    srcs = ["parameters_exceptions_and_return_types.py"],
    deps = [
        ":cache_enabled_type",
        ":color_conversion_util",
        ":dicom_instance_request",
        ":dicom_store_util",
        ":dicom_url_util",
        ":enum_types",
        ":frame_retrieval_util",
        ":icc_color_transform",
        ":metadata_util",
        ":pydicom_single_instance_read_cache",
        ":render_frame_params",
        ":user_auth_util",
    ],
)

py_library(
    name = "pydicom_single_instance_read_cache",
    srcs = [
        "pydicom_single_instance_read_cache.py",
    ],
    deps = [
        ":metadata_util",
        requirement("pydicom"),
    ],
)

py_library(
    name = "redis_cache",
    srcs = ["redis_cache.py"],
    deps = [
        ":cache_enabled_type",
        ":dicom_proxy_flags",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("google-cloud-storage"),
        requirement("psutil"),
        requirement("redis"),
    ],
)

py_library(
    name = "render_frame_params",
    srcs = ["render_frame_params.py"],
    deps = [
        ":cache_enabled_type",
        ":enum_types",
        ":proxy_const",
    ],
)

py_library(
    name = "server_lib",
    srcs = ["server.py"],
    deps = [
        requirement("absl-py"),
        ":dicom_proxy_blueprint",
        ":dicom_proxy_flags",
        ":logging_util",
        ":redis_cache",
        "//pathology/shared_libs/build_version",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("Flask"),
        requirement("Flask-Cors"),
        requirement("gunicorn"),
        requirement("werkzeug"),
    ],
)

py_library(
    name = "shared_test_util",
    srcs = ["shared_test_util.py"],
    data = [
        "testdata/mock_annotation_dicom.json",
        "testdata/mock_jpeg_dicom.json",
        "testdata/multi_frame_jpeg2000_camelyon_challenge_image.dcm",
        "testdata/multi_frame_jpeg_camelyon_challenge_image.dcm",
        "testdata/wsi_annotation.dcm",
    ],
    deps = [
        ":flask_util",
        ":metadata_util",
        ":parameters_exceptions_and_return_types",
        ":pydicom_single_instance_read_cache",
        ":render_frame_params",
        requirement("pillow"),
        requirement("numpy"),
        requirement("pydicom"),
    ],
)

py_library(
    name = "dicom_proxy_blueprint",
    srcs = ["dicom_proxy_blueprint.py"],
    deps = [
        ":annotations_util",
        ":base_dicom_request_error",
        ":bulkdata_util",
        ":dicom_instance_request",
        ":dicom_proxy_flags",
        ":dicom_store_util",
        ":dicom_url_util",
        ":downsample_util",
        ":enum_types",
        ":execution_timer",
        ":flask_util",
        ":frame_caching_util",
        ":logging_util",
        ":metadata_augmentation",
        ":metadata_util",
        ":parameters_exceptions_and_return_types",
        ":proxy_const",
        ":render_frame_params",
        ":sparse_dicom_util",
        ":user_auth_util",
        "//pathology/shared_libs/build_version",
        "//pathology/shared_libs/flags:flag_utils",
        "//pathology/shared_libs/iap_auth_lib:auth",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("Flask"),
        requirement("flask_compress"),
        requirement("requests-toolbelt"),
    ],
)

py_library(
    name = "dicom_proxy_flags",
    srcs = [
        "dicom_proxy_flags.py",
    ],
    deps = [
        requirement("absl-py"),
        ":proxy_const",
        "//pathology/shared_libs/flags:secret_flag_utils",
        "//pathology/shared_libs/iap_auth_lib:auth",
    ],
)

py_library(
    name = "user_auth_util",
    srcs = ["user_auth_util.py"],
    deps = [
        ":dicom_proxy_flags",
        ":flask_util",
        ":proxy_const",
        ":redis_cache",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("cachetools"),
        requirement("google-auth"),
        requirement("requests"),
    ],
)

py_library(
    name = "bulkdata_util",
    srcs = ["bulkdata_util.py"],
    deps = [
        ":dicom_proxy_flags",
        ":dicom_url_util",
        ":flask_util",
        requirement("Flask"),
    ],
)

py_library(
    name = "sparse_dicom_util",
    srcs = ["sparse_dicom_util.py"],
    deps = [
        ":cache_enabled_type",
        ":dicom_proxy_flags",
        ":dicom_store_util",
        ":dicom_url_util",
        ":flask_util",
        ":metadata_util",
        ":redis_cache",
        ":user_auth_util",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
    ],
)

# ==================================================
# Binaries.
py_binary(
    name = "server",
    srcs = ["server.py"],
    deps = [
        requirement("absl-py"),
        ":dicom_proxy_blueprint",
        ":dicom_proxy_flags",
        ":logging_util",
        ":redis_cache",
        "//pathology/shared_libs/build_version",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("Flask"),
        requirement("Flask-Cors"),
        requirement("gunicorn"),
        requirement("werkzeug"),
    ],
)

# ==================================================
# Tests.
py_test(
    name = "annotations_util_test",
    srcs = ["annotations_util_test.py"],
    deps = [
        requirement("absl-py"),
        ":annotations_util",
        ":dicom_store_util",
        ":dicom_url_util",
        ":proxy_const",
        ":shared_test_util",
        ":user_auth_util",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        "//pathology/shared_libs/test_utils/dicom_store_mock:dicom_store_mock_types",
        requirement("Flask"),
        requirement("google-auth"),
        requirement("pydicom"),
        requirement("requests"),
        requirement("requests_mock"),
    ],
)

py_test(
    name = "base_dicom_request_error_test",
    srcs = ["base_dicom_request_error_test.py"],
    deps = [
        requirement("absl-py"),
        ":base_dicom_request_error",
        requirement("requests"),
        requirement("requests_mock"),
    ],
)

py_test(
    name = "color_conversion_util_test",
    srcs = ["color_conversion_util_test.py"],
    deps = [
        ":cache_enabled_type",
        ":color_conversion_util",
        ":dicom_url_util",
        ":enum_types",
        ":icc_profile_metadata_cache",
        ":image_util",
        ":proxy_const",
        ":redis_cache",
        ":shared_test_util",
        ":user_auth_util",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        requirement("pillow"),
        requirement("absl-py"),
        requirement("cachetools"),
        requirement("numpy"),
        requirement("pydicom"),
        requirement("redis"),
    ],
)

py_test(
    name = "dicom_image_coordinate_util_test",
    srcs = ["dicom_image_coordinate_util_test.py"],
    deps = [
        requirement("absl-py"),
        ":dicom_image_coordinate_util",
        ":shared_test_util",
    ],
)

py_test(
    name = "dicom_instance_frame_patch_util_test",
    srcs = ["dicom_instance_frame_patch_util_test.py"],
    data = [
        ":dicom_instance_frame_patch_util_test_images",
    ],
    deps = [
        requirement("absl-py"),
        ":dicom_image_coordinate_util",
        ":dicom_instance_frame_patch_util",
        ":enum_types",
        ":image_util",
        ":shared_test_util",
    ],
)

py_test(
    name = "dicom_url_util_test",
    srcs = [
        "dicom_url_util_test.py",
    ],
    deps = [
        requirement("absl-py"),
        ":cache_enabled_type",
        ":dicom_proxy_flags",
        ":dicom_url_util",
        ":enum_types",
        ":render_frame_params",
        ":user_auth_util",
    ],
)

py_test(
    name = "downsample_util_test",
    srcs = ["downsample_util_test.py"],
    data = [":downsample_util_test_images"],
    deps = [
        ":color_conversion_util",
        ":downsample_util",
        ":enum_types",
        ":icc_color_transform",
        ":parameters_exceptions_and_return_types",
        ":proxy_const",
        ":pydicom_single_instance_read_cache",
        ":render_frame_params",
        ":shared_test_util",
        requirement("pillow"),
        requirement("absl-py"),
        requirement("pydicom"),
    ],
)

py_test(
    name = "execution_timer_test",
    srcs = ["execution_timer_test.py"],
    data = [":downsample_util_test_images"],
    deps = [
        requirement("absl-py"),
        ":execution_timer",
        ":flask_util",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
    ],
)

py_test(
    name = "flask_util_test",
    srcs = ["flask_util_test.py"],
    deps = [
        requirement("absl-py"),
        ":flask_util",
    ],
)

py_test(
    name = "frame_caching_util_test",
    srcs = ["frame_caching_util_test.py"],
    data = [":downsample_util_test_images"],
    deps = [
        requirement("absl-py"),
        ":cache_enabled_type",
        ":dicom_instance_request",
        ":dicom_url_util",
        ":enum_types",
        ":frame_caching_util",
        ":frame_retrieval_util",
        ":metadata_util",
        ":parameters_exceptions_and_return_types",
        ":redis_cache",
        ":render_frame_params",
        ":shared_test_util",
        ":user_auth_util",
        "//pathology/shared_libs/logging_lib:cloud_logging_client",
        requirement("redis"),
    ],
)

py_test(
    name = "frame_retrieval_util_test",
    srcs = ["frame_retrieval_util_test.py"],
    data = [
        ":frame_retrieval_util_test_images",
    ],
    deps = [
        ":cache_enabled_type",
        ":dicom_url_util",
        ":enum_types",
        ":frame_retrieval_util",
        ":image_util",
        ":metadata_util",
        ":pydicom_single_instance_read_cache",
        ":render_frame_params",
        ":shared_test_util",
        ":user_auth_util",
        requirement("pillow"),
        requirement("absl-py"),
        requirement("opencv-python-headless"),
        requirement("numpy"),
        requirement("redis"),
        requirement("requests_mock"),
        requirement("requests-toolbelt"),
    ],
)

py_test(
    name = "image_util_test",
    srcs = ["image_util_test.py"],
    data = [
        ":image_util_test_images",
    ],
    deps = [
        ":color_conversion_util",
        ":enum_types",
        ":image_util",
        ":shared_test_util",
        requirement("pillow"),
        requirement("absl-py"),
        requirement("opencv-python-headless"),
        requirement("numpy"),
    ],
)

py_test(
    name = "metadata_util_test",
    srcs = [
        "metadata_util_test.py",
    ],
    deps = [
        requirement("absl-py"),
        ":cache_enabled_type",
        ":dicom_proxy_flags",
        ":dicom_store_util",
        ":dicom_url_util",
        ":metadata_util",
        ":proxy_const",
        ":shared_test_util",
        ":user_auth_util",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        requirement("cachetools"),
        requirement("numpy"),
        requirement("redis"),
    ],
)

py_test(
    name = "parameters_exceptions_and_return_types_test",
    srcs = ["parameters_exceptions_and_return_types_test.py"],
    data = [
        "testdata/parameters_exceptions_and_return_types_test/frame.jpeg",
    ],
    deps = [
        requirement("absl-py"),
        ":cache_enabled_type",
        ":dicom_instance_request",
        ":dicom_url_util",
        ":enum_types",
        ":frame_retrieval_util",
        ":parameters_exceptions_and_return_types",
        ":proxy_const",
        ":pydicom_single_instance_read_cache",
        ":render_frame_params",
        ":shared_test_util",
        ":user_auth_util",
    ],
)

py_test(
    name = "pydicom_single_instance_read_cache_test",
    srcs = ["pydicom_single_instance_read_cache_test.py"],
    deps = [
        requirement("absl-py"),
        ":pydicom_single_instance_read_cache",
        ":shared_test_util",
        requirement("pydicom"),
    ],
)

py_test(
    name = "redis_cache_test",
    srcs = ["redis_cache_test.py"],
    deps = [
        requirement("absl-py"),
        ":cache_enabled_type",
        ":redis_cache",
        ":shared_test_util",
        "//pathology/shared_libs/logging_lib:cloud_logging_client_instance",
        "//pathology/shared_libs/test_utils/gcs_mock",
        requirement("psutil"),
        requirement("redis"),
    ],
)

py_test(
    name = "render_frame_params_test",
    srcs = ["render_frame_params_test.py"],
    deps = [
        requirement("absl-py"),
        ":cache_enabled_type",
        ":enum_types",
        ":proxy_const",
        ":render_frame_params",
    ],
)

py_test(
    name = "dicom_proxy_blueprint_test",
    srcs = ["dicom_proxy_blueprint_test.py"],
    deps = [
        requirement("absl-py"),
        ":bulkdata_util",
        ":cache_enabled_type",
        ":dicom_proxy_blueprint",
        ":dicom_store_util",
        ":dicom_url_util",
        ":enum_types",
        ":flask_util",
        ":metadata_util",
        ":parameters_exceptions_and_return_types",
        ":proxy_const",
        ":redis_cache",
        ":render_frame_params",
        ":server_lib",
        ":shared_test_util",
        ":user_auth_util",
        "//pathology/shared_libs/iap_auth_lib:auth",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        requirement("Flask"),
        requirement("pydicom"),
        requirement("redis"),
        requirement("requests_mock"),
        requirement("requests-toolbelt"),
        requirement("werkzeug"),
    ],
)

py_test(
    name = "dicom_store_util_test",
    srcs = ["dicom_store_util_test.py"],
    deps = [
        requirement("absl-py"),
        ":dicom_proxy_flags",
        ":dicom_store_util",
        ":dicom_url_util",
        ":flask_util",
        ":proxy_const",
        ":shared_test_util",
        ":user_auth_util",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        requirement("Flask"),
        requirement("requests"),
    ],
)

py_test(
    name = "user_auth_util_test",
    srcs = ["user_auth_util_test.py"],
    deps = [
        requirement("absl-py"),
        ":proxy_const",
        ":user_auth_util",
        requirement("cachetools"),
        requirement("google-auth"),
        requirement("requests_mock"),
    ],
)

py_test(
    name = "logging_util_test",
    srcs = ["logging_util_test.py"],
    deps = [
        requirement("absl-py"),
        ":logging_util",
        ":proxy_const",
    ],
)

py_test(
    name = "bulkdata_util_test",
    srcs = ["bulkdata_util_test.py"],
    deps = [
        requirement("absl-py"),
        ":bulkdata_util",
        ":dicom_url_util",
        ":flask_util",
        requirement("Flask"),
    ],
)

py_test(
    name = "sparse_dicom_util_test",
    srcs = ["sparse_dicom_util_test.py"],
    deps = [
        requirement("absl-py"),
        ":cache_enabled_type",
        ":dicom_proxy_flags",
        ":dicom_store_util",
        ":dicom_url_util",
        ":flask_util",
        ":proxy_const",
        ":shared_test_util",
        ":sparse_dicom_util",
        ":user_auth_util",
        "//pathology/shared_libs/test_utils/dicom_store_mock",
        requirement("pydicom"),
    ],
)

py_library(
    name = "cache_enabled_type",
    srcs = ["cache_enabled_type.py"],
)

py_test(
    name = "icc_profile_metadata_cache_test",
    srcs = ["icc_profile_metadata_cache_test.py"],
    deps = [
        requirement("absl-py"),
        ":dicom_url_util",
        ":icc_profile_metadata_cache",
        ":user_auth_util",
        requirement("cachetools"),
    ],
)

py_test(
    name = "metadata_augmentation_test",
    srcs = ["metadata_augmentation_test.py"],
    deps = [
        requirement("absl-py"),
        ":dicom_url_util",
        ":flask_util",
        ":metadata_augmentation",
        ":metadata_util",
        ":shared_test_util",
        "//pathology/shared_libs/iap_auth_lib:auth",
        requirement("Flask"),
    ],
)
