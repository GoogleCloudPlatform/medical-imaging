# *** THIS PACKAGE HAS SPECIAL LICENSING CONDITIONS.  PLEASE
#     CONSULT YOUR go/whichlawyer LEGAL TEAM MEMBER BEFORE
#     DEPENDING ON IT IN YOUR PROJECT. ***

load("@pathology_cloud_deps//:requirements.bzl", "requirement")
load("@rules_license//rules:license.bzl", "license")

package(
    default_applicable_licenses = [":license"],
    default_visibility = ["//visibility:private"],
)

license(
    name = "license",
    package_name = "pathology_cloud_icc_profile",
)

licenses(["by_exception_only"])

exports_files(["LICENSE"])

filegroup(
    name = "icc_profiles",
    srcs = glob(["**/*.icc"]),
    visibility = [
        "//cloud/healthcare/imaging/util:__pkg__",
        "//pathology/dicom_proxy:__subpackages__",
        "//pathology/ez_wsi_dicomweb:__subpackages__",
        "//pathology/transformation_pipeline:__subpackages__",
    ],
)
